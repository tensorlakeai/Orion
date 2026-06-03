use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::StreamExt;
use openraft::errors::{
    ClientWriteError, LinearizableReadError, NetworkError, RPCError, RaftError, StreamingError,
};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    TransferLeaderRequest, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{Raft, ReadPolicy, errors::ReplicationClosed};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, WriteMultipart};

use crate::HybridClock;
use crate::checkpoint_artifact::{
    ensure_checkpoint_object_path_allowed, list_slate_db_checkpoint_objects,
};
use crate::openraft_store::{
    OrionEntry, OrionNode, OrionNodeId, OrionRaftRequest, OrionRaftStateMachine, OrionSnapshot,
    OrionTypeConfig, OrionVote, snapshot_checkpoint_artifacts,
};
use crate::raft_metrics::{ClusterRaftMetricsRegistry, RaftMetricsSnapshot};
use crate::state::SlateDbStateStore;

pub mod proto {
    tonic::include_proto!("orion.raft.v1");
}

use proto::RaftMessage;
use proto::raft_transport_client::RaftTransportClient;
use proto::raft_transport_server::{RaftTransport, RaftTransportServer};

pub type OrionRaft = Raft<OrionTypeConfig, OrionRaftStateMachine>;
pub type OrionClientWriteResponse = ClientWriteResponse<OrionTypeConfig>;
pub type OrionClientWriteResult =
    Result<OrionClientWriteResponse, RaftError<OrionTypeConfig, ClientWriteError<OrionTypeConfig>>>;
pub type OrionClientBarrierResult =
    Result<(), RaftError<OrionTypeConfig, LinearizableReadError<OrionTypeConfig>>>;

const WIRE_VERSION: u16 = 1;
pub const DEFAULT_RAFT_GROUP_ID: &str = "rg_default";
const DEFAULT_SNAPSHOT_STREAM_CHUNK_SIZE: usize = 1024 * 1024;
const SNAPSHOT_RECEIVE_MULTIPART_CHUNK_SIZE: usize = 5 * 1024 * 1024;
const SNAPSHOT_RECEIVE_MULTIPART_CONCURRENCY: usize = 2;

#[derive(Debug, Clone)]
pub struct TonicRaftTransportConfig {
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub max_message_size: usize,
}

impl Default for TonicRaftTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            rpc_timeout: Duration::from_secs(5),
            max_message_size: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct TonicRaftNetwork {
    endpoints: Arc<RwLock<BTreeMap<OrionNodeId, String>>>,
    config: TonicRaftTransportConfig,
    local_node_id: Option<OrionNodeId>,
    metrics_registry: ClusterRaftMetricsRegistry,
    group_id: String,
    local_state_store: Option<SlateDbStateStore>,
}

impl Default for TonicRaftNetwork {
    fn default() -> Self {
        Self {
            endpoints: Arc::default(),
            config: TonicRaftTransportConfig::default(),
            local_node_id: None,
            metrics_registry: ClusterRaftMetricsRegistry::default(),
            group_id: DEFAULT_RAFT_GROUP_ID.to_string(),
            local_state_store: None,
        }
    }
}

impl TonicRaftNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: TonicRaftTransportConfig) -> Self {
        Self {
            endpoints: Arc::default(),
            config,
            local_node_id: None,
            metrics_registry: ClusterRaftMetricsRegistry::default(),
            group_id: DEFAULT_RAFT_GROUP_ID.to_string(),
            local_state_store: None,
        }
    }

    pub fn with_config_and_metrics(
        local_node_id: OrionNodeId,
        config: TonicRaftTransportConfig,
        metrics_registry: ClusterRaftMetricsRegistry,
    ) -> Self {
        Self {
            endpoints: Arc::default(),
            config,
            local_node_id: Some(local_node_id),
            metrics_registry,
            group_id: DEFAULT_RAFT_GROUP_ID.to_string(),
            local_state_store: None,
        }
    }

    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = group_id.into();
        self
    }

    pub fn with_local_state_store(mut self, state: SlateDbStateStore) -> Self {
        self.local_state_store = Some(state);
        self
    }

    pub fn metrics_registry(&self) -> ClusterRaftMetricsRegistry {
        self.metrics_registry.clone()
    }

    pub fn register_endpoint(
        &self,
        node_id: OrionNodeId,
        endpoint: impl Into<String>,
    ) -> Result<(), NetworkError<OrionTypeConfig>> {
        self.endpoints
            .write()
            .map_err(|_| NetworkError::from_string("tonic raft endpoint registry poisoned"))?
            .insert(node_id, normalize_endpoint(endpoint.into()));
        Ok(())
    }

    fn endpoint_for(
        &self,
        target: OrionNodeId,
        node: &OrionNode,
    ) -> Result<String, NetworkError<OrionTypeConfig>> {
        if !node.addr.is_empty() {
            return Ok(normalize_endpoint(node.addr.clone()));
        }
        self.endpoints
            .read()
            .map_err(|_| NetworkError::from_string("tonic raft endpoint registry poisoned"))?
            .get(&target)
            .cloned()
            .ok_or_else(|| NetworkError::from_string(format!("missing endpoint for node {target}")))
    }
}

impl RaftNetworkFactory<OrionTypeConfig> for TonicRaftNetwork {
    type Network = TonicRaftClient;

    async fn new_client(&mut self, target: OrionNodeId, node: &OrionNode) -> Self::Network {
        TonicRaftClient {
            target,
            endpoint: self.endpoint_for(target, node).map_err(RPCError::Network),
            config: self.config.clone(),
            client: None,
            local_node_id: self.local_node_id,
            metrics_registry: self.metrics_registry.clone(),
            group_id: self.group_id.clone(),
            local_state_store: self.local_state_store.clone(),
        }
    }
}

pub struct TonicRaftClient {
    target: OrionNodeId,
    endpoint: Result<String, RPCError<OrionTypeConfig>>,
    config: TonicRaftTransportConfig,
    client: Option<RaftTransportClient<Channel>>,
    local_node_id: Option<OrionNodeId>,
    metrics_registry: ClusterRaftMetricsRegistry,
    group_id: String,
    local_state_store: Option<SlateDbStateStore>,
}

impl TonicRaftClient {
    async fn client(
        &mut self,
    ) -> Result<&mut RaftTransportClient<Channel>, RPCError<OrionTypeConfig>> {
        if self.client.is_none() {
            let endpoint = self.endpoint.clone()?;
            let channel = Endpoint::from_shared(endpoint)
                .map_err(|err| network_error(self.target, err))?
                .connect_timeout(self.config.connect_timeout)
                .timeout(self.config.rpc_timeout)
                .connect()
                .await
                .map_err(|err| network_error(self.target, err))?;
            let client = RaftTransportClient::new(channel)
                .max_decoding_message_size(self.config.max_message_size)
                .max_encoding_message_size(self.config.max_message_size);
            self.client = Some(client);
        }
        self.client
            .as_mut()
            .ok_or_else(|| network_error(self.target, "tonic client was not initialized"))
    }
}

impl RaftNetworkV2<OrionTypeConfig> for TonicRaftClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<OrionTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<OrionTypeConfig>, RPCError<OrionTypeConfig>> {
        let message = self.message(encode(&rpc)?);
        let response = self
            .client()
            .await?
            .append_entries(message)
            .await
            .map_err(|err| network_error(self.target, err))?;
        self.record_response_metrics(response.get_ref());
        decode(&response.into_inner().payload)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<OrionTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<OrionTypeConfig>, RPCError<OrionTypeConfig>> {
        let message = self.message(encode(&rpc)?);
        let response = self
            .client()
            .await?
            .vote(message)
            .await
            .map_err(|err| network_error(self.target, err))?;
        self.record_response_metrics(response.get_ref());
        decode(&response.into_inner().payload)
    }

    async fn full_snapshot(
        &mut self,
        vote: OrionVote,
        snapshot: OrionSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<OrionTypeConfig>, StreamingError<OrionTypeConfig>> {
        let target = self.target;
        let header = WireSnapshotHeader {
            vote,
            meta: snapshot.meta,
        };
        let snapshot_bytes = snapshot.snapshot.into_inner();
        let chunk_size = snapshot_stream_chunk_size(self.config.max_message_size);
        let group_id = self.group_id.clone();
        let local_state_store = self.local_state_store.clone();
        let metrics_payload = self.local_metrics_payload();
        let metrics_registry = self.metrics_registry.clone();
        let local_node_id = self.local_node_id;
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let result = send_snapshot_stream_messages(
                tx,
                metrics_payload,
                header,
                snapshot_bytes,
                chunk_size,
                group_id,
                local_state_store,
            )
            .await;
            if let Some(node_id) = local_node_id {
                metrics_registry.record_snapshot_transfer(node_id, |metrics| match result {
                    Ok(stats) => {
                        metrics.manifest_bytes_sent += stats.manifest_bytes;
                        metrics.checkpoint_objects_sent += stats.checkpoint_objects;
                        metrics.checkpoint_object_bytes_sent += stats.checkpoint_object_bytes;
                    }
                    Err(_) => {
                        metrics.snapshot_send_failures += 1;
                    }
                });
            }
        });
        let stream = ReceiverStream::new(rx);
        let response = self
            .client()
            .await
            .map_err(StreamingError::from)?
            .full_snapshot(stream)
            .await
            .map_err(|err| {
                StreamingError::Network(NetworkError::from_string(format!("node {target}: {err}")))
            })?;
        self.record_response_metrics(response.get_ref());
        if let Some(node_id) = self.local_node_id {
            self.metrics_registry
                .record_snapshot_transfer(node_id, |metrics| {
                    metrics.snapshots_sent += 1;
                });
        }
        decode_streaming(&response.into_inner().payload)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<OrionTypeConfig>,
        _option: RPCOption,
    ) -> Result<(), RPCError<OrionTypeConfig>> {
        let message = self.message(encode(&req)?);
        self.client()
            .await?
            .transfer_leader(message)
            .await
            .map_err(|err| network_error(self.target, err))?;
        Ok(())
    }
}

impl TonicRaftClient {
    fn message(&self, payload: Vec<u8>) -> RaftMessage {
        RaftMessage {
            payload,
            metrics_payload: self.local_metrics_payload(),
            group_id: self.group_id.clone(),
        }
    }

    fn local_metrics_payload(&self) -> Vec<u8> {
        self.local_node_id
            .and_then(|node_id| self.metrics_registry.get_metrics(node_id))
            .and_then(|metrics| encode(&metrics).ok())
            .unwrap_or_default()
    }

    fn record_response_metrics(&self, message: &RaftMessage) {
        record_wire_metrics(&self.metrics_registry, message);
    }
}

#[derive(Debug)]
pub enum ClientWriteRpcError {
    Transport(String),
    Raft(RaftError<OrionTypeConfig, ClientWriteError<OrionTypeConfig>>),
}

impl std::fmt::Display for ClientWriteRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientWriteRpcError::Transport(error) => {
                write!(f, "client write transport error: {error}")
            }
            ClientWriteRpcError::Raft(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClientWriteRpcError {}

#[derive(Debug)]
pub enum ClientBarrierRpcError {
    Transport(String),
    Raft(RaftError<OrionTypeConfig, LinearizableReadError<OrionTypeConfig>>),
}

impl std::fmt::Display for ClientBarrierRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientBarrierRpcError::Transport(error) => {
                write!(f, "client barrier transport error: {error}")
            }
            ClientBarrierRpcError::Raft(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClientBarrierRpcError {}

pub async fn client_write_to_raft_endpoint(
    endpoint: impl Into<String>,
    request: OrionRaftRequest,
    config: TonicRaftTransportConfig,
) -> Result<OrionClientWriteResponse, ClientWriteRpcError> {
    client_write_to_raft_endpoint_for_group(endpoint, DEFAULT_RAFT_GROUP_ID, request, config).await
}

pub async fn client_write_to_raft_endpoint_for_group(
    endpoint: impl Into<String>,
    group_id: impl Into<String>,
    request: OrionRaftRequest,
    config: TonicRaftTransportConfig,
) -> Result<OrionClientWriteResponse, ClientWriteRpcError> {
    let endpoint = normalize_endpoint(endpoint.into());
    let group_id = group_id.into();
    if group_id.is_empty() {
        return Err(ClientWriteRpcError::Transport(
            "raft group_id must not be empty".to_string(),
        ));
    }
    let channel = Endpoint::from_shared(endpoint)
        .map_err(|err| ClientWriteRpcError::Transport(err.to_string()))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.rpc_timeout)
        .connect()
        .await
        .map_err(|err| ClientWriteRpcError::Transport(err.to_string()))?;
    let mut client = RaftTransportClient::new(channel)
        .max_decoding_message_size(config.max_message_size)
        .max_encoding_message_size(config.max_message_size);
    let response = client
        .client_write(RaftMessage {
            payload: encode(&request)
                .map_err(|err| ClientWriteRpcError::Transport(err.to_string()))?,
            metrics_payload: Vec::new(),
            group_id,
        })
        .await
        .map_err(|err| ClientWriteRpcError::Transport(err.to_string()))?;
    let result: OrionClientWriteResult = decode(&response.into_inner().payload)
        .map_err(|err| ClientWriteRpcError::Transport(err.to_string()))?;
    result.map_err(ClientWriteRpcError::Raft)
}

pub async fn client_barrier_to_raft_endpoint(
    endpoint: impl Into<String>,
    config: TonicRaftTransportConfig,
) -> Result<(), ClientBarrierRpcError> {
    client_barrier_to_raft_endpoint_for_group(endpoint, DEFAULT_RAFT_GROUP_ID, config).await
}

pub async fn client_barrier_to_raft_endpoint_for_group(
    endpoint: impl Into<String>,
    group_id: impl Into<String>,
    config: TonicRaftTransportConfig,
) -> Result<(), ClientBarrierRpcError> {
    let endpoint = normalize_endpoint(endpoint.into());
    let group_id = group_id.into();
    if group_id.is_empty() {
        return Err(ClientBarrierRpcError::Transport(
            "raft group_id must not be empty".to_string(),
        ));
    }
    let channel = Endpoint::from_shared(endpoint)
        .map_err(|err| ClientBarrierRpcError::Transport(err.to_string()))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.rpc_timeout)
        .connect()
        .await
        .map_err(|err| ClientBarrierRpcError::Transport(err.to_string()))?;
    let mut client = RaftTransportClient::new(channel)
        .max_decoding_message_size(config.max_message_size)
        .max_encoding_message_size(config.max_message_size);
    let response = client
        .client_barrier(RaftMessage {
            payload: Vec::new(),
            metrics_payload: Vec::new(),
            group_id,
        })
        .await
        .map_err(|err| ClientBarrierRpcError::Transport(err.to_string()))?;
    let result: OrionClientBarrierResult = decode(&response.into_inner().payload)
        .map_err(|err| ClientBarrierRpcError::Transport(err.to_string()))?;
    result.map_err(ClientBarrierRpcError::Raft)
}

#[derive(Clone, Default)]
pub struct TonicRaftGroupRegistry {
    groups: Arc<RwLock<BTreeMap<String, OrionRaft>>>,
}

impl TonicRaftGroupRegistry {
    pub fn single_default(raft: OrionRaft) -> Self {
        let registry = Self::default();
        registry
            .register(DEFAULT_RAFT_GROUP_ID, raft)
            .expect("new raft group registry should not be poisoned");
        registry
    }

    pub fn register(
        &self,
        group_id: impl Into<String>,
        raft: OrionRaft,
    ) -> Result<(), NetworkError<OrionTypeConfig>> {
        let group_id = group_id.into();
        if group_id.trim().is_empty() || group_id != group_id.trim() {
            return Err(NetworkError::from_string("raft group_id must not be empty"));
        }
        self.groups
            .write()
            .map_err(|_| NetworkError::from_string("tonic raft group registry poisoned"))?
            .insert(group_id, raft);
        Ok(())
    }

    pub fn get(&self, group_id: &str) -> Result<OrionRaft, Status> {
        if group_id.trim().is_empty() || group_id != group_id.trim() {
            return Err(Status::invalid_argument("raft group_id must not be empty"));
        }
        self.groups
            .read()
            .map_err(|_| Status::internal("tonic raft group registry poisoned"))?
            .get(group_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("raft group {group_id} is not loaded")))
    }
}

#[derive(Clone)]
pub struct TonicRaftService {
    groups: TonicRaftGroupRegistry,
    metrics_registry: ClusterRaftMetricsRegistry,
}

impl TonicRaftService {
    pub fn new(raft: OrionRaft) -> Self {
        Self::with_metrics_registry(raft, ClusterRaftMetricsRegistry::default())
    }

    pub fn with_metrics_registry(
        raft: OrionRaft,
        metrics_registry: ClusterRaftMetricsRegistry,
    ) -> Self {
        Self {
            groups: TonicRaftGroupRegistry::single_default(raft),
            metrics_registry,
        }
    }

    pub fn with_group_registry(
        groups: TonicRaftGroupRegistry,
        metrics_registry: ClusterRaftMetricsRegistry,
    ) -> Self {
        Self {
            groups,
            metrics_registry,
        }
    }

    fn message(&self, group_id: &str, raft: &OrionRaft, payload: Vec<u8>) -> RaftMessage {
        let metrics = self.metrics_registry.record_observed(raft);
        RaftMessage {
            payload,
            metrics_payload: encode_status(&metrics).unwrap_or_default(),
            group_id: group_id.to_string(),
        }
    }

    fn record_request_metrics(&self, request: &RaftMessage) {
        record_wire_metrics(&self.metrics_registry, request);
    }

    fn raft_for_message(&self, request: &RaftMessage) -> Result<(String, OrionRaft), Status> {
        let group_id = request.group_id.as_str();
        if group_id.trim().is_empty() || group_id != group_id.trim() {
            return Err(Status::invalid_argument("raft group_id must not be empty"));
        }
        Ok((group_id.to_string(), self.groups.get(group_id)?))
    }
}

#[tonic::async_trait]
impl RaftTransport for TonicRaftService {
    async fn append_entries(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let request = request.into_inner();
        self.record_request_metrics(&request);
        let (group_id, raft) = self.raft_for_message(&request)?;
        let rpc: AppendEntriesRequest<OrionTypeConfig> = decode_status(&request.payload)?;
        let response = raft
            .append_entries(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(self.message(
            &group_id,
            &raft,
            encode_status(&response)?,
        )))
    }

    async fn vote(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let request = request.into_inner();
        self.record_request_metrics(&request);
        let (group_id, raft) = self.raft_for_message(&request)?;
        let rpc: VoteRequest<OrionTypeConfig> = decode_status(&request.payload)?;
        let response = raft
            .vote(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(self.message(
            &group_id,
            &raft,
            encode_status(&response)?,
        )))
    }

    async fn full_snapshot(
        &self,
        request: Request<tonic::Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        let mut stream = request.into_inner();
        let header = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("missing snapshot stream header"))??;
        self.record_request_metrics(&header);
        let (group_id, raft) = self.raft_for_message(&header)?;
        let node_id = RaftMetricsSnapshot::observe(&raft).node_id;
        let (header, payload) = receive_snapshot_stream(
            &raft,
            &group_id,
            header.payload,
            &mut stream,
            &self.metrics_registry,
            node_id,
        )
        .await?;
        let wire_snapshot = WireSnapshot {
            meta: header.meta,
            payload,
        };
        let response = raft
            .install_full_snapshot(header.vote, wire_snapshot.into_snapshot())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(self.message(
            &group_id,
            &raft,
            encode_status(&response)?,
        )))
    }

    async fn transfer_leader(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let request = request.into_inner();
        self.record_request_metrics(&request);
        let (group_id, raft) = self.raft_for_message(&request)?;
        let rpc: TransferLeaderRequest<OrionTypeConfig> = decode_status(&request.payload)?;
        raft.handle_transfer_leader(rpc)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(self.message(&group_id, &raft, Vec::new())))
    }

    async fn client_write(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let request = request.into_inner();
        self.record_request_metrics(&request);
        let (group_id, raft) = self.raft_for_message(&request)?;
        let request = decode_status::<OrionRaftRequest>(&request.payload)?
            .assign_commit_timestamp(HybridClock::global());
        let response: OrionClientWriteResult = raft.client_write(request).await;
        Ok(Response::new(self.message(
            &group_id,
            &raft,
            encode_status(&response)?,
        )))
    }

    async fn client_barrier(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let request = request.into_inner();
        self.record_request_metrics(&request);
        let (group_id, raft) = self.raft_for_message(&request)?;
        let result: OrionClientBarrierResult = raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|_| ());
        Ok(Response::new(self.message(
            &group_id,
            &raft,
            encode_status(&result)?,
        )))
    }
}

fn record_wire_metrics(registry: &ClusterRaftMetricsRegistry, message: &RaftMessage) {
    if message.metrics_payload.is_empty() {
        return;
    }
    if let Ok(metrics) = decode::<RaftMetricsSnapshot>(&message.metrics_payload) {
        registry.record(metrics);
    }
}

async fn send_snapshot_stream_messages(
    tx: mpsc::Sender<RaftMessage>,
    metrics_payload: Vec<u8>,
    header: WireSnapshotHeader,
    snapshot_bytes: Vec<u8>,
    chunk_size: usize,
    group_id: String,
    local_state_store: Option<SlateDbStateStore>,
) -> anyhow::Result<SnapshotSendStats> {
    let mut stats = SnapshotSendStats::default();
    send_snapshot_frame(
        &tx,
        &group_id,
        metrics_payload,
        WireSnapshotFrame::Header(header),
    )
    .await?;
    for chunk in snapshot_bytes.chunks(chunk_size) {
        stats.manifest_bytes += chunk.len() as u64;
        send_snapshot_frame(
            &tx,
            &group_id,
            Vec::new(),
            WireSnapshotFrame::ManifestChunk(chunk.to_vec()),
        )
        .await?;
    }

    if let Some(state) = local_state_store {
        let artifacts = snapshot_checkpoint_artifacts(&snapshot_bytes)?;
        let object_store = state.object_store();
        for artifact in artifacts {
            let objects = list_slate_db_checkpoint_objects(&object_store, &artifact).await?;
            for object in objects {
                ensure_checkpoint_object_path_allowed(&artifact, &object.path)?;
                stats.checkpoint_objects += 1;
                send_snapshot_frame(
                    &tx,
                    &group_id,
                    Vec::new(),
                    WireSnapshotFrame::CheckpointObjectHeader {
                        path: object.path.clone(),
                        size: object.size,
                    },
                )
                .await?;

                let location = ObjectPath::parse(&object.path)?;
                let mut stream = object_store.get(&location).await?.into_stream();
                let mut streamed_size = 0_u64;
                while let Some(bytes) = stream.next().await {
                    let bytes = bytes?;
                    streamed_size = streamed_size.saturating_add(bytes.len() as u64);
                    stats.checkpoint_object_bytes += bytes.len() as u64;
                    if streamed_size > object.size {
                        anyhow::bail!(
                            "checkpoint object {} exceeded listed size {}",
                            object.path,
                            object.size
                        );
                    }
                    for chunk in bytes.chunks(chunk_size) {
                        send_snapshot_frame(
                            &tx,
                            &group_id,
                            Vec::new(),
                            WireSnapshotFrame::CheckpointObjectChunk(chunk.to_vec()),
                        )
                        .await?;
                    }
                }
                if streamed_size != object.size {
                    anyhow::bail!(
                        "checkpoint object {} streamed {} bytes but listed {}",
                        object.path,
                        streamed_size,
                        object.size
                    );
                }
            }
        }
    }

    send_snapshot_frame(&tx, &group_id, Vec::new(), WireSnapshotFrame::End).await?;
    Ok(stats)
}

#[derive(Debug, Default)]
struct SnapshotSendStats {
    manifest_bytes: u64,
    checkpoint_objects: u64,
    checkpoint_object_bytes: u64,
}

#[derive(Debug, Default)]
struct SnapshotReceiveStats {
    manifest_bytes: u64,
    checkpoint_objects: u64,
    checkpoint_object_bytes: u64,
}

async fn send_snapshot_frame(
    tx: &mpsc::Sender<RaftMessage>,
    group_id: &str,
    metrics_payload: Vec<u8>,
    frame: WireSnapshotFrame,
) -> anyhow::Result<()> {
    let payload = encode(&frame).map_err(|err| anyhow::anyhow!("{err}"))?;
    tx.send(RaftMessage {
        payload,
        metrics_payload,
        group_id: group_id.to_string(),
    })
    .await
    .map_err(|_| anyhow::anyhow!("snapshot receiver dropped"))
}

async fn receive_snapshot_stream(
    raft: &OrionRaft,
    group_id: &str,
    first_payload: Vec<u8>,
    stream: &mut tonic::Streaming<RaftMessage>,
    metrics_registry: &ClusterRaftMetricsRegistry,
    node_id: OrionNodeId,
) -> Result<(WireSnapshotHeader, Vec<u8>), Status> {
    let result = match decode_status::<WireSnapshotFrame>(&first_payload) {
        Ok(WireSnapshotFrame::Header(header)) => {
            receive_framed_snapshot_stream(
                raft,
                group_id,
                header,
                stream,
                metrics_registry,
                node_id,
            )
            .await
        }
        Ok(_) => Err(Status::invalid_argument(
            "first snapshot frame must be a header",
        )),
        Err(_) => {
            let header: WireSnapshotHeader = decode_status(&first_payload)?;
            let mut payload = Vec::new();
            while let Some(message) = stream.next().await {
                let message = message?;
                ensure_snapshot_group_id(&message, group_id)?;
                payload.extend_from_slice(&message.payload);
            }
            let stats = SnapshotReceiveStats {
                manifest_bytes: payload.len() as u64,
                ..SnapshotReceiveStats::default()
            };
            Ok((header, payload, stats))
        }
    };

    match result {
        Ok((header, payload, stats)) => {
            metrics_registry.record_snapshot_transfer(node_id, |metrics| {
                metrics.snapshots_received += 1;
                metrics.manifest_bytes_received += stats.manifest_bytes;
                metrics.checkpoint_objects_received += stats.checkpoint_objects;
                metrics.checkpoint_object_bytes_received += stats.checkpoint_object_bytes;
            });
            Ok((header, payload))
        }
        Err(status) => {
            metrics_registry.record_snapshot_transfer(node_id, |metrics| {
                metrics.snapshot_receive_failures += 1;
            });
            Err(status)
        }
    }
}

async fn receive_framed_snapshot_stream(
    raft: &OrionRaft,
    group_id: &str,
    header: WireSnapshotHeader,
    stream: &mut tonic::Streaming<RaftMessage>,
    metrics_registry: &ClusterRaftMetricsRegistry,
    node_id: OrionNodeId,
) -> Result<(WireSnapshotHeader, Vec<u8>, SnapshotReceiveStats), Status> {
    let target_state = raft
        .with_state_machine(|sm| Box::pin(async move { sm.state_store() }))
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut payload = Vec::new();
    let mut object: Option<ReceivingCheckpointObject> = None;
    let mut allowed_artifacts = None;
    let mut stats = SnapshotReceiveStats::default();

    while let Some(message) = stream.next().await {
        let message = message?;
        ensure_snapshot_group_id(&message, group_id)?;
        match decode_status::<WireSnapshotFrame>(&message.payload)? {
            WireSnapshotFrame::Header(_) => {
                return Err(Status::invalid_argument(
                    "snapshot stream contained duplicate header frame",
                ));
            }
            WireSnapshotFrame::ManifestChunk(chunk) => {
                stats.manifest_bytes += chunk.len() as u64;
                payload.extend_from_slice(&chunk);
            }
            WireSnapshotFrame::CheckpointObjectHeader { path, size } => {
                if allowed_artifacts.is_none() {
                    allowed_artifacts = Some(
                        snapshot_checkpoint_artifacts(&payload)
                            .map_err(|err| Status::invalid_argument(err.to_string()))?,
                    );
                }
                ensure_snapshot_object_path_allowed(
                    allowed_artifacts.as_deref().unwrap_or_default(),
                    &path,
                )?;
                if let Some(previous) = object.take() {
                    if let Err(status) = previous.finish().await {
                        metrics_registry.record_snapshot_transfer(node_id, |metrics| {
                            metrics.multipart_upload_failures += 1;
                        });
                        return Err(status);
                    }
                    stats.checkpoint_objects += 1;
                }
                object = Some(ReceivingCheckpointObject::new(&target_state, path, size).await?);
            }
            WireSnapshotFrame::CheckpointObjectChunk(chunk) => {
                let Some(current) = object.as_mut() else {
                    return Err(Status::invalid_argument(
                        "checkpoint object chunk arrived before object header",
                    ));
                };
                stats.checkpoint_object_bytes += chunk.len() as u64;
                if let Err(status) = current.write_chunk(chunk).await {
                    metrics_registry.record_snapshot_transfer(node_id, |metrics| {
                        metrics.multipart_upload_aborts += 1;
                    });
                    return Err(status);
                }
            }
            WireSnapshotFrame::End => {
                if let Some(current) = object.take() {
                    if let Err(status) = current.finish().await {
                        metrics_registry.record_snapshot_transfer(node_id, |metrics| {
                            metrics.multipart_upload_failures += 1;
                        });
                        return Err(status);
                    }
                    stats.checkpoint_objects += 1;
                }
                return Ok((header, payload, stats));
            }
        }
    }

    if let Some(mut current) = object.take() {
        current.abort().await;
        metrics_registry.record_snapshot_transfer(node_id, |metrics| {
            metrics.multipart_upload_aborts += 1;
        });
        return Err(Status::invalid_argument(
            "snapshot stream ended before checkpoint object completed",
        ));
    }
    Err(Status::invalid_argument(
        "snapshot stream ended before end frame",
    ))
}

fn ensure_snapshot_object_path_allowed(
    artifacts: &[crate::checkpoint_artifact::SlateDbCheckpointArtifact],
    object_path: &str,
) -> Result<(), Status> {
    if artifacts
        .iter()
        .any(|artifact| ensure_checkpoint_object_path_allowed(artifact, object_path).is_ok())
    {
        return Ok(());
    }
    Err(Status::invalid_argument(format!(
        "checkpoint object path {object_path} does not match snapshot artifacts"
    )))
}

fn ensure_snapshot_group_id(message: &RaftMessage, group_id: &str) -> Result<(), Status> {
    if message.group_id != group_id {
        return Err(Status::invalid_argument(format!(
            "snapshot chunk group_id {} does not match header group_id {group_id}",
            message.group_id
        )));
    }
    Ok(())
}

struct ReceivingCheckpointObject {
    path: String,
    location: ObjectPath,
    object_store: Arc<dyn ObjectStore>,
    expected_size: u64,
    received_size: u64,
    writer: Option<WriteMultipart>,
}

impl ReceivingCheckpointObject {
    async fn new(
        state: &SlateDbStateStore,
        path: String,
        expected_size: u64,
    ) -> Result<Self, Status> {
        let location = ObjectPath::parse(&path).map_err(|err| {
            Status::invalid_argument(format!("invalid checkpoint object path: {err}"))
        })?;
        let object_store = state.object_store();
        let upload = object_store.put_multipart(&location).await.map_err(|err| {
            Status::internal(format!("starting checkpoint object upload failed: {err}"))
        })?;
        Ok(Self {
            path,
            location,
            object_store,
            expected_size,
            received_size: 0,
            writer: Some(WriteMultipart::new_with_chunk_size(
                upload,
                SNAPSHOT_RECEIVE_MULTIPART_CHUNK_SIZE,
            )),
        })
    }

    async fn write_chunk(&mut self, chunk: Vec<u8>) -> Result<(), Status> {
        self.received_size = self.received_size.saturating_add(chunk.len() as u64);
        if self.received_size > self.expected_size {
            self.abort().await;
            return Err(Status::invalid_argument(format!(
                "checkpoint object {} exceeded expected size {}",
                self.path, self.expected_size
            )));
        }
        let Some(writer) = self.writer.as_mut() else {
            return Err(Status::internal(format!(
                "checkpoint object {} upload already closed",
                self.path
            )));
        };
        writer
            .wait_for_capacity(SNAPSHOT_RECEIVE_MULTIPART_CONCURRENCY)
            .await
            .map_err(|err| {
                Status::internal(format!("checkpoint upload backpressure failed: {err}"))
            })?;
        writer.put(chunk.into());
        Ok(())
    }

    async fn finish(mut self) -> Result<(), Status> {
        if self.received_size != self.expected_size {
            self.abort().await;
            return Err(Status::invalid_argument(format!(
                "checkpoint object {} byte count mismatch: received {}, expected {}",
                self.path, self.received_size, self.expected_size
            )));
        }
        let Some(writer) = self.writer.take() else {
            return Err(Status::internal(format!(
                "checkpoint object {} upload already closed",
                self.path
            )));
        };
        let result = writer
            .finish()
            .await
            .map_err(|err| Status::internal(format!("writing checkpoint object failed: {err}")))?;
        let _ = result;
        let meta = self
            .object_store
            .head(&self.location)
            .await
            .map_err(|err| Status::internal(format!("checking checkpoint object failed: {err}")))?;
        if meta.size != self.expected_size {
            return Err(Status::internal(format!(
                "checkpoint object {} completed with size {}, expected {}",
                self.location, meta.size, self.expected_size
            )));
        }
        Ok(())
    }

    async fn abort(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.abort().await;
        }
    }
}

pub async fn serve_raft_transport(
    listener: TcpListener,
    raft: OrionRaft,
) -> Result<(), tonic::transport::Error> {
    serve_raft_transport_with_config(listener, raft, TonicRaftTransportConfig::default()).await
}

pub async fn serve_raft_transport_with_config(
    listener: TcpListener,
    raft: OrionRaft,
    config: TonicRaftTransportConfig,
) -> Result<(), tonic::transport::Error> {
    serve_raft_transport_with_config_and_shutdown(listener, raft, config, std::future::pending())
        .await
}

pub async fn serve_raft_transport_with_config_and_shutdown<F>(
    listener: TcpListener,
    raft: OrionRaft,
    config: TonicRaftTransportConfig,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    serve_raft_transport_with_config_metrics_and_shutdown(
        listener,
        raft,
        config,
        ClusterRaftMetricsRegistry::default(),
        shutdown,
    )
    .await
}

pub async fn serve_raft_transport_with_config_metrics_and_shutdown<F>(
    listener: TcpListener,
    raft: OrionRaft,
    config: TonicRaftTransportConfig,
    metrics_registry: ClusterRaftMetricsRegistry,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    let service = RaftTransportServer::new(TonicRaftService::with_metrics_registry(
        raft,
        metrics_registry,
    ))
    .max_decoding_message_size(config.max_message_size)
    .max_encoding_message_size(config.max_message_size);
    serve_raft_transport_server(listener, service, config, shutdown).await
}

pub async fn serve_raft_transport_group_registry_with_config_metrics_and_shutdown<F>(
    listener: TcpListener,
    groups: TonicRaftGroupRegistry,
    config: TonicRaftTransportConfig,
    metrics_registry: ClusterRaftMetricsRegistry,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    let service = RaftTransportServer::new(TonicRaftService::with_group_registry(
        groups,
        metrics_registry,
    ))
    .max_decoding_message_size(config.max_message_size)
    .max_encoding_message_size(config.max_message_size);
    serve_raft_transport_server(listener, service, config, shutdown).await
}

async fn serve_raft_transport_server<F>(
    listener: TcpListener,
    service: RaftTransportServer<TonicRaftService>,
    config: TonicRaftTransportConfig,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    Server::builder()
        .timeout(config.rpc_timeout)
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

pub async fn bind_raft_transport(
    addr: SocketAddr,
    raft: OrionRaft,
) -> anyhow::Result<(
    SocketAddr,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_raft_transport(listener, raft));
    Ok((local_addr, handle))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireSnapshot {
    meta: openraft::SnapshotMeta<
        <OrionEntry as openraft::entry::RaftEntry>::CommittedLeaderId,
        OrionNodeId,
        OrionNode,
    >,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireSnapshotHeader {
    vote: OrionVote,
    meta: openraft::SnapshotMeta<
        <OrionEntry as openraft::entry::RaftEntry>::CommittedLeaderId,
        OrionNodeId,
        OrionNode,
    >,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WireSnapshotFrame {
    Header(WireSnapshotHeader),
    ManifestChunk(Vec<u8>),
    CheckpointObjectHeader { path: String, size: u64 },
    CheckpointObjectChunk(Vec<u8>),
    End,
}

impl WireSnapshot {
    fn into_snapshot(self) -> OrionSnapshot {
        Snapshot {
            meta: self.meta,
            snapshot: std::io::Cursor::new(self.payload),
        }
    }
}

fn normalize_endpoint(endpoint: String) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    }
}

fn snapshot_stream_chunk_size(max_message_size: usize) -> usize {
    max_message_size
        .saturating_sub(1024)
        .clamp(1, DEFAULT_SNAPSHOT_STREAM_CHUNK_SIZE)
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RPCError<OrionTypeConfig>> {
    crate::codec::to_versioned_vec(WIRE_VERSION, value)
        .map_err(|err| RPCError::Network(NetworkError::from_string(err.to_string())))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RPCError<OrionTypeConfig>> {
    let (version, value): (u16, T) = crate::codec::from_versioned_bytes(bytes)
        .map_err(|err| RPCError::Network(NetworkError::from_string(err.to_string())))?;
    if version != WIRE_VERSION {
        return Err(RPCError::Network(NetworkError::from_string(format!(
            "unsupported raft transport wire version {version}"
        ))));
    }
    Ok(value)
}

fn decode_streaming<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, StreamingError<OrionTypeConfig>> {
    let (version, value): (u16, T) = crate::codec::from_versioned_bytes(bytes)
        .map_err(|err| StreamingError::Network(NetworkError::from_string(err.to_string())))?;
    if version != WIRE_VERSION {
        return Err(StreamingError::Network(NetworkError::from_string(format!(
            "unsupported raft transport wire version {version}"
        ))));
    }
    Ok(value)
}

fn encode_status<T: Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    crate::codec::to_versioned_vec(WIRE_VERSION, value)
        .map_err(|err| Status::internal(err.to_string()))
}

fn decode_status<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    let (version, value): (u16, T) = crate::codec::from_versioned_bytes(bytes)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    if version != WIRE_VERSION {
        return Err(Status::invalid_argument(format!(
            "unsupported raft transport wire version {version}"
        )));
    }
    Ok(value)
}

fn network_error(target: OrionNodeId, error: impl std::fmt::Display) -> RPCError<OrionTypeConfig> {
    RPCError::Network(NetworkError::from_string(format!("node {target}: {error}")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use openraft::network::RaftNetworkFactory;
    use openraft::{BasicNode, Config, Raft, SnapshotPolicy};
    use orion_sqlite::{FileKind, VfsFileOp, VfsSyncBatch, VfsWrite};
    use slatedb::DbWriteOps;
    use slatedb::object_store::Error as ObjectStoreError;
    use slatedb::object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    use super::*;
    use crate::openraft_store::{
        LargeSqliteBatchChunk, LargeSqliteBatchManifest, LargeSqliteBatchRequest,
    };
    use crate::types::SqliteVfsOp;
    use crate::{
        HybridTimestamp, LargeBatchOptions, ORION_SYSTEM_DATABASE, OpenRaftSqliteCommitSink,
        OrionRaftLogStore, OrionRaftRequest, OrionRaftRequestMeta, OrionRaftStateMachine,
        OrionSqliteRaftClient, OrionSqliteRuntime, OrionSqliteRuntimeConfig, RaftMetricsSnapshot,
        SlateDbSqliteFileStore, SlateDbStateStore, SqliteFileKind, SqlitePageCompactionMetrics,
        SqliteVfsBatch, SqliteVfsWrite,
    };

    fn test_config() -> anyhow::Result<Arc<Config>> {
        test_config_with_snapshot_policy(SnapshotPolicy::Never)
    }

    fn test_config_with_snapshot_policy(
        snapshot_policy: SnapshotPolicy,
    ) -> anyhow::Result<Arc<Config>> {
        let config = Config {
            cluster_name: "orion-tonic-test".to_string(),
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            replication_lag_threshold: 8,
            snapshot_policy,
            max_in_snapshot_log_to_keep: 1,
            purge_batch_size: 1,
            ..Default::default()
        };
        Ok(Arc::new(config.validate()?))
    }

    async fn build_node(
        id: u64,
        config: Arc<Config>,
        network: TonicRaftNetwork,
    ) -> anyhow::Result<(OrionRaft, TempDir)> {
        let dir = TempDir::new()?;
        let log_store = OrionRaftLogStore::open(dir.path())?;
        let state = SlateDbStateStore::open_in_memory(&format!("node-{id}")).await?;
        let raft = build_node_with_stores(id, config, network, log_store, state).await?;
        Ok((raft, dir))
    }

    async fn build_node_with_stores(
        id: u64,
        config: Arc<Config>,
        network: TonicRaftNetwork,
        log_store: OrionRaftLogStore,
        state: SlateDbStateStore,
    ) -> anyhow::Result<OrionRaft> {
        let network = network.with_local_state_store(state.clone());
        let state_machine = OrionRaftStateMachine::new(state);
        let raft = Raft::new(id, config, network, log_store, state_machine).await?;
        Ok(raft)
    }

    async fn start_server(raft: OrionRaft) -> anyhow::Result<SocketAddr> {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let (addr, _handle) = bind_raft_transport(bind_addr, raft).await?;
        Ok(addr)
    }

    async fn start_server_with_metrics(
        raft: OrionRaft,
        registry: ClusterRaftMetricsRegistry,
    ) -> anyhow::Result<SocketAddr> {
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
        let addr = listener.local_addr()?;
        tokio::spawn(serve_raft_transport_with_config_metrics_and_shutdown(
            listener,
            raft,
            TonicRaftTransportConfig::default(),
            registry,
            std::future::pending(),
        ));
        Ok(addr)
    }

    async fn build_snapshot_stream_test_fixture() -> anyhow::Result<(
        OrionRaft,
        SocketAddr,
        WireSnapshotHeader,
        Vec<u8>,
        String,
        Arc<dyn ObjectStore>,
    )> {
        let source_network = TonicRaftNetwork::new();
        let target_network = TonicRaftNetwork::new();
        let config = test_config()?;

        let source_log_dir = TempDir::new()?;
        let target_log_dir = TempDir::new()?;
        let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_state =
            SlateDbStateStore::open("snapshot-negative-source", source_store).await?;
        let target_state =
            SlateDbStateStore::open("snapshot-negative-target", Arc::clone(&target_store)).await?;

        DbWriteOps::put(source_state.db.as_ref(), b"snapshot-negative", b"value").await?;
        DbWriteOps::flush(source_state.db.as_ref()).await?;

        let source_raft = build_node_with_stores(
            1,
            config.clone(),
            source_network,
            OrionRaftLogStore::open(source_log_dir.path())?,
            source_state,
        )
        .await?;
        let target_raft = build_node_with_stores(
            2,
            config,
            target_network,
            OrionRaftLogStore::open(target_log_dir.path())?,
            target_state,
        )
        .await?;
        let target_addr = start_server(target_raft.clone()).await?;

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        );
        source_raft.initialize(members).await?;
        source_raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(1, "source leader")
            .await?;
        source_raft.trigger().snapshot().await?;
        let mut snapshot = None;
        for _ in 0..20 {
            snapshot = source_raft.get_snapshot().await?;
            if snapshot.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let snapshot = snapshot.expect("snapshot should be published");
        let payload = snapshot.snapshot.into_inner();
        let artifact = snapshot_checkpoint_artifacts(&payload)?
            .into_iter()
            .next()
            .expect("snapshot should include checkpoint artifact");
        let object_path = format!("{}/malformed-object", artifact.object_prefix);
        let header = WireSnapshotHeader {
            vote: OrionVote::new_committed(1, 1),
            meta: snapshot.meta,
        };
        source_raft.shutdown().await?;

        Ok((
            target_raft,
            target_addr,
            header,
            payload,
            object_path,
            target_store,
        ))
    }

    async fn send_raw_snapshot_frames(
        target_addr: SocketAddr,
        header: WireSnapshotHeader,
        frames: Vec<WireSnapshotFrame>,
    ) -> Result<RaftMessage, Status> {
        let (tx, rx) = mpsc::channel(frames.len().saturating_add(1));
        send_snapshot_frame(
            &tx,
            DEFAULT_RAFT_GROUP_ID,
            Vec::new(),
            WireSnapshotFrame::Header(header),
        )
        .await
        .unwrap();
        for frame in frames {
            send_snapshot_frame(&tx, DEFAULT_RAFT_GROUP_ID, Vec::new(), frame)
                .await
                .unwrap();
        }
        drop(tx);

        let mut client = RaftTransportClient::connect(format!("http://{target_addr}"))
            .await
            .unwrap();
        client
            .full_snapshot(Request::new(ReceiverStream::new(rx)))
            .await
            .map(|response| response.into_inner())
    }

    async fn assert_object_not_committed(object_store: Arc<dyn ObjectStore>, path: &str) {
        let location = ObjectPath::parse(path).unwrap();
        match object_store.head(&location).await {
            Err(ObjectStoreError::NotFound { .. }) => {}
            Ok(meta) => panic!("object {path} was committed with size {}", meta.size),
            Err(error) => panic!("checking object {path} failed: {error}"),
        }
    }

    async fn reserve_local_addr() -> anyhow::Result<SocketAddr> {
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
        Ok(listener.local_addr()?)
    }

    fn sqlite_write_request(
        database: &str,
        path: &str,
        bytes: impl Into<Vec<u8>>,
    ) -> OrionRaftRequest {
        OrionRaftRequest::sqlite_batch(SqliteVfsBatch {
            database: database.to_string(),
            file_path: path.to_string(),
            file_kind: SqliteFileKind::Wal,
            ops: vec![SqliteVfsOp::Write(SqliteVfsWrite {
                offset: 0,
                bytes: bytes.into(),
            })],
        })
    }

    fn blob_payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let mixed = index.wrapping_mul(31) ^ index.wrapping_div(7) ^ 0x5a;
                mixed as u8
            })
            .collect()
    }

    fn sqlite_runtime_config(cache_root: &std::path::Path) -> OrionSqliteRuntimeConfig {
        let mut config = OrionSqliteRuntimeConfig::new(cache_root.to_path_buf());
        config.large_batch_threshold_bytes = 16 * 1024;
        config.large_batch_chunk_bytes = 8 * 1024;
        config
    }

    async fn sqlite_file_store(
        state: &SlateDbStateStore,
        database: &str,
    ) -> SlateDbSqliteFileStore {
        let database_state = state.sqlite_database_state(database).await.unwrap();
        SlateDbSqliteFileStore::new(&database_state, database)
    }

    #[tokio::test]
    async fn single_node_raft_proposes_sqlite_batch_into_slate_db() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let dir = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(dir.path()).unwrap();
        let state = SlateDbStateStore::open_in_memory("single-node-sqlite-batch")
            .await
            .unwrap();
        let file_store = sqlite_file_store(&state, "tenant-a").await;
        let raft = build_node_with_stores(1, config, network, log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let response = raft
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"ready",
            ))
            .await
            .unwrap();

        assert_eq!(response.data.sqlite_batches_applied, 1);
        let commit_ts = response
            .data
            .commit_ts
            .expect("SQLite write should return commit timestamp");
        let bytes = file_store.read_file("tenant-a.db-wal").await.unwrap();
        assert_eq!(&bytes[..5], b"ready");
        let applied_commit_ts = raft
            .with_state_machine(|sm| {
                Box::pin(async move { sm.applied_commit_timestamp().await.unwrap() })
            })
            .await
            .unwrap();
        assert_eq!(applied_commit_ts, Some(commit_ts));
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn large_sqlite_commit_is_chunked_through_raft_log() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let dir = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(dir.path()).unwrap();
        let state = SlateDbStateStore::open_in_memory("large-sqlite-chunked")
            .await
            .unwrap();
        let file_store = sqlite_file_store(&state, "tenant-a").await;
        let raft = build_node_with_stores(1, config, network, log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let payload = vec![7_u8; 1024 * 1024 + 17];
        let sink = OpenRaftSqliteCommitSink::with_large_batch_options(
            Some(raft.clone()),
            LargeBatchOptions {
                threshold_bytes: 64 * 1024,
                chunk_bytes: 128 * 1024,
            },
        );
        let decision = sink
            .commit_batch(VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "tenant-a.db-wal".to_string(),
                file_kind: FileKind::Wal,
                ops: vec![VfsFileOp::Write(VfsWrite {
                    offset: 0,
                    bytes: payload.clone(),
                })],
            })
            .await
            .unwrap();

        assert!(decision.raft_log_index >= 4);
        let bytes = file_store.read_file("tenant-a.db-wal").await.unwrap();
        assert_eq!(bytes, payload);
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn large_page_delta_runtime_apply_is_chunked_through_raft_log() {
        let source = SlateDbStateStore::open_in_memory("large-page-delta-runtime-source")
            .await
            .unwrap();
        let source_store = sqlite_file_store(&source, "tenant-a").await;
        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![9; 64 * 1024],
                    })],
                },
                5,
            )
            .await
            .unwrap();
        let delta = source
            .export_sqlite_database_pages_since("tenant-a", 0)
            .await
            .unwrap();

        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let dir = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(dir.path()).unwrap();
        let target = SlateDbStateStore::open_in_memory("large-page-delta-runtime-target")
            .await
            .unwrap();
        let target_store = sqlite_file_store(&target, "tenant-a").await;
        let raft = build_node_with_stores(1, config, network, log_store, target)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let target_state = raft
            .with_state_machine(|sm| Box::pin(async move { sm.state_store() }))
            .await
            .unwrap();
        let cache = TempDir::new().unwrap();
        let mut runtime_config = OrionSqliteRuntimeConfig::new(cache.path().to_path_buf());
        runtime_config.large_batch_threshold_bytes = 1024;
        runtime_config.large_batch_chunk_bytes = 2048;
        let runtime = OrionSqliteRuntime::new(raft.clone(), target_state, runtime_config);
        let log_index = runtime
            .apply_database_page_delta_through_raft("tenant-a", delta)
            .unwrap()
            .expect("chunked page-delta apply should commit through raft");

        assert!(log_index > 2, "expected begin/chunks/commit log entries");
        assert_eq!(
            target_store.read_file("main.db").await.unwrap(),
            vec![9; 64 * 1024]
        );
        let metrics = runtime.large_payload_metrics().await.unwrap();
        assert_eq!(metrics.uploads_started, 1);
        assert!(metrics.chunks_staged > 1);
        assert_eq!(metrics.uploads_committed, 1);
        assert_eq!(metrics.active_uploads, 0);
        assert_eq!(metrics.active_bytes, 0);
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn large_sqlite_commit_rejects_missing_or_mismatched_chunks() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let dir = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(dir.path()).unwrap();
        let state = SlateDbStateStore::open_in_memory("large-sqlite-invalid")
            .await
            .unwrap();
        let file_store = sqlite_file_store(&state, "tenant-a").await;
        let raft = build_node_with_stores(1, config, network, log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let upload_id = "invalid-upload".to_string();
        raft.client_write(OrionRaftRequest {
            meta: None,
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: Some(LargeSqliteBatchRequest::Begin(LargeSqliteBatchManifest {
                upload_id: upload_id.clone(),
                database: "tenant-a".to_string(),
                file_path: "tenant-a.db-wal".to_string(),
                file_kind: SqliteFileKind::Wal,
                total_chunks: 2,
                total_bytes: 4,
                created_at_ms: 1,
            })),
        })
        .await
        .unwrap();
        raft.client_write(OrionRaftRequest {
            meta: None,
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: Some(LargeSqliteBatchRequest::Chunk(LargeSqliteBatchChunk {
                upload_id: upload_id.clone(),
                chunk_index: 0,
                ops: vec![SqliteVfsOp::Write(SqliteVfsWrite {
                    offset: 0,
                    bytes: b"abc".to_vec(),
                })],
            })),
        })
        .await
        .unwrap();

        let error = raft
            .client_write(OrionRaftRequest {
                meta: None,
                sqlite_batches: Vec::new(),
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: None,
                large_sqlite_batch: Some(LargeSqliteBatchRequest::Commit { upload_id }),
            })
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("missing large SQLite batch chunk")
                || error.contains("byte count mismatch"),
            "unexpected error: {error}"
        );
        assert!(
            file_store
                .read_file("tenant-a.db-wal")
                .await
                .unwrap()
                .is_empty()
        );
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn tonic_client_write_overwrites_request_commit_timestamp_at_leader() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let (raft, _dir) = build_node(1, config, network).await.unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node tonic leader")
            .await
            .unwrap();

        let stale_ts = HybridTimestamp {
            physical_ms: 1,
            logical: 0,
        };
        let response = client_write_to_raft_endpoint(
            addr.to_string(),
            OrionRaftRequest {
                meta: Some(OrionRaftRequestMeta::new(stale_ts)),
                sqlite_batches: vec![SqliteVfsBatch {
                    database: "tenant-a".to_string(),
                    file_path: "tenant-a.db-wal".to_string(),
                    file_kind: SqliteFileKind::Wal,
                    ops: vec![SqliteVfsOp::Write(SqliteVfsWrite {
                        offset: 0,
                        bytes: b"ts".to_vec(),
                    })],
                }],
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: None,
                large_sqlite_batch: None,
            },
            TonicRaftTransportConfig::default(),
        )
        .await
        .unwrap();

        let commit_ts = response
            .data
            .commit_ts
            .expect("SQLite tonic write should return commit timestamp");
        assert!(commit_ts > stale_ts);
        let applied_commit_ts = raft
            .with_state_machine(|sm| {
                Box::pin(async move { sm.applied_commit_timestamp().await.unwrap() })
            })
            .await
            .unwrap();
        assert_eq!(applied_commit_ts, Some(commit_ts));
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_transaction_commits_through_vfs_openraft_and_slate_db() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let log_dir = TempDir::new().unwrap();
        let sqlite_cache = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state = SlateDbStateStore::open_in_memory("sqlite-e2e-node-1")
            .await
            .unwrap();
        let runtime_state = state.clone();
        let file_store = Arc::new(sqlite_file_store(&state, "tenant-a").await);
        let raft = build_node_with_stores(1, config, network, log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "sqlite e2e single-node leader")
            .await
            .unwrap();

        let runtime = OrionSqliteRuntime::new(
            raft.clone(),
            runtime_state,
            OrionSqliteRuntimeConfig::new(sqlite_cache.path().to_path_buf()),
        );
        let db = runtime.open_database("tenant-a").unwrap();
        let conn = db.connect().unwrap();
        let journal_mode: String = conn
            .query_row("pragma journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "delete");

        conn.execute_batch(
            r#"
            create table services (tenant_id text, service_id text, weight integer);
            begin immediate;
            insert into services values ('acme', 'api', 20);
            commit;
            "#,
        )
        .unwrap();

        let weight: i64 = conn
            .query_row(
                "select weight from services where tenant_id = 'acme' and service_id = 'api'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(weight, 20);

        let main_db = file_store.read_file("tenant-a.db").await.unwrap();
        assert!(
            !main_db.is_empty(),
            "expected SQLite main database bytes to be materialized in SlateDB"
        );

        drop(conn);
        let reopened = db.connect().unwrap();
        let reopened_weight: i64 = reopened
            .query_row(
                "select weight from services where tenant_id = 'acme' and service_id = 'api'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reopened_weight, 20);

        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_checkpoint_restore_preserves_large_write_path_database() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let source_log_dir = TempDir::new().unwrap();
        let source_cache = TempDir::new().unwrap();
        let source_object_dir = TempDir::new().unwrap();
        let target_cache = TempDir::new().unwrap();
        let target_object_dir = TempDir::new().unwrap();
        let source_object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(source_object_dir.path()).unwrap());
        let target_object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(target_object_dir.path()).unwrap());
        let source_log_store = OrionRaftLogStore::open(source_log_dir.path()).unwrap();
        let source_state =
            SlateDbStateStore::open("checkpoint-write-source", Arc::clone(&source_object_store))
                .await
                .unwrap();
        let source_runtime_state = source_state.clone();
        let source_raft =
            build_node_with_stores(1, config, network, source_log_store, source_state)
                .await
                .unwrap();
        let source_addr = start_server(source_raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: source_addr.to_string(),
            },
        );
        source_raft.initialize(members).await.unwrap();
        source_raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(1, "checkpoint source single-node leader")
            .await
            .unwrap();

        let source_runtime = OrionSqliteRuntime::new(
            source_raft.clone(),
            source_runtime_state,
            OrionSqliteRuntimeConfig::new(source_cache.path().to_path_buf()),
        );
        let total_started = Instant::now();
        let source_db = source_runtime.open_database("tenant-checkpoint").unwrap();
        let source_conn = source_db.connect().unwrap();
        source_conn
            .execute_batch(
                r#"
                create table payloads(id integer primary key, payload blob not null);
                "#,
            )
            .unwrap();
        let total_bytes = std::env::var("ORION_CHECKPOINT_RESTORE_TEST_BYTES")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(4 * 1024 * 1024)
            .max(128 * 1024);
        let blob_bytes = 128 * 1024_i64;
        let row_count = (total_bytes + blob_bytes - 1) / blob_bytes;
        let write_started = Instant::now();
        let tx = source_conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("insert into payloads(id, payload) values (?, zeroblob(?))")
                .unwrap();
            for id in 1..=row_count {
                stmt.execute((id, blob_bytes)).unwrap();
            }
        }
        tx.commit().unwrap();
        eprintln!(
            "checkpoint restore test: wrote {} bytes in {:?}",
            row_count * blob_bytes,
            write_started.elapsed()
        );
        let source_verify_started = Instant::now();
        let source_quick_check: String = source_conn
            .query_row("pragma quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(source_quick_check, "ok");
        let source_bytes: i64 = source_conn
            .query_row("select sum(length(payload)) from payloads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_bytes, row_count * blob_bytes);
        eprintln!(
            "checkpoint restore test: source verified in {:?}",
            source_verify_started.elapsed()
        );
        drop(source_conn);
        drop(source_db);

        let checkpoint_started = Instant::now();
        let artifact = source_runtime
            .database_checkpoint_artifact("tenant-checkpoint", "large-write-checkpoint")
            .unwrap();
        let objects = crate::checkpoint_artifact::list_slate_db_checkpoint_objects(
            &source_object_store,
            &artifact,
        )
        .await
        .unwrap();
        eprintln!(
            "checkpoint restore test: checkpoint listed {} objects / {} bytes in {:?}",
            objects.len(),
            objects.iter().map(|object| object.size).sum::<u64>(),
            checkpoint_started.elapsed()
        );
        assert!(
            objects.iter().map(|object| object.size).sum::<u64>() >= source_bytes as u64,
            "checkpoint should contain the large SQLite payload"
        );

        let target_state =
            SlateDbStateStore::open("checkpoint-write-target", Arc::clone(&target_object_store))
                .await
                .unwrap();
        let materialize_started = Instant::now();
        target_state
            .materialize_sqlite_database_checkpoint_incremental(
                "tenant-checkpoint",
                &artifact,
                Arc::clone(&source_object_store),
            )
            .await
            .unwrap();
        eprintln!(
            "checkpoint restore test: materialized checkpoint in {:?}",
            materialize_started.elapsed()
        );
        let target_runtime = OrionSqliteRuntime::new(
            source_raft.clone(),
            target_state,
            OrionSqliteRuntimeConfig::new(target_cache.path().to_path_buf()),
        );
        let target_db = target_runtime
            .open_existing_database("tenant-checkpoint")
            .unwrap()
            .expect("checkpoint materialization should mark tenant database present");
        let target_conn = target_db.connect().unwrap();
        let target_verify_started = Instant::now();
        let target_quick_check: String = target_conn
            .query_row("pragma quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(target_quick_check, "ok");
        let target_rows: i64 = target_conn
            .query_row("select count(*) from payloads", [], |row| row.get(0))
            .unwrap();
        let target_bytes: i64 = target_conn
            .query_row("select sum(length(payload)) from payloads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(target_rows, row_count);
        assert_eq!(target_bytes, source_bytes);
        eprintln!(
            "checkpoint restore test: target verified in {:?}; total {:?}",
            target_verify_started.elapsed(),
            total_started.elapsed()
        );

        source_raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_system_namespace_records_compaction_metrics() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let log_dir = TempDir::new().unwrap();
        let sqlite_cache = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state = SlateDbStateStore::open_in_memory("system-namespace-node-1")
            .await
            .unwrap();
        let runtime_state = state.clone();
        let raft = build_node_with_stores(1, config, network, log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "system namespace single-node leader")
            .await
            .unwrap();

        let runtime = OrionSqliteRuntime::new(
            raft.clone(),
            runtime_state,
            OrionSqliteRuntimeConfig::new(sqlite_cache.path().to_path_buf()),
        );
        let db = runtime.open_system_database().unwrap();
        assert!(db.uri().contains(ORION_SYSTEM_DATABASE));

        db.record_compaction_run(
            1_000,
            1_025,
            "ok",
            &SqlitePageCompactionMetrics {
                files_scanned: 3,
                files_compacted: 2,
                versions_scanned: 11,
                obsolete_versions: 5,
                deleted_versions: 4,
                highest_deleted_version: Some(10),
                bytes_scanned: 2_048,
                obsolete_bytes: 1_024,
                deleted_bytes: 512,
                duration_ms: 25,
            },
            None,
        )
        .unwrap();

        let conn = db.connect().unwrap();
        let run_count: i64 = conn
            .query_row("select count(*) from compaction_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(run_count, 1);
        let totals: (i64, i64, i64, i64) = conn
            .query_row(
                "select total_runs, total_errors, total_deleted_versions, total_deleted_bytes from compaction_state where id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(totals, (1, 0, 4, 512));

        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn committed_sqlite_transaction_reopens_after_raft_restart() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let log_dir = TempDir::new().unwrap();
        let first_sqlite_cache = TempDir::new().unwrap();
        let restarted_sqlite_cache = TempDir::new().unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state_path = "sqlite-restart-node-1";

        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state = SlateDbStateStore::open(state_path, Arc::clone(&object_store))
            .await
            .unwrap();
        let runtime_state = state.clone();
        let file_store = Arc::new(sqlite_file_store(&state, "tenant-a").await);
        let raft = build_node_with_stores(1, config.clone(), network.clone(), log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "sqlite restart single-node leader")
            .await
            .unwrap();

        let runtime = OrionSqliteRuntime::new(
            raft.clone(),
            runtime_state,
            OrionSqliteRuntimeConfig::new(first_sqlite_cache.path().to_path_buf()),
        );
        let db = runtime.open_database("tenant-a").unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(
            r#"
            create table services (tenant_id text, service_id text, weight integer);
            begin immediate;
            insert into services values ('acme', 'api', 20);
            insert into services values ('acme', 'worker', 7);
            commit;
            "#,
        )
        .unwrap();

        let weight: i64 = conn
            .query_row(
                "select weight from services where tenant_id = 'acme' and service_id = 'api'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(weight, 20);
        assert!(
            !file_store
                .read_file("tenant-a.db")
                .await
                .unwrap()
                .is_empty(),
            "expected SQLite main database bytes to be stored before restart"
        );

        drop(conn);
        drop(db);
        drop(runtime);
        drop(file_store);
        raft.shutdown().await.unwrap();

        let restarted_log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let restarted_state = SlateDbStateStore::open(state_path, object_store)
            .await
            .unwrap();
        let restarted_runtime_state = restarted_state.clone();
        let restarted =
            build_node_with_stores(1, config, network, restarted_log_store, restarted_state)
                .await
                .unwrap();
        let _addr = start_server(restarted.clone()).await.unwrap();
        restarted
            .wait(Some(Duration::from_secs(3)))
            .current_leader(1, "sqlite restart recovered leader")
            .await
            .unwrap();

        let restarted_runtime = OrionSqliteRuntime::new(
            restarted.clone(),
            restarted_runtime_state,
            OrionSqliteRuntimeConfig::new(restarted_sqlite_cache.path().to_path_buf()),
        );
        let reopened = restarted_runtime.open_database("tenant-a").unwrap();
        let reopened_conn = reopened.connect().unwrap();
        let rows: i64 = reopened_conn
            .query_row(
                "select count(*) from services where tenant_id = 'acme'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let total_weight: i64 = reopened_conn
            .query_row(
                "select sum(weight) from services where tenant_id = 'acme'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(total_weight, 27);

        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_node_restarts_from_persisted_log_and_state_machine() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let log_dir = TempDir::new().unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state_path = "restart-node-1";

        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state = SlateDbStateStore::open(state_path, Arc::clone(&object_store))
            .await
            .unwrap();
        let raft = build_node_with_stores(1, config.clone(), network.clone(), log_store, state)
            .await
            .unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "leader before restart")
            .await
            .unwrap();
        let response = raft
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"survived",
            ))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .applied_index_at_least(Some(response.log_id.index), "applied before restart")
            .await
            .unwrap();
        raft.shutdown().await.unwrap();

        let restarted_log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let restarted_state = SlateDbStateStore::open(state_path, object_store)
            .await
            .unwrap();
        let restarted =
            build_node_with_stores(1, config, network, restarted_log_store, restarted_state)
                .await
                .unwrap();
        let _addr = start_server(restarted.clone()).await.unwrap();
        restarted
            .wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(response.log_id.index), "applied after restart")
            .await
            .unwrap();
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn three_node_tonic_cluster_replicates_a_sqlite_batch() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft3, _dir3) = build_node(3, config, network).await.unwrap();

        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();
        let addr3 = start_server(raft3.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        members.insert(
            3,
            BasicNode {
                addr: addr3.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "three-node leader")
                .await
                .unwrap();
        }

        let response = raft1
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"replicated",
            ))
            .await
            .unwrap();
        assert_eq!(response.data.sqlite_batches_applied, 1);

        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .log_index_at_least(Some(response.log_id.index), "replicated log")
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(3)))
                .applied_index_at_least(Some(response.log_id.index), "applied log")
                .await
                .unwrap();
        }

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
        raft3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn three_node_sqlite_commit_reopens_after_leader_failover() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let log_dir1 = TempDir::new().unwrap();
        let log_dir2 = TempDir::new().unwrap();
        let log_dir3 = TempDir::new().unwrap();
        let leader_sqlite_cache = TempDir::new().unwrap();
        let recovered_sqlite_cache = TempDir::new().unwrap();

        let log_store1 = OrionRaftLogStore::open(log_dir1.path()).unwrap();
        let log_store2 = OrionRaftLogStore::open(log_dir2.path()).unwrap();
        let log_store3 = OrionRaftLogStore::open(log_dir3.path()).unwrap();
        let state1 = SlateDbStateStore::open_in_memory("sqlite-three-node-1")
            .await
            .unwrap();
        let state2 = SlateDbStateStore::open_in_memory("sqlite-three-node-2")
            .await
            .unwrap();
        let state3 = SlateDbStateStore::open_in_memory("sqlite-three-node-3")
            .await
            .unwrap();
        let leader_runtime_state = state1.clone();
        let recovered_runtime_state = state2.clone();
        let recovered_file_store = Arc::new(sqlite_file_store(&state2, "tenant-a").await);

        let raft1 = build_node_with_stores(1, config.clone(), network.clone(), log_store1, state1)
            .await
            .unwrap();
        let raft2 = build_node_with_stores(2, config.clone(), network.clone(), log_store2, state2)
            .await
            .unwrap();
        let raft3 = build_node_with_stores(3, config, network, log_store3, state3)
            .await
            .unwrap();

        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();
        let addr3 = start_server(raft3.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        members.insert(
            3,
            BasicNode {
                addr: addr3.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "three-node sqlite leader")
                .await
                .unwrap();
        }

        let leader_runtime = OrionSqliteRuntime::new(
            raft1.clone(),
            leader_runtime_state,
            OrionSqliteRuntimeConfig::new(leader_sqlite_cache.path().to_path_buf()),
        );
        let leader_db = leader_runtime.open_database("tenant-a").unwrap();
        let leader_conn = leader_db.connect().unwrap();
        leader_conn
            .execute_batch(
                r#"
                create table services (tenant_id text, service_id text, weight integer);
                begin immediate;
                insert into services values ('acme', 'api', 20);
                insert into services values ('acme', 'worker', 7);
                commit;
                "#,
            )
            .unwrap();
        let leader_rows: i64 = leader_conn
            .query_row(
                "select count(*) from services where tenant_id = 'acme'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leader_rows, 2);

        let sqlite_commit_index = RaftMetricsSnapshot::observe(&raft1)
            .committed_index
            .expect("sqlite write should commit through raft");
        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .applied_index_at_least(Some(sqlite_commit_index), "sqlite batch applied")
                .await
                .unwrap();
        }
        assert!(
            !recovered_file_store
                .read_file("tenant-a.db")
                .await
                .unwrap()
                .is_empty(),
            "expected SQLite main database bytes to replicate to node 2 before failover"
        );

        drop(leader_conn);
        drop(leader_db);
        drop(leader_runtime);

        raft1.trigger().transfer_leader(2).await.unwrap();
        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(2, "sqlite leader transferred to node 2")
                .await
                .unwrap();
        }
        raft1.shutdown().await.unwrap();
        for raft in [&raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(2, "sqlite leader remains after node 1 shutdown")
                .await
                .unwrap();
        }

        let recovered_runtime = OrionSqliteRuntime::new(
            raft2.clone(),
            recovered_runtime_state,
            OrionSqliteRuntimeConfig::new(recovered_sqlite_cache.path().to_path_buf()),
        );
        let recovered_db = recovered_runtime.open_database("tenant-a").unwrap();
        let recovered_conn = recovered_db.connect().unwrap();
        let total_weight: i64 = recovered_conn
            .query_row(
                "select sum(weight) from services where tenant_id = 'acme'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let service_ids: String = recovered_conn
            .query_row(
                "select group_concat(service_id, ',') from services where tenant_id = 'acme' order by service_id",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(total_weight, 27);
        assert_eq!(service_ids, "api,worker");

        raft2.shutdown().await.unwrap();
        raft3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn three_node_blob_heavy_sqlite_write_replicates_and_reads_after_failover() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let log_dir1 = TempDir::new().unwrap();
        let log_dir2 = TempDir::new().unwrap();
        let log_dir3 = TempDir::new().unwrap();
        let leader_sqlite_cache = TempDir::new().unwrap();
        let follower_sqlite_cache = TempDir::new().unwrap();
        let recovered_sqlite_cache = TempDir::new().unwrap();

        let log_store1 = OrionRaftLogStore::open(log_dir1.path()).unwrap();
        let log_store2 = OrionRaftLogStore::open(log_dir2.path()).unwrap();
        let log_store3 = OrionRaftLogStore::open(log_dir3.path()).unwrap();
        let state1 = SlateDbStateStore::open_in_memory("sqlite-blob-node-1")
            .await
            .unwrap();
        let state2 = SlateDbStateStore::open_in_memory("sqlite-blob-node-2")
            .await
            .unwrap();
        let state3 = SlateDbStateStore::open_in_memory("sqlite-blob-node-3")
            .await
            .unwrap();
        let leader_runtime_state = state1.clone();
        let follower_runtime_state = state2.clone();
        let recovered_runtime_state = state2.clone();

        let raft1 = build_node_with_stores(1, config.clone(), network.clone(), log_store1, state1)
            .await
            .unwrap();
        let raft2 = build_node_with_stores(2, config.clone(), network.clone(), log_store2, state2)
            .await
            .unwrap();
        let raft3 = build_node_with_stores(3, config, network, log_store3, state3)
            .await
            .unwrap();

        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();
        let addr3 = start_server(raft3.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        members.insert(
            3,
            BasicNode {
                addr: addr3.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "three-node blob leader")
                .await
                .unwrap();
        }

        let payload = blob_payload(192 * 1024 + 333);
        let follower_runtime = OrionSqliteRuntime::new(
            raft2.clone(),
            follower_runtime_state,
            sqlite_runtime_config(follower_sqlite_cache.path()),
        );
        let follower_db = follower_runtime.open_database("tenant-a").unwrap();
        let follower_conn = follower_db.connect().unwrap();
        follower_conn
            .execute_batch(
                r#"
                create table blobs (
                    id integer primary key,
                    name text not null,
                    body blob not null
                );
                "#,
            )
            .unwrap();
        follower_conn
            .execute(
                "insert into blobs (id, name, body) values (?1, ?2, ?3)",
                rusqlite::params![1_i64, "blob-from-follower", payload],
            )
            .unwrap();

        let follower_len: i64 = follower_conn
            .query_row("select length(body) from blobs where id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(follower_len, 192 * 1024 + 333);

        let sqlite_commit_index = RaftMetricsSnapshot::observe(&raft2)
            .applied_index
            .expect("follower SQLite write should apply locally after forwarding");
        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(sqlite_commit_index), "blob SQLite batch applied")
                .await
                .unwrap();
        }

        let expected = blob_payload(192 * 1024 + 333);
        let leader_runtime = OrionSqliteRuntime::new(
            raft1.clone(),
            leader_runtime_state,
            sqlite_runtime_config(leader_sqlite_cache.path()),
        );
        let leader_db = leader_runtime.open_database("tenant-a").unwrap();
        let leader_conn = leader_db.connect().unwrap();
        let leader_blob: Vec<u8> = leader_conn
            .query_row("select body from blobs where id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(leader_blob, expected);

        let follower_blob: Vec<u8> = follower_conn
            .query_row("select body from blobs where id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(follower_blob, expected);

        drop(leader_conn);
        drop(leader_db);
        drop(leader_runtime);
        drop(follower_conn);
        drop(follower_db);
        drop(follower_runtime);

        raft1.trigger().transfer_leader(2).await.unwrap();
        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(2, "blob leader transferred to node 2")
                .await
                .unwrap();
        }
        raft1.shutdown().await.unwrap();
        for raft in [&raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(2, "blob leader remains after node 1 shutdown")
                .await
                .unwrap();
        }

        let recovered_runtime = OrionSqliteRuntime::new(
            raft2.clone(),
            recovered_runtime_state,
            sqlite_runtime_config(recovered_sqlite_cache.path()),
        );
        let recovered_db = recovered_runtime.open_database("tenant-a").unwrap();
        let recovered_conn = recovered_db.connect().unwrap();
        let recovered_blob: Vec<u8> = recovered_conn
            .query_row("select body from blobs where id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(recovered_blob, expected);

        raft2.shutdown().await.unwrap();
        raft3.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_writes_sent_to_follower_are_forwarded_to_leader() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft3, _dir3) = build_node(3, config, network).await.unwrap();

        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();
        let addr3 = start_server(raft3.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        members.insert(
            3,
            BasicNode {
                addr: addr3.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        for raft in [&raft1, &raft2, &raft3] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "sql forwarding leader")
                .await
                .unwrap();
        }

        let follower_client = OrionSqliteRaftClient::new(Some(raft2.clone()));
        let log_index = follower_client
            .propose(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"forwarded",
            ))
            .await
            .unwrap()
            .unwrap();
        raft2
            .wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(log_index), "forwarded SQLite write applied locally")
            .await
            .unwrap();

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
        raft3.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sqlite_forwarding_retries_until_leader_transport_is_reachable() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config, network).await.unwrap();

        let delayed_addr1 = reserve_local_addr().await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: delayed_addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        raft2
            .wait(Some(Duration::from_secs(3)))
            .current_leader(1, "follower learns leader before leader transport starts")
            .await
            .unwrap();

        let leader = raft1.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            bind_raft_transport(delayed_addr1, leader).await.unwrap();
        });

        let follower_client = OrionSqliteRaftClient::with_transport_config(
            Some(raft2.clone()),
            TonicRaftTransportConfig {
                connect_timeout: Duration::from_millis(50),
                rpc_timeout: Duration::from_millis(250),
                max_message_size: 8 * 1024 * 1024,
            },
        );
        let log_index = follower_client
            .propose(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"retry-forwarded",
            ))
            .await
            .unwrap()
            .unwrap();
        raft2
            .wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(log_index), "retry-forwarded SQLite write applied")
            .await
            .unwrap();

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn follower_sqlite_write_during_leader_outage_returns_clean_error() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config, network).await.unwrap();

        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();

        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "two-node leader")
                .await
                .unwrap();
        }

        let follower_client = OrionSqliteRaftClient::new(Some(raft2.clone()));
        follower_client
            .propose(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"before-outage",
            ))
            .await
            .unwrap();

        raft1.shutdown().await.unwrap();
        let error = follower_client
            .propose(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"during-outage",
            ))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("failed to forward write")
                || error.contains("not the Raft leader")
                || error.contains("could not reach the Raft leader")
                || error.contains("raft stopped"),
            "{error}"
        );

        raft2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_metrics_snapshot_reports_leader_and_replication_state() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config, network).await.unwrap();
        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();
        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "two-node leader")
                .await
                .unwrap();
        }

        let response = raft1
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"metrics",
            ))
            .await
            .unwrap();
        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .applied_index_at_least(Some(response.log_id.index), "metrics write applied")
                .await
                .unwrap();
        }

        let leader_metrics = RaftMetricsSnapshot::observe(&raft1);
        assert_eq!(leader_metrics.node_id, 1);
        assert_eq!(leader_metrics.current_leader, Some(1));
        assert!(leader_metrics.is_leader());
        assert!(leader_metrics.is_voter());
        assert!(leader_metrics.is_ready_for_linearizable_reads());
        assert_eq!(leader_metrics.applied_index, Some(response.log_id.index));
        assert_eq!(leader_metrics.voter_ids, vec![1, 2]);
        assert!(
            leader_metrics
                .replication
                .iter()
                .any(|peer| peer.node_id == 2
                    && peer.matched_index == Some(response.log_id.index)
                    && peer.replication_lag == Some(0))
        );

        let follower_metrics = RaftMetricsSnapshot::observe(&raft2);
        assert!(!follower_metrics.is_leader());
        assert!(follower_metrics.has_known_leader());
        assert!(follower_metrics.is_voter());
        assert!(follower_metrics.is_ready_for_linearizable_reads());

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raft_transport_piggybacks_metrics_without_raft_log_entries() {
        let registry = ClusterRaftMetricsRegistry::default();
        let network = TonicRaftNetwork::with_config_and_metrics(
            1,
            TonicRaftTransportConfig::default(),
            registry.clone(),
        );
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config, network).await.unwrap();
        let addr1 = start_server_with_metrics(raft1.clone(), registry.clone())
            .await
            .unwrap();
        let addr2 = start_server_with_metrics(raft2.clone(), registry.clone())
            .await
            .unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: addr2.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();
        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "two-node leader for metrics piggyback")
                .await
                .unwrap();
        }

        registry.record_observed(&raft1);
        let before = RaftMetricsSnapshot::observe(&raft1).last_log_index;
        let mut barrier = raft1.ensure_linearizable(ReadPolicy::ReadIndex).await;
        for _ in 0..20 {
            if barrier.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            barrier = raft1.ensure_linearizable(ReadPolicy::ReadIndex).await;
        }
        assert!(barrier.is_ok(), "read index barrier failed: {barrier:?}");

        tokio::time::sleep(Duration::from_millis(100)).await;
        let leader = registry.get_metrics(1).unwrap();
        let follower = registry.get_metrics(2).unwrap();
        assert_eq!(leader.node_id, 1);
        assert_eq!(follower.node_id, 2);

        let after = RaftMetricsSnapshot::observe(&raft1).last_log_index;
        assert_eq!(after, before);

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn tonic_network_reports_missing_endpoint_without_panicking() {
        let mut network = TonicRaftNetwork::new();
        let client = network
            .new_client(
                42,
                &BasicNode {
                    addr: String::new(),
                },
            )
            .await;
        assert!(client.endpoint.is_err());
    }

    #[tokio::test]
    async fn tonic_service_rejects_missing_or_unknown_raft_group_id() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let (raft, _dir) = build_node(1, config, network).await.unwrap();
        let service = TonicRaftService::new(raft.clone());

        let empty = RaftTransport::client_barrier(
            &service,
            Request::new(RaftMessage {
                payload: Vec::new(),
                metrics_payload: Vec::new(),
                group_id: String::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(empty.code(), tonic::Code::InvalidArgument);

        let padded = RaftTransport::client_barrier(
            &service,
            Request::new(RaftMessage {
                payload: Vec::new(),
                metrics_payload: Vec::new(),
                group_id: format!(" {DEFAULT_RAFT_GROUP_ID} "),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(padded.code(), tonic::Code::InvalidArgument);

        let unknown = RaftTransport::client_barrier(
            &service,
            Request::new(RaftMessage {
                payload: Vec::new(),
                metrics_payload: Vec::new(),
                group_id: "rg_missing".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(unknown.code(), tonic::Code::NotFound);

        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn tonic_service_routes_client_barrier_by_raft_group_id() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let (raft, _dir) = build_node(1, config, network).await.unwrap();
        let service = TonicRaftService::new(raft.clone());

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: String::new(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node grouped barrier leader")
            .await
            .unwrap();

        let response = RaftTransport::client_barrier(
            &service,
            Request::new(RaftMessage {
                payload: Vec::new(),
                metrics_payload: Vec::new(),
                group_id: DEFAULT_RAFT_GROUP_ID.to_string(),
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.group_id, DEFAULT_RAFT_GROUP_ID);
        let result: OrionClientBarrierResult = decode_status(&response.payload).unwrap();
        assert!(result.is_ok());

        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn membership_change_promotes_learner_over_tonic() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let (raft1, _dir1) = build_node(1, config.clone(), network.clone())
            .await
            .unwrap();
        let (raft2, _dir2) = build_node(2, config, network).await.unwrap();
        let addr1 = start_server(raft1.clone()).await.unwrap();
        let addr2 = start_server(raft2.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr1.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();
        raft1
            .wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single voter leader")
            .await
            .unwrap();

        raft1
            .add_learner(
                2,
                BasicNode {
                    addr: addr2.to_string(),
                },
                true,
            )
            .await
            .unwrap();
        raft1.change_membership([1, 2], false).await.unwrap();

        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .metrics(
                    |metrics| {
                        metrics
                            .membership_config
                            .membership()
                            .voter_ids()
                            .eq([1, 2])
                    },
                    "two voters after membership change",
                )
                .await
                .unwrap();
        }

        let response = raft1
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"member",
            ))
            .await
            .unwrap();
        raft2
            .wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(response.log_id.index), "learner promoted and applied")
            .await
            .unwrap();
        raft1.trigger().transfer_leader(2).await.unwrap();
        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(2, "leader transferred to promoted voter")
                .await
                .unwrap();
        }

        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_node_raft_builds_snapshot_and_purges_logs() {
        let network = TonicRaftNetwork::new();
        let config = test_config().unwrap();
        let (raft, _dir) = build_node(1, config, network).await.unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let response = raft
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"snapshotted",
            ))
            .await
            .unwrap();

        raft.trigger().snapshot().await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .snapshot(response.log_id.clone(), "snapshot built")
            .await
            .unwrap();
        let snapshot = raft.get_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(response.log_id.clone()));

        raft.trigger()
            .purge_log(response.log_id.index)
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .purged(Some(response.log_id.clone()), "purged through snapshot")
            .await
            .unwrap();

        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_snapshot_stream_materializes_checkpoint_objects_across_stores() {
        let source_network = TonicRaftNetwork::new();
        let target_network = TonicRaftNetwork::new();
        let config = test_config().unwrap();

        let source_log_dir = TempDir::new().unwrap();
        let target_log_dir = TempDir::new().unwrap();
        let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_state = SlateDbStateStore::open("snapshot-xstore-source", source_store)
            .await
            .unwrap();
        let target_state = SlateDbStateStore::open("snapshot-xstore-target", target_store)
            .await
            .unwrap();
        let source_runtime_state = source_state.clone();

        let source_raft = build_node_with_stores(
            1,
            config.clone(),
            source_network,
            OrionRaftLogStore::open(source_log_dir.path()).unwrap(),
            source_state,
        )
        .await
        .unwrap();
        let target_raft = build_node_with_stores(
            2,
            config,
            target_network,
            OrionRaftLogStore::open(target_log_dir.path()).unwrap(),
            target_state,
        )
        .await
        .unwrap();
        let target_addr = start_server(target_raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        );
        source_raft.initialize(members).await.unwrap();
        source_raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(1, "source leader")
            .await
            .unwrap();
        let source_cache = TempDir::new().unwrap();
        let source_runtime = OrionSqliteRuntime::new(
            source_raft.clone(),
            source_runtime_state.clone(),
            OrionSqliteRuntimeConfig::new(source_cache.path().to_path_buf()),
        );
        let source_db = source_runtime.open_database("tenant-x").unwrap();
        let source_conn = source_db.connect().unwrap();
        source_conn
            .execute_batch(
                r#"
                create table snapshot_items(id integer primary key, value text not null);
                insert into snapshot_items values (1, 'xstore');
                "#,
            )
            .unwrap();
        drop(source_conn);
        drop(source_db);
        drop(source_runtime);

        source_raft.trigger().snapshot().await.unwrap();
        let mut snapshot = None;
        for _ in 0..20 {
            snapshot = source_raft.get_snapshot().await.unwrap();
            if snapshot.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let snapshot = snapshot.expect("snapshot should be published");
        let vote = OrionVote::new_committed(1, 1);
        let client_metrics = ClusterRaftMetricsRegistry::default();
        let mut client = TonicRaftClient {
            target: 2,
            endpoint: Ok(format!("http://{target_addr}")),
            config: TonicRaftTransportConfig::default(),
            client: None,
            local_node_id: Some(1),
            metrics_registry: client_metrics.clone(),
            group_id: DEFAULT_RAFT_GROUP_ID.to_string(),
            local_state_store: Some(source_runtime_state),
        };
        client
            .full_snapshot(
                vote,
                snapshot,
                std::future::pending::<ReplicationClosed>(),
                RPCOption::new(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let transfer_metrics = client_metrics.snapshot_transfer(1);
        assert_eq!(transfer_metrics.snapshots_sent, 1);
        assert!(transfer_metrics.manifest_bytes_sent > 0);
        assert!(transfer_metrics.checkpoint_objects_sent > 0);
        assert!(transfer_metrics.checkpoint_object_bytes_sent > 0);

        let installed_target_state = target_raft
            .with_state_machine(|sm| Box::pin(async move { sm.state_store() }))
            .await
            .unwrap();
        let target_cache = TempDir::new().unwrap();
        let target_runtime = OrionSqliteRuntime::new(
            target_raft.clone(),
            installed_target_state,
            OrionSqliteRuntimeConfig::new(target_cache.path().to_path_buf()),
        );
        let target_db = target_runtime.open_database("tenant-x").unwrap();
        let target_conn = target_db.connect().unwrap();
        let value: String = target_conn
            .query_row("select value from snapshot_items where id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, "xstore");

        source_raft.shutdown().await.unwrap();
        target_raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn framed_snapshot_truncated_checkpoint_object_aborts_upload() {
        let (target_raft, target_addr, header, payload, object_path, target_store) =
            build_snapshot_stream_test_fixture().await.unwrap();

        let result = send_raw_snapshot_frames(
            target_addr,
            header,
            vec![
                WireSnapshotFrame::ManifestChunk(payload),
                WireSnapshotFrame::CheckpointObjectHeader {
                    path: object_path.clone(),
                    size: 4,
                },
                WireSnapshotFrame::CheckpointObjectChunk(b"ab".to_vec()),
            ],
        )
        .await;

        let status = result.expect_err("truncated stream should be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("snapshot stream ended before checkpoint object completed"),
            "unexpected status: {status}"
        );
        assert_object_not_committed(target_store, &object_path).await;

        target_raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn framed_snapshot_oversized_checkpoint_object_chunk_aborts_upload() {
        let (target_raft, target_addr, header, payload, object_path, target_store) =
            build_snapshot_stream_test_fixture().await.unwrap();

        let result = send_raw_snapshot_frames(
            target_addr,
            header,
            vec![
                WireSnapshotFrame::ManifestChunk(payload),
                WireSnapshotFrame::CheckpointObjectHeader {
                    path: object_path.clone(),
                    size: 3,
                },
                WireSnapshotFrame::CheckpointObjectChunk(b"toolong".to_vec()),
                WireSnapshotFrame::End,
            ],
        )
        .await;

        let status = result.expect_err("oversized object chunk should be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("exceeded expected size"),
            "unexpected status: {status}"
        );
        assert_object_not_committed(target_store, &object_path).await;

        target_raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn framed_snapshot_checkpoint_object_path_outside_artifact_is_rejected() {
        let (target_raft, target_addr, header, payload, object_path, target_store) =
            build_snapshot_stream_test_fixture().await.unwrap();
        let outside_path = "outside-snapshot-artifact/malformed-object";

        let result = send_raw_snapshot_frames(
            target_addr,
            header,
            vec![
                WireSnapshotFrame::ManifestChunk(payload),
                WireSnapshotFrame::CheckpointObjectHeader {
                    path: outside_path.to_string(),
                    size: 4,
                },
                WireSnapshotFrame::CheckpointObjectChunk(b"data".to_vec()),
                WireSnapshotFrame::End,
            ],
        )
        .await;

        let status = result.expect_err("outside artifact path should be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("does not match snapshot artifacts"),
            "unexpected status: {status}"
        );
        assert_object_not_committed(Arc::clone(&target_store), outside_path).await;
        assert_object_not_committed(target_store, &object_path).await;

        target_raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn configured_snapshot_policy_builds_snapshot_without_manual_trigger() {
        let network = TonicRaftNetwork::new();
        let config = test_config_with_snapshot_policy(SnapshotPolicy::LogsSinceLast(1)).unwrap();
        let (raft, _dir) = build_node(1, config, network).await.unwrap();
        let addr = start_server(raft.clone()).await.unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(1, "single-node leader")
            .await
            .unwrap();

        let response = raft
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"auto-snap",
            ))
            .await
            .unwrap();
        let second_response = raft
            .client_write(sqlite_write_request(
                "tenant-a",
                "tenant-a.db-wal",
                b"auto-snap-2",
            ))
            .await
            .unwrap();

        raft.wait(Some(Duration::from_secs(3)))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .as_ref()
                        .is_some_and(|log_id| log_id.index >= 1)
                },
                "policy-built snapshot",
            )
            .await
            .unwrap();
        assert!(second_response.log_id.index > response.log_id.index);
        raft.shutdown().await.unwrap();
    }
}
