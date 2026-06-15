use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail, ensure};
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Config, Raft, ReadPolicy, SnapshotPolicy};
use serde::Deserialize;
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use tokio::net::TcpListener;

use crate::libsql_http::{
    LibsqlHttpAuthConfig, LibsqlHttpConfig, LibsqlHttpIdempotencyConfig, ORION_CATALOG_DATABASE,
    PlacementNodeConfig, ReplicationGroupRegistry, RuntimeGroupCatalogSnapshot,
    ensure_valid_runtime_group_id, list_runtime_group_catalog_snapshots,
    node_software_capabilities, serve_libsql_http_with_shutdown,
};
use crate::storage_node::StorageNodePlan;

use orion_raft::openraft_store::{OrionRaftLogStore, OrionRaftStateMachine};
use orion_raft::raft_metrics::RaftMetricsSnapshot;
use orion_raft::tonic_transport::{
    OrionRaft, TonicRaftGroupRegistry, TonicRaftNetwork, TonicRaftTransportConfig,
    client_barrier_to_raft_endpoint,
    serve_raft_transport_group_registry_with_config_metrics_and_shutdown,
};
use orion_raft::{
    ClusterRaftMetricsRegistry, DEFAULT_LARGE_BATCH_CHUNK_BYTES,
    DEFAULT_LARGE_BATCH_THRESHOLD_BYTES, LargePayloadConfig, OrionSqliteRuntime,
    OrionSqliteRuntimeConfig, SlateDbStateStore, SqlitePageCompactionMetrics,
    SqlitePageCompactionPolicy, compact_sqlite_page_versions_excluding,
};
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub node: NodeIdentityConfig,
    #[serde(default = "default_roles")]
    pub roles: BTreeSet<ServiceRole>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub bootstrap: BootstrapConfig,
    #[serde(default)]
    pub raft: RaftRuntimeConfig,
    #[serde(default)]
    pub transport: TransportRuntimeConfig,
    #[serde(default)]
    pub metrics: MetricsRuntimeConfig,
    #[serde(default)]
    pub compaction: CompactionRuntimeConfig,
    #[serde(default)]
    pub readiness: ReadinessRuntimeConfig,
    #[serde(default = "default_libsql_http")]
    pub libsql_http: Option<LibsqlHttpRuntimeConfig>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node: NodeIdentityConfig::default(),
            roles: default_roles(),
            peers: Vec::new(),
            storage: StorageConfig::default(),
            runtime: RuntimeConfig::default(),
            bootstrap: BootstrapConfig::default(),
            raft: RaftRuntimeConfig::default(),
            transport: TransportRuntimeConfig::default(),
            metrics: MetricsRuntimeConfig::default(),
            compaction: CompactionRuntimeConfig::default(),
            readiness: ReadinessRuntimeConfig::default(),
            libsql_http: default_libsql_http(),
        }
    }
}

pub struct DefaultConfig;

impl DefaultConfig {
    pub fn one_node() -> NodeConfig {
        NodeConfig::default()
    }
}

impl NodeConfig {
    pub fn from_yaml_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes =
            std::fs::read(path).with_context(|| format!("reading config {}", path.display()))?;
        let config: Self = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parsing config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("validating config {}", path.display()))?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.node.validate()?;
        validate_roles(&self.roles)?;
        ensure!(
            self.runs_storage(),
            "only storage/all role is implemented in this build; configured roles: {}",
            role_list(&self.roles)
        );

        let mut peer_ids = BTreeSet::new();
        for peer in &self.peers {
            ensure!(
                peer.id != self.node.id,
                "peers must not include this node's own id"
            );
            ensure!(peer_ids.insert(peer.id), "duplicate peer id {}", peer.id);
            ensure!(
                !peer.raft_addr.is_empty(),
                "peer {} raft_addr must not be empty",
                peer.id
            );
            if let Some(addr) = &peer.libsql_http_addr {
                ensure!(
                    !addr.is_empty(),
                    "peer {} libsql_http_addr must not be empty when set",
                    peer.id
                );
            }
            peer.topology
                .validate(&format!("peer {} topology", peer.id))?;
        }

        self.storage.validate()?;
        self.runtime.validate()?;
        self.bootstrap.validate(self)?;
        self.raft.validate()?;
        self.transport.validate()?;
        self.compaction.validate()?;
        self.readiness.validate()?;
        if let Some(libsql_http) = &self.libsql_http {
            libsql_http.validate()?;
        }
        Ok(())
    }

    pub fn commented_example_yaml() -> &'static str {
        COMMENTED_EXAMPLE_CONFIG
    }

    pub fn raft_log_root(&self) -> String {
        self.storage.local.raft_log_root()
    }

    pub fn raft_log_root_for_group(&self, group_id: &str) -> anyhow::Result<String> {
        ensure_valid_runtime_group_id(group_id)?;
        if group_id == self.bootstrap.default_group.group_id {
            Ok(self.raft_log_root())
        } else {
            Ok(child_path(
                &self.storage.local.group_data_root(),
                &format!("{group_id}/raft"),
            ))
        }
    }

    pub fn node_state_prefix(&self) -> String {
        self.storage.object_prefixes.node_state.clone()
    }

    pub fn node_state_prefix_for_group(&self, group_id: &str) -> anyhow::Result<String> {
        ensure_valid_runtime_group_id(group_id)?;
        if group_id == self.bootstrap.default_group.group_id {
            Ok(self.node_state_prefix())
        } else {
            Ok(format!(
                "{}/replication-groups/{group_id}/state",
                self.node_state_prefix().trim_end_matches('/')
            ))
        }
    }

    pub fn sqlite_cache_root_for_group(&self, group_id: &str) -> anyhow::Result<String> {
        ensure_valid_runtime_group_id(group_id)?;
        if group_id == self.bootstrap.default_group.group_id {
            Ok(self.storage.local.sqlite_cache_root())
        } else {
            Ok(child_path(
                &self.storage.local.sqlite_cache_root(),
                &format!("groups/{group_id}"),
            ))
        }
    }

    pub fn advertised_raft_addr(&self) -> String {
        self.node
            .advertised_raft_addr
            .clone()
            .unwrap_or_else(|| self.node.raft_addr.clone())
    }

    pub fn human_summary(&self, source: &str) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Orion startup configuration");
        let _ = writeln!(out, "  source: {source}");
        let _ = writeln!(out, "  node.id: {}", self.node.id);
        let _ = writeln!(out, "  roles: {}", role_list(&self.roles));
        let _ = writeln!(out, "  node.topology: {}", self.node.topology.label());
        let _ = writeln!(
            out,
            "  bootstrap.create_default_group: {}",
            yes_no(self.bootstrap.create_default_group)
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "Cluster");
        let _ = writeln!(out, "  raft.cluster_name: {}", self.raft.cluster_name);
        let _ = writeln!(
            out,
            "  bootstrap.default_group.group_id: {}",
            self.bootstrap.default_group.group_id
        );
        let _ = writeln!(out, "  voters configured: {}", self.peers.len() + 1);
        if self.peers.is_empty() {
            let _ = writeln!(out, "  peers: none (single-node cluster)");
        } else {
            for peer in &self.peers {
                let _ = writeln!(
                    out,
                    "  peer {}: {} [{}]",
                    peer.id,
                    peer.raft_addr,
                    peer.topology.label()
                );
                if let Some(addr) = &peer.libsql_http_addr {
                    let _ = writeln!(out, "    libsql_http_addr: {addr}");
                }
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "Storage");
        let _ = writeln!(
            out,
            "  storage.local.data_root: {}",
            display_local_path(&self.storage.local.data_root)
        );
        let _ = writeln!(
            out,
            "  storage.local.raft_log_root: {}",
            display_local_path(&self.raft_log_root())
        );
        let _ = writeln!(
            out,
            "  storage.local.group_data_root: {}",
            display_local_path(&self.storage.local.group_data_root())
        );
        let _ = writeln!(
            out,
            "  storage.local.sqlite_cache_root: {}",
            display_local_path(&self.storage.local.sqlite_cache_root())
        );
        let _ = writeln!(
            out,
            "  storage.local.nvme_cache_root: {}",
            display_local_path(&self.storage.local.nvme_cache_root())
        );
        let _ = writeln!(
            out,
            "  storage.object_prefixes.node_state: {}",
            self.node_state_prefix()
        );
        match &self.storage.object_store {
            ObjectStoreConfig::Local { root } => {
                let _ = writeln!(out, "  storage.object_store: local");
                let _ = writeln!(
                    out,
                    "  storage.object_store.root: {}",
                    display_local_path(root)
                );
            }
        }
        let _ = writeln!(
            out,
            "  storage.limits.max_group_data_bytes: {}",
            self.storage.limits.max_group_data_bytes
        );
        let _ = writeln!(
            out,
            "  storage.limits.max_sqlite_cache_bytes: {}",
            self.storage.limits.max_sqlite_cache_bytes
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "Network");
        let _ = writeln!(out, "  node.raft_addr: {}", self.node.raft_addr);
        let _ = writeln!(
            out,
            "  node.advertised_raft_addr: {}",
            self.node
                .advertised_raft_addr
                .as_deref()
                .unwrap_or("(same as node.raft_addr)")
        );
        if let Some(libsql_http) = &self.libsql_http {
            let _ = writeln!(out, "  libsql_http.bind_addr: {}", libsql_http.bind_addr);
            let _ = writeln!(
                out,
                "  libsql_http.session_idle_timeout_ms: {}",
                libsql_http.session_idle_timeout_ms
            );
            let _ = writeln!(
                out,
                "  libsql_http.blob_max_chunk_bytes: {}",
                libsql_http.blob_max_chunk_bytes
            );
            let _ = writeln!(
                out,
                "  libsql_http.idempotency.enabled: {}",
                yes_no(libsql_http.idempotency.enabled)
            );
            let _ = writeln!(
                out,
                "  libsql_http.idempotency.committed_ttl_ms: {}",
                libsql_http.idempotency.committed_ttl_ms
            );
            let _ = writeln!(
                out,
                "  libsql_http.idempotency.pending_ttl_ms: {}",
                libsql_http.idempotency.pending_ttl_ms
            );
            let _ = writeln!(
                out,
                "  libsql_http.auth: {}",
                if libsql_http.auth.tokens.is_empty() {
                    "disabled (no bearer tokens configured)".to_string()
                } else {
                    format!("enabled ({} token rule(s))", libsql_http.auth.tokens.len())
                }
            );
        } else {
            let _ = writeln!(out, "  libsql_http: disabled");
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "Raft Runtime");
        let _ = writeln!(
            out,
            "  raft.heartbeat_interval_ms: {}",
            self.raft.heartbeat_interval_ms
        );
        let _ = writeln!(
            out,
            "  raft.election_timeout_min_ms: {}",
            self.raft.election_timeout_min_ms
        );
        let _ = writeln!(
            out,
            "  raft.election_timeout_max_ms: {}",
            self.raft.election_timeout_max_ms
        );
        let _ = writeln!(
            out,
            "  raft.replication_lag_threshold: {}",
            self.raft.replication_lag_threshold
        );
        let _ = writeln!(
            out,
            "  raft.install_snapshot_timeout_ms: {}",
            self.raft.install_snapshot_timeout_ms
        );
        let _ = writeln!(
            out,
            "  raft.max_payload_entries: {}",
            self.raft.max_payload_entries
        );
        let _ = writeln!(
            out,
            "  raft.max_append_entries: {}",
            option_u64(self.raft.max_append_entries)
        );
        let _ = writeln!(
            out,
            "  raft.snapshot_max_chunk_size: {}",
            self.raft.snapshot_max_chunk_size
        );
        let _ = writeln!(
            out,
            "  raft.snapshot_policy: {}",
            self.raft.snapshot_policy.summary()
        );
        let _ = writeln!(
            out,
            "  raft.max_in_snapshot_log_to_keep: {}",
            self.raft.max_in_snapshot_log_to_keep
        );
        let _ = writeln!(
            out,
            "  raft.purge_batch_size: {}",
            self.raft.purge_batch_size
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_threshold_bytes: {}",
            self.raft.large_payload_threshold_bytes
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_chunk_bytes: {}",
            self.raft.large_payload_chunk_bytes
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_max_staged_uploads: {}",
            self.raft.large_payload_max_staged_uploads
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_max_staged_bytes: {}",
            self.raft.large_payload_max_staged_bytes
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_staging_ttl_ms: {}",
            self.raft.large_payload_staging_ttl_ms
        );
        let _ = writeln!(
            out,
            "  raft.large_payload_cleanup_batch_size: {}",
            self.raft.large_payload_cleanup_batch_size
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "Transport");
        let _ = writeln!(
            out,
            "  transport.connect_timeout_ms: {}",
            self.transport.connect_timeout_ms
        );
        let _ = writeln!(
            out,
            "  transport.rpc_timeout_ms: {}",
            self.transport.rpc_timeout_ms
        );
        let _ = writeln!(
            out,
            "  transport.max_message_size: {}",
            self.transport.max_message_size
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "Readiness And Metrics");
        let _ = writeln!(out, "  readiness.timeout_ms: {}", self.readiness.timeout_ms);
        let _ = writeln!(
            out,
            "  readiness.probe_interval_ms: {}",
            self.readiness.probe_interval_ms
        );
        let _ = writeln!(
            out,
            "  readiness.probe_rpc_timeout_ms: {}",
            self.readiness.probe_rpc_timeout_ms
        );
        let _ = writeln!(
            out,
            "  metrics.log_interval_ms: {}",
            self.metrics.log_interval_ms
        );
        let _ = writeln!(out);

        let _ = writeln!(out, "Compaction");
        let _ = writeln!(
            out,
            "  compaction.enabled: {}",
            yes_no(self.compaction.enabled)
        );
        let _ = writeln!(
            out,
            "  compaction.interval_ms: {}",
            self.compaction.interval_ms
        );
        let _ = writeln!(
            out,
            "  compaction.obsolete_versions_per_file: {}",
            self.compaction.obsolete_versions_per_file
        );
        let _ = writeln!(
            out,
            "  compaction.obsolete_version_ratio: {:.2}",
            self.compaction.obsolete_version_ratio
        );
        let _ = writeln!(
            out,
            "  compaction.obsolete_bytes_per_file: {}",
            self.compaction.obsolete_bytes_per_file
        );
        let _ = writeln!(
            out,
            "  compaction.max_versions_per_pass: {}",
            self.compaction.max_versions_per_pass
        );
        let _ = writeln!(
            out,
            "  compaction.max_bytes_per_pass: {}",
            self.compaction.max_bytes_per_pass
        );

        out
    }

    pub fn runs_storage(&self) -> bool {
        self.roles.contains(&ServiceRole::All) || self.roles.contains(&ServiceRole::Storage)
    }

    pub fn runs_builtin_libsql_frontend(&self) -> bool {
        self.roles.contains(&ServiceRole::All) || self.roles.contains(&ServiceRole::Compute)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeIdentityConfig {
    #[serde(default = "default_node_id")]
    pub id: u64,
    #[serde(default = "default_raft_addr")]
    pub raft_addr: String,
    #[serde(default)]
    pub advertised_raft_addr: Option<String>,
    #[serde(default)]
    pub topology: TopologyConfig,
}

impl Default for NodeIdentityConfig {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            raft_addr: default_raft_addr(),
            advertised_raft_addr: None,
            topology: TopologyConfig::default(),
        }
    }
}

impl NodeIdentityConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.id > 0, "node.id must be greater than zero");
        ensure!(
            !self.raft_addr.is_empty(),
            "node.raft_addr must not be empty"
        );
        self.raft_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("parsing node.raft_addr {}", self.raft_addr))?;
        if let Some(advertised_raft_addr) = &self.advertised_raft_addr {
            ensure!(
                !advertised_raft_addr.is_empty(),
                "node.advertised_raft_addr must not be empty when set"
            );
        }
        self.topology.validate("node.topology")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRole {
    All,
    Router,
    Compute,
    Storage,
    Controller,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    Local { root: String },
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        default_object_store()
    }
}

impl ObjectStoreConfig {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Local { root } => {
                ensure!(!root.is_empty(), "object_store.root must not be empty")
            }
        }
        Ok(())
    }

    pub fn label(&self) -> String {
        match self {
            Self::Local { root } => format!("local:{root}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
    #[serde(default)]
    pub local: StorageLocalConfig,
    #[serde(default)]
    pub object_prefixes: StorageObjectPrefixesConfig,
    #[serde(default)]
    pub limits: StorageLimitsConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            object_store: ObjectStoreConfig::default(),
            local: StorageLocalConfig::default(),
            object_prefixes: StorageObjectPrefixesConfig::default(),
            limits: StorageLimitsConfig::default(),
        }
    }
}

impl StorageConfig {
    fn validate(&self) -> anyhow::Result<()> {
        self.object_store.validate()?;
        self.local.validate()?;
        self.object_prefixes.validate()?;
        self.limits.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageLocalConfig {
    #[serde(default = "default_storage_data_root")]
    pub data_root: String,
    #[serde(default)]
    pub raft_log_root: Option<String>,
    #[serde(default)]
    pub group_data_root: Option<String>,
    #[serde(default)]
    pub sqlite_cache_root: Option<String>,
    #[serde(default)]
    pub nvme_cache_root: Option<String>,
}

impl Default for StorageLocalConfig {
    fn default() -> Self {
        Self {
            data_root: default_storage_data_root(),
            raft_log_root: None,
            group_data_root: None,
            sqlite_cache_root: None,
            nvme_cache_root: None,
        }
    }
}

impl StorageLocalConfig {
    pub fn raft_log_root(&self) -> String {
        self.raft_log_root
            .clone()
            .unwrap_or_else(|| child_path(&self.data_root, "raft"))
    }

    pub fn group_data_root(&self) -> String {
        self.group_data_root
            .clone()
            .unwrap_or_else(|| child_path(&self.data_root, "groups"))
    }

    pub fn sqlite_cache_root(&self) -> String {
        self.sqlite_cache_root
            .clone()
            .unwrap_or_else(|| child_path(&self.data_root, "sqlite-cache"))
    }

    pub fn nvme_cache_root(&self) -> String {
        self.nvme_cache_root
            .clone()
            .unwrap_or_else(|| child_path(&self.data_root, "nvme-cache"))
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.data_root.is_empty(),
            "storage.local.data_root must not be empty"
        );
        ensure!(
            !self.raft_log_root().is_empty(),
            "storage.local.raft_log_root must not be empty"
        );
        ensure!(
            !self.group_data_root().is_empty(),
            "storage.local.group_data_root must not be empty"
        );
        ensure!(
            !self.sqlite_cache_root().is_empty(),
            "storage.local.sqlite_cache_root must not be empty"
        );
        ensure!(
            !self.nvme_cache_root().is_empty(),
            "storage.local.nvme_cache_root must not be empty"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageObjectPrefixesConfig {
    #[serde(default = "default_node_state_prefix")]
    pub node_state: String,
}

impl Default for StorageObjectPrefixesConfig {
    fn default() -> Self {
        Self {
            node_state: default_node_state_prefix(),
        }
    }
}

impl StorageObjectPrefixesConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.node_state.is_empty(),
            "storage.object_prefixes.node_state must not be empty"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageLimitsConfig {
    #[serde(default = "default_max_object_upload_bytes")]
    pub max_object_upload_bytes: u64,
    #[serde(default = "default_max_group_data_bytes")]
    pub max_group_data_bytes: u64,
    #[serde(default = "default_max_sqlite_cache_bytes")]
    pub max_sqlite_cache_bytes: u64,
    #[serde(default = "default_max_nvme_cache_bytes")]
    pub max_nvme_cache_bytes: u64,
}

impl Default for StorageLimitsConfig {
    fn default() -> Self {
        Self {
            max_object_upload_bytes: default_max_object_upload_bytes(),
            max_group_data_bytes: default_max_group_data_bytes(),
            max_sqlite_cache_bytes: default_max_sqlite_cache_bytes(),
            max_nvme_cache_bytes: default_max_nvme_cache_bytes(),
        }
    }
}

impl StorageLimitsConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.max_object_upload_bytes > 0,
            "storage.limits.max_object_upload_bytes must be greater than zero"
        );
        ensure!(
            self.max_group_data_bytes > 0,
            "storage.limits.max_group_data_bytes must be greater than zero"
        );
        ensure!(
            self.max_sqlite_cache_bytes > 0,
            "storage.limits.max_sqlite_cache_bytes must be greater than zero"
        );
        ensure!(
            self.max_nvme_cache_bytes > 0,
            "storage.limits.max_nvme_cache_bytes must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_max_hosted_groups")]
    pub max_hosted_groups: usize,
    #[serde(default = "default_max_open_databases")]
    pub max_open_databases: usize,
    #[serde(default = "default_group_start_concurrency")]
    pub group_start_concurrency: usize,
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_hosted_groups: default_max_hosted_groups(),
            max_open_databases: default_max_open_databases(),
            group_start_concurrency: default_group_start_concurrency(),
            shutdown_grace_ms: default_shutdown_grace_ms(),
        }
    }
}

impl RuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.max_hosted_groups > 0,
            "runtime.max_hosted_groups must be greater than zero"
        );
        ensure!(
            self.max_open_databases > 0,
            "runtime.max_open_databases must be greater than zero"
        );
        ensure!(
            self.group_start_concurrency > 0,
            "runtime.group_start_concurrency must be greater than zero"
        );
        ensure!(
            self.shutdown_grace_ms > 0,
            "runtime.shutdown_grace_ms must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default = "default_bootstrap_create_default_group")]
    pub create_default_group: bool,
    #[serde(default)]
    pub default_group: BootstrapDefaultGroupConfig,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            create_default_group: default_bootstrap_create_default_group(),
            default_group: BootstrapDefaultGroupConfig::default(),
        }
    }
}

impl BootstrapConfig {
    fn validate(&self, config: &NodeConfig) -> anyhow::Result<()> {
        self.default_group.validate(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapDefaultGroupConfig {
    #[serde(default = "default_replication_group_id_config")]
    pub group_id: String,
    #[serde(default)]
    pub voters: Vec<u64>,
}

impl Default for BootstrapDefaultGroupConfig {
    fn default() -> Self {
        Self {
            group_id: default_replication_group_id_config(),
            voters: Vec::new(),
        }
    }
}

impl BootstrapDefaultGroupConfig {
    fn validate(&self, config: &NodeConfig) -> anyhow::Result<()> {
        ensure!(
            !self.group_id.is_empty(),
            "bootstrap.default_group.group_id must not be empty"
        );
        let known_node_ids = std::iter::once(config.node.id)
            .chain(config.peers.iter().map(|peer| peer.id))
            .collect::<BTreeSet<_>>();
        let mut seen_voters = BTreeSet::new();
        for voter in &self.voters {
            ensure!(
                known_node_ids.contains(voter),
                "bootstrap.default_group.voters contains unknown node id {}",
                voter
            );
            ensure!(
                seen_voters.insert(*voter),
                "bootstrap.default_group.voters contains duplicate node id {}",
                voter
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub raft_addr: String,
    #[serde(default)]
    pub libsql_http_addr: Option<String>,
    #[serde(default)]
    pub topology: TopologyConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TopologyConfig {
    #[serde(default = "default_cloud")]
    pub cloud: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_zone")]
    pub zone: String,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            cloud: default_cloud(),
            region: default_region(),
            zone: default_zone(),
        }
    }
}

impl TopologyConfig {
    fn validate(&self, path: &str) -> anyhow::Result<()> {
        ensure!(!self.cloud.is_empty(), "{path}.cloud must not be empty");
        ensure!(!self.region.is_empty(), "{path}.region must not be empty");
        ensure!(!self.zone.is_empty(), "{path}.zone must not be empty");
        Ok(())
    }

    pub(crate) fn label(&self) -> String {
        format!("{}/{}/{}", self.cloud, self.region, self.zone)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RaftRuntimeConfig {
    #[serde(default = "default_cluster_name")]
    pub cluster_name: String,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_election_timeout_min_ms")]
    pub election_timeout_min_ms: u64,
    #[serde(default = "default_election_timeout_max_ms")]
    pub election_timeout_max_ms: u64,
    #[serde(default = "default_replication_lag_threshold")]
    pub replication_lag_threshold: u64,
    #[serde(default = "default_install_snapshot_timeout_ms")]
    pub install_snapshot_timeout_ms: u64,
    #[serde(default = "default_max_payload_entries")]
    pub max_payload_entries: u64,
    #[serde(default)]
    pub max_append_entries: Option<u64>,
    #[serde(default = "default_snapshot_max_chunk_size")]
    pub snapshot_max_chunk_size: u64,
    #[serde(default = "default_snapshot_policy")]
    pub snapshot_policy: SnapshotPolicyConfig,
    #[serde(default = "default_max_in_snapshot_log_to_keep")]
    pub max_in_snapshot_log_to_keep: u64,
    #[serde(default = "default_purge_batch_size")]
    pub purge_batch_size: u64,
    #[serde(default = "default_large_payload_threshold_bytes")]
    pub large_payload_threshold_bytes: usize,
    #[serde(default = "default_large_payload_chunk_bytes")]
    pub large_payload_chunk_bytes: usize,
    #[serde(default = "default_large_payload_max_staged_uploads")]
    pub large_payload_max_staged_uploads: usize,
    #[serde(default = "default_large_payload_max_staged_bytes")]
    pub large_payload_max_staged_bytes: u64,
    #[serde(default = "default_large_payload_staging_ttl_ms")]
    pub large_payload_staging_ttl_ms: u64,
    #[serde(default = "default_large_payload_cleanup_batch_size")]
    pub large_payload_cleanup_batch_size: usize,
}

impl Default for RaftRuntimeConfig {
    fn default() -> Self {
        Self {
            cluster_name: default_cluster_name(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            election_timeout_min_ms: default_election_timeout_min_ms(),
            election_timeout_max_ms: default_election_timeout_max_ms(),
            replication_lag_threshold: default_replication_lag_threshold(),
            install_snapshot_timeout_ms: default_install_snapshot_timeout_ms(),
            max_payload_entries: default_max_payload_entries(),
            max_append_entries: None,
            snapshot_max_chunk_size: default_snapshot_max_chunk_size(),
            snapshot_policy: default_snapshot_policy(),
            max_in_snapshot_log_to_keep: default_max_in_snapshot_log_to_keep(),
            purge_batch_size: default_purge_batch_size(),
            large_payload_threshold_bytes: default_large_payload_threshold_bytes(),
            large_payload_chunk_bytes: default_large_payload_chunk_bytes(),
            large_payload_max_staged_uploads: default_large_payload_max_staged_uploads(),
            large_payload_max_staged_bytes: default_large_payload_max_staged_bytes(),
            large_payload_staging_ttl_ms: default_large_payload_staging_ttl_ms(),
            large_payload_cleanup_batch_size: default_large_payload_cleanup_batch_size(),
        }
    }
}

impl RaftRuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.cluster_name.is_empty(),
            "raft.cluster_name must not be empty"
        );
        ensure!(
            self.heartbeat_interval_ms > 0,
            "raft.heartbeat_interval_ms must be greater than zero"
        );
        ensure!(
            self.election_timeout_min_ms > self.heartbeat_interval_ms,
            "raft.election_timeout_min_ms must be greater than heartbeat_interval_ms"
        );
        ensure!(
            self.election_timeout_max_ms > self.election_timeout_min_ms,
            "raft.election_timeout_max_ms must be greater than election_timeout_min_ms"
        );
        ensure!(
            self.replication_lag_threshold > 0,
            "raft.replication_lag_threshold must be greater than zero"
        );
        ensure!(
            self.install_snapshot_timeout_ms > 0,
            "raft.install_snapshot_timeout_ms must be greater than zero"
        );
        ensure!(
            self.max_payload_entries > 0,
            "raft.max_payload_entries must be greater than zero"
        );
        ensure!(
            self.max_append_entries.unwrap_or(1) > 0,
            "raft.max_append_entries must be greater than zero"
        );
        ensure!(
            self.snapshot_max_chunk_size > 0,
            "raft.snapshot_max_chunk_size must be greater than zero"
        );
        ensure!(
            self.large_payload_threshold_bytes > 0,
            "raft.large_payload_threshold_bytes must be greater than zero"
        );
        ensure!(
            self.large_payload_chunk_bytes > 0,
            "raft.large_payload_chunk_bytes must be greater than zero"
        );
        ensure!(
            self.large_payload_max_staged_uploads > 0,
            "raft.large_payload_max_staged_uploads must be greater than zero"
        );
        ensure!(
            self.large_payload_max_staged_bytes > 0,
            "raft.large_payload_max_staged_bytes must be greater than zero"
        );
        ensure!(
            self.large_payload_cleanup_batch_size > 0,
            "raft.large_payload_cleanup_batch_size must be greater than zero"
        );
        self.snapshot_policy.validate()?;
        if let Some(logs_since_last) = self.snapshot_policy.logs_since_last {
            ensure!(
                self.replication_lag_threshold > logs_since_last,
                "raft.replication_lag_threshold must be greater than raft.snapshot_policy.logs_since_last"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPolicyConfig {
    #[serde(default)]
    pub never: bool,
    #[serde(default)]
    pub logs_since_last: Option<u64>,
}

impl SnapshotPolicyConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !(self.never && self.logs_since_last.is_some()),
            "raft.snapshot_policy cannot set both never and logs_since_last"
        );
        ensure!(
            self.never || self.logs_since_last.unwrap_or(5_000) > 0,
            "raft.snapshot_policy.logs_since_last must be greater than zero"
        );
        Ok(())
    }

    fn to_openraft(&self) -> SnapshotPolicy {
        if self.never {
            SnapshotPolicy::Never
        } else {
            SnapshotPolicy::LogsSinceLast(self.logs_since_last.unwrap_or(5_000))
        }
    }

    fn summary(&self) -> String {
        if self.never {
            "never".to_string()
        } else {
            format!("logs_since_last={}", self.logs_since_last.unwrap_or(5_000))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportRuntimeConfig {
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
}

impl Default for TransportRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            rpc_timeout_ms: default_rpc_timeout_ms(),
            max_message_size: default_max_message_size(),
        }
    }
}

impl From<&TransportRuntimeConfig> for TonicRaftTransportConfig {
    fn from(value: &TransportRuntimeConfig) -> Self {
        Self {
            connect_timeout: Duration::from_millis(value.connect_timeout_ms),
            rpc_timeout: Duration::from_millis(value.rpc_timeout_ms),
            max_message_size: value.max_message_size,
        }
    }
}

impl TransportRuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.connect_timeout_ms > 0,
            "transport.connect_timeout_ms must be greater than zero"
        );
        ensure!(
            self.rpc_timeout_ms > 0,
            "transport.rpc_timeout_ms must be greater than zero"
        );
        ensure!(
            self.max_message_size > 0,
            "transport.max_message_size must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsRuntimeConfig {
    #[serde(default = "default_metrics_log_interval_ms")]
    pub log_interval_ms: u64,
}

impl Default for MetricsRuntimeConfig {
    fn default() -> Self {
        Self {
            log_interval_ms: default_metrics_log_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompactionRuntimeConfig {
    #[serde(default = "default_compaction_enabled")]
    pub enabled: bool,
    #[serde(default = "default_compaction_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_compaction_obsolete_versions_per_file")]
    pub obsolete_versions_per_file: usize,
    #[serde(default = "default_compaction_obsolete_version_ratio")]
    pub obsolete_version_ratio: f64,
    #[serde(default = "default_compaction_obsolete_bytes_per_file")]
    pub obsolete_bytes_per_file: u64,
    #[serde(default = "default_compaction_max_versions_per_pass")]
    pub max_versions_per_pass: usize,
    #[serde(default = "default_compaction_max_bytes_per_pass")]
    pub max_bytes_per_pass: u64,
    #[serde(default = "default_compaction_retain_recent_versions")]
    pub retain_recent_versions: usize,
}

impl Default for CompactionRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: default_compaction_enabled(),
            interval_ms: default_compaction_interval_ms(),
            obsolete_versions_per_file: default_compaction_obsolete_versions_per_file(),
            obsolete_version_ratio: default_compaction_obsolete_version_ratio(),
            obsolete_bytes_per_file: default_compaction_obsolete_bytes_per_file(),
            max_versions_per_pass: default_compaction_max_versions_per_pass(),
            max_bytes_per_pass: default_compaction_max_bytes_per_pass(),
            retain_recent_versions: default_compaction_retain_recent_versions(),
        }
    }
}

impl CompactionRuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.interval_ms > 0,
            "compaction.interval_ms must be greater than zero"
        );
        ensure!(
            self.obsolete_versions_per_file > 0,
            "compaction.obsolete_versions_per_file must be greater than zero"
        );
        ensure!(
            self.obsolete_version_ratio.is_finite() && self.obsolete_version_ratio > 0.0,
            "compaction.obsolete_version_ratio must be a finite positive number"
        );
        ensure!(
            self.obsolete_bytes_per_file > 0,
            "compaction.obsolete_bytes_per_file must be greater than zero"
        );
        ensure!(
            self.max_versions_per_pass > 0,
            "compaction.max_versions_per_pass must be greater than zero"
        );
        ensure!(
            self.max_bytes_per_pass > 0,
            "compaction.max_bytes_per_pass must be greater than zero"
        );
        Ok(())
    }

    fn to_policy(&self) -> SqlitePageCompactionPolicy {
        SqlitePageCompactionPolicy {
            obsolete_versions_per_file: self.obsolete_versions_per_file,
            obsolete_version_ratio: self.obsolete_version_ratio,
            obsolete_bytes_per_file: self.obsolete_bytes_per_file,
            max_versions_per_pass: self.max_versions_per_pass,
            max_bytes_per_pass: self.max_bytes_per_pass,
            retain_recent_versions: self.retain_recent_versions,
            min_retained_version: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadinessRuntimeConfig {
    #[serde(default = "default_readiness_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_readiness_probe_interval_ms")]
    pub probe_interval_ms: u64,
    #[serde(default = "default_readiness_probe_rpc_timeout_ms")]
    pub probe_rpc_timeout_ms: u64,
}

impl Default for ReadinessRuntimeConfig {
    fn default() -> Self {
        Self {
            timeout_ms: default_readiness_timeout_ms(),
            probe_interval_ms: default_readiness_probe_interval_ms(),
            probe_rpc_timeout_ms: default_readiness_probe_rpc_timeout_ms(),
        }
    }
}

impl ReadinessRuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.timeout_ms > 0,
            "readiness.timeout_ms must be greater than zero"
        );
        ensure!(
            self.probe_interval_ms > 0,
            "readiness.probe_interval_ms must be greater than zero"
        );
        ensure!(
            self.probe_rpc_timeout_ms > 0,
            "readiness.probe_rpc_timeout_ms must be greater than zero"
        );
        ensure!(
            self.probe_rpc_timeout_ms <= self.timeout_ms,
            "readiness.probe_rpc_timeout_ms must be less than or equal to readiness.timeout_ms"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LibsqlHttpRuntimeConfig {
    #[serde(default = "default_libsql_http_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_libsql_session_idle_timeout_ms")]
    pub session_idle_timeout_ms: u64,
    #[serde(default = "default_libsql_blob_max_chunk_bytes")]
    pub blob_max_chunk_bytes: usize,
    #[serde(default)]
    pub idempotency: LibsqlHttpIdempotencyConfig,
    #[serde(default)]
    pub auth: LibsqlHttpAuthConfig,
}

impl Default for LibsqlHttpRuntimeConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_libsql_http_bind_addr(),
            session_idle_timeout_ms: default_libsql_session_idle_timeout_ms(),
            blob_max_chunk_bytes: default_libsql_blob_max_chunk_bytes(),
            idempotency: LibsqlHttpIdempotencyConfig::default(),
            auth: LibsqlHttpAuthConfig::default(),
        }
    }
}

impl LibsqlHttpRuntimeConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.bind_addr.is_empty(),
            "libsql_http.bind_addr must not be empty"
        );
        self.bind_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("parsing libsql_http.bind_addr {}", self.bind_addr))?;
        ensure!(
            self.session_idle_timeout_ms > 0,
            "libsql_http.session_idle_timeout_ms must be greater than zero"
        );
        ensure!(
            self.blob_max_chunk_bytes > 0,
            "libsql_http.blob_max_chunk_bytes must be greater than zero"
        );
        self.idempotency.validate()?;
        self.auth.validate()?;
        Ok(())
    }
}

pub async fn run_node(config: NodeConfig) -> anyhow::Result<()> {
    config.validate()?;
    let storage_plan = StorageNodePlan::from_config(&config);
    println!(
        "orion storage node {} [{}] enabled components: {}",
        storage_plan.node_id,
        storage_plan.topology.label(),
        storage_plan.component_labels()
    );
    let group_manager = NodeRaftGroupManager::start_default_group(&config).await?;
    let raft = group_manager.default_raft.clone();
    let sqlite_state = group_manager.default_state.clone();
    let metrics_registry = group_manager.metrics_registry.clone();
    let raft_addr: SocketAddr = config
        .node
        .raft_addr
        .parse()
        .with_context(|| format!("parsing node.raft_addr {}", config.node.raft_addr))?;
    let listener = TcpListener::bind(raft_addr)
        .await
        .with_context(|| format!("binding raft transport {}", raft_addr))?;
    let bound_addr = listener.local_addr()?;
    println!(
        "orion node {} [{}] serving raft transport on {}",
        config.node.id,
        config.node.topology.label(),
        bound_addr
    );
    if !config.peers.is_empty() {
        let peers = config
            .peers
            .iter()
            .map(|peer| format!("{}@{} [{}]", peer.id, peer.raft_addr, peer.topology.label()))
            .collect::<Vec<_>>()
            .join(", ");
        println!("orion node {} configured peers: {}", config.node.id, peers);
    }

    let transport_config = TonicRaftTransportConfig::from(&config.transport);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_raft_groups = group_manager.tonic_registry.clone();
    let server_metrics_registry = metrics_registry.clone();
    let mut raft_shutdown_rx = shutdown_rx.clone();
    let raft_server = tokio::spawn(async move {
        serve_raft_transport_group_registry_with_config_metrics_and_shutdown(
            listener,
            server_raft_groups,
            transport_config,
            server_metrics_registry,
            async move {
                let _ = raft_shutdown_rx.changed().await;
            },
        )
        .await
    });

    if config.bootstrap.create_default_group {
        group_manager
            .bootstrap_default_group_if_needed(&config)
            .await?;
    }

    let libsql_http_server = if config.runs_builtin_libsql_frontend() {
        config.libsql_http.clone()
    } else {
        None
    };
    let libsql_http_server = if let Some(libsql_http) = libsql_http_server {
        wait_for_sql_readiness(&config, &raft).await?;
        let libsql_http_addr: SocketAddr = libsql_http
            .bind_addr
            .parse()
            .with_context(|| format!("parsing libsql_http.bind_addr {}", libsql_http.bind_addr))?;
        let libsql_listener = TcpListener::bind(libsql_http_addr)
            .await
            .with_context(|| format!("binding libSQL HTTP listener {}", libsql_http_addr))?;
        let bound_libsql_addr = libsql_listener.local_addr()?;
        println!(
            "orion node {} serving libSQL HTTP on {}",
            config.node.id, bound_libsql_addr
        );
        let libsql_raft = raft.clone();
        let libsql_state = sqlite_state.clone();
        let sqlite_cache_root = config.storage.local.sqlite_cache_root().clone();
        let libsql_metrics_registry = metrics_registry.clone();
        let libsql_compaction_policy = config.compaction.to_policy();
        let libsql_replication_groups = group_manager.sql_registry.clone();
        let node_id = config.node.id;
        let peer_http_endpoints = config
            .peers
            .iter()
            .filter_map(|peer| {
                peer.libsql_http_addr
                    .as_ref()
                    .map(|addr| (peer.id, normalize_http_endpoint(addr)))
            })
            .collect();
        let placement_nodes = placement_nodes_from_config(&config);
        let mut libsql_shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            serve_libsql_http_with_shutdown(
                libsql_listener,
                libsql_raft,
                libsql_state,
                LibsqlHttpConfig {
                    sqlite_cache_root: sqlite_cache_root.into(),
                    session_idle_timeout: Duration::from_millis(
                        libsql_http.session_idle_timeout_ms,
                    ),
                    blob_max_chunk_bytes: libsql_http.blob_max_chunk_bytes,
                    idempotency: libsql_http.idempotency,
                    auth: libsql_http.auth,
                    node_id,
                    metrics_registry: libsql_metrics_registry,
                    compaction_policy: libsql_compaction_policy,
                    peer_http_endpoints,
                    placement_nodes,
                    replication_groups: Some(libsql_replication_groups),
                },
                async move {
                    let _ = libsql_shutdown_rx.changed().await;
                },
            )
            .await
        }))
    } else {
        None
    };
    let metrics = tokio::spawn(log_metrics_loop(
        raft.clone(),
        Duration::from_millis(config.metrics.log_interval_ms),
        metrics_registry.clone(),
    ));
    let compaction = if config.compaction.enabled {
        let compaction_state = sqlite_state.clone();
        let compaction_runtime = OrionSqliteRuntime::new(
            raft.clone(),
            sqlite_state.clone(),
            OrionSqliteRuntimeConfig {
                cache_root: config.storage.local.sqlite_cache_root().clone().into(),
                large_batch_threshold_bytes: config.raft.large_payload_threshold_bytes,
                large_batch_chunk_bytes: config.raft.large_payload_chunk_bytes,
                large_payload_max_staged_uploads: config.raft.large_payload_max_staged_uploads,
                large_payload_max_staged_bytes: config.raft.large_payload_max_staged_bytes,
                large_payload_staging_ttl_ms: config.raft.large_payload_staging_ttl_ms,
                large_payload_cleanup_batch_size: config.raft.large_payload_cleanup_batch_size,
            },
        );
        let compaction_policy = config.compaction.to_policy();
        let compaction_interval = Duration::from_millis(config.compaction.interval_ms);
        let mut compaction_shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            run_compaction_loop(
                compaction_runtime,
                compaction_state,
                compaction_policy,
                compaction_interval,
                &mut compaction_shutdown_rx,
            )
            .await;
            Ok::<(), anyhow::Error>(())
        }))
    } else {
        None
    };
    let placement_reconciler = {
        let manager = group_manager.clone();
        let config = config.clone();
        let mut placement_shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            run_placement_group_reconciler_loop(
                manager,
                config,
                Duration::from_millis(5_000),
                &mut placement_shutdown_rx,
            )
            .await;
            Ok::<(), anyhow::Error>(())
        }))
    };

    tokio::signal::ctrl_c()
        .await
        .context("waiting for shutdown signal")?;
    println!("orion node {} shutting down", config.node.id);
    let _ = shutdown_tx.send(true);

    await_server_shutdown("raft transport", raft_server).await;
    if let Some(libsql_http_server) = libsql_http_server {
        await_server_shutdown("libSQL HTTP", libsql_http_server).await;
    }
    metrics.abort();
    let _ = metrics.await;
    if let Some(compaction) = compaction {
        await_server_shutdown("sqlite compactor", compaction).await;
    }
    if let Some(placement_reconciler) = placement_reconciler {
        await_server_shutdown("placement group reconciler", placement_reconciler).await;
    }

    raft.shutdown().await.context("shutting down raft")?;
    sqlite_state
        .close()
        .await
        .context("closing SlateDB state store")?;
    Ok(())
}

async fn await_server_shutdown<T, E>(name: &str, handle: tokio::task::JoinHandle<Result<T, E>>)
where
    E: std::fmt::Display,
{
    match tokio::time::timeout(Duration::from_secs(10), handle).await {
        Ok(Ok(Ok(_))) => {}
        Ok(Ok(Err(error))) => eprintln!("{name} shutdown returned error: {error}"),
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => eprintln!("{name} task failed during shutdown: {error}"),
        Err(_) => eprintln!("{name} did not stop within shutdown grace period"),
    }
}

#[derive(Clone)]
struct NodeRaftGroupManager {
    tonic_registry: TonicRaftGroupRegistry,
    sql_registry: ReplicationGroupRegistry,
    default_raft: OrionRaft,
    default_state: SlateDbStateStore,
    metrics_registry: ClusterRaftMetricsRegistry,
    default_has_existing_raft_state: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RuntimeGroupReconcileOutcome {
    loaded: usize,
    membership_changes: usize,
}

impl NodeRaftGroupManager {
    async fn start_default_group(config: &NodeConfig) -> anyhow::Result<Self> {
        let tonic_registry = TonicRaftGroupRegistry::default();
        let sql_registry = ReplicationGroupRegistry::empty();
        let metrics_registry = ClusterRaftMetricsRegistry::default();
        metrics_registry.set_local_capabilities(node_software_capabilities());
        let default_group_id = config.bootstrap.default_group.group_id.clone();
        let group = open_raft_group(config, &default_group_id, metrics_registry.clone()).await?;
        tonic_registry
            .register(&default_group_id, group.raft.clone())
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        sql_registry.register(
            &default_group_id,
            OrionSqliteRuntime::new(
                group.raft.clone(),
                group.state.clone(),
                OrionSqliteRuntimeConfig {
                    cache_root: config
                        .sqlite_cache_root_for_group(&default_group_id)?
                        .clone()
                        .into(),
                    large_batch_threshold_bytes: config.raft.large_payload_threshold_bytes,
                    large_batch_chunk_bytes: config.raft.large_payload_chunk_bytes,
                    large_payload_max_staged_uploads: config.raft.large_payload_max_staged_uploads,
                    large_payload_max_staged_bytes: config.raft.large_payload_max_staged_bytes,
                    large_payload_staging_ttl_ms: config.raft.large_payload_staging_ttl_ms,
                    large_payload_cleanup_batch_size: config.raft.large_payload_cleanup_batch_size,
                },
            ),
        )?;
        Ok(Self {
            tonic_registry,
            sql_registry,
            default_raft: group.raft,
            default_state: group.state,
            metrics_registry,
            default_has_existing_raft_state: group.has_existing_raft_state,
        })
    }

    async fn bootstrap_default_group_if_needed(&self, config: &NodeConfig) -> anyhow::Result<()> {
        bootstrap_raft_if_needed(
            config,
            &self.default_raft,
            self.default_has_existing_raft_state,
        )
        .await
    }

    async fn reconcile_catalog_groups(
        &self,
        config: &NodeConfig,
    ) -> anyhow::Result<RuntimeGroupReconcileOutcome> {
        let default_runtime = self.sql_registry.default_runtime()?;
        let snapshots = list_runtime_group_catalog_snapshots(&default_runtime)?;
        let mut loaded_group_ids = self
            .sql_registry
            .loaded_group_ids()?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut outcome = RuntimeGroupReconcileOutcome::default();
        for snapshot in snapshots {
            if snapshot.group_id == config.bootstrap.default_group.group_id {
                continue;
            }
            if !snapshot_contains_member(&snapshot, config.node.id) {
                continue;
            }

            if !loaded_group_ids.contains(&snapshot.group_id) {
                self.load_group_from_catalog_snapshot(config, &snapshot)
                    .await?;
                loaded_group_ids.insert(snapshot.group_id.clone());
                outcome.loaded += 1;
            }

            if self
                .reconcile_loaded_group_membership(config, &snapshot)
                .await?
            {
                outcome.membership_changes += 1;
            }
        }
        Ok(outcome)
    }

    async fn load_group_from_catalog_snapshot(
        &self,
        config: &NodeConfig,
        snapshot: &RuntimeGroupCatalogSnapshot,
    ) -> anyhow::Result<()> {
        ensure_valid_runtime_group_id(&snapshot.group_id)?;
        let group = open_raft_group(config, &snapshot.group_id, self.metrics_registry.clone())
            .await
            .with_context(|| format!("opening raft group {}", snapshot.group_id))?;
        self.tonic_registry
            .register(&snapshot.group_id, group.raft.clone())
            .map_err(|err| anyhow::anyhow!(err.to_string()))
            .with_context(|| format!("registering raft group {}", snapshot.group_id))?;
        self.sql_registry.register(
            &snapshot.group_id,
            OrionSqliteRuntime::new(
                group.raft.clone(),
                group.state.clone(),
                OrionSqliteRuntimeConfig {
                    cache_root: config
                        .sqlite_cache_root_for_group(&snapshot.group_id)?
                        .clone()
                        .into(),
                    large_batch_threshold_bytes: config.raft.large_payload_threshold_bytes,
                    large_batch_chunk_bytes: config.raft.large_payload_chunk_bytes,
                    large_payload_max_staged_uploads: config.raft.large_payload_max_staged_uploads,
                    large_payload_max_staged_bytes: config.raft.large_payload_max_staged_bytes,
                    large_payload_staging_ttl_ms: config.raft.large_payload_staging_ttl_ms,
                    large_payload_cleanup_batch_size: config.raft.large_payload_cleanup_batch_size,
                },
            ),
        )?;
        if !group.has_existing_raft_state && snapshot_contains_voter(snapshot, config.node.id) {
            let members = raft_members_for_voters(config, &snapshot.voter_ids())?;
            println!(
                "orion node {} bootstrapping raft group {} with {} voter(s)",
                config.node.id,
                snapshot.group_id,
                members.len()
            );
            group
                .raft
                .initialize(members)
                .await
                .with_context(|| format!("initializing raft group {}", snapshot.group_id))?;
            if snapshot.voter_ids().len() == 1 && snapshot.voter_ids().contains(&config.node.id) {
                group.raft.trigger().elect().await.with_context(|| {
                    format!("triggering election for raft group {}", snapshot.group_id)
                })?;
            }
        }
        println!(
            "orion node {} loaded raft group {} from placement catalog",
            config.node.id, snapshot.group_id
        );
        Ok(())
    }

    async fn reconcile_loaded_group_membership(
        &self,
        config: &NodeConfig,
        snapshot: &RuntimeGroupCatalogSnapshot,
    ) -> anyhow::Result<bool> {
        let desired_voters = sorted_unique_node_ids(snapshot.voter_ids());
        let desired_learners = sorted_unique_node_ids(
            snapshot
                .learner_ids()
                .into_iter()
                .filter(|node_id| !desired_voters.contains(node_id))
                .collect(),
        );
        ensure!(
            !desired_voters.is_empty(),
            "raft group {} has no desired voters",
            snapshot.group_id
        );

        let raft = self
            .tonic_registry
            .get(&snapshot.group_id)
            .map_err(|status| anyhow::anyhow!(status.to_string()))
            .with_context(|| format!("looking up raft group {}", snapshot.group_id))?;
        let metrics = RaftMetricsSnapshot::observe(&raft);
        let current_voters = sorted_unique_node_ids(metrics.voter_ids.clone());
        let current_learners = sorted_unique_node_ids(metrics.learner_ids.clone());
        let missing_learners = desired_learners
            .iter()
            .chain(desired_voters.iter())
            .filter(|node_id| {
                !current_voters.contains(node_id) && !current_learners.contains(node_id)
            })
            .copied()
            .collect::<Vec<_>>();
        if current_voters == desired_voters && missing_learners.is_empty() {
            return Ok(false);
        }

        if !metrics.is_leader() {
            return Ok(false);
        }

        let desired_members = raft_members_for_voters(config, &snapshot.member_ids())?;
        for voter_id in desired_voters.iter().filter(|voter_id| {
            !current_voters.contains(voter_id) && !current_learners.contains(voter_id)
        }) {
            let node = desired_members
                .get(voter_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing raft endpoint for voter {voter_id}"))?;
            raft.add_learner(*voter_id, node, true)
                .await
                .with_context(|| {
                    format!(
                        "adding learner {voter_id} before changing membership for raft group {}",
                        snapshot.group_id
                    )
                })?;
        }

        for learner_id in desired_learners
            .iter()
            .filter(|learner_id| !current_learners.contains(learner_id))
        {
            let node = desired_members
                .get(learner_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing raft endpoint for learner {learner_id}"))?;
            raft.add_learner(*learner_id, node, true)
                .await
                .with_context(|| {
                    format!(
                        "adding learner {learner_id} for raft group {}",
                        snapshot.group_id
                    )
                })?;
        }

        if current_voters != desired_voters {
            raft.change_membership(desired_voters.clone(), false)
                .await
                .with_context(|| {
                    format!(
                        "changing membership for raft group {} to {:?}",
                        snapshot.group_id, desired_voters
                    )
                })?;
        }
        println!(
            "orion node {} reconciled raft group {} voters from {:?} to {:?}, learners from {:?} to {:?}",
            config.node.id,
            snapshot.group_id,
            current_voters,
            desired_voters,
            current_learners,
            desired_learners
        );
        Ok(true)
    }
}

fn snapshot_contains_member(snapshot: &RuntimeGroupCatalogSnapshot, node_id: u64) -> bool {
    snapshot
        .members
        .iter()
        .any(|member| member.node_id == node_id)
}

fn snapshot_contains_voter(snapshot: &RuntimeGroupCatalogSnapshot, node_id: u64) -> bool {
    snapshot
        .members
        .iter()
        .any(|member| member.node_id == node_id && member.role == "voter")
}

fn sorted_unique_node_ids(mut node_ids: Vec<u64>) -> Vec<u64> {
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
}

struct OpenedRaftGroup {
    raft: OrionRaft,
    state: SlateDbStateStore,
    has_existing_raft_state: bool,
}

async fn open_raft_group(
    config: &NodeConfig,
    group_id: &str,
    metrics_registry: ClusterRaftMetricsRegistry,
) -> anyhow::Result<OpenedRaftGroup> {
    let raft_log_root = config.raft_log_root_for_group(group_id)?;
    std::fs::create_dir_all(&raft_log_root)
        .with_context(|| format!("creating Raft log dir {raft_log_root}"))?;
    let has_existing_raft_state = persisted_raft_log_state_initialized_at(&raft_log_root).await?;

    let object_store = open_object_store(&config.storage.object_store)?;
    let log_store = OrionRaftLogStore::open(&raft_log_root)?;
    let state_prefix = config.node_state_prefix_for_group(group_id)?;
    let state = SlateDbStateStore::open(&state_prefix, object_store).await?;
    let sqlite_state = state.clone();
    let state_machine = OrionRaftStateMachine::new_with_sqlite_cache_and_large_payload_config(
        state,
        config.sqlite_cache_root_for_group(group_id)?,
        large_payload_config(&config.raft),
    );
    let network = build_network_for_group(config, group_id, metrics_registry.clone())?
        .with_local_state_store(sqlite_state.clone());
    let raft_config = Arc::new(build_openraft_config(&config.raft)?.validate()?);

    let raft = Raft::new(
        config.node.id,
        raft_config,
        network,
        log_store,
        state_machine,
    )
    .await?;
    Ok(OpenedRaftGroup {
        raft,
        state: sqlite_state,
        has_existing_raft_state,
    })
}

fn build_network_for_group(
    config: &NodeConfig,
    group_id: &str,
    metrics_registry: ClusterRaftMetricsRegistry,
) -> anyhow::Result<TonicRaftNetwork> {
    let network = TonicRaftNetwork::with_config_and_metrics(
        config.node.id,
        TonicRaftTransportConfig::from(&config.transport),
        metrics_registry.clone(),
    )
    .with_group_id(group_id);
    for peer in &config.peers {
        network
            .register_endpoint(peer.id, &peer.raft_addr)
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    }
    Ok(network)
}

fn open_object_store(config: &ObjectStoreConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::Local { root } => {
            std::fs::create_dir_all(root)
                .with_context(|| format!("creating SlateDB object-store root {root}"))?;
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(root)
                    .with_context(|| format!("opening local object store {root}"))?,
            ))
        }
    }
}

fn build_openraft_config(config: &RaftRuntimeConfig) -> anyhow::Result<Config> {
    Ok(Config {
        cluster_name: config.cluster_name.clone(),
        heartbeat_interval: config.heartbeat_interval_ms,
        election_timeout_min: config.election_timeout_min_ms,
        election_timeout_max: config.election_timeout_max_ms,
        replication_lag_threshold: config.replication_lag_threshold,
        install_snapshot_timeout: config.install_snapshot_timeout_ms,
        max_payload_entries: config.max_payload_entries,
        max_append_entries: config.max_append_entries,
        snapshot_max_chunk_size: config.snapshot_max_chunk_size,
        snapshot_policy: config.snapshot_policy.to_openraft(),
        max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
        purge_batch_size: config.purge_batch_size,
        ..Default::default()
    })
}

fn large_payload_config(config: &RaftRuntimeConfig) -> LargePayloadConfig {
    LargePayloadConfig {
        max_staged_uploads: config.large_payload_max_staged_uploads,
        max_staged_bytes: config.large_payload_max_staged_bytes,
        staging_ttl_ms: config.large_payload_staging_ttl_ms,
        cleanup_batch_size: config.large_payload_cleanup_batch_size,
    }
}

fn bootstrap_members(config: &NodeConfig) -> BTreeMap<u64, BasicNode> {
    let voter_ids = if config.bootstrap.default_group.voters.is_empty() {
        std::iter::once(config.node.id)
            .chain(config.peers.iter().map(|peer| peer.id))
            .collect::<Vec<_>>()
    } else {
        config.bootstrap.default_group.voters.clone()
    };
    raft_members_for_voters(config, &voter_ids).expect("validated bootstrap voters must be known")
}

fn raft_members_for_voters(
    config: &NodeConfig,
    voter_ids: &[u64],
) -> anyhow::Result<BTreeMap<u64, BasicNode>> {
    let mut members = BTreeMap::new();
    for voter_id in voter_ids {
        if *voter_id == config.node.id {
            members.insert(
                *voter_id,
                BasicNode {
                    addr: config.advertised_raft_addr(),
                },
            );
            continue;
        }
        let peer = config
            .peers
            .iter()
            .find(|peer| peer.id == *voter_id)
            .ok_or_else(|| {
                anyhow::anyhow!("raft group voter {voter_id} is not a configured node")
            })?;
        members.insert(
            *voter_id,
            BasicNode {
                addr: peer.raft_addr.clone(),
            },
        );
    }
    Ok(members)
}

fn placement_nodes_from_config(config: &NodeConfig) -> BTreeMap<u64, PlacementNodeConfig> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        config.node.id,
        PlacementNodeConfig {
            node_id: config.node.id,
            cloud: config.node.topology.cloud.clone(),
            region: config.node.topology.region.clone(),
            zone: config.node.topology.zone.clone(),
            raft_addr: config.advertised_raft_addr(),
            libsql_http_addr: config
                .libsql_http
                .as_ref()
                .map(|libsql_http| normalize_http_endpoint(&libsql_http.bind_addr)),
        },
    );
    for peer in &config.peers {
        nodes.insert(
            peer.id,
            PlacementNodeConfig {
                node_id: peer.id,
                cloud: peer.topology.cloud.clone(),
                region: peer.topology.region.clone(),
                zone: peer.topology.zone.clone(),
                raft_addr: peer.raft_addr.clone(),
                libsql_http_addr: peer
                    .libsql_http_addr
                    .as_ref()
                    .map(|addr| normalize_http_endpoint(addr)),
            },
        );
    }
    nodes
}

async fn bootstrap_raft_if_needed(
    config: &NodeConfig,
    raft: &OrionRaft,
    has_existing_raft_state: bool,
) -> anyhow::Result<()> {
    if has_existing_raft_state {
        let metrics = RaftMetricsSnapshot::observe(raft);
        println!(
            "orion node {} skipping raft bootstrap; existing raft state found at log={:?} applied={:?} voters={:?}",
            config.node.id, metrics.last_log_index, metrics.applied_index, metrics.voter_ids
        );
        return Ok(());
    }

    let members = bootstrap_members(config);
    println!(
        "orion node {} bootstrapping raft cluster with {} member(s)",
        config.node.id,
        members.len()
    );
    raft.initialize(members)
        .await
        .context("initializing raft membership")?;
    if config.peers.is_empty() {
        raft.trigger()
            .elect()
            .await
            .context("triggering single-node raft election")?;
    }
    Ok(())
}

async fn persisted_raft_log_state_initialized_at(raft_log_root: &str) -> anyhow::Result<bool> {
    std::fs::create_dir_all(raft_log_root)
        .with_context(|| format!("creating Raft log dir {raft_log_root}"))?;
    let mut log_store = OrionRaftLogStore::open(raft_log_root)?;
    let log_state = log_store
        .get_log_state()
        .await
        .with_context(|| format!("reading persisted Raft log state from {}", raft_log_root))?;
    Ok(log_state.last_log_id.is_some() || log_state.last_purged_log_id.is_some())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlReadiness {
    LocalLeader,
    RemoteLeader { node_id: u64, endpoint: String },
}

async fn wait_for_sql_readiness(config: &NodeConfig, raft: &OrionRaft) -> anyhow::Result<()> {
    let timeout = Duration::from_millis(config.readiness.timeout_ms);
    let probe_interval = Duration::from_millis(config.readiness.probe_interval_ms);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_error: String;

    println!(
        "orion node {} waiting up to {:?} for Raft SQL readiness",
        config.node.id, timeout
    );

    if config.peers.is_empty() {
        raft.wait(Some(timeout))
            .current_leader(config.node.id, "single-node SQL readiness")
            .await
            .context("waiting for single-node Raft leadership")?;
        raft.ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .context("single-node leader failed read-index readiness probe")?;
        println!(
            "orion node {} Raft SQL readiness satisfied as single-node leader",
            config.node.id
        );
        return Ok(());
    }

    loop {
        match probe_sql_readiness(config, raft).await {
            Ok(SqlReadiness::LocalLeader) => {
                println!(
                    "orion node {} Raft SQL readiness satisfied as local leader",
                    config.node.id
                );
                return Ok(());
            }
            Ok(SqlReadiness::RemoteLeader { node_id, endpoint }) => {
                println!(
                    "orion node {} Raft SQL readiness satisfied via leader node {} at {}",
                    config.node.id, node_id, endpoint
                );
                return Ok(());
            }
            Err(error) => {
                last_error = error;
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!(
                "Raft SQL readiness timed out after {:?}: {}",
                timeout,
                last_error
            );
        }

        tokio::time::sleep(std::cmp::min(probe_interval, deadline - now)).await;
    }
}

async fn probe_sql_readiness(
    config: &NodeConfig,
    raft: &OrionRaft,
) -> Result<SqlReadiness, String> {
    let target = sql_readiness_target(config, &RaftMetricsSnapshot::observe(raft))?;
    match target {
        SqlReadiness::LocalLeader => {
            raft.ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .map_err(|err| format!("local leader failed read-index readiness probe: {err}"))?;
            Ok(SqlReadiness::LocalLeader)
        }
        SqlReadiness::RemoteLeader { node_id, endpoint } => {
            client_barrier_to_raft_endpoint(
                endpoint.clone(),
                readiness_transport_config(config),
            )
            .await
            .map_err(|err| {
                format!(
                    "leader node {node_id} at {endpoint} failed Raft barrier readiness probe: {err}"
                )
            })?;
            Ok(SqlReadiness::RemoteLeader { node_id, endpoint })
        }
    }
}

fn sql_readiness_target(
    config: &NodeConfig,
    metrics: &RaftMetricsSnapshot,
) -> Result<SqlReadiness, String> {
    if !metrics.running {
        return Err("Raft runtime is not running".to_string());
    }

    let leader_id = metrics
        .current_leader
        .ok_or_else(|| "Raft leader is not known yet".to_string())?;
    if leader_id == config.node.id {
        return Ok(SqlReadiness::LocalLeader);
    }

    let endpoint = leader_endpoint_from_config(config, leader_id).ok_or_else(|| {
        format!("Raft leader is node {leader_id}, but this node has no configured endpoint for it")
    })?;
    Ok(SqlReadiness::RemoteLeader {
        node_id: leader_id,
        endpoint,
    })
}

fn leader_endpoint_from_config(config: &NodeConfig, leader_id: u64) -> Option<String> {
    if leader_id == config.node.id {
        Some(config.advertised_raft_addr())
    } else {
        config
            .peers
            .iter()
            .find(|peer| peer.id == leader_id)
            .map(|peer| peer.raft_addr.clone())
    }
}

fn readiness_transport_config(config: &NodeConfig) -> TonicRaftTransportConfig {
    let probe_timeout = Duration::from_millis(config.readiness.probe_rpc_timeout_ms);
    TonicRaftTransportConfig {
        connect_timeout: std::cmp::min(
            Duration::from_millis(config.transport.connect_timeout_ms),
            probe_timeout,
        ),
        rpc_timeout: std::cmp::min(
            Duration::from_millis(config.transport.rpc_timeout_ms),
            probe_timeout,
        ),
        max_message_size: config.transport.max_message_size,
    }
}

async fn log_metrics_loop(
    raft: OrionRaft,
    interval: Duration,
    metrics_registry: ClusterRaftMetricsRegistry,
) {
    if interval.is_zero() {
        return;
    }
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let metrics = metrics_registry.record_observed(&raft);
        println!(
            "raft metrics node={} state={} leader={:?} term={} log={:?} applied={:?} snapshot={:?} voters={:?}",
            metrics.node_id,
            metrics.state,
            metrics.current_leader,
            metrics.current_term,
            metrics.last_log_index,
            metrics.applied_index,
            metrics.snapshot_index,
            metrics.voter_ids
        );
    }
}

async fn run_compaction_loop(
    runtime: OrionSqliteRuntime,
    state: SlateDbStateStore,
    base_policy: SqlitePageCompactionPolicy,
    interval: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) {
    if interval.is_zero() {
        return;
    }
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(interval) => {
                if !runtime.metrics().is_leader() {
                    continue;
                }
                let system_db = match runtime.open_system_database() {
                    Ok(system_db) => system_db,
                    Err(error) => {
                        eprintln!("sqlite compaction could not open system database: {error}");
                        continue;
                    }
                };
                if let Err(error) = system_db.ensure_system_schema() {
                    eprintln!("sqlite compaction could not initialize system schema: {error}");
                    continue;
                }
                let control = match system_db.compaction_control() {
                    Ok(control) => control,
                    Err(error) => {
                        eprintln!("reading sqlite compaction control failed: {error}");
                        continue;
                    }
                };
                if control.paused {
                    continue;
                }
                let lease_ttl_ms = interval.as_millis().try_into().unwrap_or(u64::MAX).max(15_000).saturating_mul(3);
                match system_db.acquire_compaction_lease(
                    "sqlite-page-compactor",
                    runtime.metrics().node_id,
                    lease_ttl_ms,
                ) {
                    Ok(Some(_lease)) => {}
                    Ok(None) => continue,
                    Err(error) => {
                        eprintln!("acquiring sqlite compaction lease failed: {error}");
                        continue;
                    }
                }
                let mut policy = base_policy.clone();
                match system_db.compaction_retention_floor() {
                    Ok(floor) => {
                        policy.min_retained_version = floor.min_retained_version;
                    }
                    Err(error) => {
                        eprintln!("reading sqlite compaction retention floor failed: {error}");
                        continue;
                    }
                }
                if control.force_requested {
                    policy.obsolete_versions_per_file = 1;
                    policy.obsolete_version_ratio = 0.0;
                    policy.obsolete_bytes_per_file = 1;
                }
                let started_at_ms = current_time_millis();
                match compact_sqlite_page_versions_excluding(
                    &state,
                    &policy,
                    &[ORION_CATALOG_DATABASE],
                )
                .await
                {
                    Ok(metrics) => {
                        let finished_at_ms = current_time_millis();
                        if metrics.deleted_versions > 0 || metrics.obsolete_versions > 0 {
                            println!(
                                "sqlite compaction files_scanned={} files_compacted={} versions_scanned={} obsolete_versions={} deleted_versions={} obsolete_bytes={} deleted_bytes={} duration_ms={}",
                                metrics.files_scanned,
                                metrics.files_compacted,
                                metrics.versions_scanned,
                                metrics.obsolete_versions,
                                metrics.deleted_versions,
                                metrics.obsolete_bytes,
                                metrics.deleted_bytes,
                                metrics.duration_ms
                            );
                            if let Err(error) = system_db.record_compaction_run(
                                started_at_ms,
                                finished_at_ms,
                                "ok",
                                &metrics,
                                None,
                            ) {
                                eprintln!("recording sqlite compaction metrics failed: {error}");
                            }
                        }
                        if control.force_requested
                            && let Err(error) = system_db.clear_compaction_request()
                        {
                            eprintln!("clearing sqlite compaction force request failed: {error}");
                        }
                    }
                    Err(error) => {
                        let finished_at_ms = current_time_millis();
                        let error = error.to_string();
                        eprintln!("sqlite compaction failed: {error}");
                        if let Err(record_error) = system_db.record_compaction_run(
                            started_at_ms,
                            finished_at_ms,
                            "error",
                            &SqlitePageCompactionMetrics {
                                duration_ms: finished_at_ms.saturating_sub(started_at_ms),
                                ..SqlitePageCompactionMetrics::default()
                            },
                            Some(&error),
                        ) {
                            eprintln!("recording sqlite compaction failure failed: {record_error}");
                        }
                    },
                }
            }
        }
    }
}

async fn run_placement_group_reconciler_loop(
    manager: NodeRaftGroupManager,
    config: NodeConfig,
    interval: Duration,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match manager.default_state.list_sqlite_databases().await {
                    Ok(databases) if databases.iter().any(|database| database == ORION_CATALOG_DATABASE) => {}
                    Ok(_) => continue,
                    Err(error) => {
                        eprintln!("placement group reconciliation could not inspect local sqlite databases: {error}");
                        continue;
                    }
                }
                match manager.reconcile_catalog_groups(&config).await {
                    Ok(outcome) if outcome.loaded > 0 || outcome.membership_changes > 0 => {
                        println!(
                            "orion node {} placement reconciler loaded {} raft group(s), applied {} membership change(s)",
                            config.node.id, outcome.loaded, outcome.membership_changes
                        );
                    }
                    Ok(_) => {}
                    Err(error) if placement_reconcile_error_is_transient(&error) => {}
                    Err(error) => eprintln!("placement group reconciliation failed: {error}"),
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return;
                }
            }
        }
    }
}

fn placement_reconcile_error_is_transient(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("database catalog")
        || message.contains("no such table")
        || message.contains("has not been created")
        || message.contains("database disk image is malformed")
        || message.contains("file is not a database")
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn default_node_id() -> u64 {
    1
}

fn default_cluster_name() -> String {
    "orion".to_string()
}

fn default_cloud() -> String {
    "local".to_string()
}

fn default_roles() -> BTreeSet<ServiceRole> {
    BTreeSet::from([ServiceRole::All])
}

fn default_raft_addr() -> String {
    "127.0.0.1:7101".to_string()
}

fn default_storage_data_root() -> String {
    "./data/node-1".to_string()
}

fn default_node_state_prefix() -> String {
    "node-1/state".to_string()
}

fn default_object_store() -> ObjectStoreConfig {
    ObjectStoreConfig::Local {
        root: "./data/object-store".to_string(),
    }
}

fn default_bootstrap_create_default_group() -> bool {
    true
}

fn default_replication_group_id_config() -> String {
    "rg_default".to_string()
}

fn child_path(parent: &str, child: &str) -> String {
    PathBuf::from(parent).join(child).display().to_string()
}

fn default_max_object_upload_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_max_group_data_bytes() -> u64 {
    128 * 1024 * 1024 * 1024
}

fn default_max_sqlite_cache_bytes() -> u64 {
    16 * 1024 * 1024 * 1024
}

fn default_max_nvme_cache_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}

fn default_max_hosted_groups() -> usize {
    128
}

fn default_max_open_databases() -> usize {
    10_000
}

fn default_group_start_concurrency() -> usize {
    8
}

fn default_shutdown_grace_ms() -> u64 {
    10_000
}

fn validate_roles(roles: &BTreeSet<ServiceRole>) -> anyhow::Result<()> {
    ensure!(!roles.is_empty(), "roles must not be empty");
    ensure!(
        !roles.contains(&ServiceRole::All) || roles.len() == 1,
        "role all cannot be combined with other roles"
    );
    Ok(())
}

fn role_list(roles: &BTreeSet<ServiceRole>) -> String {
    roles
        .iter()
        .map(|role| match role {
            ServiceRole::All => "all",
            ServiceRole::Router => "router",
            ServiceRole::Compute => "compute",
            ServiceRole::Storage => "storage",
            ServiceRole::Controller => "controller",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn display_local_path(path: &str) -> String {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path.display().to_string()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => format!("{} ({})", path.display(), cwd.join(&path).display()),
            Err(_) => path.display().to_string(),
        }
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "default".to_string())
}

fn default_region() -> String {
    "local".to_string()
}

fn default_zone() -> String {
    "local".to_string()
}

fn default_heartbeat_interval_ms() -> u64 {
    50
}

fn default_election_timeout_min_ms() -> u64 {
    150
}

fn default_election_timeout_max_ms() -> u64 {
    300
}

fn default_replication_lag_threshold() -> u64 {
    10_000
}

fn default_install_snapshot_timeout_ms() -> u64 {
    30_000
}

fn default_max_payload_entries() -> u64 {
    300
}

fn default_snapshot_max_chunk_size() -> u64 {
    3 * 1024 * 1024
}

fn default_snapshot_policy() -> SnapshotPolicyConfig {
    SnapshotPolicyConfig {
        never: false,
        logs_since_last: Some(5_000),
    }
}

fn default_max_in_snapshot_log_to_keep() -> u64 {
    1_000
}

fn default_purge_batch_size() -> u64 {
    256
}

fn default_large_payload_threshold_bytes() -> usize {
    DEFAULT_LARGE_BATCH_THRESHOLD_BYTES
}

fn default_large_payload_chunk_bytes() -> usize {
    DEFAULT_LARGE_BATCH_CHUNK_BYTES
}

fn default_large_payload_max_staged_uploads() -> usize {
    LargePayloadConfig::default().max_staged_uploads
}

fn default_large_payload_max_staged_bytes() -> u64 {
    LargePayloadConfig::default().max_staged_bytes
}

fn default_large_payload_staging_ttl_ms() -> u64 {
    LargePayloadConfig::default().staging_ttl_ms
}

fn default_large_payload_cleanup_batch_size() -> usize {
    LargePayloadConfig::default().cleanup_batch_size
}

fn default_connect_timeout_ms() -> u64 {
    2_000
}

fn default_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_max_message_size() -> usize {
    8 * 1024 * 1024
}

fn default_metrics_log_interval_ms() -> u64 {
    10_000
}

fn default_compaction_enabled() -> bool {
    true
}

fn default_compaction_interval_ms() -> u64 {
    30_000
}

fn default_compaction_obsolete_versions_per_file() -> usize {
    512
}

fn default_compaction_obsolete_version_ratio() -> f64 {
    2.0
}

fn default_compaction_obsolete_bytes_per_file() -> u64 {
    32 * 1024 * 1024
}

fn default_compaction_max_versions_per_pass() -> usize {
    10_000
}

fn default_compaction_max_bytes_per_pass() -> u64 {
    64 * 1024 * 1024
}

fn default_compaction_retain_recent_versions() -> usize {
    2
}

fn default_readiness_timeout_ms() -> u64 {
    30_000
}

fn default_readiness_probe_interval_ms() -> u64 {
    100
}

fn default_readiness_probe_rpc_timeout_ms() -> u64 {
    1_000
}

fn default_libsql_session_idle_timeout_ms() -> u64 {
    5 * 60 * 1_000
}

fn default_libsql_blob_max_chunk_bytes() -> usize {
    DEFAULT_LARGE_BATCH_CHUNK_BYTES
}

fn default_libsql_http() -> Option<LibsqlHttpRuntimeConfig> {
    Some(LibsqlHttpRuntimeConfig::default())
}

fn default_libsql_http_bind_addr() -> String {
    "127.0.0.1:8091".to_string()
}

fn normalize_http_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

const COMMENTED_EXAMPLE_CONFIG: &str = r#"# Orion single-node starter config.
#
# Run it with:
#   orion server --config orion.yaml
#
# The default shape is a one-process development node:
# - one OpenRaft voter
# - local Fjall Raft log
# - local SlateDB object-store root
# - libSQL/Hrana HTTP on 127.0.0.1:8091
#
# Connect with:
#   scripts/orion-libsql-shell.mjs http://127.0.0.1:8091/appdb

node:
  id: 1
  # Raft transport bind address. In a one-node deployment this only needs to be
  # reachable by this process. In a cluster, peers dial this address unless
  # advertised_raft_addr is set.
  raft_addr: "127.0.0.1:7101"

  # Optional: set when other nodes need to dial a different address, such as a
  # Docker Compose service name or Kubernetes DNS name.
  # advertised_raft_addr: "orion-node1:7101"

  topology:
    cloud: "local"
    region: "local"
    zone: "local"

roles: ["all"]

storage:
  # Local object-store emulation. For production this boundary becomes
  # S3/GCS/Azure object storage. Database keyspaces are created from catalog
  # conventions rather than hand-written per-database paths in this file.
  object_store:
    type: local
    root: "./data/object-store"

  local:
    # Root for node-local durable state and cache. The paths below derive from
    # data_root unless explicitly overridden.
    data_root: "./data/node-1"
    # raft_log_root: "./data/node-1/raft"
    # group_data_root: "./data/node-1/groups"
    # sqlite_cache_root: "./data/node-1/sqlite-cache"
    # nvme_cache_root: "./data/node-1/nvme-cache"

  object_prefixes:
    # Node-local materialized state prefix. User databases get their own
    # keyspaces through the catalog and placement APIs.
    node_state: "node-1/state"

  limits:
    max_object_upload_bytes: 536870912
    max_group_data_bytes: 137438953472
    max_sqlite_cache_bytes: 17179869184
    max_nvme_cache_bytes: 68719476736

# Empty peers means single-node mode. For a three-node cluster, add the other
# voter ids and their raft_addr values here, and use the same member list on
# every node. Set libsql_http_addr when this node should forward strong,
# session, or bounded-staleness reads to a peer's SQL endpoint.
peers: []
# - id: 2
#   raft_addr: "127.0.0.1:7102"
#   libsql_http_addr: "http://127.0.0.1:8092"
#   topology:
#     cloud: "local"
#     region: "local"
#     zone: "local-2"

# Bootstrap only controls initial default-group creation. Replication groups
# and database placement are managed dynamically through operator APIs.
bootstrap:
  create_default_group: true
  default_group:
    group_id: "rg_default"
    # Empty voters means this node plus all configured peers.
    voters: []

runtime:
  max_hosted_groups: 128
  max_open_databases: 10000
  group_start_concurrency: 8
  shutdown_grace_ms: 10000

raft:
  cluster_name: "orion-dev"

  # Local development timings. Increase these for cross-region WAN clusters.
  heartbeat_interval_ms: 50
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300

  # Keep this above snapshot_policy.logs_since_last so lagging replicas are not
  # considered stale before snapshotting can make progress.
  replication_lag_threshold: 10000

  install_snapshot_timeout_ms: 30000
  max_payload_entries: 300
  max_append_entries: 1024
  snapshot_max_chunk_size: 3145728
  snapshot_policy:
    logs_since_last: 5000
  max_in_snapshot_log_to_keep: 1000
  purge_batch_size: 256

transport:
  connect_timeout_ms: 2000
  rpc_timeout_ms: 5000
  max_message_size: 8388608

metrics:
  log_interval_ms: 10000

compaction:
  # Compaction is opportunistic cleanup of obsolete SQLite page versions in
  # SlateDB. It is not required for foreground correctness.
  enabled: true
  interval_ms: 30000
  obsolete_versions_per_file: 512
  obsolete_version_ratio: 2.0
  obsolete_bytes_per_file: 33554432
  max_versions_per_pass: 10000
  max_bytes_per_pass: 67108864
  retain_recent_versions: 2

readiness:
  timeout_ms: 30000
  probe_interval_ms: 100
  probe_rpc_timeout_ms: 1000

libsql_http:
  bind_addr: "127.0.0.1:8091"
  session_idle_timeout_ms: 300000
  blob_max_chunk_bytes: 524288
  idempotency:
    # Enables x-orion-idempotency-key for standalone write pipelines.
    enabled: true
    # Committed keys are retained for retry/replay during this window.
    committed_ttl_ms: 86400000
    # Very old pending keys are treated as stuck and are eligible for bounded GC.
    pending_ttl_ms: 604800000
    gc_interval_ms: 60000
    gc_max_records_per_pass: 1000

  # Leave tokens empty for local development. For shared environments, configure
  # bearer tokens and restrict them to database name prefixes. Operator access
  # to _orion requires system_permissions.
  auth:
    tokens: []
    # - token: "replace-me"
    #   database_prefixes: ["app", "tenant_"]
    # - token: "operator-token"
    #   system_permissions: ["read"]
    # - token: "operator-admin-token"
    #   system_permissions: ["admin"]

"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_node_config_from_yaml() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
  topology:
    cloud: "aws"
    region: "us-east-1"
    zone: "use1-az1"
storage:
  object_store:
    type: local
    root: "./data/object-store"
  local:
    data_root: "./data/node-1"
    sqlite_cache_root: "./data/sqlite-cache"
  object_prefixes:
    node_state: "node-1/state"
peers:
  - id: 2
    raft_addr: "127.0.0.1:7102"
    topology:
      cloud: "gcp"
      region: "us-central1"
      zone: "us-central1-a"
bootstrap:
  create_default_group: true
  default_group:
    group_id: "rg_default"
    voters: [1, 2]
raft:
  heartbeat_interval_ms: 250
  election_timeout_min_ms: 2500
  election_timeout_max_ms: 5000
  replication_lag_threshold: 20000
  install_snapshot_timeout_ms: 60000
  max_payload_entries: 512
  max_append_entries: 4096
  snapshot_max_chunk_size: 4194304
  snapshot_policy:
    logs_since_last: 10000
libsql_http:
  bind_addr: "127.0.0.1:8080"
  session_idle_timeout_ms: 45000
  blob_max_chunk_bytes: 524288
  auth:
    tokens:
      - token: "dev-token"
        database_prefixes: ["app", "orion"]
readiness:
  timeout_ms: 12000
  probe_interval_ms: 250
  probe_rpc_timeout_ms: 750
compaction:
  enabled: true
  interval_ms: 15000
  obsolete_versions_per_file: 128
  obsolete_version_ratio: 1.5
  obsolete_bytes_per_file: 1048576
  max_versions_per_pass: 2048
  max_bytes_per_pass: 4194304
  retain_recent_versions: 3
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
        assert_eq!(config.node.id, 1);
        assert_eq!(config.roles, BTreeSet::from([ServiceRole::All]));
        assert_eq!(config.node.topology.cloud, "aws");
        assert_eq!(config.node.topology.region, "us-east-1");
        assert_eq!(config.node.topology.zone, "use1-az1");
        assert!(config.bootstrap.create_default_group);
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].topology.cloud, "gcp");
        assert_eq!(
            config.storage.local.sqlite_cache_root(),
            "./data/sqlite-cache"
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().bind_addr,
            "127.0.0.1:8080"
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().session_idle_timeout_ms,
            45_000
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().blob_max_chunk_bytes,
            524_288
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().auth.tokens[0].database_prefixes,
            vec!["app".to_string(), "orion".to_string()]
        );
        assert_eq!(config.raft.snapshot_policy.logs_since_last, Some(10_000));
        assert_eq!(config.raft.heartbeat_interval_ms, 250);
        assert_eq!(config.raft.election_timeout_min_ms, 2_500);
        assert_eq!(config.raft.election_timeout_max_ms, 5_000);
        assert_eq!(config.raft.replication_lag_threshold, 20_000);
        assert_eq!(config.raft.install_snapshot_timeout_ms, 60_000);
        assert_eq!(config.raft.max_payload_entries, 512);
        assert_eq!(config.raft.max_append_entries, Some(4_096));
        assert_eq!(config.raft.snapshot_max_chunk_size, 4_194_304);
        assert!(!config.raft.snapshot_policy.never);
        assert_eq!(config.readiness.timeout_ms, 12_000);
        assert_eq!(config.readiness.probe_interval_ms, 250);
        assert_eq!(config.readiness.probe_rpc_timeout_ms, 750);
        assert!(config.compaction.enabled);
        assert_eq!(config.compaction.interval_ms, 15_000);
        assert_eq!(config.compaction.obsolete_versions_per_file, 128);
        assert_eq!(config.compaction.obsolete_version_ratio, 1.5);
        assert_eq!(config.compaction.obsolete_bytes_per_file, 1_048_576);
        assert_eq!(config.compaction.max_versions_per_pass, 2_048);
        assert_eq!(config.compaction.max_bytes_per_pass, 4_194_304);
        assert_eq!(config.compaction.retain_recent_versions, 3);
    }

    #[test]
    fn default_node_config_is_single_node_libsql() {
        let config = NodeConfig::default();
        config.validate().unwrap();

        assert_eq!(config.node.id, 1);
        assert_eq!(config.roles, BTreeSet::from([ServiceRole::All]));
        assert_eq!(config.node.raft_addr, "127.0.0.1:7101");
        assert!(config.bootstrap.create_default_group);
        assert!(config.peers.is_empty());
        assert_eq!(config.raft_log_root(), "./data/node-1/raft");
        assert_eq!(config.node_state_prefix(), "node-1/state");
        assert_eq!(
            config.libsql_http.as_ref().unwrap().bind_addr,
            "127.0.0.1:8091"
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().blob_max_chunk_bytes,
            DEFAULT_LARGE_BATCH_CHUNK_BYTES
        );
        assert!(config.compaction.enabled);
        assert_eq!(config.compaction.interval_ms, 30_000);
        assert_eq!(config.compaction.obsolete_versions_per_file, 512);
    }

    #[test]
    fn node_config_derives_non_default_group_paths() {
        let config = NodeConfig::default();

        assert_eq!(
            config.raft_log_root_for_group("rg_default").unwrap(),
            "./data/node-1/raft"
        );
        assert_eq!(
            config.node_state_prefix_for_group("rg_default").unwrap(),
            "node-1/state"
        );
        assert_eq!(
            config.sqlite_cache_root_for_group("rg_default").unwrap(),
            "./data/node-1/sqlite-cache"
        );
        assert_eq!(
            config.raft_log_root_for_group("rg_app").unwrap(),
            "./data/node-1/groups/rg_app/raft"
        );
        assert_eq!(
            config.node_state_prefix_for_group("rg_app").unwrap(),
            "node-1/state/replication-groups/rg_app/state"
        );
        assert_eq!(
            config.sqlite_cache_root_for_group("rg_app").unwrap(),
            "./data/node-1/sqlite-cache/groups/rg_app"
        );
        assert!(config.raft_log_root_for_group("../bad").is_err());
    }

    #[test]
    fn node_config_keeps_dynamic_group_state_prefixes_per_replica() {
        let mut node1 = NodeConfig::default();
        node1.storage.object_prefixes.node_state = "node-1/state".to_string();

        let mut node2 = NodeConfig::default();
        node2.node.id = 2;
        node2.storage.object_prefixes.node_state = "node-2/state".to_string();

        assert_eq!(
            node1.node_state_prefix_for_group("rg_app").unwrap(),
            "node-1/state/replication-groups/rg_app/state"
        );
        assert_eq!(
            node2.node_state_prefix_for_group("rg_app").unwrap(),
            "node-2/state/replication-groups/rg_app/state"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_manager_loads_active_catalog_group_for_local_voter() {
        let object_store_dir = tempfile::TempDir::new().unwrap();
        let data_dir = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::default();
        config.storage.object_store = ObjectStoreConfig::Local {
            root: object_store_dir.path().display().to_string(),
        };
        config.storage.local.data_root = data_dir.path().join("node-1").display().to_string();
        config.storage.local.sqlite_cache_root =
            Some(data_dir.path().join("sqlite-cache").display().to_string());
        config.storage.object_prefixes.node_state = "node-1/state".to_string();
        config.raft.cluster_name = format!("dynamic-group-test-{}", current_time_millis());
        config.raft.snapshot_policy = SnapshotPolicyConfig {
            never: true,
            logs_since_last: None,
        };
        config.libsql_http = None;
        config.validate().unwrap();

        let manager = NodeRaftGroupManager::start_default_group(&config)
            .await
            .unwrap();
        manager
            .bootstrap_default_group_if_needed(&config)
            .await
            .unwrap();

        let default_runtime = manager.sql_registry.default_runtime().unwrap();
        let catalog = default_runtime.open_database("orion_catalog").unwrap();
        let conn = catalog.connect().unwrap();
        crate::libsql_http::ensure_database_catalog_schema_for_runtime(&default_runtime).unwrap();
        let now = sqlite_i64_for_test(current_time_millis());
        conn.execute(
            r#"
            insert into replication_groups (
                group_id, state, placement_mode, object_prefix,
                failover_automatic, failover_promote_after_ms, created_at_ms, updated_at_ms
            )
            values ('rg_dynamic_test', 'active', 'manual', 'replication-groups/rg_dynamic_test', 1, 1000, ?, ?)
            "#,
            rusqlite::params![now, now],
        )
        .unwrap();
        conn.execute(
            r#"
            insert into replication_group_members (
                group_id, node_id, role, cloud, region, zone, priority, created_at_ms, updated_at_ms
            )
            values ('rg_dynamic_test', 1, 'voter', 'local', 'local', 'local', 0, ?, ?)
            "#,
            rusqlite::params![now, now],
        )
        .unwrap();
        drop(conn);

        let outcome = manager.reconcile_catalog_groups(&config).await.unwrap();
        assert_eq!(
            outcome,
            RuntimeGroupReconcileOutcome {
                loaded: 1,
                membership_changes: 0,
            }
        );
        assert!(manager.sql_registry.contains("rg_dynamic_test").unwrap());
        assert!(manager.tonic_registry.get("rg_dynamic_test").is_ok());

        manager
            .tonic_registry
            .get("rg_dynamic_test")
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        manager.default_raft.shutdown().await.unwrap();
        manager.default_state.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_manager_loads_active_catalog_group_for_local_read_replica() {
        let object_store_dir = tempfile::TempDir::new().unwrap();
        let data_dir = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::default();
        config.peers.push(PeerConfig {
            id: 2,
            raft_addr: "127.0.0.1:27102".to_string(),
            libsql_http_addr: None,
            topology: TopologyConfig {
                cloud: "local".to_string(),
                region: "local".to_string(),
                zone: "local-2".to_string(),
            },
        });
        config.bootstrap.default_group.voters = vec![1];
        config.storage.object_store = ObjectStoreConfig::Local {
            root: object_store_dir.path().display().to_string(),
        };
        config.storage.local.data_root = data_dir.path().join("node-1").display().to_string();
        config.storage.local.sqlite_cache_root =
            Some(data_dir.path().join("sqlite-cache").display().to_string());
        config.storage.object_prefixes.node_state = "node-1/state".to_string();
        config.raft.cluster_name =
            format!("dynamic-read-replica-group-test-{}", current_time_millis());
        config.raft.snapshot_policy = SnapshotPolicyConfig {
            never: true,
            logs_since_last: None,
        };
        config.libsql_http = None;
        config.validate().unwrap();

        let manager = NodeRaftGroupManager::start_default_group(&config)
            .await
            .unwrap();
        manager
            .bootstrap_default_group_if_needed(&config)
            .await
            .unwrap();

        let default_runtime = manager.sql_registry.default_runtime().unwrap();
        let catalog = default_runtime.open_database("orion_catalog").unwrap();
        let conn = catalog.connect().unwrap();
        crate::libsql_http::ensure_database_catalog_schema_for_runtime(&default_runtime).unwrap();
        let now = sqlite_i64_for_test(current_time_millis());
        conn.execute(
            r#"
            insert into replication_groups (
                group_id, state, placement_mode, object_prefix,
                failover_automatic, failover_promote_after_ms, created_at_ms, updated_at_ms
            )
            values ('rg_read_replica_test', 'active', 'manual', 'replication-groups/rg_read_replica_test', 1, 1000, ?, ?)
            "#,
            rusqlite::params![now, now],
        )
        .unwrap();
        for (node_id, role, priority) in [(2, "voter", 0), (1, "read_replica", 1)] {
            conn.execute(
                r#"
                insert into replication_group_members (
                    group_id, node_id, role, cloud, region, zone, priority, created_at_ms, updated_at_ms
                )
                values ('rg_read_replica_test', ?, ?, 'local', 'local', 'local', ?, ?, ?)
                "#,
                rusqlite::params![node_id, role, priority, now, now],
            )
            .unwrap();
        }
        drop(conn);

        let outcome = manager.reconcile_catalog_groups(&config).await.unwrap();
        assert_eq!(
            outcome,
            RuntimeGroupReconcileOutcome {
                loaded: 1,
                membership_changes: 0,
            }
        );
        assert!(
            manager
                .sql_registry
                .contains("rg_read_replica_test")
                .unwrap()
        );
        assert!(manager.tonic_registry.get("rg_read_replica_test").is_ok());

        let metrics = RaftMetricsSnapshot::observe(
            &manager.tonic_registry.get("rg_read_replica_test").unwrap(),
        );
        assert_eq!(metrics.voter_ids, Vec::<u64>::new());

        manager
            .tonic_registry
            .get("rg_read_replica_test")
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        manager.default_raft.shutdown().await.unwrap();
        manager.default_state.close().await.unwrap();
    }

    #[test]
    fn empty_yaml_uses_single_node_defaults() {
        let config: NodeConfig = serde_yaml::from_str("{}").unwrap();
        config.validate().unwrap();

        assert_eq!(config.node.id, 1);
        assert_eq!(config.node.raft_addr, "127.0.0.1:7101");
        assert!(config.bootstrap.create_default_group);
        assert!(config.peers.is_empty());
        assert_eq!(
            config.libsql_http.as_ref().unwrap().bind_addr,
            "127.0.0.1:8091"
        );
        assert_eq!(
            config.libsql_http.as_ref().unwrap().blob_max_chunk_bytes,
            DEFAULT_LARGE_BATCH_CHUNK_BYTES
        );
    }

    #[test]
    fn commented_example_config_is_valid() {
        let config: NodeConfig =
            serde_yaml::from_str(NodeConfig::commented_example_yaml()).unwrap();
        config.validate().unwrap();

        assert_eq!(
            config.libsql_http.as_ref().unwrap().bind_addr,
            "127.0.0.1:8091"
        );
        assert!(config.bootstrap.create_default_group);
        assert!(config.peers.is_empty());
    }

    #[test]
    fn human_summary_includes_default_operator_details() {
        let config = DefaultConfig::one_node();
        let summary = config.human_summary("test default");

        assert!(summary.contains("source: test default"));
        assert!(summary.contains("voters configured: 1"));
        assert!(summary.contains("peers: none (single-node cluster)"));
        assert!(summary.contains("storage.local.raft_log_root: ./data/node-1/raft"));
        assert!(summary.contains("storage.object_store.root: ./data/object-store"));
        assert!(summary.contains("storage.object_prefixes.node_state: node-1/state"));
        assert!(summary.contains("storage.local.sqlite_cache_root: ./data/node-1/sqlite-cache"));
        assert!(summary.contains("libsql_http.bind_addr: 127.0.0.1:8091"));
        assert!(summary.contains("libsql_http.blob_max_chunk_bytes: 524288"));
        assert!(summary.contains("libsql_http.auth: disabled"));
    }

    #[test]
    fn rejects_incomplete_topology() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
  topology:
    cloud: "aws"
    region: ""
    zone: "use1-az1"
storage:
  object_store:
    type: local
    root: "./data/object-store"
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("node.topology.region must not be empty"));
    }

    #[test]
    fn parses_storage_role_from_yaml() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
roles: ["storage"]
storage:
  object_store:
    type: local
    root: "./data/object-store"
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();

        assert!(config.runs_storage());
        assert_eq!(config.roles, BTreeSet::from([ServiceRole::Storage]));
    }

    #[test]
    fn rejects_non_storage_role_until_role_is_implemented() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
roles: ["router"]
storage:
  object_store:
    type: local
    root: "./data/object-store"
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("only storage/all role is implemented"));
    }

    #[test]
    fn rejects_all_role_combined_with_specific_roles() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
roles: ["all", "storage"]
storage:
  object_store:
    type: local
    root: "./data/object-store"
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("role all cannot be combined"));
    }

    #[test]
    fn rejects_replication_lag_threshold_below_snapshot_policy() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
storage:
  object_store:
    type: local
    root: "./data/object-store"
raft:
  replication_lag_threshold: 100
  snapshot_policy:
    logs_since_last: 100
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("replication_lag_threshold"));
    }

    #[test]
    fn rejects_readiness_probe_timeout_above_total_timeout() {
        let yaml = r#"
node:
  id: 1
  raft_addr: "127.0.0.1:7101"
storage:
  object_store:
    type: local
    root: "./data/object-store"
readiness:
  timeout_ms: 100
  probe_rpc_timeout_ms: 101
"#;
        let config: NodeConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("probe_rpc_timeout_ms"));
    }

    #[test]
    fn maps_wan_raft_runtime_config_to_openraft() {
        let runtime = RaftRuntimeConfig {
            cluster_name: "orion-wan".to_string(),
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 2_500,
            election_timeout_max_ms: 5_000,
            replication_lag_threshold: 20_000,
            install_snapshot_timeout_ms: 60_000,
            max_payload_entries: 512,
            max_append_entries: Some(4_096),
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            snapshot_policy: SnapshotPolicyConfig {
                never: false,
                logs_since_last: Some(10_000),
            },
            max_in_snapshot_log_to_keep: 2_000,
            purge_batch_size: 512,
            large_payload_threshold_bytes: default_large_payload_threshold_bytes(),
            large_payload_chunk_bytes: default_large_payload_chunk_bytes(),
            large_payload_max_staged_uploads: default_large_payload_max_staged_uploads(),
            large_payload_max_staged_bytes: default_large_payload_max_staged_bytes(),
            large_payload_staging_ttl_ms: default_large_payload_staging_ttl_ms(),
            large_payload_cleanup_batch_size: default_large_payload_cleanup_batch_size(),
        };

        let config = build_openraft_config(&runtime).unwrap();

        assert_eq!(config.cluster_name, "orion-wan");
        assert_eq!(config.heartbeat_interval, 250);
        assert_eq!(config.election_timeout_min, 2_500);
        assert_eq!(config.election_timeout_max, 5_000);
        assert_eq!(config.replication_lag_threshold, 20_000);
        assert_eq!(config.install_snapshot_timeout, 60_000);
        assert_eq!(config.max_payload_entries, 512);
        assert_eq!(config.max_append_entries, Some(4_096));
        assert_eq!(config.snapshot_max_chunk_size, 4 * 1024 * 1024);
        assert_eq!(config.max_in_snapshot_log_to_keep, 2_000);
        assert_eq!(config.purge_batch_size, 512);
    }

    #[test]
    fn parses_example_node_configs() {
        let root = env!("CARGO_MANIFEST_DIR");
        for file in ["node1.yaml", "node2.yaml", "node3.yaml"] {
            let path = format!("{root}/examples/{file}");
            NodeConfig::from_yaml_file(&path).unwrap();
        }
    }

    #[test]
    fn parses_docker_cluster_node_configs() {
        let root = env!("CARGO_MANIFEST_DIR");
        for file in ["node1.yaml", "node2.yaml", "node3.yaml"] {
            let path = format!("{root}/docker/cluster/{file}");
            let config = NodeConfig::from_yaml_file(&path).unwrap();
            assert!(
                config
                    .node
                    .advertised_raft_addr
                    .as_deref()
                    .unwrap()
                    .starts_with("orion-node")
            );
        }
    }

    #[test]
    fn readiness_target_accepts_local_leader() {
        let config = readiness_test_config();
        let metrics = readiness_test_metrics(Some(1), true);

        let target = sql_readiness_target(&config, &metrics).unwrap();

        assert_eq!(target, SqlReadiness::LocalLeader);
    }

    #[test]
    fn readiness_target_accepts_configured_remote_leader() {
        let config = readiness_test_config();
        let metrics = readiness_test_metrics(Some(2), true);

        let target = sql_readiness_target(&config, &metrics).unwrap();

        assert_eq!(
            target,
            SqlReadiness::RemoteLeader {
                node_id: 2,
                endpoint: "127.0.0.1:7102".to_string()
            }
        );
    }

    #[test]
    fn readiness_target_rejects_unknown_leader() {
        let config = readiness_test_config();
        let metrics = readiness_test_metrics(None, true);

        let err = sql_readiness_target(&config, &metrics).unwrap_err();

        assert!(err.contains("leader is not known"));
    }

    #[test]
    fn readiness_target_rejects_unconfigured_remote_leader() {
        let config = readiness_test_config();
        let metrics = readiness_test_metrics(Some(3), true);

        let err = sql_readiness_target(&config, &metrics).unwrap_err();

        assert!(err.contains("no configured endpoint"));
    }

    #[test]
    fn readiness_probe_transport_is_bounded_by_probe_timeout() {
        let mut config = readiness_test_config();
        config.transport.connect_timeout_ms = 5_000;
        config.transport.rpc_timeout_ms = 15_000;
        config.readiness.probe_rpc_timeout_ms = 750;

        let transport = readiness_transport_config(&config);

        assert_eq!(transport.connect_timeout, Duration::from_millis(750));
        assert_eq!(transport.rpc_timeout, Duration::from_millis(750));
        assert_eq!(transport.max_message_size, default_max_message_size());
    }

    fn readiness_test_config() -> NodeConfig {
        NodeConfig {
            node: NodeIdentityConfig {
                id: 1,
                raft_addr: "127.0.0.1:7101".to_string(),
                advertised_raft_addr: None,
                topology: TopologyConfig::default(),
            },
            roles: default_roles(),
            storage: StorageConfig {
                object_store: ObjectStoreConfig::Local {
                    root: "./data/object-store".to_string(),
                },
                local: StorageLocalConfig {
                    data_root: "./data/node-1".to_string(),
                    raft_log_root: None,
                    group_data_root: None,
                    sqlite_cache_root: None,
                    nvme_cache_root: None,
                },
                object_prefixes: StorageObjectPrefixesConfig {
                    node_state: "node-1/state".to_string(),
                },
                limits: StorageLimitsConfig::default(),
            },
            peers: vec![PeerConfig {
                id: 2,
                raft_addr: "127.0.0.1:7102".to_string(),
                libsql_http_addr: None,
                topology: TopologyConfig::default(),
            }],
            runtime: RuntimeConfig::default(),
            bootstrap: BootstrapConfig {
                create_default_group: false,
                default_group: BootstrapDefaultGroupConfig::default(),
            },
            raft: RaftRuntimeConfig::default(),
            transport: TransportRuntimeConfig::default(),
            metrics: MetricsRuntimeConfig::default(),
            compaction: CompactionRuntimeConfig::default(),
            readiness: ReadinessRuntimeConfig::default(),
            libsql_http: None,
        }
    }

    fn readiness_test_metrics(current_leader: Option<u64>, running: bool) -> RaftMetricsSnapshot {
        RaftMetricsSnapshot {
            node_id: 1,
            state: "Follower".to_string(),
            running,
            current_term: 1,
            current_leader,
            last_log_index: Some(1),
            committed_index: Some(1),
            applied_index: Some(1),
            snapshot_index: None,
            purged_index: None,
            voter_ids: vec![1, 2],
            learner_ids: vec![],
            replication: vec![],
            snapshot_transfer: Default::default(),
            capabilities: None,
        }
    }

    fn sqlite_i64_for_test(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }
}
