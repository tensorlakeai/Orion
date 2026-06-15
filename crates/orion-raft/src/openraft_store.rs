use std::fmt;
use std::io;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use futures_util::{Stream, StreamExt};
use openraft::entry::RaftEntry;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
    RaftStateMachine, Snapshot,
};
use openraft::{EntryPayload, RaftTypeConfig, SnapshotMeta, StoredMembership};
use orion_sqlite::{FileKind, VfsFileOp, VfsSyncBatch, VfsWrite};
use serde::{Deserialize, Serialize};
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions as SlateWriteOptions};
use slatedb::object_store::ObjectStore;
use slatedb::{DbReadOps, DbWriteOps, WriteBatch as SlateWriteBatch};
use uuid::Uuid;

use crate::checkpoint_artifact::SlateDbCheckpointArtifact;
use crate::slatedb_sqlite_store::{
    SqliteDatabasePageSyncDelta, apply_sqlite_batch_to_slate_db_at_version,
    apply_sqlite_op_chunks_to_slate_db_at_version,
};
use crate::state::{SlateDbStateStore, sanitize_path_segment};
use crate::types::{SqliteFileKind, SqliteVfsBatch, SqliteVfsOp};
use crate::{HybridClock, HybridTimestamp};

openraft::declare_raft_types!(
    pub OrionTypeConfig:
        D = OrionRaftRequest,
        R = OrionRaftResponse,
);

pub type OrionNodeId = <OrionTypeConfig as RaftTypeConfig>::NodeId;
pub type OrionNode = <OrionTypeConfig as RaftTypeConfig>::Node;
pub type OrionLogId = openraft::type_config::alias::LogIdOf<OrionTypeConfig>;
pub type OrionVote = openraft::type_config::alias::VoteOf<OrionTypeConfig>;
pub type OrionEntry = openraft::type_config::alias::EntryOf<OrionTypeConfig>;
pub type OrionStoredMembership = openraft::type_config::alias::StoredMembershipOf<OrionTypeConfig>;
pub type OrionSnapshot = openraft::type_config::alias::SnapshotOf<OrionTypeConfig>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrionRaftRequest {
    #[serde(default)]
    pub meta: Option<OrionRaftRequestMeta>,
    #[serde(default)]
    pub sqlite_batches: Vec<SqliteVfsBatch>,
    #[serde(default)]
    pub sqlite_page_deltas: Vec<SqliteDatabasePageDeltaRequest>,
    #[serde(default)]
    pub large_sqlite_page_delta: Option<LargeSqlitePageDeltaRequest>,
    #[serde(default)]
    pub large_sqlite_batch: Option<LargeSqliteBatchRequest>,
}

impl OrionRaftRequest {
    pub fn sqlite_batch(batch: SqliteVfsBatch) -> Self {
        Self {
            meta: Some(OrionRaftRequestMeta::new(HybridClock::global().next())),
            sqlite_batches: vec![batch],
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: None,
        }
    }

    pub fn sqlite_page_delta(
        database: impl Into<String>,
        delta: SqliteDatabasePageSyncDelta,
    ) -> Self {
        Self {
            meta: Some(OrionRaftRequestMeta::new(HybridClock::global().next())),
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: vec![SqliteDatabasePageDeltaRequest {
                database: database.into(),
                delta,
            }],
            large_sqlite_page_delta: None,
            large_sqlite_batch: None,
        }
    }

    pub fn assign_commit_timestamp(mut self, clock: &HybridClock) -> Self {
        let observed = self.meta.as_ref().map(|meta| meta.commit_ts);
        self.meta = Some(OrionRaftRequestMeta::new(clock.next_after(observed)));
        self
    }

    pub fn commit_timestamp(&self) -> Option<HybridTimestamp> {
        self.meta.as_ref().map(|meta| meta.commit_ts)
    }
}

impl fmt::Display for OrionRaftRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.large_sqlite_batch, &self.large_sqlite_page_delta) {
            (Some(large), _) => write!(f, "large sqlite batch {large:?}"),
            (_, Some(large)) => write!(f, "large sqlite page delta {large:?}"),
            (None, None) => write!(
                f,
                "{} sqlite batch(es), {} sqlite page delta(s)",
                self.sqlite_batches.len(),
                self.sqlite_page_deltas.len()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabasePageDeltaRequest {
    pub database: String,
    pub delta: SqliteDatabasePageSyncDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LargeSqlitePageDeltaRequest {
    Begin(LargeSqlitePageDeltaManifest),
    Chunk(LargeSqlitePageDeltaChunk),
    Commit { upload_id: String },
    Abort { upload_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeSqlitePageDeltaManifest {
    pub upload_id: String,
    pub database: String,
    pub min_exclusive_version: u64,
    pub total_chunks: u32,
    pub total_bytes: u64,
    pub metadata_deletes: Vec<Vec<u8>>,
    pub current_page_deletes: Vec<crate::slatedb_sqlite_store::SqliteCurrentPageDeleteRange>,
    #[serde(default)]
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeSqlitePageDeltaChunk {
    pub upload_id: String,
    pub chunk_index: u32,
    pub entries: Vec<crate::slatedb_sqlite_store::SqliteDatabasePageSyncEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LargeSqliteBatchRequest {
    Begin(LargeSqliteBatchManifest),
    Chunk(LargeSqliteBatchChunk),
    Commit { upload_id: String },
    Abort { upload_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeSqliteBatchManifest {
    pub upload_id: String,
    pub database: String,
    pub file_path: String,
    pub file_kind: SqliteFileKind,
    pub total_chunks: u32,
    pub total_bytes: u64,
    #[serde(default)]
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargeSqliteBatchChunk {
    pub upload_id: String,
    pub chunk_index: u32,
    pub ops: Vec<SqliteVfsOp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrionRaftRequestMeta {
    pub commit_ts: HybridTimestamp,
}

impl OrionRaftRequestMeta {
    pub fn new(commit_ts: HybridTimestamp) -> Self {
        Self { commit_ts }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrionRaftResponse {
    #[serde(default)]
    pub sqlite_batches_applied: usize,
    #[serde(default)]
    pub sqlite_page_deltas_applied: usize,
    #[serde(default)]
    pub commit_ts: Option<HybridTimestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargePayloadConfig {
    pub max_staged_uploads: usize,
    pub max_staged_bytes: u64,
    pub staging_ttl_ms: u64,
    pub cleanup_batch_size: usize,
}

impl Default for LargePayloadConfig {
    fn default() -> Self {
        Self {
            max_staged_uploads: 32,
            max_staged_bytes: 512 * 1024 * 1024,
            staging_ttl_ms: 30 * 60 * 1000,
            cleanup_batch_size: 128,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LargePayloadMetrics {
    pub uploads_started: u64,
    pub chunks_staged: u64,
    pub bytes_staged: u64,
    pub uploads_committed: u64,
    pub bytes_committed: u64,
    pub uploads_aborted: u64,
    pub uploads_rejected: u64,
    pub cleanup_runs: u64,
    pub cleanup_uploads: u64,
    pub active_uploads: u64,
    pub active_bytes: u64,
}

const META_CF: &str = "raft_meta";
const LOG_CF: &str = "raft_log";
const META_VOTE: &[u8] = b"vote";
const META_COMMITTED: &[u8] = b"committed";
const META_PURGED: &[u8] = b"purged";
const META_LAST_LOG_ID: &[u8] = b"last_log_id";
const RAFT_STORE_FORMAT_VERSION: u16 = 1;
const SNAPSHOT_MANIFEST_FORMAT_VERSION: u16 = 2;
const MIN_SUPPORTED_SNAPSHOT_MANIFEST_FORMAT_VERSION: u16 = 1;

const SM_LAST_LOG_ID: &[u8] = b"openraft/sm/last_log_id";
const SM_MEMBERSHIP: &[u8] = b"openraft/sm/membership";
const SM_SNAPSHOT: &[u8] = b"openraft/sm/current_snapshot";
const SM_APPLIED_COMMIT_TS: &[u8] = b"openraft/sm/applied_commit_ts";
const STATE_APPLIED_COMMIT_TS: &[u8] = b"sys/ranges/default/applied_commit_ts";
const LARGE_SQLITE_BATCH_PREFIX: &str = "openraft/large_sqlite_batch";
const LARGE_SQLITE_PAGE_DELTA_PREFIX: &str = "openraft/large_sqlite_page_delta";
const LARGE_SQLITE_BATCH_METRICS: &[u8] = b"openraft/large_sqlite_batch_metrics";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OrionSnapshotManifest {
    format_version: u16,
    meta: SnapshotMeta<<OrionEntry as RaftEntry>::CommittedLeaderId, OrionNodeId, OrionNode>,
    slate_db_path: String,
    checkpoint_id: String,
    checkpoint_manifest_id: u64,
    #[serde(default)]
    checkpoint_artifact: Option<SlateDbCheckpointArtifact>,
    sqlite_databases: Vec<OrionSnapshotSqliteDatabase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OrionSnapshotSqliteDatabase {
    database: String,
    slate_db_path: String,
    checkpoint_id: String,
    checkpoint_manifest_id: u64,
    #[serde(default)]
    checkpoint_artifact: Option<SlateDbCheckpointArtifact>,
}

#[derive(Clone)]
pub struct OrionRaftLogStore {
    db: Database,
    meta: Keyspace,
    log: Keyspace,
}

impl OrionRaftLogStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::builder(path)
            .open()
            .context("opening OpenRaft Fjall store")?;
        let meta = db
            .keyspace(META_CF, KeyspaceCreateOptions::default)
            .context("opening OpenRaft Fjall metadata keyspace")?;
        let log = db
            .keyspace(LOG_CF, KeyspaceCreateOptions::default)
            .context("opening OpenRaft Fjall log keyspace")?;
        Ok(Self { db, meta, log })
    }

    fn sync_write(&self, batch: fjall::OwnedWriteBatch) -> io::Result<()> {
        let started = Instant::now();
        let result = batch
            .durability(Some(PersistMode::SyncAll))
            .commit()
            .map_err(to_io_error);
        trace_latency(format_args!(
            "fjall_sync_write elapsed_ms={:.3} ok={}",
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok()
        ));
        result
    }

    fn put_encoded<T: Serialize>(&self, key: &[u8], value: &T) -> io::Result<()> {
        let mut batch = self.db.batch();
        batch.insert(&self.meta, key, encode_store_value(value)?);
        self.sync_write(batch)
    }

    fn get_encoded<T: for<'de> Deserialize<'de>>(&self, key: &[u8]) -> io::Result<Option<T>> {
        self.meta
            .get(key)
            .map_err(to_io_error)?
            .map(|bytes| decode_store_value(bytes.as_ref()))
            .transpose()
    }

    fn sync_optional_log_id(&self, key: &[u8], log_id: Option<&OrionLogId>) -> io::Result<()> {
        let mut batch = self.db.batch();
        match log_id {
            Some(log_id) => batch.insert(&self.meta, key, encode_store_value(log_id)?),
            None => batch.remove(&self.meta, key),
        }
        self.sync_write(batch)
    }

    fn last_present_log_id(&self) -> io::Result<Option<OrionLogId>> {
        self.get_encoded(META_LAST_LOG_ID)
    }
}

impl RaftLogReader<OrionTypeConfig> for OrionRaftLogStore {
    async fn try_get_log_entries<
        RB: RangeBounds<u64> + Clone + fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<OrionEntry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index + 1,
            Bound::Unbounded => 0,
        };
        let end_exclusive = match range.end_bound() {
            Bound::Included(index) => index + 1,
            Bound::Excluded(index) => *index,
            Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        let iter = self.log.range(log_key(start)..);

        for item in iter {
            let (key, value) = item.into_inner().map_err(to_io_error)?;
            let index = index_from_log_key(key.as_ref())?;
            if index >= end_exclusive {
                break;
            }
            entries.push(decode_store_value(value.as_ref())?);
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<OrionVote>, io::Error> {
        self.get_encoded(META_VOTE)
    }
}

impl RaftLogStorage<OrionTypeConfig> for OrionRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<OrionTypeConfig>, io::Error> {
        let last_purged_log_id = self.get_encoded(META_PURGED)?;
        let last_log_id = self
            .last_present_log_id()?
            .or_else(|| last_purged_log_id.clone());
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &OrionVote) -> Result<(), io::Error> {
        self.put_encoded(META_VOTE, vote)
    }

    async fn save_committed(&mut self, committed: Option<OrionLogId>) -> Result<(), io::Error> {
        self.sync_optional_log_id(META_COMMITTED, committed.as_ref())
    }

    async fn read_committed(&mut self) -> Result<Option<OrionLogId>, io::Error> {
        self.get_encoded(META_COMMITTED)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<OrionTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = OrionEntry> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut batch = self.db.batch();
        let mut last_log_id = None;
        for entry in entries {
            last_log_id = Some(entry.log_id());
            batch.insert(
                &self.log,
                log_key(entry.index()),
                encode_store_value(&entry)?,
            );
        }
        if let Some(log_id) = last_log_id {
            batch.insert(&self.meta, META_LAST_LOG_ID, encode_store_value(&log_id)?);
        }
        let result = self.sync_write(batch);
        callback.io_completed(result.as_ref().map(|_| ()).map_err(clone_io_error));
        result
    }

    async fn truncate_after(&mut self, last_log_id: Option<OrionLogId>) -> Result<(), io::Error> {
        let keep_index = last_log_id.as_ref().map(|log_id| log_id.index).unwrap_or(0);
        let mut batch = self.db.batch();
        let iter = self.log.range(log_key(keep_index + 1)..);

        for item in iter {
            let (key, _) = item.into_inner().map_err(to_io_error)?;
            batch.remove(&self.log, key);
        }
        match &last_log_id {
            Some(log_id) => batch.insert(&self.meta, META_LAST_LOG_ID, encode_store_value(log_id)?),
            None => batch.remove(&self.meta, META_LAST_LOG_ID),
        }
        self.sync_write(batch)
    }

    async fn purge(&mut self, log_id: OrionLogId) -> Result<(), io::Error> {
        let mut batch = self.db.batch();
        let iter = self.log.range(log_key(0)..);

        for item in iter {
            let (key, _) = item.into_inner().map_err(to_io_error)?;
            if index_from_log_key(key.as_ref())? > log_id.index {
                break;
            }
            batch.remove(&self.log, key);
        }
        batch.insert(&self.meta, META_PURGED, encode_store_value(&log_id)?);
        if self
            .last_present_log_id()?
            .as_ref()
            .is_some_and(|last| last.index <= log_id.index)
        {
            batch.insert(&self.meta, META_LAST_LOG_ID, encode_store_value(&log_id)?);
        }
        self.sync_write(batch)
    }
}

pub struct OrionRaftStateMachine {
    state: SlateDbStateStore,
    large_payload_config: LargePayloadConfig,
}

#[derive(Debug, Clone, Copy, Default)]
struct LargePayloadPressure {
    uploads: usize,
    bytes: u64,
}

impl OrionRaftStateMachine {
    pub fn new(state: SlateDbStateStore) -> Self {
        Self::with_large_payload_config(state, LargePayloadConfig::default())
    }

    pub fn with_large_payload_config(
        state: SlateDbStateStore,
        large_payload_config: LargePayloadConfig,
    ) -> Self {
        Self {
            state,
            large_payload_config: sanitize_large_payload_config(large_payload_config),
        }
    }

    pub fn new_with_sqlite_cache(
        state: SlateDbStateStore,
        _sqlite_cache_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self::new(state)
    }

    pub fn new_with_sqlite_cache_and_large_payload_config(
        state: SlateDbStateStore,
        _sqlite_cache_root: impl Into<std::path::PathBuf>,
        large_payload_config: LargePayloadConfig,
    ) -> Self {
        Self::with_large_payload_config(state, large_payload_config)
    }

    pub fn state_store(&self) -> SlateDbStateStore {
        self.state.clone()
    }

    async fn last_applied_log_id(&self) -> Result<Option<OrionLogId>, io::Error> {
        DbReadOps::get(self.state.db.as_ref(), SM_LAST_LOG_ID)
            .await
            .map_err(to_io_error)?
            .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
            .transpose()
    }

    async fn stored_membership(&self) -> Result<OrionStoredMembership, io::Error> {
        Ok(DbReadOps::get(self.state.db.as_ref(), SM_MEMBERSHIP)
            .await
            .map_err(to_io_error)?
            .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
            .transpose()?
            .unwrap_or_default())
    }

    pub async fn applied_commit_timestamp(&self) -> anyhow::Result<Option<HybridTimestamp>> {
        DbReadOps::get(self.state.db.as_ref(), SM_APPLIED_COMMIT_TS)
            .await
            .map_err(to_io_error)?
            .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn large_payload_metrics(&self) -> anyhow::Result<LargePayloadMetrics> {
        let mut metrics = self.read_large_payload_metrics().await?;
        let pressure = self.large_payload_pressure().await?;
        metrics.active_uploads = pressure.uploads as u64;
        metrics.active_bytes = pressure.bytes;
        Ok(metrics)
    }

    async fn read_large_payload_metrics(&self) -> io::Result<LargePayloadMetrics> {
        DbReadOps::get(self.state.db.as_ref(), LARGE_SQLITE_BATCH_METRICS)
            .await
            .map_err(to_io_error)?
            .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
            .transpose()
            .map(|metrics| metrics.unwrap_or_default())
    }

    async fn write_large_payload_metrics(&self, metrics: &LargePayloadMetrics) -> io::Result<()> {
        DbWriteOps::put(
            self.state.db.as_ref(),
            LARGE_SQLITE_BATCH_METRICS,
            &crate::codec::to_vec(metrics).map_err(to_io_error)?,
        )
        .await
        .map_err(to_io_error)
        .map(|_| ())
    }

    async fn mutate_large_payload_metrics(
        &self,
        mutate: impl FnOnce(&mut LargePayloadMetrics),
    ) -> io::Result<()> {
        let mut metrics = self.read_large_payload_metrics().await?;
        mutate(&mut metrics);
        self.write_large_payload_metrics(&metrics).await
    }

    async fn persist_metadata(
        &self,
        batch: &mut SlateWriteBatch,
        log_id: &OrionLogId,
        membership: Option<OrionStoredMembership>,
    ) -> Result<(), io::Error> {
        batch.put(
            SM_LAST_LOG_ID,
            crate::codec::to_vec(log_id).map_err(to_io_error)?,
        );
        batch.put(
            SM_APPLIED_COMMIT_TS,
            crate::codec::to_vec(&HybridClock::global().next()).map_err(to_io_error)?,
        );
        if let Some(membership) = membership {
            batch.put(
                SM_MEMBERSHIP,
                crate::codec::to_vec(&membership).map_err(to_io_error)?,
            );
        }
        Ok(())
    }

    async fn apply_sqlite_batch(&self, batch: &SqliteVfsBatch, version: u64) -> io::Result<()> {
        if matches!(batch.file_kind, SqliteFileKind::Temp) {
            return Ok(());
        }
        let started = Instant::now();
        let result = apply_sqlite_batch_to_slate_db_at_version(
            &self.state,
            &convert_sqlite_batch(batch),
            version,
        )
        .await
        .map_err(|err| {
            eprintln!(
                "state machine failed to apply sqlite batch version={} database={} file_path={} file_kind={:?} ops={} error={err:#}",
                version,
                batch.database,
                batch.file_path,
                batch.file_kind,
                batch.ops.len()
            );
            io::Error::new(io::ErrorKind::Other, err.to_string())
        });
        trace_latency(format_args!(
            "state_machine_apply_sqlite file_kind={:?} ops={} elapsed_ms={:.3} ok={}",
            batch.file_kind,
            batch.ops.len(),
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok()
        ));
        result
    }

    async fn apply_sqlite_page_delta(
        &self,
        request: &SqliteDatabasePageDeltaRequest,
    ) -> io::Result<()> {
        self.state
            .apply_sqlite_database_page_delta(&request.database, &request.delta)
            .await
            .map(|_| ())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))
    }

    async fn apply_large_sqlite_batch_request(
        &self,
        request: &LargeSqliteBatchRequest,
        version: u64,
    ) -> io::Result<usize> {
        match request {
            LargeSqliteBatchRequest::Begin(manifest) => {
                let expired = self.cleanup_expired_large_payloads().await?;
                let manifest = if manifest.created_at_ms == 0 {
                    let mut manifest = manifest.clone();
                    manifest.created_at_ms = current_time_millis();
                    manifest
                } else {
                    manifest.clone()
                };
                if manifest.total_chunks == 0 {
                    self.record_large_payload_rejection().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("large SQLite batch {} has no chunks", manifest.upload_id),
                    ));
                }
                self.ensure_large_payload_admitted("large SQLite batch", manifest.total_bytes)
                    .await?;
                self.cleanup_large_sqlite_batch(&manifest.upload_id).await?;
                let mut batch = SlateWriteBatch::new();
                batch.put(
                    large_sqlite_manifest_key(&manifest.upload_id),
                    crate::codec::to_vec(&manifest).map_err(to_io_error)?,
                );
                DbWriteOps::write_with_options(
                    self.state.db.as_ref(),
                    batch,
                    &non_durable_slate_write_options(),
                )
                .await
                .map_err(to_io_error)?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.uploads_started += 1;
                    metrics.cleanup_uploads += expired as u64;
                })
                .await?;
                Ok(0)
            }
            LargeSqliteBatchRequest::Chunk(chunk) => {
                let Some(_manifest) = self.read_large_sqlite_manifest(&chunk.upload_id).await?
                else {
                    self.record_large_payload_rejection().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("missing large SQLite batch manifest {}", chunk.upload_id),
                    ));
                };
                let staged_bytes = sqlite_ops_payload_bytes(&chunk.ops) as u64;
                let mut batch = SlateWriteBatch::new();
                batch.put(
                    large_sqlite_chunk_key(&chunk.upload_id, chunk.chunk_index),
                    crate::codec::to_vec(&chunk.ops).map_err(to_io_error)?,
                );
                DbWriteOps::write_with_options(
                    self.state.db.as_ref(),
                    batch,
                    &non_durable_slate_write_options(),
                )
                .await
                .map_err(to_io_error)?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.chunks_staged += 1;
                    metrics.bytes_staged += staged_bytes;
                })
                .await?;
                Ok(0)
            }
            LargeSqliteBatchRequest::Commit { upload_id } => {
                self.commit_large_sqlite_batch(upload_id, version).await?;
                Ok(1)
            }
            LargeSqliteBatchRequest::Abort { upload_id } => {
                self.cleanup_large_sqlite_batch(upload_id).await?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.uploads_aborted += 1;
                })
                .await?;
                Ok(0)
            }
        }
    }

    async fn apply_large_sqlite_page_delta_request(
        &self,
        request: &LargeSqlitePageDeltaRequest,
    ) -> io::Result<usize> {
        match request {
            LargeSqlitePageDeltaRequest::Begin(manifest) => {
                let expired = self.cleanup_expired_large_payloads().await?;
                let manifest = if manifest.created_at_ms == 0 {
                    let mut manifest = manifest.clone();
                    manifest.created_at_ms = current_time_millis();
                    manifest
                } else {
                    manifest.clone()
                };
                if manifest.total_chunks == 0 {
                    self.record_large_payload_rejection().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "large SQLite page delta {} has no chunks",
                            manifest.upload_id
                        ),
                    ));
                }
                self.ensure_large_payload_admitted("large SQLite page delta", manifest.total_bytes)
                    .await?;
                self.cleanup_large_sqlite_page_delta(&manifest.upload_id)
                    .await?;
                let mut batch = SlateWriteBatch::new();
                batch.put(
                    large_sqlite_page_delta_manifest_key(&manifest.upload_id),
                    crate::codec::to_vec(&manifest).map_err(to_io_error)?,
                );
                DbWriteOps::write_with_options(
                    self.state.db.as_ref(),
                    batch,
                    &non_durable_slate_write_options(),
                )
                .await
                .map_err(to_io_error)?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.uploads_started += 1;
                    metrics.cleanup_uploads += expired as u64;
                })
                .await?;
                Ok(0)
            }
            LargeSqlitePageDeltaRequest::Chunk(chunk) => {
                let Some(_manifest) = self
                    .read_large_sqlite_page_delta_manifest(&chunk.upload_id)
                    .await?
                else {
                    self.record_large_payload_rejection().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "missing large SQLite page delta manifest {}",
                            chunk.upload_id
                        ),
                    ));
                };
                let staged_bytes = sqlite_page_delta_entries_payload_bytes(&chunk.entries) as u64;
                let mut batch = SlateWriteBatch::new();
                batch.put(
                    large_sqlite_page_delta_chunk_key(&chunk.upload_id, chunk.chunk_index),
                    crate::codec::to_vec(&chunk.entries).map_err(to_io_error)?,
                );
                DbWriteOps::write_with_options(
                    self.state.db.as_ref(),
                    batch,
                    &non_durable_slate_write_options(),
                )
                .await
                .map_err(to_io_error)?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.chunks_staged += 1;
                    metrics.bytes_staged += staged_bytes;
                })
                .await?;
                Ok(0)
            }
            LargeSqlitePageDeltaRequest::Commit { upload_id } => {
                self.commit_large_sqlite_page_delta(upload_id).await?;
                Ok(1)
            }
            LargeSqlitePageDeltaRequest::Abort { upload_id } => {
                self.cleanup_large_sqlite_page_delta(upload_id).await?;
                self.mutate_large_payload_metrics(|metrics| {
                    metrics.uploads_aborted += 1;
                })
                .await?;
                Ok(0)
            }
        }
    }

    async fn commit_large_sqlite_batch(&self, upload_id: &str, version: u64) -> io::Result<()> {
        let manifest = self
            .read_large_sqlite_manifest(upload_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("missing large SQLite batch manifest {upload_id}"),
                )
            })?;

        let state_store = self.state.clone();
        let upload_id = upload_id.to_string();
        if manifest.total_chunks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("large SQLite batch {upload_id} has no chunks"),
            ));
        }
        let total_chunks = manifest.total_chunks;
        let expected_total_bytes = manifest.total_bytes;
        let chunks = futures_util::stream::try_unfold((0_u32, 0_u64), move |stream_state| {
            let state_store = state_store.clone();
            let upload_id = upload_id.clone();
            async move {
                let (chunk_index, bytes_seen) = stream_state;
                if chunk_index >= total_chunks {
                    if bytes_seen != expected_total_bytes {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "large SQLite batch {upload_id} byte count mismatch: expected {expected_total_bytes}, got {bytes_seen}"
                            ),
                        ));
                    }
                    return Ok(None);
                }
                let bytes = DbReadOps::get(
                    state_store.db.as_ref(),
                    &large_sqlite_chunk_key(&upload_id, chunk_index),
                )
                .await
                .map_err(to_io_error)?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("missing large SQLite batch chunk {upload_id}/{chunk_index}"),
                    )
                })?;
                let ops: Vec<SqliteVfsOp> =
                    crate::codec::from_bytes(&bytes).map_err(to_io_error)?;
                let chunk_bytes = sqlite_ops_payload_bytes(&ops) as u64;
                let ops = ops
                    .into_iter()
                    .map(convert_sqlite_op)
                    .collect::<Vec<VfsFileOp>>();
                Ok(Some((ops, (chunk_index + 1, bytes_seen + chunk_bytes))))
            }
        });

        apply_sqlite_op_chunks_to_slate_db_at_version(
            &self.state,
            &manifest.database,
            &manifest.file_path,
            convert_sqlite_file_kind(manifest.file_kind),
            chunks,
            version,
        )
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let _ = self.cleanup_large_sqlite_batch(&manifest.upload_id).await;
        self.mutate_large_payload_metrics(|metrics| {
            metrics.uploads_committed += 1;
            metrics.bytes_committed += manifest.total_bytes;
        })
        .await?;
        Ok(())
    }

    async fn commit_large_sqlite_page_delta(&self, upload_id: &str) -> io::Result<()> {
        let manifest = self
            .read_large_sqlite_page_delta_manifest(upload_id)
            .await?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("missing large SQLite page delta manifest {upload_id}"),
                )
            })?;

        if manifest.total_chunks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("large SQLite page delta {upload_id} has no chunks"),
            ));
        }

        let mut entries = Vec::new();
        let mut bytes_seen = 0_u64;
        for chunk_index in 0..manifest.total_chunks {
            let bytes = DbReadOps::get(
                self.state.db.as_ref(),
                &large_sqlite_page_delta_chunk_key(upload_id, chunk_index),
            )
            .await
            .map_err(to_io_error)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("missing large SQLite page delta chunk {upload_id}/{chunk_index}"),
                )
            })?;
            let mut chunk_entries: Vec<crate::slatedb_sqlite_store::SqliteDatabasePageSyncEntry> =
                crate::codec::from_bytes(&bytes).map_err(to_io_error)?;
            bytes_seen = bytes_seen
                .saturating_add(sqlite_page_delta_entries_payload_bytes(&chunk_entries) as u64);
            entries.append(&mut chunk_entries);
        }
        if bytes_seen != manifest.total_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "large SQLite page delta {upload_id} byte count mismatch: expected {}, got {bytes_seen}",
                    manifest.total_bytes
                ),
            ));
        }

        let delta = SqliteDatabasePageSyncDelta {
            min_exclusive_version: manifest.min_exclusive_version,
            entries,
            metadata_deletes: manifest.metadata_deletes.clone(),
            current_page_deletes: manifest.current_page_deletes.clone(),
        };
        self.state
            .apply_sqlite_database_page_delta(&manifest.database, &delta)
            .await
            .map(|_| ())
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let _ = self
            .cleanup_large_sqlite_page_delta(&manifest.upload_id)
            .await;
        self.mutate_large_payload_metrics(|metrics| {
            metrics.uploads_committed += 1;
            metrics.bytes_committed += manifest.total_bytes;
        })
        .await?;
        Ok(())
    }

    async fn read_large_sqlite_manifest(
        &self,
        upload_id: &str,
    ) -> io::Result<Option<LargeSqliteBatchManifest>> {
        DbReadOps::get(
            self.state.db.as_ref(),
            &large_sqlite_manifest_key(upload_id),
        )
        .await
        .map_err(to_io_error)?
        .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
        .transpose()
    }

    async fn read_large_sqlite_page_delta_manifest(
        &self,
        upload_id: &str,
    ) -> io::Result<Option<LargeSqlitePageDeltaManifest>> {
        DbReadOps::get(
            self.state.db.as_ref(),
            &large_sqlite_page_delta_manifest_key(upload_id),
        )
        .await
        .map_err(to_io_error)?
        .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
        .transpose()
    }

    async fn ensure_large_payload_admitted(&self, kind: &str, total_bytes: u64) -> io::Result<()> {
        let pressure = self.large_payload_pressure().await?;
        let max_uploads = self.large_payload_config.max_staged_uploads;
        let max_bytes = self.large_payload_config.max_staged_bytes;
        if pressure.uploads.saturating_add(1) > max_uploads {
            self.record_large_payload_rejection().await?;
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "{kind} admission rejected: active uploads {} exceeds limit {}",
                    pressure.uploads + 1,
                    max_uploads
                ),
            ));
        }
        if pressure.bytes.saturating_add(total_bytes) > max_bytes {
            self.record_large_payload_rejection().await?;
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "{kind} admission rejected: staged bytes {} exceeds limit {}",
                    pressure.bytes.saturating_add(total_bytes),
                    max_bytes
                ),
            ));
        }
        Ok(())
    }

    async fn record_large_payload_rejection(&self) -> io::Result<()> {
        self.mutate_large_payload_metrics(|metrics| {
            metrics.uploads_rejected += 1;
        })
        .await
    }

    async fn large_payload_pressure(&self) -> io::Result<LargePayloadPressure> {
        let mut pressure = LargePayloadPressure::default();
        self.add_large_sqlite_batch_pressure(&mut pressure).await?;
        self.add_large_sqlite_page_delta_pressure(&mut pressure)
            .await?;
        Ok(pressure)
    }

    async fn add_large_sqlite_batch_pressure(
        &self,
        pressure: &mut LargePayloadPressure,
    ) -> io::Result<()> {
        let mut iter = DbReadOps::scan_prefix(
            self.state.db.as_ref(),
            format!("{LARGE_SQLITE_BATCH_PREFIX}/").as_bytes(),
        )
        .await
        .map_err(to_io_error)?;
        while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
            if !large_payload_is_manifest_key(key_value.key.as_ref()) {
                continue;
            }
            let manifest: LargeSqliteBatchManifest =
                crate::codec::from_bytes(&key_value.value).map_err(to_io_error)?;
            pressure.uploads += 1;
            pressure.bytes = pressure.bytes.saturating_add(manifest.total_bytes);
        }
        Ok(())
    }

    async fn add_large_sqlite_page_delta_pressure(
        &self,
        pressure: &mut LargePayloadPressure,
    ) -> io::Result<()> {
        let mut iter = DbReadOps::scan_prefix(
            self.state.db.as_ref(),
            format!("{LARGE_SQLITE_PAGE_DELTA_PREFIX}/").as_bytes(),
        )
        .await
        .map_err(to_io_error)?;
        while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
            if !large_payload_is_manifest_key(key_value.key.as_ref()) {
                continue;
            }
            let manifest: LargeSqlitePageDeltaManifest =
                crate::codec::from_bytes(&key_value.value).map_err(to_io_error)?;
            pressure.uploads += 1;
            pressure.bytes = pressure.bytes.saturating_add(manifest.total_bytes);
        }
        Ok(())
    }

    async fn cleanup_expired_large_payloads(&self) -> io::Result<usize> {
        let ttl_ms = self.large_payload_config.staging_ttl_ms;
        if ttl_ms == 0 {
            return Ok(0);
        }
        let now = current_time_millis();
        let mut expired_batches = Vec::new();
        let mut iter = DbReadOps::scan_prefix(
            self.state.db.as_ref(),
            format!("{LARGE_SQLITE_BATCH_PREFIX}/").as_bytes(),
        )
        .await
        .map_err(to_io_error)?;
        while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
            if expired_batches.len() >= self.large_payload_config.cleanup_batch_size {
                break;
            }
            if !large_payload_is_manifest_key(key_value.key.as_ref()) {
                continue;
            }
            let manifest: LargeSqliteBatchManifest =
                crate::codec::from_bytes(&key_value.value).map_err(to_io_error)?;
            if manifest.created_at_ms > 0 && now.saturating_sub(manifest.created_at_ms) >= ttl_ms {
                expired_batches.push(manifest.upload_id);
            }
        }
        let mut expired_page_deltas = Vec::new();
        if expired_batches.len() < self.large_payload_config.cleanup_batch_size {
            let mut iter = DbReadOps::scan_prefix(
                self.state.db.as_ref(),
                format!("{LARGE_SQLITE_PAGE_DELTA_PREFIX}/").as_bytes(),
            )
            .await
            .map_err(to_io_error)?;
            while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
                if expired_batches.len() + expired_page_deltas.len()
                    >= self.large_payload_config.cleanup_batch_size
                {
                    break;
                }
                if !large_payload_is_manifest_key(key_value.key.as_ref()) {
                    continue;
                }
                let manifest: LargeSqlitePageDeltaManifest =
                    crate::codec::from_bytes(&key_value.value).map_err(to_io_error)?;
                if manifest.created_at_ms > 0
                    && now.saturating_sub(manifest.created_at_ms) >= ttl_ms
                {
                    expired_page_deltas.push(manifest.upload_id);
                }
            }
        }
        if expired_batches.is_empty() && expired_page_deltas.is_empty() {
            return Ok(0);
        }
        for upload_id in &expired_batches {
            self.cleanup_large_sqlite_batch(upload_id).await?;
        }
        for upload_id in &expired_page_deltas {
            self.cleanup_large_sqlite_page_delta(upload_id).await?;
        }
        self.mutate_large_payload_metrics(|metrics| {
            metrics.cleanup_runs += 1;
        })
        .await?;
        Ok(expired_batches.len() + expired_page_deltas.len())
    }

    async fn cleanup_large_sqlite_batch(&self, upload_id: &str) -> io::Result<()> {
        let prefix = large_sqlite_batch_prefix(upload_id);
        let mut iter = DbReadOps::scan_prefix(self.state.db.as_ref(), prefix.as_bytes())
            .await
            .map_err(to_io_error)?;
        let mut batch = SlateWriteBatch::new();
        let mut found = false;
        while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
            found = true;
            batch.delete(key_value.key.as_ref());
        }
        if !found {
            return Ok(());
        }
        DbWriteOps::write_with_options(
            self.state.db.as_ref(),
            batch,
            &non_durable_slate_write_options(),
        )
        .await
        .map_err(to_io_error)?;
        Ok(())
    }

    async fn cleanup_large_sqlite_page_delta(&self, upload_id: &str) -> io::Result<()> {
        let prefix = large_sqlite_page_delta_prefix(upload_id);
        let mut iter = DbReadOps::scan_prefix(self.state.db.as_ref(), prefix.as_bytes())
            .await
            .map_err(to_io_error)?;
        let mut batch = SlateWriteBatch::new();
        let mut found = false;
        while let Some(key_value) = iter.next().await.map_err(to_io_error)? {
            found = true;
            batch.delete(key_value.key.as_ref());
        }
        if !found {
            return Ok(());
        }
        DbWriteOps::write_with_options(
            self.state.db.as_ref(),
            batch,
            &non_durable_slate_write_options(),
        )
        .await
        .map_err(to_io_error)?;
        Ok(())
    }
}

impl RaftStateMachine<OrionTypeConfig> for OrionRaftStateMachine {
    type SnapshotBuilder = OrionSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<OrionLogId>, OrionStoredMembership), io::Error> {
        Ok((
            self.last_applied_log_id().await?,
            self.stored_membership().await?,
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<OrionTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        let mut responses = Vec::new();

        while let Some(next) = entries.next().await {
            let (entry, responder) = next?;
            let log_id = entry.log_id();
            let mut sqlite_batches_applied = 0;
            let mut sqlite_page_deltas_applied = 0;
            let mut commit_ts = None;
            let mut membership_to_store = None;
            let mut batch = SlateWriteBatch::new();

            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(request) => {
                    commit_ts = Some(request.commit_timestamp().unwrap_or_else(|| {
                        let timestamp = HybridClock::global().next();
                        HybridClock::global().observe(timestamp);
                        timestamp
                    }));
                    if let Some(commit_ts) = commit_ts {
                        HybridClock::global().observe(commit_ts);
                    }
                    for sqlite_batch in &request.sqlite_batches {
                        self.apply_sqlite_batch(sqlite_batch, log_id.index).await?;
                        sqlite_batches_applied += 1;
                    }
                    for sqlite_page_delta in &request.sqlite_page_deltas {
                        self.apply_sqlite_page_delta(sqlite_page_delta).await?;
                        sqlite_page_deltas_applied += 1;
                    }
                    if let Some(large_page_delta) = &request.large_sqlite_page_delta {
                        sqlite_page_deltas_applied += self
                            .apply_large_sqlite_page_delta_request(large_page_delta)
                            .await?;
                    }
                    if let Some(large_request) = &request.large_sqlite_batch {
                        sqlite_batches_applied += self
                            .apply_large_sqlite_batch_request(large_request, log_id.index)
                            .await?;
                    }
                }
                EntryPayload::Membership(membership) => {
                    membership_to_store = Some(StoredMembership::new(
                        Some(log_id.clone()),
                        membership.clone(),
                    ));
                }
            }

            self.persist_metadata(&mut batch, &log_id, membership_to_store)
                .await?;
            if let Some(commit_ts) = commit_ts {
                let encoded = crate::codec::to_vec(&commit_ts).map_err(to_io_error)?;
                batch.put(SM_APPLIED_COMMIT_TS, encoded.clone());
                batch.put(STATE_APPLIED_COMMIT_TS, encoded);
            }
            DbWriteOps::write_with_options(
                self.state.db.as_ref(),
                batch,
                &non_durable_slate_write_options(),
            )
            .await
            .map_err(to_io_error)?;

            if let Some(responder) = responder {
                responses.push((
                    responder,
                    OrionRaftResponse {
                        sqlite_batches_applied,
                        sqlite_page_deltas_applied,
                        commit_ts,
                    },
                ));
            }
        }

        for (responder, response) in responses {
            responder.send(response);
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        OrionSnapshotBuilder {
            state: self.state.clone(),
            last_log_id: self.last_applied_log_id().await.ok().flatten(),
            membership: self.stored_membership().await.unwrap_or_default(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<<OrionEntry as RaftEntry>::CommittedLeaderId, OrionNodeId, OrionNode>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let manifest: OrionSnapshotManifest =
            crate::codec::from_bytes(snapshot.get_ref()).map_err(to_io_error)?;

        if !supported_snapshot_manifest_format(manifest.format_version) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported snapshot manifest version {}",
                    manifest.format_version
                ),
            ));
        }
        if &manifest.meta != meta {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot metadata does not match payload",
            ));
        }

        let root_artifact = manifest.checkpoint_artifact.as_ref();
        let checkpoint_id = Uuid::parse_str(
            root_artifact
                .map(|artifact| artifact.checkpoint_id.as_str())
                .unwrap_or(&manifest.checkpoint_id),
        )
        .map_err(to_io_error)?;
        let install_path = format!(
            "{}/installed-snapshot-{}-{}",
            self.state.path,
            sanitize_path_segment(&meta.snapshot_id),
            sanitize_path_segment(
                root_artifact
                    .map(|artifact| artifact.checkpoint_id.as_str())
                    .unwrap_or(&manifest.checkpoint_id),
            )
        );
        AdminBuilder::new(install_path.clone(), Arc::clone(&self.state.object_store))
            .build()
            .create_clone_builder(
                root_artifact
                    .map(|artifact| artifact.db_path.clone())
                    .unwrap_or_else(|| manifest.slate_db_path.clone()),
                Some(checkpoint_id),
            )
            .build()
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        for database in &manifest.sqlite_databases {
            let database_artifact = database.checkpoint_artifact.as_ref();
            let checkpoint_id = Uuid::parse_str(
                database_artifact
                    .map(|artifact| artifact.checkpoint_id.as_str())
                    .unwrap_or(&database.checkpoint_id),
            )
            .map_err(to_io_error)?;
            let install_database_path = format!(
                "{}__sqlite/databases/{}/state",
                install_path,
                sanitize_path_segment(&database.database)
            );
            AdminBuilder::new(install_database_path, Arc::clone(&self.state.object_store))
                .build()
                .create_clone_builder(
                    database_artifact
                        .map(|artifact| artifact.db_path.clone())
                        .unwrap_or_else(|| database.slate_db_path.clone()),
                    Some(checkpoint_id),
                )
                .build()
                .await
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        }
        validate_cloned_snapshot(&install_path, Arc::clone(&self.state.object_store), meta).await?;
        self.state
            .swap_to_path(install_path)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let mut batch = SlateWriteBatch::new();
        match &meta.last_log_id {
            Some(log_id) => batch.put(
                SM_LAST_LOG_ID,
                crate::codec::to_vec(log_id).map_err(to_io_error)?,
            ),
            None => batch.delete(SM_LAST_LOG_ID),
        }
        batch.put(
            SM_MEMBERSHIP,
            crate::codec::to_vec(&meta.last_membership).map_err(to_io_error)?,
        );
        batch.put(SM_SNAPSHOT, snapshot.into_inner());
        DbWriteOps::write_with_options(
            self.state.db.as_ref(),
            batch,
            &non_durable_slate_write_options(),
        )
        .await
        .map_err(to_io_error)?;
        DbWriteOps::flush(self.state.db.as_ref())
            .await
            .map_err(to_io_error)?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<OrionSnapshot>, io::Error> {
        let Some(bytes) = DbReadOps::get(self.state.db.as_ref(), SM_SNAPSHOT)
            .await
            .map_err(to_io_error)?
        else {
            return Ok(None);
        };
        let manifest: OrionSnapshotManifest =
            crate::codec::from_bytes(&bytes).map_err(to_io_error)?;
        if !supported_snapshot_manifest_format(manifest.format_version) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported snapshot manifest version {}",
                    manifest.format_version
                ),
            ));
        }
        Ok(Some(Snapshot {
            meta: manifest.meta,
            snapshot: Cursor::new(bytes.to_vec()),
        }))
    }
}

pub struct OrionSnapshotBuilder {
    state: SlateDbStateStore,
    last_log_id: Option<OrionLogId>,
    membership: OrionStoredMembership,
}

impl RaftSnapshotBuilder<OrionTypeConfig> for OrionSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<OrionSnapshot, io::Error> {
        let snapshot_id = match &self.last_log_id {
            Some(log_id) => format!("{}-{}", log_id.committed_leader_id(), log_id.index),
            None => "empty".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id: self.last_log_id.clone(),
            last_membership: self.membership.clone(),
            snapshot_id,
        };
        DbWriteOps::flush(self.state.db.as_ref())
            .await
            .map_err(to_io_error)?;
        let checkpoint = self
            .state
            .db
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some(format!("openraft-snapshot-{}", meta.snapshot_id)),
                    ..CheckpointOptions::default()
                },
            )
            .await
            .map_err(to_io_error)?;
        let mut sqlite_databases = Vec::new();
        for database in self
            .state
            .list_sqlite_databases()
            .await
            .map_err(anyhow_to_io_error)?
        {
            let database_state = self
                .state
                .sqlite_database_state(&database)
                .await
                .map_err(anyhow_to_io_error)?;
            DbWriteOps::flush(database_state.db.as_ref())
                .await
                .map_err(to_io_error)?;
            let database_checkpoint = database_state
                .db
                .create_checkpoint(
                    CheckpointScope::All,
                    &CheckpointOptions {
                        name: Some(format!(
                            "openraft-snapshot-{}-{}",
                            meta.snapshot_id, database
                        )),
                        ..CheckpointOptions::default()
                    },
                )
                .await
                .map_err(to_io_error)?;
            sqlite_databases.push(OrionSnapshotSqliteDatabase {
                database,
                slate_db_path: database_state.path.clone(),
                checkpoint_id: database_checkpoint.id.to_string(),
                checkpoint_manifest_id: database_checkpoint.manifest_id,
                checkpoint_artifact: Some(SlateDbCheckpointArtifact {
                    db_path: database_state.path.clone(),
                    checkpoint_id: database_checkpoint.id.to_string(),
                    checkpoint_manifest_id: database_checkpoint.manifest_id,
                    object_prefix: database_state.path.clone(),
                }),
            });
        }
        let manifest = OrionSnapshotManifest {
            format_version: SNAPSHOT_MANIFEST_FORMAT_VERSION,
            meta: meta.clone(),
            slate_db_path: self.state.path.clone(),
            checkpoint_id: checkpoint.id.to_string(),
            checkpoint_manifest_id: checkpoint.manifest_id,
            checkpoint_artifact: Some(SlateDbCheckpointArtifact {
                db_path: self.state.path.clone(),
                checkpoint_id: checkpoint.id.to_string(),
                checkpoint_manifest_id: checkpoint.manifest_id,
                object_prefix: self.state.path.clone(),
            }),
            sqlite_databases,
        };
        let bytes = crate::codec::to_vec(&manifest).map_err(to_io_error)?;
        DbWriteOps::put(self.state.db.as_ref(), SM_SNAPSHOT, &bytes)
            .await
            .map_err(to_io_error)?;
        DbWriteOps::flush(self.state.db.as_ref())
            .await
            .map_err(to_io_error)?;
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

fn log_key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

fn index_from_log_key(key: &[u8]) -> io::Result<u64> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid log index"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn to_io_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error)
}

fn anyhow_to_io_error(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error.to_string())
}

fn encode_store_value<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    crate::codec::to_versioned_vec(RAFT_STORE_FORMAT_VERSION, value).map_err(to_io_error)
}

fn decode_store_value<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> io::Result<T> {
    let (version, value): (u16, T) =
        crate::codec::from_versioned_bytes(bytes).map_err(to_io_error)?;
    if version != RAFT_STORE_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported raft store format version {version}"),
        ));
    }
    Ok(value)
}

async fn validate_cloned_snapshot(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    meta: &SnapshotMeta<<OrionEntry as RaftEntry>::CommittedLeaderId, OrionNodeId, OrionNode>,
) -> io::Result<()> {
    let db = slatedb::Db::open(path.to_string(), object_store)
        .await
        .map_err(to_io_error)?;
    let last_log_id: Option<OrionLogId> = DbReadOps::get(&db, SM_LAST_LOG_ID)
        .await
        .map_err(to_io_error)?
        .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
        .transpose()?;
    let membership: OrionStoredMembership = DbReadOps::get(&db, SM_MEMBERSHIP)
        .await
        .map_err(to_io_error)?
        .map(|bytes| crate::codec::from_bytes(&bytes).map_err(to_io_error))
        .transpose()?
        .unwrap_or_default();
    db.close().await.map_err(to_io_error)?;

    if last_log_id != meta.last_log_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cloned snapshot last log id does not match metadata",
        ));
    }
    if membership != meta.last_membership {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cloned snapshot membership does not match metadata",
        ));
    }
    Ok(())
}

fn clone_io_error(error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

fn sanitize_large_payload_config(config: LargePayloadConfig) -> LargePayloadConfig {
    LargePayloadConfig {
        max_staged_uploads: config.max_staged_uploads.max(1),
        max_staged_bytes: config.max_staged_bytes.max(1),
        staging_ttl_ms: config.staging_ttl_ms,
        cleanup_batch_size: config.cleanup_batch_size.max(1),
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn trace_latency(args: std::fmt::Arguments<'_>) {
    if std::env::var_os("ORION_TRACE_LATENCY").is_some() {
        eprintln!("orion latency {args}");
    }
}

fn non_durable_slate_write_options() -> SlateWriteOptions {
    SlateWriteOptions {
        await_durable: false,
        ..SlateWriteOptions::default()
    }
}

fn convert_sqlite_batch(batch: &SqliteVfsBatch) -> VfsSyncBatch {
    VfsSyncBatch {
        database: batch.database.clone(),
        file_path: batch.file_path.clone(),
        file_kind: convert_sqlite_file_kind(batch.file_kind),
        ops: batch.ops.iter().cloned().map(convert_sqlite_op).collect(),
    }
}

fn convert_sqlite_file_kind(file_kind: SqliteFileKind) -> FileKind {
    match file_kind {
        SqliteFileKind::MainDb => FileKind::MainDb,
        SqliteFileKind::Wal => FileKind::Wal,
        SqliteFileKind::Journal => FileKind::Journal,
        SqliteFileKind::Temp => FileKind::Temp,
        SqliteFileKind::Other => FileKind::Other,
    }
}

fn convert_sqlite_op(op: SqliteVfsOp) -> VfsFileOp {
    match op {
        SqliteVfsOp::Write(write) => VfsFileOp::Write(VfsWrite {
            offset: write.offset,
            bytes: write.bytes,
        }),
        SqliteVfsOp::Truncate { size } => VfsFileOp::Truncate { size },
        SqliteVfsOp::Delete => VfsFileOp::Delete,
    }
}

fn sqlite_ops_payload_bytes(ops: &[SqliteVfsOp]) -> usize {
    ops.iter()
        .map(|op| match op {
            SqliteVfsOp::Write(write) => write.bytes.len(),
            SqliteVfsOp::Truncate { .. } | SqliteVfsOp::Delete => 0,
        })
        .sum()
}

fn sqlite_page_delta_entries_payload_bytes(
    entries: &[crate::slatedb_sqlite_store::SqliteDatabasePageSyncEntry],
) -> usize {
    entries
        .iter()
        .map(|entry| entry.key.len().saturating_add(entry.value.len()))
        .sum()
}

fn large_sqlite_batch_prefix(upload_id: &str) -> String {
    format!("{LARGE_SQLITE_BATCH_PREFIX}/{upload_id}/")
}

fn large_sqlite_manifest_key(upload_id: &str) -> Vec<u8> {
    format!("{}manifest", large_sqlite_batch_prefix(upload_id)).into_bytes()
}

fn large_sqlite_page_delta_prefix(upload_id: &str) -> String {
    format!("{LARGE_SQLITE_PAGE_DELTA_PREFIX}/{upload_id}/")
}

fn large_sqlite_page_delta_manifest_key(upload_id: &str) -> Vec<u8> {
    format!("{}manifest", large_sqlite_page_delta_prefix(upload_id)).into_bytes()
}

fn supported_snapshot_manifest_format(format_version: u16) -> bool {
    (MIN_SUPPORTED_SNAPSHOT_MANIFEST_FORMAT_VERSION..=SNAPSHOT_MANIFEST_FORMAT_VERSION)
        .contains(&format_version)
}

pub(crate) fn snapshot_checkpoint_artifacts(
    snapshot_payload: &[u8],
) -> anyhow::Result<Vec<SlateDbCheckpointArtifact>> {
    let manifest: OrionSnapshotManifest = crate::codec::from_bytes(snapshot_payload)?;
    anyhow::ensure!(
        supported_snapshot_manifest_format(manifest.format_version),
        "unsupported snapshot manifest version {}",
        manifest.format_version
    );
    let mut artifacts = Vec::new();
    if let Some(artifact) = manifest.checkpoint_artifact {
        artifacts.push(artifact);
    }
    artifacts.extend(
        manifest
            .sqlite_databases
            .into_iter()
            .filter_map(|database| database.checkpoint_artifact),
    );
    Ok(artifacts)
}

fn large_sqlite_chunk_key(upload_id: &str, chunk_index: u32) -> Vec<u8> {
    format!(
        "{}chunk/{chunk_index:020}",
        large_sqlite_batch_prefix(upload_id)
    )
    .into_bytes()
}

fn large_sqlite_page_delta_chunk_key(upload_id: &str, chunk_index: u32) -> Vec<u8> {
    format!(
        "{}chunk/{chunk_index:020}",
        large_sqlite_page_delta_prefix(upload_id)
    )
    .into_bytes()
}

fn large_payload_is_manifest_key(key: &[u8]) -> bool {
    key.ends_with(b"/manifest")
}

#[cfg(test)]
mod tests {
    use openraft::{Entry, LogId};
    use tempfile::TempDir;

    use super::*;

    fn test_log_id(term: u64, node_id: u64, index: u64) -> OrionLogId {
        LogId::new(
            OrionVote::new_committed(term, node_id).leader_id().clone(),
            index,
        )
    }

    fn test_sqlite_batch(database: &str, path: &str, bytes: &[u8]) -> SqliteVfsBatch {
        SqliteVfsBatch {
            database: database.to_string(),
            file_path: path.to_string(),
            file_kind: SqliteFileKind::Wal,
            ops: vec![SqliteVfsOp::Write(crate::types::SqliteVfsWrite {
                offset: 0,
                bytes: bytes.to_vec(),
            })],
        }
    }

    fn test_entry(index: u64) -> OrionEntry {
        Entry::new_normal(
            test_log_id(1, 1, index),
            OrionRaftRequest::sqlite_batch(test_sqlite_batch(
                "tenant-a",
                "tenant-a.db-wal",
                format!("entry-{index}").as_bytes(),
            )),
        )
    }

    fn test_entry_with_timestamp(index: u64, commit_ts: HybridTimestamp) -> OrionEntry {
        Entry::new_normal(
            test_log_id(1, 1, index),
            OrionRaftRequest {
                meta: Some(OrionRaftRequestMeta::new(commit_ts)),
                sqlite_batches: vec![test_sqlite_batch("tenant-a", "tenant-a.db-wal", b"ts")],
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: None,
                large_sqlite_batch: None,
            },
        )
    }

    fn large_request_entry(index: u64, request: LargeSqliteBatchRequest) -> OrionEntry {
        Entry::new_normal(
            test_log_id(1, 1, index),
            OrionRaftRequest {
                meta: Some(OrionRaftRequestMeta::new(HybridClock::global().next())),
                sqlite_batches: Vec::new(),
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: None,
                large_sqlite_batch: Some(request),
            },
        )
    }

    fn large_page_delta_entry(index: u64, request: LargeSqlitePageDeltaRequest) -> OrionEntry {
        Entry::new_normal(
            test_log_id(1, 1, index),
            OrionRaftRequest {
                meta: Some(OrionRaftRequestMeta::new(HybridClock::global().next())),
                sqlite_batches: Vec::new(),
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: Some(request),
                large_sqlite_batch: None,
            },
        )
    }

    fn large_manifest(
        upload_id: &str,
        total_chunks: u32,
        total_bytes: u64,
    ) -> LargeSqliteBatchManifest {
        LargeSqliteBatchManifest {
            upload_id: upload_id.to_string(),
            database: "tenant-a".to_string(),
            file_path: "tenant-a.db-wal".to_string(),
            file_kind: SqliteFileKind::Wal,
            total_chunks,
            total_bytes,
            created_at_ms: current_time_millis(),
        }
    }

    fn large_chunk(
        upload_id: &str,
        chunk_index: u32,
        offset: u64,
        bytes: &[u8],
    ) -> LargeSqliteBatchChunk {
        LargeSqliteBatchChunk {
            upload_id: upload_id.to_string(),
            chunk_index,
            ops: vec![SqliteVfsOp::Write(crate::types::SqliteVfsWrite {
                offset,
                bytes: bytes.to_vec(),
            })],
        }
    }

    fn large_page_delta_manifest(
        upload_id: &str,
        delta: &SqliteDatabasePageSyncDelta,
        total_chunks: u32,
    ) -> LargeSqlitePageDeltaManifest {
        LargeSqlitePageDeltaManifest {
            upload_id: upload_id.to_string(),
            database: "tenant-a".to_string(),
            min_exclusive_version: delta.min_exclusive_version,
            total_chunks,
            total_bytes: sqlite_page_delta_entries_payload_bytes(&delta.entries) as u64,
            metadata_deletes: delta.metadata_deletes.clone(),
            current_page_deletes: delta.current_page_deletes.clone(),
            created_at_ms: current_time_millis(),
        }
    }

    #[tokio::test]
    async fn openraft_log_store_persists_entries_vote_and_commit() {
        let dir = TempDir::new().unwrap();
        let mut store = OrionRaftLogStore::open(dir.path()).unwrap();
        let vote = OrionVote::new_committed(1, 1);

        store.save_vote(&vote).await.unwrap();
        store
            .append([test_entry(1)], IOFlushed::noop())
            .await
            .unwrap();
        store
            .save_committed(Some(test_log_id(1, 1, 1)))
            .await
            .unwrap();
        drop(store);

        let mut reopened = OrionRaftLogStore::open(dir.path()).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        assert_eq!(
            reopened.read_committed().await.unwrap(),
            Some(test_log_id(1, 1, 1))
        );
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_log_id,
            Some(test_log_id(1, 1, 1))
        );
        assert_eq!(reopened.try_get_log_entries(1..2).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn openraft_log_store_truncates_and_purges_logs() {
        let dir = TempDir::new().unwrap();
        let mut store = OrionRaftLogStore::open(dir.path()).unwrap();
        store
            .append(
                [test_entry(1), test_entry(2), test_entry(3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(test_log_id(1, 1, 3))
        );

        store
            .truncate_after(Some(test_log_id(1, 1, 2)))
            .await
            .unwrap();
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(test_log_id(1, 1, 2))
        );
        assert_eq!(store.try_get_log_entries(1..4).await.unwrap().len(), 2);

        store.purge(test_log_id(1, 1, 2)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(test_log_id(1, 1, 2)));
        assert_eq!(state.last_log_id, Some(test_log_id(1, 1, 2)));
        assert!(store.try_get_log_entries(1..4).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn openraft_state_machine_applies_normal_entries_to_slate_db() {
        let state = SlateDbStateStore::open_in_memory("openraft-sm")
            .await
            .unwrap();
        let mut machine = OrionRaftStateMachine::new(state);
        let commit_ts = HybridTimestamp {
            physical_ms: 4_000,
            logical: 7,
        };
        let entry = test_entry_with_timestamp(1, commit_ts);

        let stream = futures_util::stream::iter([Ok((entry, None))]);
        machine.apply(stream).await.unwrap();

        assert_eq!(
            machine.applied_state().await.unwrap().0,
            Some(test_log_id(1, 1, 1))
        );
        assert_eq!(
            machine.applied_commit_timestamp().await.unwrap(),
            Some(commit_ts)
        );
    }

    #[tokio::test]
    async fn openraft_state_machine_applies_sqlite_vfs_batches_to_materialized_files() {
        let state = SlateDbStateStore::open_in_memory("openraft-sqlite-sm")
            .await
            .unwrap();
        let database_state = state.sqlite_database_state("tenant-a").await.unwrap();
        let store =
            crate::slatedb_sqlite_store::SlateDbSqliteFileStore::new(&database_state, "tenant-a");
        let sqlite_dir = TempDir::new().unwrap();
        let mut machine = OrionRaftStateMachine::new_with_sqlite_cache(state, sqlite_dir.path());
        let entry = Entry::new_normal(
            test_log_id(1, 1, 1),
            OrionRaftRequest::sqlite_batch(SqliteVfsBatch {
                database: "tenant-a".to_string(),
                file_path: "tenant-a.db-wal".to_string(),
                file_kind: SqliteFileKind::Wal,
                ops: vec![SqliteVfsOp::Write(crate::types::SqliteVfsWrite {
                    offset: 16,
                    bytes: b"wal-frame".to_vec(),
                })],
            }),
        );

        machine
            .apply(futures_util::stream::iter([Ok((entry, None))]))
            .await
            .unwrap();

        let bytes = store.read_file("tenant-a.db-wal").await.unwrap();
        assert_eq!(&bytes[16..25], b"wal-frame");
    }

    #[tokio::test]
    async fn large_payload_admission_rejects_too_many_active_uploads() {
        let state = SlateDbStateStore::open_in_memory("openraft-large-admission")
            .await
            .unwrap();
        let mut machine = OrionRaftStateMachine::with_large_payload_config(
            state,
            LargePayloadConfig {
                max_staged_uploads: 1,
                max_staged_bytes: 1024,
                staging_ttl_ms: 60_000,
                cleanup_batch_size: 8,
            },
        );

        machine
            .apply(futures_util::stream::iter([Ok((
                large_request_entry(
                    1,
                    LargeSqliteBatchRequest::Begin(large_manifest("upload-a", 1, 40)),
                ),
                None,
            ))]))
            .await
            .unwrap();
        let error = machine
            .apply(futures_util::stream::iter([Ok((
                large_request_entry(
                    2,
                    LargeSqliteBatchRequest::Begin(large_manifest("upload-b", 1, 40)),
                ),
                None,
            ))]))
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("admission rejected"));
        let metrics = machine.large_payload_metrics().await.unwrap();
        assert_eq!(metrics.uploads_started, 1);
        assert_eq!(metrics.uploads_rejected, 1);
        assert_eq!(metrics.active_uploads, 1);
        assert_eq!(metrics.active_bytes, 40);
    }

    #[tokio::test]
    async fn large_payload_begin_cleans_expired_uploads() {
        let state = SlateDbStateStore::open_in_memory("openraft-large-ttl")
            .await
            .unwrap();
        let mut machine = OrionRaftStateMachine::with_large_payload_config(
            state,
            LargePayloadConfig {
                max_staged_uploads: 8,
                max_staged_bytes: 1024,
                staging_ttl_ms: 1_000,
                cleanup_batch_size: 80,
            },
        );
        let mut expired = large_manifest("expired", 1, 40);
        expired.created_at_ms = 1_000;
        let fresh = large_manifest("fresh", 1, 40);

        machine
            .apply(futures_util::stream::iter([Ok((
                large_request_entry(1, LargeSqliteBatchRequest::Begin(expired)),
                None,
            ))]))
            .await
            .unwrap();
        machine
            .apply(futures_util::stream::iter([Ok((
                large_request_entry(2, LargeSqliteBatchRequest::Begin(fresh)),
                None,
            ))]))
            .await
            .unwrap();

        let metrics = machine.large_payload_metrics().await.unwrap();
        assert_eq!(metrics.uploads_started, 2);
        assert_eq!(metrics.cleanup_runs, 1);
        assert_eq!(metrics.cleanup_uploads, 1);
        assert_eq!(metrics.active_uploads, 1);
        assert_eq!(metrics.active_bytes, 40);
    }

    #[tokio::test]
    async fn large_payload_metrics_track_successful_commit() {
        let state = SlateDbStateStore::open_in_memory("openraft-large-metrics")
            .await
            .unwrap();
        let database_state = state.sqlite_database_state("tenant-a").await.unwrap();
        let store =
            crate::slatedb_sqlite_store::SlateDbSqliteFileStore::new(&database_state, "tenant-a");
        let mut machine = OrionRaftStateMachine::new(state);
        let upload_id = "commit-upload";

        machine
            .apply(futures_util::stream::iter([
                Ok((
                    large_request_entry(
                        1,
                        LargeSqliteBatchRequest::Begin(large_manifest(upload_id, 2, 6)),
                    ),
                    None,
                )),
                Ok((
                    large_request_entry(
                        2,
                        LargeSqliteBatchRequest::Chunk(large_chunk(upload_id, 0, 0, b"abc")),
                    ),
                    None,
                )),
                Ok((
                    large_request_entry(
                        3,
                        LargeSqliteBatchRequest::Chunk(large_chunk(upload_id, 1, 3, b"def")),
                    ),
                    None,
                )),
                Ok((
                    large_request_entry(
                        4,
                        LargeSqliteBatchRequest::Commit {
                            upload_id: upload_id.to_string(),
                        },
                    ),
                    None,
                )),
            ]))
            .await
            .unwrap();

        assert_eq!(store.read_file("tenant-a.db-wal").await.unwrap(), b"abcdef");
        let metrics = machine.large_payload_metrics().await.unwrap();
        assert_eq!(metrics.uploads_started, 1);
        assert_eq!(metrics.chunks_staged, 2);
        assert_eq!(metrics.bytes_staged, 6);
        assert_eq!(metrics.uploads_committed, 1);
        assert_eq!(metrics.bytes_committed, 6);
        assert_eq!(metrics.active_uploads, 0);
        assert_eq!(metrics.active_bytes, 0);
    }

    #[tokio::test]
    async fn large_page_delta_commit_is_staged_through_raft_state_machine() {
        let source = SlateDbStateStore::open_in_memory("openraft-large-page-delta-source")
            .await
            .unwrap();
        let source_database = source.sqlite_database_state("tenant-a").await.unwrap();
        let source_store =
            crate::slatedb_sqlite_store::SlateDbSqliteFileStore::new(&source_database, "tenant-a");
        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![7; 4096 * 2],
                    })],
                },
                9,
            )
            .await
            .unwrap();
        let delta = source
            .export_sqlite_database_pages_since("tenant-a", 0)
            .await
            .unwrap();
        assert!(delta.entries.len() > 1);

        let target = SlateDbStateStore::open_in_memory("openraft-large-page-delta-target")
            .await
            .unwrap();
        let target_database = target.sqlite_database_state("tenant-a").await.unwrap();
        let target_store =
            crate::slatedb_sqlite_store::SlateDbSqliteFileStore::new(&target_database, "tenant-a");
        let mut machine = OrionRaftStateMachine::new(target);
        let upload_id = "page-delta-upload";
        let midpoint = delta.entries.len() / 2;
        let chunks = [
            delta.entries[..midpoint].to_vec(),
            delta.entries[midpoint..].to_vec(),
        ];

        machine
            .apply(futures_util::stream::iter([
                Ok((
                    large_page_delta_entry(
                        1,
                        LargeSqlitePageDeltaRequest::Begin(large_page_delta_manifest(
                            upload_id,
                            &delta,
                            chunks.len() as u32,
                        )),
                    ),
                    None,
                )),
                Ok((
                    large_page_delta_entry(
                        2,
                        LargeSqlitePageDeltaRequest::Chunk(LargeSqlitePageDeltaChunk {
                            upload_id: upload_id.to_string(),
                            chunk_index: 0,
                            entries: chunks[0].clone(),
                        }),
                    ),
                    None,
                )),
                Ok((
                    large_page_delta_entry(
                        3,
                        LargeSqlitePageDeltaRequest::Chunk(LargeSqlitePageDeltaChunk {
                            upload_id: upload_id.to_string(),
                            chunk_index: 1,
                            entries: chunks[1].clone(),
                        }),
                    ),
                    None,
                )),
                Ok((
                    large_page_delta_entry(
                        4,
                        LargeSqlitePageDeltaRequest::Commit {
                            upload_id: upload_id.to_string(),
                        },
                    ),
                    None,
                )),
            ]))
            .await
            .unwrap();

        assert_eq!(
            target_store.read_file("main.db").await.unwrap(),
            vec![7; 4096 * 2]
        );
        let metrics = machine.large_payload_metrics().await.unwrap();
        assert_eq!(metrics.uploads_started, 1);
        assert_eq!(metrics.chunks_staged, 2);
        assert_eq!(metrics.uploads_committed, 1);
        assert_eq!(metrics.active_uploads, 0);
        assert_eq!(metrics.active_bytes, 0);
    }

    #[tokio::test]
    async fn snapshot_manifest_clones_slate_checkpoint_and_restores_metadata() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let source_state = SlateDbStateStore::open("snapshot-source", Arc::clone(&object_store))
            .await
            .unwrap();
        let mut source = OrionRaftStateMachine::new(source_state);
        let entry = test_entry(3);
        source
            .apply(futures_util::stream::iter([Ok((entry, None))]))
            .await
            .unwrap();
        let sqlite_entry = Entry::new_normal(
            test_log_id(1, 1, 4),
            OrionRaftRequest::sqlite_batch(SqliteVfsBatch {
                database: "tenant-a".to_string(),
                file_path: "tenant-a.db-wal".to_string(),
                file_kind: SqliteFileKind::Wal,
                ops: vec![SqliteVfsOp::Write(crate::types::SqliteVfsWrite {
                    offset: 0,
                    bytes: b"snapshot-wal".to_vec(),
                })],
            }),
        );
        source
            .apply(futures_util::stream::iter([Ok((sqlite_entry, None))]))
            .await
            .unwrap();

        let mut builder = source.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let manifest: OrionSnapshotManifest =
            crate::codec::from_bytes(snapshot.snapshot.get_ref()).unwrap();
        assert_eq!(manifest.format_version, SNAPSHOT_MANIFEST_FORMAT_VERSION);
        assert_eq!(manifest.slate_db_path, "snapshot-source");
        assert_eq!(manifest.meta, snapshot.meta);
        assert_eq!(
            manifest
                .checkpoint_artifact
                .as_ref()
                .map(|artifact| artifact.db_path.as_str()),
            Some("snapshot-source")
        );
        assert_eq!(manifest.sqlite_databases.len(), 1);
        assert_eq!(manifest.sqlite_databases[0].database, "tenant-a");
        assert_eq!(
            manifest.sqlite_databases[0]
                .checkpoint_artifact
                .as_ref()
                .map(|artifact| artifact.db_path.as_str()),
            Some("snapshot-source__sqlite/databases/tenant-a/state")
        );
        assert!(snapshot.snapshot.get_ref().len() < 512);

        let target_state = SlateDbStateStore::open("snapshot-target", object_store)
            .await
            .unwrap();
        let mut target = OrionRaftStateMachine::new(target_state);
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert_eq!(
            target.applied_state().await.unwrap().0,
            Some(test_log_id(1, 1, 4))
        );
        assert!(target.get_current_snapshot().await.unwrap().is_some());
        let restored_database_state = target
            .state
            .sqlite_database_state("tenant-a")
            .await
            .unwrap();
        let restored_store = crate::slatedb_sqlite_store::SlateDbSqliteFileStore::new(
            &restored_database_state,
            "tenant-a",
        );
        let restored_wal = restored_store.read_file("tenant-a.db-wal").await.unwrap();
        assert_eq!(&restored_wal[..12], b"snapshot-wal");
    }
}
