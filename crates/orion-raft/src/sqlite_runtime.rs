use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::ReadPolicy;
use orion_sqlite::{OrionVfs, OrionVfsConfig, register_orion_vfs};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use slatedb::object_store::ObjectStore;
use uuid::Uuid;

use crate::checkpoint_artifact::{SlateDbCheckpointArtifact, SlateDbCheckpointMaterializeMetrics};
use crate::openraft_store::{
    LargeSqlitePageDeltaChunk, LargeSqlitePageDeltaManifest, LargeSqlitePageDeltaRequest,
};
use crate::raft_metrics::RaftMetricsSnapshot;
use crate::slatedb_sqlite_store::{
    SlateDbSqliteFileStore, SqliteDatabaseFileSnapshot, SqliteDatabasePageSyncDelta,
    SqliteDatabasePageSyncEntry, SqliteDatabasePageSyncMetrics, SqlitePageCompactionMetrics,
};
use crate::sqlite_commit_sink::{
    DEFAULT_LARGE_BATCH_CHUNK_BYTES, DEFAULT_LARGE_BATCH_THRESHOLD_BYTES, LargeBatchOptions,
    OpenRaftSqliteCommitSink,
};
use crate::sqlite_raft_client::OrionSqliteRaftClient;
use crate::state::SlateDbStateStore;
use crate::tonic_transport::OrionRaft;
use crate::{
    HybridClock, HybridTimestamp, LargePayloadMetrics, OrionRaftRequest, OrionRaftRequestMeta,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrionSqliteReplicaFreshness {
    pub raft: RaftMetricsSnapshot,
    pub applied_commit_ts: Option<HybridTimestamp>,
    pub closed_commit_ts: Option<HybridTimestamp>,
    pub staleness_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrionSqliteRuntimeWatermark {
    pub applied_index: Option<u64>,
    pub applied_commit_ts: Option<HybridTimestamp>,
}

impl OrionSqliteReplicaFreshness {
    pub fn can_serve_at_timestamp(&self, read_ts: HybridTimestamp) -> bool {
        self.raft.is_ready_for_linearizable_reads()
            && self
                .closed_commit_ts
                .is_some_and(|closed_ts| closed_ts >= read_ts)
            && self
                .applied_commit_ts
                .is_some_and(|applied_ts| applied_ts >= read_ts)
    }
}

fn large_sqlite_page_delta_request(request: LargeSqlitePageDeltaRequest) -> OrionRaftRequest {
    OrionRaftRequest {
        meta: Some(OrionRaftRequestMeta::new(HybridClock::global().next())),
        sqlite_batches: Vec::new(),
        sqlite_page_deltas: Vec::new(),
        large_sqlite_page_delta: Some(request),
        large_sqlite_batch: None,
    }
}

async fn abort_large_sqlite_page_delta(raft_client: &OrionSqliteRaftClient, upload_id: &str) {
    let abort = large_sqlite_page_delta_request(LargeSqlitePageDeltaRequest::Abort {
        upload_id: upload_id.to_string(),
    });
    let _ = raft_client.propose(abort).await;
}

fn sqlite_page_delta_payload_bytes(delta: &SqliteDatabasePageSyncDelta) -> usize {
    sqlite_page_delta_entries_payload_bytes(&delta.entries)
        .saturating_add(delta.metadata_deletes.iter().map(Vec::len).sum::<usize>())
        .saturating_add(
            delta
                .current_page_deletes
                .iter()
                .map(|delete| delete.current_pages_prefix.len().saturating_add(8))
                .sum::<usize>(),
        )
}

fn sqlite_page_delta_entries_payload_bytes(entries: &[SqliteDatabasePageSyncEntry]) -> usize {
    entries
        .iter()
        .map(|entry| entry.key.len().saturating_add(entry.value.len()))
        .sum()
}

fn split_page_delta_entries(
    entries: Vec<SqliteDatabasePageSyncEntry>,
    target_chunk_bytes: usize,
) -> Vec<Vec<SqliteDatabasePageSyncEntry>> {
    let target_chunk_bytes = target_chunk_bytes.max(1);
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes: usize = 0;

    for entry in entries {
        let entry_bytes = entry.key.len().saturating_add(entry.value.len());
        if !current.is_empty() && current_bytes.saturating_add(entry_bytes) > target_chunk_bytes {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(entry);
        current_bytes = current_bytes.saturating_add(entry_bytes);
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    if chunks.is_empty() {
        chunks.push(Vec::new());
    }
    chunks
}

#[derive(Debug, Clone)]
pub struct OrionSqliteRuntimeConfig {
    pub cache_root: PathBuf,
    pub large_batch_threshold_bytes: usize,
    pub large_batch_chunk_bytes: usize,
    pub large_payload_max_staged_uploads: usize,
    pub large_payload_max_staged_bytes: u64,
    pub large_payload_staging_ttl_ms: u64,
    pub large_payload_cleanup_batch_size: usize,
}

impl OrionSqliteRuntimeConfig {
    pub fn new(cache_root: PathBuf) -> Self {
        Self {
            cache_root,
            large_batch_threshold_bytes: DEFAULT_LARGE_BATCH_THRESHOLD_BYTES,
            large_batch_chunk_bytes: DEFAULT_LARGE_BATCH_CHUNK_BYTES,
            large_payload_max_staged_uploads: crate::LargePayloadConfig::default()
                .max_staged_uploads,
            large_payload_max_staged_bytes: crate::LargePayloadConfig::default().max_staged_bytes,
            large_payload_staging_ttl_ms: crate::LargePayloadConfig::default().staging_ttl_ms,
            large_payload_cleanup_batch_size: crate::LargePayloadConfig::default()
                .cleanup_batch_size,
        }
    }

    fn large_batch_options(&self) -> LargeBatchOptions {
        LargeBatchOptions {
            threshold_bytes: self.large_batch_threshold_bytes,
            chunk_bytes: self.large_batch_chunk_bytes,
        }
    }
}

pub const ORION_SYSTEM_DATABASE: &str = "_orion";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrionCompactionControl {
    pub paused: bool,
    pub force_requested: bool,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrionCompactionLease {
    pub name: String,
    pub owner_node_id: u64,
    pub epoch: u64,
    pub acquired_at_ms: u64,
    pub renewed_at_ms: u64,
    pub expires_at_ms: u64,
    pub last_compacted_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrionCompactionRetentionFloor {
    pub min_retained_version: Option<u64>,
    pub reason: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Clone)]
pub struct OrionSqliteRuntime {
    raft: OrionRaft,
    state: SlateDbStateStore,
    config: OrionSqliteRuntimeConfig,
}

impl OrionSqliteRuntime {
    pub fn new(
        raft: OrionRaft,
        state: SlateDbStateStore,
        config: OrionSqliteRuntimeConfig,
    ) -> Self {
        Self {
            raft,
            state,
            config,
        }
    }

    pub fn open_database(&self, database: impl Into<String>) -> anyhow::Result<OrionSqliteDb> {
        let database = database.into();
        let database_state = crate::slatedb_sqlite_store::block_on_store(
            self.state.sqlite_database_state(&database),
        )?;
        self.open_database_with_state(database, database_state)
    }

    pub fn open_existing_database(
        &self,
        database: impl Into<String>,
    ) -> anyhow::Result<Option<OrionSqliteDb>> {
        let database = database.into();
        let Some(database_state) = crate::slatedb_sqlite_store::block_on_store(
            self.state.existing_sqlite_database_state(&database),
        )?
        else {
            return Ok(None);
        };
        self.open_database_with_state(database, database_state)
            .map(Some)
    }

    fn open_database_with_state(
        &self,
        database: String,
        database_state: SlateDbStateStore,
    ) -> anyhow::Result<OrionSqliteDb> {
        let file_store = Arc::new(SlateDbSqliteFileStore::new(
            &database_state,
            database.clone(),
        ));
        let sink = Arc::new(OpenRaftSqliteCommitSink::with_large_batch_options(
            Some(self.raft.clone()),
            self.config.large_batch_options(),
        ));
        let vfs_name = unique_vfs_name("orion");
        let vfs = OrionVfs::with_file_store(
            OrionVfsConfig::new(database.clone(), self.config.cache_root.join(&database)),
            sink,
            file_store,
        )?;
        register_orion_vfs(&vfs_name, vfs, false)?;
        Ok(OrionSqliteDb { database, vfs_name })
    }

    pub fn open_system_database(&self) -> anyhow::Result<OrionSqliteDb> {
        self.open_database(ORION_SYSTEM_DATABASE)
    }

    pub async fn ensure_linearizable_read(&self) -> anyhow::Result<RaftMetricsSnapshot> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|error| {
                anyhow::anyhow!("could not perform linearizable SQLite read: {error}")
            })?;
        Ok(self.metrics())
    }

    pub fn metrics(&self) -> RaftMetricsSnapshot {
        RaftMetricsSnapshot::observe(&self.raft)
    }

    pub async fn large_payload_metrics(&self) -> anyhow::Result<LargePayloadMetrics> {
        self.raft
            .with_state_machine(|sm| Box::pin(async move { sm.large_payload_metrics().await }))
            .await
            .map_err(|error| anyhow::anyhow!("could not read large payload metrics: {error}"))?
    }

    pub fn durability_watermark(&self) -> anyhow::Result<OrionSqliteRuntimeWatermark> {
        Ok(OrionSqliteRuntimeWatermark {
            applied_index: self.metrics().applied_index,
            applied_commit_ts: crate::slatedb_sqlite_store::block_on_store(
                self.state.applied_commit_timestamp(),
            )?,
        })
    }

    pub fn state_store(&self) -> SlateDbStateStore {
        self.state.clone()
    }

    pub fn clone_database_from(
        &self,
        database: &str,
        source: &OrionSqliteRuntime,
    ) -> anyhow::Result<()> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .clone_sqlite_database_from(database, &source.state_store()),
        )?;
        Ok(())
    }

    pub fn sync_database_pages_from(
        &self,
        database: &str,
        source: &OrionSqliteRuntime,
        min_exclusive_version: u64,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            crate::slatedb_sqlite_store::sync_sqlite_database_pages_since(
                &self.state,
                database,
                &source.state_store(),
                min_exclusive_version,
            ),
        )
    }

    pub fn export_database_pages_since(
        &self,
        database: &str,
        min_exclusive_version: u64,
    ) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .export_sqlite_database_pages_since(database, min_exclusive_version),
        )
    }

    pub fn export_database_live_snapshot(
        &self,
        database: &str,
    ) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state.export_sqlite_database_live_snapshot(database),
        )
    }

    pub fn apply_database_page_delta(
        &self,
        database: &str,
        delta: &SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state.apply_sqlite_database_page_delta(database, delta),
        )
    }

    pub fn materialize_database_live_snapshot_from(
        &self,
        database: &str,
        source: &OrionSqliteRuntime,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .materialize_sqlite_database_live_snapshot(database, &source.state_store()),
        )
    }

    pub fn materialize_database_live_snapshot_delta(
        &self,
        database: &str,
        delta: &SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .materialize_sqlite_database_live_snapshot_delta(database, delta),
        )
    }

    pub fn export_database_file_snapshot(
        &self,
        database: &str,
        target_chunk_bytes: usize,
    ) -> anyhow::Result<SqliteDatabaseFileSnapshot> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .export_sqlite_database_file_snapshot(database, target_chunk_bytes),
        )
    }

    pub fn materialize_database_file_snapshot(
        &self,
        database: &str,
        snapshot: &SqliteDatabaseFileSnapshot,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .materialize_sqlite_database_file_snapshot(database, snapshot),
        )
    }

    pub fn apply_database_page_delta_through_raft(
        &self,
        database: &str,
        delta: SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<Option<u64>> {
        if sqlite_page_delta_payload_bytes(&delta) > self.config.large_batch_threshold_bytes {
            return self.apply_large_database_page_delta_through_raft(database, delta);
        }
        let request = OrionRaftRequest::sqlite_page_delta(database.to_string(), delta);
        crate::slatedb_sqlite_store::block_on_store(async move {
            OrionSqliteRaftClient::new(Some(self.raft.clone()))
                .propose(request)
                .await
                .map_err(|error| anyhow::anyhow!(error))
        })
    }

    fn apply_large_database_page_delta_through_raft(
        &self,
        database: &str,
        delta: SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<Option<u64>> {
        let upload_id = Uuid::new_v4().to_string();
        let chunks = split_page_delta_entries(delta.entries, self.config.large_batch_chunk_bytes);
        let total_bytes = chunks
            .iter()
            .map(|chunk| sqlite_page_delta_entries_payload_bytes(chunk) as u64)
            .sum();
        let manifest = LargeSqlitePageDeltaManifest {
            upload_id: upload_id.clone(),
            database: database.to_string(),
            min_exclusive_version: delta.min_exclusive_version,
            total_chunks: chunks.len() as u32,
            total_bytes,
            metadata_deletes: delta.metadata_deletes,
            current_page_deletes: delta.current_page_deletes,
            created_at_ms: current_time_millis(),
        };
        crate::slatedb_sqlite_store::block_on_store(async move {
            let raft_client = OrionSqliteRaftClient::new(Some(self.raft.clone()));
            let begin =
                large_sqlite_page_delta_request(LargeSqlitePageDeltaRequest::Begin(manifest));
            raft_client
                .propose(begin)
                .await
                .map_err(|error| anyhow::anyhow!(error))?;

            for (chunk_index, entries) in chunks.into_iter().enumerate() {
                let chunk = large_sqlite_page_delta_request(LargeSqlitePageDeltaRequest::Chunk(
                    LargeSqlitePageDeltaChunk {
                        upload_id: upload_id.clone(),
                        chunk_index: chunk_index as u32,
                        entries,
                    },
                ));
                if let Err(error) = raft_client.propose(chunk).await {
                    abort_large_sqlite_page_delta(&raft_client, &upload_id).await;
                    return Err(anyhow::anyhow!(error));
                }
            }

            let commit = large_sqlite_page_delta_request(LargeSqlitePageDeltaRequest::Commit {
                upload_id: upload_id.clone(),
            });
            match raft_client.propose(commit).await {
                Ok(index) => Ok(index),
                Err(error) => {
                    abort_large_sqlite_page_delta(&raft_client, &upload_id).await;
                    Err(anyhow::anyhow!(error))
                }
            }
        })
    }

    pub fn database_checkpoint_artifact(
        &self,
        database: &str,
        name: impl Into<String>,
    ) -> anyhow::Result<SlateDbCheckpointArtifact> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .sqlite_database_checkpoint_artifact(database, name),
        )
    }

    pub fn materialize_database_checkpoint_incremental(
        &self,
        database: &str,
        artifact: &SlateDbCheckpointArtifact,
        source_object_store: Arc<dyn ObjectStore>,
    ) -> anyhow::Result<SlateDbCheckpointMaterializeMetrics> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .materialize_sqlite_database_checkpoint_incremental(
                    database,
                    artifact,
                    source_object_store,
                ),
        )
    }

    pub fn clone_database_checkpoint_from_local_objects(
        &self,
        database: &str,
        artifact: &SlateDbCheckpointArtifact,
    ) -> anyhow::Result<()> {
        crate::slatedb_sqlite_store::block_on_store(
            self.state
                .clone_sqlite_database_checkpoint_from_local_objects(database, artifact),
        )
    }

    pub fn state_store_path(&self) -> &str {
        self.state.path()
    }

    pub fn mark_database_ready(&self, database: &str) -> anyhow::Result<()> {
        crate::slatedb_sqlite_store::block_on_store(self.state.mark_sqlite_database_ready(database))
    }

    pub fn database_ready(&self, database: &str) -> anyhow::Result<bool> {
        crate::slatedb_sqlite_store::block_on_store(self.state.sqlite_database_ready(database))
    }

    pub async fn applied_commit_timestamp(&self) -> anyhow::Result<Option<HybridTimestamp>> {
        self.state.applied_commit_timestamp().await
    }

    pub async fn replica_freshness(&self) -> anyhow::Result<OrionSqliteReplicaFreshness> {
        let applied_commit_ts = self.applied_commit_timestamp().await?;
        Ok(OrionSqliteReplicaFreshness {
            raft: self.metrics(),
            applied_commit_ts,
            closed_commit_ts: applied_commit_ts,
            staleness_ms: applied_commit_ts
                .map(|timestamp| current_time_millis().saturating_sub(timestamp.physical_ms)),
        })
    }

    pub async fn wait_for_applied_index(
        &self,
        min_applied_index: u64,
        timeout: std::time::Duration,
    ) -> anyhow::Result<RaftMetricsSnapshot> {
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(min_applied_index), "SQLite session read policy")
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "local replica has not applied required session index {min_applied_index}: {error}"
                )
            })?;
        Ok(self.metrics())
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .expect("system clock millis do not fit in u64")
}

pub struct OrionSqliteDb {
    database: String,
    vfs_name: String,
}

impl OrionSqliteDb {
    pub fn connect(&self) -> rusqlite::Result<Connection> {
        let conn = Connection::open_with_flags(
            self.uri(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        configure_connection(&conn)?;
        Ok(conn)
    }

    pub fn connect_read_only(&self) -> rusqlite::Result<Connection> {
        let conn = Connection::open_with_flags(
            self.uri(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )?;
        configure_read_only_connection(&conn)?;
        Ok(conn)
    }

    pub fn uri(&self) -> String {
        format!("file:{}.db?vfs={}", self.database, self.vfs_name)
    }

    pub fn execute_batch(&self, sql: &str) -> anyhow::Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(sql)?;
        Ok(())
    }

    pub fn query_one_i64(&self, sql: &str) -> anyhow::Result<i64> {
        let conn = self.connect()?;
        Ok(conn.query_row(sql, [], |row| row.get(0))?)
    }

    pub fn ensure_system_schema(&self) -> anyhow::Result<()> {
        ensure_system_schema(&self.connect()?)
    }

    pub fn record_compaction_run(
        &self,
        started_at_ms: u64,
        finished_at_ms: u64,
        status: &str,
        metrics: &SqlitePageCompactionMetrics,
        error: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        record_compaction_run(&conn, started_at_ms, finished_at_ms, status, metrics, error)
    }

    pub fn compaction_control(&self) -> anyhow::Result<OrionCompactionControl> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        read_compaction_control(&conn)
    }

    pub fn set_compaction_paused(&self, paused: bool) -> anyhow::Result<OrionCompactionControl> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        set_compaction_paused(&conn, paused)?;
        read_compaction_control(&conn)
    }

    pub fn request_compaction(&self) -> anyhow::Result<OrionCompactionControl> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        set_force_compaction_requested(&conn, true)?;
        read_compaction_control(&conn)
    }

    pub fn clear_compaction_request(&self) -> anyhow::Result<OrionCompactionControl> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        set_force_compaction_requested(&conn, false)?;
        read_compaction_control(&conn)
    }

    pub fn compaction_retention_floor(&self) -> anyhow::Result<OrionCompactionRetentionFloor> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        read_compaction_retention_floor(&conn)
    }

    pub fn set_compaction_retention_floor(
        &self,
        min_retained_version: u64,
        reason: Option<&str>,
    ) -> anyhow::Result<OrionCompactionRetentionFloor> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        set_compaction_retention_floor(&conn, Some(min_retained_version), reason)?;
        read_compaction_retention_floor(&conn)
    }

    pub fn clear_compaction_retention_floor(
        &self,
    ) -> anyhow::Result<OrionCompactionRetentionFloor> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        set_compaction_retention_floor(&conn, None, None)?;
        read_compaction_retention_floor(&conn)
    }

    pub fn acquire_compaction_lease(
        &self,
        name: &str,
        owner_node_id: u64,
        ttl_ms: u64,
    ) -> anyhow::Result<Option<OrionCompactionLease>> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        acquire_compaction_lease(&conn, name, owner_node_id, ttl_ms)
    }

    pub fn compaction_leases(&self) -> anyhow::Result<Vec<OrionCompactionLease>> {
        let conn = self.connect()?;
        ensure_system_schema(&conn)?;
        read_compaction_leases(&conn)
    }
}

fn ensure_system_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table if not exists compaction_runs (
            id integer primary key autoincrement,
            started_at_ms integer not null,
            finished_at_ms integer not null,
            duration_ms integer not null,
            status text not null check (status in ('ok', 'error')),
            files_scanned integer not null,
            files_compacted integer not null,
            versions_scanned integer not null,
            obsolete_versions integer not null,
            deleted_versions integer not null,
            bytes_scanned integer not null,
            obsolete_bytes integer not null,
            deleted_bytes integer not null,
            error text
        );

        create table if not exists compaction_state (
            id integer primary key check (id = 1),
            updated_at_ms integer not null,
            last_status text not null check (last_status in ('ok', 'error')),
            last_error text,
            last_duration_ms integer not null,
            total_runs integer not null,
            total_errors integer not null,
            total_deleted_versions integer not null,
            total_deleted_bytes integer not null,
            last_files_scanned integer not null,
            last_files_compacted integer not null,
            last_versions_scanned integer not null,
            last_obsolete_versions integer not null,
            last_deleted_versions integer not null,
            last_bytes_scanned integer not null,
            last_obsolete_bytes integer not null,
            last_deleted_bytes integer not null
        );

        create table if not exists compaction_control (
            id integer primary key check (id = 1),
            paused integer not null,
            force_requested integer not null,
            updated_at_ms integer not null
        );

        create table if not exists compaction_retention_floor (
            id integer primary key check (id = 1),
            min_retained_version integer,
            reason text,
            updated_at_ms integer not null
        );

        create table if not exists compaction_leases (
            name text primary key,
            owner_node_id integer not null,
            epoch integer not null,
            acquired_at_ms integer not null,
            renewed_at_ms integer not null,
            expires_at_ms integer not null,
            last_compacted_version integer
        );

        insert into compaction_control (id, paused, force_requested, updated_at_ms)
        values (1, 0, 0, 0)
        on conflict(id) do nothing;

        insert into compaction_retention_floor (id, min_retained_version, reason, updated_at_ms)
        values (1, null, null, 0)
        on conflict(id) do nothing;

        "#,
    )?;
    Ok(())
}

fn record_compaction_run(
    conn: &Connection,
    started_at_ms: u64,
    finished_at_ms: u64,
    status: &str,
    metrics: &SqlitePageCompactionMetrics,
    error: Option<&str>,
) -> anyhow::Result<()> {
    let duration_ms = finished_at_ms.saturating_sub(started_at_ms);
    let started_at_ms = sqlite_i64(started_at_ms);
    let finished_at_ms = sqlite_i64(finished_at_ms);
    let duration_ms = sqlite_i64(duration_ms);
    let files_scanned = sqlite_usize(metrics.files_scanned);
    let files_compacted = sqlite_usize(metrics.files_compacted);
    let versions_scanned = sqlite_usize(metrics.versions_scanned);
    let obsolete_versions = sqlite_usize(metrics.obsolete_versions);
    let deleted_versions = sqlite_usize(metrics.deleted_versions);
    let bytes_scanned = sqlite_i64(metrics.bytes_scanned);
    let obsolete_bytes = sqlite_i64(metrics.obsolete_bytes);
    let deleted_bytes = sqlite_i64(metrics.deleted_bytes);
    let total_errors = i64::from(status == "error");

    let tx = conn.unchecked_transaction()?;
    tx.execute(
        r#"
        insert into compaction_runs (
            started_at_ms,
            finished_at_ms,
            duration_ms,
            status,
            files_scanned,
            files_compacted,
            versions_scanned,
            obsolete_versions,
            deleted_versions,
            bytes_scanned,
            obsolete_bytes,
            deleted_bytes,
            error
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            started_at_ms,
            finished_at_ms,
            duration_ms,
            status,
            files_scanned,
            files_compacted,
            versions_scanned,
            obsolete_versions,
            deleted_versions,
            bytes_scanned,
            obsolete_bytes,
            deleted_bytes,
            error,
        ],
    )?;
    tx.execute(
        r#"
        insert into compaction_state (
            id,
            updated_at_ms,
            last_status,
            last_error,
            last_duration_ms,
            total_runs,
            total_errors,
            total_deleted_versions,
            total_deleted_bytes,
            last_files_scanned,
            last_files_compacted,
            last_versions_scanned,
            last_obsolete_versions,
            last_deleted_versions,
            last_bytes_scanned,
            last_obsolete_bytes,
            last_deleted_bytes
        ) values (1, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(id) do update set
            updated_at_ms = excluded.updated_at_ms,
            last_status = excluded.last_status,
            last_error = excluded.last_error,
            last_duration_ms = excluded.last_duration_ms,
            total_runs = compaction_state.total_runs + 1,
            total_errors = compaction_state.total_errors + excluded.total_errors,
            total_deleted_versions =
                compaction_state.total_deleted_versions + excluded.total_deleted_versions,
            total_deleted_bytes =
                compaction_state.total_deleted_bytes + excluded.total_deleted_bytes,
            last_files_scanned = excluded.last_files_scanned,
            last_files_compacted = excluded.last_files_compacted,
            last_versions_scanned = excluded.last_versions_scanned,
            last_obsolete_versions = excluded.last_obsolete_versions,
            last_deleted_versions = excluded.last_deleted_versions,
            last_bytes_scanned = excluded.last_bytes_scanned,
            last_obsolete_bytes = excluded.last_obsolete_bytes,
            last_deleted_bytes = excluded.last_deleted_bytes
        "#,
        params![
            finished_at_ms,
            status,
            error,
            duration_ms,
            total_errors,
            deleted_versions,
            deleted_bytes,
            files_scanned,
            files_compacted,
            versions_scanned,
            obsolete_versions,
            deleted_versions,
            bytes_scanned,
            obsolete_bytes,
            deleted_bytes,
        ],
    )?;
    tx.execute(
        r#"
        update compaction_leases
        set last_compacted_version = case
                when ? > coalesce(last_compacted_version, 0) then ?
                else last_compacted_version
            end
        where name = 'sqlite-page-compactor'
        "#,
        params![
            metrics.highest_deleted_version.map(sqlite_i64),
            metrics.highest_deleted_version.map(sqlite_i64)
        ],
    )?;
    tx.commit()?;
    Ok(())
}

fn read_compaction_control(conn: &Connection) -> anyhow::Result<OrionCompactionControl> {
    Ok(conn.query_row(
        "select paused, force_requested, updated_at_ms from compaction_control where id = 1",
        [],
        |row| {
            Ok(OrionCompactionControl {
                paused: row.get::<_, i64>(0)? != 0,
                force_requested: row.get::<_, i64>(1)? != 0,
                updated_at_ms: sqlite_u64(row.get(2)?),
            })
        },
    )?)
}

fn set_compaction_paused(conn: &Connection, paused: bool) -> anyhow::Result<()> {
    conn.execute(
        r#"
        insert into compaction_control (id, paused, force_requested, updated_at_ms)
        values (1, ?, 0, ?)
        on conflict(id) do update set
            paused = excluded.paused,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![i64::from(paused), sqlite_i64(current_time_millis())],
    )?;
    Ok(())
}

fn set_force_compaction_requested(conn: &Connection, force_requested: bool) -> anyhow::Result<()> {
    conn.execute(
        r#"
        insert into compaction_control (id, paused, force_requested, updated_at_ms)
        values (1, 0, ?, ?)
        on conflict(id) do update set
            force_requested = excluded.force_requested,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            i64::from(force_requested),
            sqlite_i64(current_time_millis())
        ],
    )?;
    Ok(())
}

fn read_compaction_retention_floor(
    conn: &Connection,
) -> anyhow::Result<OrionCompactionRetentionFloor> {
    Ok(conn.query_row(
        "select min_retained_version, reason, updated_at_ms from compaction_retention_floor where id = 1",
        [],
        |row| {
            let min_retained_version = row.get::<_, Option<i64>>(0)?.map(sqlite_u64);
            Ok(OrionCompactionRetentionFloor {
                min_retained_version,
                reason: row.get(1)?,
                updated_at_ms: sqlite_u64(row.get(2)?),
            })
        },
    )?)
}

fn set_compaction_retention_floor(
    conn: &Connection,
    min_retained_version: Option<u64>,
    reason: Option<&str>,
) -> anyhow::Result<()> {
    conn.execute(
        r#"
        insert into compaction_retention_floor (id, min_retained_version, reason, updated_at_ms)
        values (1, ?, ?, ?)
        on conflict(id) do update set
            min_retained_version = excluded.min_retained_version,
            reason = excluded.reason,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            min_retained_version.map(sqlite_i64),
            reason,
            sqlite_i64(current_time_millis()),
        ],
    )?;
    Ok(())
}

fn acquire_compaction_lease(
    conn: &Connection,
    name: &str,
    owner_node_id: u64,
    ttl_ms: u64,
) -> anyhow::Result<Option<OrionCompactionLease>> {
    let now = current_time_millis();
    let owner_node_id_i64 = sqlite_i64(owner_node_id);
    let now_i64 = sqlite_i64(now);
    let expires_at_ms = sqlite_i64(now.saturating_add(ttl_ms));
    let tx = conn.unchecked_transaction()?;
    let existing = read_compaction_lease(&tx, name)?;
    if let Some(existing) = existing.as_ref()
        && existing.owner_node_id != owner_node_id
        && existing.expires_at_ms > now
    {
        tx.commit()?;
        return Ok(None);
    }
    let next_epoch = existing
        .as_ref()
        .map(|lease| lease.epoch.saturating_add(1))
        .unwrap_or(1);
    let acquired_at_ms = existing
        .as_ref()
        .filter(|lease| lease.owner_node_id == owner_node_id)
        .map(|lease| lease.acquired_at_ms)
        .unwrap_or(now);
    tx.execute(
        r#"
        insert into compaction_leases (
            name,
            owner_node_id,
            epoch,
            acquired_at_ms,
            renewed_at_ms,
            expires_at_ms,
            last_compacted_version
        ) values (?, ?, ?, ?, ?, ?, ?)
        on conflict(name) do update set
            owner_node_id = excluded.owner_node_id,
            epoch = excluded.epoch,
            acquired_at_ms = excluded.acquired_at_ms,
            renewed_at_ms = excluded.renewed_at_ms,
            expires_at_ms = excluded.expires_at_ms
        "#,
        params![
            name,
            owner_node_id_i64,
            sqlite_i64(next_epoch),
            sqlite_i64(acquired_at_ms),
            now_i64,
            expires_at_ms,
            existing
                .as_ref()
                .and_then(|lease| lease.last_compacted_version)
                .map(sqlite_i64),
        ],
    )?;
    let lease = read_compaction_lease(&tx, name)?;
    tx.commit()?;
    Ok(lease)
}

fn read_compaction_leases(conn: &Connection) -> anyhow::Result<Vec<OrionCompactionLease>> {
    let mut stmt = conn.prepare(
        r#"
        select name, owner_node_id, epoch, acquired_at_ms, renewed_at_ms, expires_at_ms, last_compacted_version
        from compaction_leases
        order by name
        "#,
    )?;
    stmt.query_map([], compaction_lease_from_row)?
        .map(|row| row.map_err(anyhow::Error::from))
        .collect()
}

fn read_compaction_lease(
    conn: &Connection,
    name: &str,
) -> anyhow::Result<Option<OrionCompactionLease>> {
    conn.query_row(
        r#"
        select name, owner_node_id, epoch, acquired_at_ms, renewed_at_ms, expires_at_ms, last_compacted_version
        from compaction_leases
        where name = ?
        "#,
        [name],
        compaction_lease_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn compaction_lease_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrionCompactionLease> {
    Ok(OrionCompactionLease {
        name: row.get(0)?,
        owner_node_id: sqlite_u64(row.get(1)?),
        epoch: sqlite_u64(row.get(2)?),
        acquired_at_ms: sqlite_u64(row.get(3)?),
        renewed_at_ms: sqlite_u64(row.get(4)?),
        expires_at_ms: sqlite_u64(row.get(5)?),
        last_compacted_version: row.get::<_, Option<i64>>(6)?.map(sqlite_u64),
    })
}

fn sqlite_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn sqlite_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn configure_connection(conn: &Connection) -> rusqlite::Result<()> {
    install_orion_authorizer(conn)?;
    let journal_mode: String =
        conn.query_row("pragma journal_mode = delete", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("delete") {
        return Err(rusqlite::Error::ExecuteReturnedResults);
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "busy_timeout", 5_000)?;
    Ok(())
}

fn configure_read_only_connection(conn: &Connection) -> rusqlite::Result<()> {
    install_orion_authorizer(conn)?;
    conn.pragma_update(None, "busy_timeout", 5_000)?;
    Ok(())
}

fn install_orion_authorizer(conn: &Connection) -> rusqlite::Result<()> {
    conn.authorizer(Some(authorize_orion_sqlite_action))
}

fn authorize_orion_sqlite_action(ctx: AuthContext<'_>) -> Authorization {
    match ctx.action {
        AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropVtable { .. } => Authorization::Deny,
        AuthAction::Function { function_name }
            if function_name.eq_ignore_ascii_case("load_extension") =>
        {
            Authorization::Deny
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => authorize_pragma(pragma_name, pragma_value),
        _ => Authorization::Allow,
    }
}

fn authorize_pragma(name: &str, value: Option<&str>) -> Authorization {
    let name = name.to_ascii_lowercase();
    let normalized_value = value.map(normalize_pragma_value);
    match name.as_str() {
        "journal_mode" => match normalized_value.as_deref() {
            None | Some("delete") => Authorization::Allow,
            _ => Authorization::Deny,
        },
        "synchronous" => match normalized_value.as_deref() {
            None | Some("full") | Some("extra") | Some("2") | Some("3") => Authorization::Allow,
            _ => Authorization::Deny,
        },
        "locking_mode" => match normalized_value.as_deref() {
            None | Some("normal") => Authorization::Allow,
            _ => Authorization::Deny,
        },
        "writable_schema" => match normalized_value.as_deref() {
            None | Some("0") | Some("off") | Some("false") | Some("no") => Authorization::Allow,
            _ => Authorization::Deny,
        },
        "temp_store_directory" | "data_store_directory" => Authorization::Deny,
        _ => Authorization::Allow,
    }
}

fn normalize_pragma_value(value: &str) -> String {
    value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn unique_vfs_name(prefix: &str) -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorizer_test_connection() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        install_orion_authorizer(&conn).unwrap();
        conn
    }

    #[test]
    fn authorizer_allows_safe_pragmas() {
        let conn = authorizer_test_connection();

        conn.execute_batch(
            r#"
            pragma journal_mode;
            pragma journal_mode = delete;
            pragma synchronous = full;
            pragma locking_mode = normal;
            pragma writable_schema = off;
            pragma busy_timeout = 1000;
            "#,
        )
        .unwrap();
    }

    #[test]
    fn authorizer_denies_pragmas_that_bypass_orion_durability_or_storage_boundary() {
        let conn = authorizer_test_connection();

        for sql in [
            "pragma journal_mode = wal",
            "pragma journal_mode = off",
            "pragma synchronous = off",
            "pragma locking_mode = exclusive",
            "pragma writable_schema = on",
            "pragma temp_store_directory = '/tmp'",
        ] {
            let err = conn.execute_batch(sql).unwrap_err();
            assert!(
                err.to_string().contains("not authorized"),
                "expected {sql:?} to be denied, got {err}"
            );
        }
    }

    #[test]
    fn authorizer_denies_external_database_and_native_extension_surfaces() {
        let conn = authorizer_test_connection();

        for sql in [
            "attach database ':memory:' as other",
            "select load_extension('not-real')",
            "create virtual table docs using fts5(body)",
        ] {
            let err = conn.execute_batch(sql).unwrap_err();
            assert!(
                err.to_string().contains("not authorized"),
                "expected {sql:?} to be denied, got {err}"
            );
        }
    }
}
