use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read, Seek};
use std::path::Path as FsPath;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, ensure};
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use orion_raft::sqlite_runtime::OrionSqliteRuntimeWatermark;
use orion_raft::{
    ClusterRaftMetricsEntry, ClusterRaftMetricsRegistry, HybridTimestamp, LargePayloadMetrics,
    NodeSoftwareCapabilities, ORION_SYSTEM_DATABASE, OrionRaft, OrionSqliteDb,
    OrionSqliteReplicaFreshness, OrionSqliteRuntime, OrionSqliteRuntimeConfig,
    SlateDbCheckpointArtifact, SlateDbStateStore, SqliteDatabaseFileSnapshot,
    SqliteDatabasePageSyncDelta, SqliteDatabasePurgeMetrics, SqliteDatabasePurgePolicy,
    SqlitePageCompactionPolicy, SqliteStoragePressureMetrics, list_slate_db_checkpoint_objects,
    purge_tombstoned_sqlite_database, sqlite_storage_pressure,
};
use orion_sqlite::clear_orion_vfs_shared_state;
use rusqlite::backup::Backup;
use rusqlite::ffi::ErrorCode as SqliteErrorCode;
use rusqlite::types::{Type as SqliteType, Value as SqliteValue, ValueRef};
use rusqlite::{Connection, OptionalExtension, Row, ToSql, params, params_from_iter};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, WriteMultipart};
use tempfile::{NamedTempFile, TempPath};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const DEFAULT_DATABASE: &str = "orion";
const SESSION_GC_INTERVAL: Duration = Duration::from_secs(5);
const DATABASE_LIFECYCLE_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const DATABASE_CATALOG_BOOTSTRAP_SCHEMA_VERSION: u32 = 7;
const DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION: u32 = 10;
const DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION: u32 = 10;
const READ_POLICY_HEADER: &str = "x-orion-read-policy";
const MIN_APPLIED_INDEX_HEADER: &str = "x-orion-min-applied-index";
const SESSION_TOKEN_HEADER: &str = "x-orion-session-token";
const READ_TIMEOUT_MS_HEADER: &str = "x-orion-read-timeout-ms";
const MAX_STALENESS_MS_HEADER: &str = "x-orion-max-staleness-ms";
const BLOB_ID_HEADER: &str = "x-orion-blob-id";
const BLOB_OFFSET_HEADER: &str = "x-orion-blob-offset";
const BLOB_SIZE_HEADER: &str = "x-orion-blob-size";
const BLOB_BYTES_READ_HEADER: &str = "x-orion-blob-bytes-read";
const BLOB_BYTES_WRITTEN_HEADER: &str = "x-orion-blob-bytes-written";
const IDEMPOTENCY_KEY_HEADER: &str = "x-orion-idempotency-key";
const STANDBY_EXPORT_SOURCE_GROUP_HEADER: &str = "x-orion-source-group-id";
const STANDBY_EXPORT_APPLIED_INDEX_HEADER: &str = "x-orion-source-applied-index";
const STANDBY_EXPORT_COMMIT_TS_PHYSICAL_MS_HEADER: &str = "x-orion-source-commit-ts-physical-ms";
const STANDBY_EXPORT_COMMIT_TS_LOGICAL_HEADER: &str = "x-orion-source-commit-ts-logical";
const STANDBY_EXPORT_SHA256_HEADER: &str = "x-orion-snapshot-sha256";
const STANDBY_EXPORT_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_READ_TIMEOUT_MS: u64 = 1_000;
const RAFT_METRICS_STALE_AFTER_MS: u64 = 15_000;
const MAX_OPEN_BLOB_HANDLES_PER_SESSION: usize = 1024;
const IDEMPOTENCY_TABLE: &str = "__orion_idempotency";
const LIFECYCLE_IDEMPOTENCY_TABLE: &str = "database_lifecycle_idempotency";
pub(crate) const ORION_CATALOG_DATABASE: &str = "orion_catalog";
const DEFAULT_REPLICATION_GROUP_ID: &str = "rg_default";
const IDEMPOTENCY_PENDING_RECONCILE_TIMEOUT: Duration = Duration::from_secs(1);
const IDEMPOTENCY_PENDING_RECONCILE_POLL: Duration = Duration::from_millis(25);
const PLACEMENT_OPERATION_GC_DEFAULT_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const PLACEMENT_OPERATION_GC_DEFAULT_LIMIT: usize = 1_000;
const PLACEMENT_FILE_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const PLACEMENT_CHECKPOINT_OBJECT_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const PLACEMENT_CHECKPOINT_OBJECT_UPLOAD_CONCURRENCY: usize = 4;
const STANDBY_REFRESH_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
const STANDBY_REFRESH_MIN_INTERVAL_MS: u64 = 5_000;

pub(crate) fn node_software_capabilities() -> NodeSoftwareCapabilities {
    NodeSoftwareCapabilities {
        catalog_min_read_schema_version: 1,
        catalog_max_read_schema_version: DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION,
        catalog_max_write_schema_version: DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION,
    }
}

fn default_idempotency_enabled() -> bool {
    true
}

fn default_idempotency_committed_ttl_ms() -> u64 {
    24 * 60 * 60 * 1_000
}

fn default_idempotency_pending_ttl_ms() -> u64 {
    7 * 24 * 60 * 60 * 1_000
}

fn default_idempotency_gc_interval_ms() -> u64 {
    60_000
}

fn default_idempotency_gc_max_records_per_pass() -> usize {
    1_000
}

fn error_chain_message(error: &anyhow::Error) -> String {
    let mut messages = error.chain().map(ToString::to_string);
    let Some(first) = messages.next() else {
        return error.to_string();
    };
    messages.fold(first, |mut acc, message| {
        if !acc.contains(&message) {
            acc.push_str(": ");
            acc.push_str(&message);
        }
        acc
    })
}

fn error_chain_contains(error: &anyhow::Error, needle: &str) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains(needle))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OrionReadPolicy {
    Strong,
    RevocationSafe,
    Session {
        min_applied_index: Option<u64>,
        timeout_ms: u64,
    },
    BoundedStaleness {
        max_staleness_ms: u64,
    },
    Local,
}

impl OrionReadPolicy {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::RevocationSafe => "revocation_safe",
            Self::Session { .. } => "session",
            Self::BoundedStaleness { .. } => "bounded_staleness",
            Self::Local => "local",
        }
    }
}

#[derive(Clone)]
pub struct LibsqlHttpConfig {
    pub sqlite_cache_root: std::path::PathBuf,
    pub session_idle_timeout: Duration,
    pub blob_max_chunk_bytes: usize,
    pub idempotency: LibsqlHttpIdempotencyConfig,
    pub auth: LibsqlHttpAuthConfig,
    pub node_id: u64,
    pub peer_http_endpoints: BTreeMap<u64, String>,
    pub placement_nodes: BTreeMap<u64, PlacementNodeConfig>,
    pub metrics_registry: ClusterRaftMetricsRegistry,
    pub compaction_policy: SqlitePageCompactionPolicy,
    pub replication_groups: Option<ReplicationGroupRegistry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementNodeConfig {
    pub node_id: u64,
    pub cloud: String,
    pub region: String,
    pub zone: String,
    pub raft_addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub libsql_http_addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeGroupCatalogSnapshot {
    pub group_id: String,
    pub state: String,
    pub members: Vec<RuntimeGroupMemberSnapshot>,
}

impl RuntimeGroupCatalogSnapshot {
    pub fn voter_ids(&self) -> Vec<u64> {
        self.members
            .iter()
            .filter(|member| member.role == "voter")
            .map(|member| member.node_id)
            .collect()
    }

    pub fn learner_ids(&self) -> Vec<u64> {
        self.members
            .iter()
            .filter(|member| member.role == "learner" || member.role == "read_replica")
            .map(|member| member.node_id)
            .collect()
    }

    pub fn member_ids(&self) -> Vec<u64> {
        self.members.iter().map(|member| member.node_id).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeGroupMemberSnapshot {
    pub node_id: u64,
    pub role: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LibsqlHttpIdempotencyConfig {
    #[serde(default = "default_idempotency_enabled")]
    pub enabled: bool,
    #[serde(default = "default_idempotency_committed_ttl_ms")]
    pub committed_ttl_ms: u64,
    #[serde(default = "default_idempotency_pending_ttl_ms")]
    pub pending_ttl_ms: u64,
    #[serde(default = "default_idempotency_gc_interval_ms")]
    pub gc_interval_ms: u64,
    #[serde(default = "default_idempotency_gc_max_records_per_pass")]
    pub gc_max_records_per_pass: usize,
}

impl Default for LibsqlHttpIdempotencyConfig {
    fn default() -> Self {
        Self {
            enabled: default_idempotency_enabled(),
            committed_ttl_ms: default_idempotency_committed_ttl_ms(),
            pending_ttl_ms: default_idempotency_pending_ttl_ms(),
            gc_interval_ms: default_idempotency_gc_interval_ms(),
            gc_max_records_per_pass: default_idempotency_gc_max_records_per_pass(),
        }
    }
}

impl LibsqlHttpIdempotencyConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.committed_ttl_ms > 0,
            "libsql_http.idempotency.committed_ttl_ms must be greater than zero"
        );
        ensure!(
            self.pending_ttl_ms > 0,
            "libsql_http.idempotency.pending_ttl_ms must be greater than zero"
        );
        ensure!(
            self.gc_interval_ms > 0,
            "libsql_http.idempotency.gc_interval_ms must be greater than zero"
        );
        ensure!(
            self.gc_max_records_per_pass > 0,
            "libsql_http.idempotency.gc_max_records_per_pass must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LibsqlHttpAuthConfig {
    #[serde(default)]
    pub tokens: Vec<LibsqlHttpAuthTokenConfig>,
}

impl LibsqlHttpAuthConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        for token in &self.tokens {
            ensure!(
                !token.token.is_empty(),
                "libsql_http.auth.tokens[].token must not be empty"
            );
            ensure!(
                !token.database_prefixes.is_empty() || !token.system_permissions.is_empty(),
                "libsql_http.auth.tokens[] must include database_prefixes or system_permissions"
            );
            for prefix in &token.database_prefixes {
                ensure!(
                    is_valid_database_prefix(prefix),
                    "libsql_http.auth.tokens[].database_prefixes contains invalid prefix {prefix:?}"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LibsqlHttpAuthTokenConfig {
    pub token: String,
    #[serde(default)]
    pub database_prefixes: Vec<String>,
    #[serde(default)]
    pub system_permissions: Vec<LibsqlHttpSystemPermission>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LibsqlHttpSystemPermission {
    Read,
    Admin,
}

#[derive(Clone)]
struct LibsqlHttpState {
    replication_groups: ReplicationGroupRegistry,
    sqlite_state: SlateDbStateStore,
    sqlite_cache_root: std::path::PathBuf,
    databases: Arc<Mutex<HashMap<String, DatabaseCacheEntry>>>,
    sessions: Arc<Mutex<HashMap<String, Arc<Mutex<LibsqlSession>>>>>,
    next_baton_id: Arc<AtomicU64>,
    session_idle_timeout: Duration,
    blob_max_chunk_bytes: usize,
    blob_metrics: Arc<BlobApiMetrics>,
    standby_checkpoint_metrics: Arc<StandbyCheckpointMetrics>,
    standby_page_delta_metrics: Arc<StandbyPageDeltaMetrics>,
    placement_move_transfer_metrics: Arc<PlacementMoveTransferMetrics>,
    idempotency_config: LibsqlHttpIdempotencyConfig,
    idempotency_metrics: Arc<IdempotencyMetrics>,
    auth: LibsqlHttpAuthConfig,
    node_id: u64,
    peer_http_endpoints: Arc<BTreeMap<u64, String>>,
    placement_nodes: Arc<BTreeMap<u64, PlacementNodeConfig>>,
    metrics_registry: ClusterRaftMetricsRegistry,
    compaction_policy: SqlitePageCompactionPolicy,
    http_client: reqwest::Client,
    standby_refreshes: Arc<Mutex<HashSet<String>>>,
}

struct StandbyRefreshGuard {
    key: String,
    active: Arc<Mutex<HashSet<String>>>,
}

impl Drop for StandbyRefreshGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.key);
        }
    }
}

#[derive(Clone)]
struct DatabaseCacheEntry {
    group_id: String,
    db: Arc<OrionSqliteDb>,
}

#[derive(Clone)]
struct ReplicationGroupRuntime {
    group_id: String,
    runtime: OrionSqliteRuntime,
    loaded_at_ms: u64,
}

impl ReplicationGroupRuntime {
    fn new(group_id: impl Into<String>, runtime: OrionSqliteRuntime) -> Self {
        Self {
            group_id: group_id.into(),
            runtime,
            loaded_at_ms: current_time_millis(),
        }
    }
}

#[derive(Clone)]
pub struct ReplicationGroupRegistry {
    groups: Arc<Mutex<BTreeMap<String, ReplicationGroupRuntime>>>,
}

impl ReplicationGroupRegistry {
    pub fn single_default(runtime: OrionSqliteRuntime) -> Self {
        Self {
            groups: Arc::new(Mutex::new(BTreeMap::from([(
                DEFAULT_REPLICATION_GROUP_ID.to_string(),
                ReplicationGroupRuntime::new(DEFAULT_REPLICATION_GROUP_ID, runtime),
            )]))),
        }
    }

    pub fn empty() -> Self {
        Self {
            groups: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn register(
        &self,
        group_id: impl Into<String>,
        runtime: OrionSqliteRuntime,
    ) -> anyhow::Result<()> {
        let group_id = group_id.into();
        ensure_valid_runtime_group_id(&group_id)?;
        self.groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .insert(
                group_id.clone(),
                ReplicationGroupRuntime::new(group_id, runtime),
            );
        Ok(())
    }

    pub fn default_runtime(&self) -> anyhow::Result<OrionSqliteRuntime> {
        self.runtime(DEFAULT_REPLICATION_GROUP_ID)
    }

    pub fn runtime(&self, group_id: &str) -> anyhow::Result<OrionSqliteRuntime> {
        self.groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .get(group_id)
            .map(|group| {
                debug_assert_eq!(group.group_id, group_id);
                group.runtime.clone()
            })
            .ok_or_else(|| anyhow!("replication group {group_id} is not loaded on this node"))
    }

    pub fn contains(&self, group_id: &str) -> anyhow::Result<bool> {
        Ok(self
            .groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .contains_key(group_id))
    }

    pub fn loaded_group_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    async fn large_payload_metrics(&self) -> anyhow::Result<Vec<LargePayloadMetricsRow>> {
        let groups = self
            .groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut rows = Vec::with_capacity(groups.len());
        for group in groups {
            rows.push(LargePayloadMetricsRow {
                group_id: group.group_id,
                loaded_at_ms: group.loaded_at_ms,
                metrics: group.runtime.large_payload_metrics().await?,
            });
        }
        Ok(rows)
    }

    #[cfg(test)]
    fn register_for_test(
        &self,
        group_id: impl Into<String>,
        runtime: OrionSqliteRuntime,
    ) -> anyhow::Result<()> {
        let group_id = group_id.into();
        self.register(group_id, runtime)
    }

    #[cfg(test)]
    fn unregister_for_test(&self, group_id: &str) -> anyhow::Result<Option<OrionSqliteRuntime>> {
        Ok(self
            .groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?
            .remove(group_id)
            .map(|group| group.runtime))
    }

    fn health_records(&self) -> anyhow::Result<Vec<ReplicationGroupRuntimeRecord>> {
        let groups = self
            .groups
            .lock()
            .map_err(|_| anyhow!("replication group runtime registry mutex poisoned"))?;
        Ok(groups
            .values()
            .map(|group| {
                let metrics = group.runtime.metrics();
                let ready_for_linearizable_reads = metrics.is_ready_for_linearizable_reads();
                ReplicationGroupRuntimeRecord {
                    group_id: group.group_id.clone(),
                    loaded: true,
                    loaded_at_ms: Some(group.loaded_at_ms),
                    current_leader: metrics.current_leader,
                    voter_ids: metrics.voter_ids,
                    learner_ids: metrics.learner_ids,
                    ready_for_linearizable_reads,
                    error: None,
                }
            })
            .collect())
    }

    fn state_store(&self) -> SlateDbStateStore {
        self.groups
            .lock()
            .expect("replication group runtime registry mutex must not be poisoned")
            .get(DEFAULT_REPLICATION_GROUP_ID)
            .expect("default replication group runtime must exist")
            .runtime
            .state_store()
    }
}

#[derive(Debug, Clone, Copy)]
enum BlobApiOp {
    Open,
    Read,
    Write,
    Reopen,
    Close,
}

#[derive(Default)]
struct BlobApiMetrics {
    open_requests: AtomicU64,
    read_requests: AtomicU64,
    write_requests: AtomicU64,
    reopen_requests: AtomicU64,
    close_requests: AtomicU64,
    failed_requests: AtomicU64,
    rejected_open_handles: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    request_latency_ns: AtomicU64,
    max_open_handles_observed: AtomicU64,
}

#[derive(Default)]
struct StandbyCheckpointMetrics {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    fallback_to_backup: AtomicU64,
    objects_seen: AtomicU64,
    objects_copied: AtomicU64,
    objects_reused: AtomicU64,
    bytes_seen: AtomicU64,
    bytes_copied: AtomicU64,
}

impl StandbyCheckpointMetrics {
    fn record_attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self, stats: StandbyCheckpointFetchStats) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.objects_seen
            .fetch_add(stats.objects_seen, Ordering::Relaxed);
        self.objects_copied
            .fetch_add(stats.objects_copied, Ordering::Relaxed);
        self.objects_reused
            .fetch_add(stats.objects_reused, Ordering::Relaxed);
        self.bytes_seen
            .fetch_add(stats.bytes_seen, Ordering::Relaxed);
        self.bytes_copied
            .fetch_add(stats.bytes_copied, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_fallback_to_backup(&self) {
        self.fallback_to_backup.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StandbyCheckpointMetricsSnapshot {
        StandbyCheckpointMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            fallback_to_backup: self.fallback_to_backup.load(Ordering::Relaxed),
            objects_seen: self.objects_seen.load(Ordering::Relaxed),
            objects_copied: self.objects_copied.load(Ordering::Relaxed),
            objects_reused: self.objects_reused.load(Ordering::Relaxed),
            bytes_seen: self.bytes_seen.load(Ordering::Relaxed),
            bytes_copied: self.bytes_copied.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct StandbyCheckpointFetchStats {
    objects_seen: u64,
    objects_copied: u64,
    objects_reused: u64,
    bytes_seen: u64,
    bytes_copied: u64,
}

impl StandbyCheckpointFetchStats {
    fn add_checkpoint_response(&mut self, response: &PlacementCheckpointMaterializeResponse) {
        self.objects_seen = self
            .objects_seen
            .saturating_add(response.checkpoint_objects_seen);
        self.objects_copied = self
            .objects_copied
            .saturating_add(response.checkpoint_objects_copied);
        self.objects_reused = self
            .objects_reused
            .saturating_add(response.checkpoint_objects_reused);
        self.bytes_seen = self
            .bytes_seen
            .saturating_add(response.checkpoint_bytes_seen);
        self.bytes_copied = self
            .bytes_copied
            .saturating_add(response.checkpoint_bytes_copied);
    }
}

#[derive(Debug, Clone, Serialize)]
struct StandbyCheckpointMetricsSnapshot {
    attempts: u64,
    successes: u64,
    failures: u64,
    fallback_to_backup: u64,
    objects_seen: u64,
    objects_copied: u64,
    objects_reused: u64,
    bytes_seen: u64,
    bytes_copied: u64,
}

#[derive(Default)]
struct StandbyPageDeltaMetrics {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    fallback_to_checkpoint: AtomicU64,
    entries_seen: AtomicU64,
    entries_applied: AtomicU64,
    deletes_applied: AtomicU64,
    bytes_received: AtomicU64,
    bytes_applied: AtomicU64,
}

impl StandbyPageDeltaMetrics {
    fn record_attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_success(&self, stats: StandbyPageDeltaStats) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.entries_seen
            .fetch_add(stats.entries_seen, Ordering::Relaxed);
        self.entries_applied
            .fetch_add(stats.entries_applied, Ordering::Relaxed);
        self.deletes_applied
            .fetch_add(stats.deletes_applied, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(stats.bytes_received, Ordering::Relaxed);
        self.bytes_applied
            .fetch_add(stats.bytes_applied, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_fallback_to_checkpoint(&self) {
        self.fallback_to_checkpoint.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StandbyPageDeltaMetricsSnapshot {
        StandbyPageDeltaMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            fallback_to_checkpoint: self.fallback_to_checkpoint.load(Ordering::Relaxed),
            entries_seen: self.entries_seen.load(Ordering::Relaxed),
            entries_applied: self.entries_applied.load(Ordering::Relaxed),
            deletes_applied: self.deletes_applied.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_applied: self.bytes_applied.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct StandbyPageDeltaStats {
    entries_seen: u64,
    entries_applied: u64,
    deletes_applied: u64,
    bytes_received: u64,
    bytes_applied: u64,
}

#[derive(Debug, Clone, Serialize)]
struct StandbyPageDeltaMetricsSnapshot {
    attempts: u64,
    successes: u64,
    failures: u64,
    fallback_to_checkpoint: u64,
    entries_seen: u64,
    entries_applied: u64,
    deletes_applied: u64,
    bytes_received: u64,
    bytes_applied: u64,
}

#[derive(Default)]
struct PlacementMoveTransferMetrics {
    page_delta_attempts: AtomicU64,
    page_delta_successes: AtomicU64,
    page_delta_failures: AtomicU64,
    checkpoint_attempts: AtomicU64,
    checkpoint_successes: AtomicU64,
    checkpoint_failures: AtomicU64,
    backup_attempts: AtomicU64,
    backup_successes: AtomicU64,
    backup_failures: AtomicU64,
    checkpoint_objects_seen: AtomicU64,
    checkpoint_objects_copied: AtomicU64,
    checkpoint_objects_reused: AtomicU64,
    checkpoint_bytes_seen: AtomicU64,
    checkpoint_bytes_copied: AtomicU64,
    page_delta_entries_applied: AtomicU64,
    page_delta_deletes_applied: AtomicU64,
    page_delta_bytes_applied: AtomicU64,
}

impl PlacementMoveTransferMetrics {
    fn record_checkpoint_attempt(&self) {
        self.checkpoint_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_checkpoint_success(&self, stats: StandbyCheckpointFetchStats) {
        self.checkpoint_successes.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_objects_seen
            .fetch_add(stats.objects_seen, Ordering::Relaxed);
        self.checkpoint_objects_copied
            .fetch_add(stats.objects_copied, Ordering::Relaxed);
        self.checkpoint_objects_reused
            .fetch_add(stats.objects_reused, Ordering::Relaxed);
        self.checkpoint_bytes_seen
            .fetch_add(stats.bytes_seen, Ordering::Relaxed);
        self.checkpoint_bytes_copied
            .fetch_add(stats.bytes_copied, Ordering::Relaxed);
    }

    fn record_checkpoint_failure(&self) {
        self.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PlacementMoveTransferMetricsSnapshot {
        PlacementMoveTransferMetricsSnapshot {
            page_delta_attempts: self.page_delta_attempts.load(Ordering::Relaxed),
            page_delta_successes: self.page_delta_successes.load(Ordering::Relaxed),
            page_delta_failures: self.page_delta_failures.load(Ordering::Relaxed),
            checkpoint_attempts: self.checkpoint_attempts.load(Ordering::Relaxed),
            checkpoint_successes: self.checkpoint_successes.load(Ordering::Relaxed),
            checkpoint_failures: self.checkpoint_failures.load(Ordering::Relaxed),
            backup_attempts: self.backup_attempts.load(Ordering::Relaxed),
            backup_successes: self.backup_successes.load(Ordering::Relaxed),
            backup_failures: self.backup_failures.load(Ordering::Relaxed),
            checkpoint_objects_seen: self.checkpoint_objects_seen.load(Ordering::Relaxed),
            checkpoint_objects_copied: self.checkpoint_objects_copied.load(Ordering::Relaxed),
            checkpoint_objects_reused: self.checkpoint_objects_reused.load(Ordering::Relaxed),
            checkpoint_bytes_seen: self.checkpoint_bytes_seen.load(Ordering::Relaxed),
            checkpoint_bytes_copied: self.checkpoint_bytes_copied.load(Ordering::Relaxed),
            page_delta_entries_applied: self.page_delta_entries_applied.load(Ordering::Relaxed),
            page_delta_deletes_applied: self.page_delta_deletes_applied.load(Ordering::Relaxed),
            page_delta_bytes_applied: self.page_delta_bytes_applied.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PlacementMoveTransferMetricsSnapshot {
    page_delta_attempts: u64,
    page_delta_successes: u64,
    page_delta_failures: u64,
    checkpoint_attempts: u64,
    checkpoint_successes: u64,
    checkpoint_failures: u64,
    backup_attempts: u64,
    backup_successes: u64,
    backup_failures: u64,
    checkpoint_objects_seen: u64,
    checkpoint_objects_copied: u64,
    checkpoint_objects_reused: u64,
    checkpoint_bytes_seen: u64,
    checkpoint_bytes_copied: u64,
    page_delta_entries_applied: u64,
    page_delta_deletes_applied: u64,
    page_delta_bytes_applied: u64,
}

impl BlobApiMetrics {
    fn record_request(
        &self,
        op: BlobApiOp,
        elapsed: Duration,
        result: Result<BlobRequestStats, bool>,
    ) {
        match op {
            BlobApiOp::Open => self.open_requests.fetch_add(1, Ordering::Relaxed),
            BlobApiOp::Read => self.read_requests.fetch_add(1, Ordering::Relaxed),
            BlobApiOp::Write => self.write_requests.fetch_add(1, Ordering::Relaxed),
            BlobApiOp::Reopen => self.reopen_requests.fetch_add(1, Ordering::Relaxed),
            BlobApiOp::Close => self.close_requests.fetch_add(1, Ordering::Relaxed),
        };
        self.request_latency_ns.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        match result {
            Ok(stats) => {
                self.bytes_read
                    .fetch_add(stats.bytes_read as u64, Ordering::Relaxed);
                self.bytes_written
                    .fetch_add(stats.bytes_written as u64, Ordering::Relaxed);
                self.observe_open_handles(stats.open_handles);
            }
            Err(rejected_open_handles) => {
                self.failed_requests.fetch_add(1, Ordering::Relaxed);
                if rejected_open_handles {
                    self.rejected_open_handles.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn observe_open_handles(&self, open_handles: usize) {
        let open_handles = open_handles as u64;
        let mut current = self.max_open_handles_observed.load(Ordering::Relaxed);
        while open_handles > current {
            match self.max_open_handles_observed.compare_exchange_weak(
                current,
                open_handles,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn snapshot(
        &self,
        max_chunk_bytes: u64,
        max_open_handles_per_session: u64,
        sessions: u64,
        current_open_handles: u64,
    ) -> BlobApiMetricsSnapshot {
        let open_requests = self.open_requests.load(Ordering::Relaxed);
        let read_requests = self.read_requests.load(Ordering::Relaxed);
        let write_requests = self.write_requests.load(Ordering::Relaxed);
        let reopen_requests = self.reopen_requests.load(Ordering::Relaxed);
        let close_requests = self.close_requests.load(Ordering::Relaxed);
        let total_requests =
            open_requests + read_requests + write_requests + reopen_requests + close_requests;
        BlobApiMetricsSnapshot {
            max_chunk_bytes,
            max_open_handles_per_session,
            sessions,
            current_open_handles,
            max_open_handles_observed: self.max_open_handles_observed.load(Ordering::Relaxed),
            requests: BlobApiRequestMetricsSnapshot {
                total: total_requests,
                open: open_requests,
                read: read_requests,
                write: write_requests,
                reopen: reopen_requests,
                close: close_requests,
                failed: self.failed_requests.load(Ordering::Relaxed),
                rejected_open_handles: self.rejected_open_handles.load(Ordering::Relaxed),
            },
            bytes: BlobApiByteMetricsSnapshot {
                read: self.bytes_read.load(Ordering::Relaxed),
                written: self.bytes_written.load(Ordering::Relaxed),
            },
            latency: BlobApiLatencyMetricsSnapshot {
                total_ns: self.request_latency_ns.load(Ordering::Relaxed),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BlobRequestStats {
    bytes_read: usize,
    bytes_written: usize,
    open_handles: usize,
}

impl BlobRequestStats {
    fn from_response(response: &BlobResponseKind, open_handles: usize) -> Self {
        match response {
            BlobResponseKind::Read { bytes_read, .. } => Self {
                bytes_read: *bytes_read,
                bytes_written: 0,
                open_handles,
            },
            BlobResponseKind::Write { bytes_written, .. } => Self {
                bytes_read: 0,
                bytes_written: *bytes_written,
                open_handles,
            },
            _ => Self {
                bytes_read: 0,
                bytes_written: 0,
                open_handles,
            },
        }
    }

    fn from_blob_read(response: &BlobBytesReadResponse, open_handles: usize) -> Self {
        Self {
            bytes_read: response.bytes_read,
            bytes_written: 0,
            open_handles,
        }
    }

    fn from_blob_write(response: &BlobBytesWriteResponse, open_handles: usize) -> Self {
        Self {
            bytes_read: 0,
            bytes_written: response.bytes_written,
            open_handles,
        }
    }
}

#[derive(Default)]
struct IdempotencyMetrics {
    requests: AtomicU64,
    committed_new: AtomicU64,
    committed_reused: AtomicU64,
    conflicts: AtomicU64,
    commit_unknown: AtomicU64,
    rejected: AtomicU64,
    gc_runs: AtomicU64,
    gc_failures: AtomicU64,
    gc_deleted_committed: AtomicU64,
    gc_deleted_pending: AtomicU64,
}

impl IdempotencyMetrics {
    fn snapshot(
        &self,
        config: &LibsqlHttpIdempotencyConfig,
        active_session_databases: u64,
    ) -> IdempotencyMetricsSnapshot {
        IdempotencyMetricsSnapshot {
            enabled: config.enabled,
            committed_ttl_ms: config.committed_ttl_ms,
            pending_ttl_ms: config.pending_ttl_ms,
            gc_interval_ms: config.gc_interval_ms,
            gc_max_records_per_pass: config.gc_max_records_per_pass as u64,
            active_session_databases,
            requests: self.requests.load(Ordering::Relaxed),
            committed_new: self.committed_new.load(Ordering::Relaxed),
            committed_reused: self.committed_reused.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            commit_unknown: self.commit_unknown.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            gc: IdempotencyGcMetricsSnapshot {
                runs: self.gc_runs.load(Ordering::Relaxed),
                failures: self.gc_failures.load(Ordering::Relaxed),
                deleted_committed: self.gc_deleted_committed.load(Ordering::Relaxed),
                deleted_pending: self.gc_deleted_pending.load(Ordering::Relaxed),
            },
        }
    }

    fn record_gc(&self, stats: IdempotencyGcStats) {
        self.gc_runs.fetch_add(1, Ordering::Relaxed);
        self.gc_deleted_committed
            .fetch_add(stats.deleted_committed as u64, Ordering::Relaxed);
        self.gc_deleted_pending
            .fetch_add(stats.deleted_pending as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Serialize)]
struct IdempotencyMetricsSnapshot {
    enabled: bool,
    committed_ttl_ms: u64,
    pending_ttl_ms: u64,
    gc_interval_ms: u64,
    gc_max_records_per_pass: u64,
    active_session_databases: u64,
    requests: u64,
    committed_new: u64,
    committed_reused: u64,
    conflicts: u64,
    commit_unknown: u64,
    rejected: u64,
    gc: IdempotencyGcMetricsSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct IdempotencyGcMetricsSnapshot {
    runs: u64,
    failures: u64,
    deleted_committed: u64,
    deleted_pending: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct IdempotencyGcStats {
    deleted_committed: usize,
    deleted_pending: usize,
}

impl IdempotencyGcStats {
    fn deleted_total(self) -> usize {
        self.deleted_committed + self.deleted_pending
    }
}

#[derive(Debug, Clone, Serialize)]
struct BlobApiMetricsSnapshot {
    max_chunk_bytes: u64,
    max_open_handles_per_session: u64,
    sessions: u64,
    current_open_handles: u64,
    max_open_handles_observed: u64,
    requests: BlobApiRequestMetricsSnapshot,
    bytes: BlobApiByteMetricsSnapshot,
    latency: BlobApiLatencyMetricsSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct BlobApiRequestMetricsSnapshot {
    total: u64,
    open: u64,
    read: u64,
    write: u64,
    reopen: u64,
    close: u64,
    failed: u64,
    rejected_open_handles: u64,
}

#[derive(Debug, Clone, Serialize)]
struct BlobApiByteMetricsSnapshot {
    read: u64,
    written: u64,
}

#[derive(Debug, Clone, Serialize)]
struct BlobApiLatencyMetricsSnapshot {
    total_ns: u64,
}

impl LibsqlHttpState {
    fn new(runtime: OrionSqliteRuntime, config: &LibsqlHttpConfig) -> Self {
        let replication_groups = config
            .replication_groups
            .clone()
            .unwrap_or_else(|| ReplicationGroupRegistry::single_default(runtime));
        let sqlite_state = replication_groups.state_store();
        Self {
            replication_groups,
            sqlite_state,
            sqlite_cache_root: config.sqlite_cache_root.clone(),
            databases: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_baton_id: Arc::new(AtomicU64::new(1)),
            session_idle_timeout: config.session_idle_timeout,
            blob_max_chunk_bytes: config.blob_max_chunk_bytes,
            blob_metrics: Arc::new(BlobApiMetrics::default()),
            standby_checkpoint_metrics: Arc::new(StandbyCheckpointMetrics::default()),
            standby_page_delta_metrics: Arc::new(StandbyPageDeltaMetrics::default()),
            placement_move_transfer_metrics: Arc::new(PlacementMoveTransferMetrics::default()),
            idempotency_config: config.idempotency.clone(),
            idempotency_metrics: Arc::new(IdempotencyMetrics::default()),
            auth: config.auth.clone(),
            node_id: config.node_id,
            peer_http_endpoints: Arc::new(config.peer_http_endpoints.clone()),
            placement_nodes: Arc::new(config.placement_nodes.clone()),
            metrics_registry: config.metrics_registry.clone(),
            compaction_policy: config.compaction_policy.clone(),
            http_client: reqwest::Client::new(),
            standby_refreshes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn database(&self, name: &str) -> anyhow::Result<Arc<OrionSqliteDb>> {
        self.ensure_database_not_fenced_for_placement(name)?;
        let (runtime, group_id) = self.runtime_and_group_for_database(name)?;
        {
            let databases = self
                .databases
                .lock()
                .map_err(|_| anyhow!("libSQL database cache mutex poisoned"))?;
            if let Some(entry) = databases.get(name)
                && entry.group_id == group_id
            {
                return Ok(Arc::clone(&entry.db));
            }
        }
        let db = Arc::new(runtime.open_database(name)?);
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| anyhow!("libSQL database cache mutex poisoned"))?;
        if let Some(existing) = databases.get(name)
            && existing.group_id == group_id
        {
            return Ok(Arc::clone(&existing.db));
        }
        databases.insert(
            name.to_string(),
            DatabaseCacheEntry {
                group_id,
                db: Arc::clone(&db),
            },
        );
        Ok(db)
    }

    fn ensure_database_not_fenced_for_placement(&self, name: &str) -> anyhow::Result<()> {
        if name == ORION_CATALOG_DATABASE || name == ORION_SYSTEM_DATABASE {
            return Ok(());
        }
        let conn = open_system_catalog_connection(self)?;
        let Some(record) = read_database_catalog_record_from_conn(&conn, name)? else {
            return Ok(());
        };
        if let Some(operation) =
            read_running_placement_operation_for_database(&conn, &record.database_id)?
        {
            anyhow::bail!(
                "database {name} is fenced for placement operation {} at phase {}",
                operation.operation_id,
                operation.phase
            );
        }
        Ok(())
    }

    fn runtime_for_database(&self, name: &str) -> anyhow::Result<OrionSqliteRuntime> {
        self.runtime_and_group_for_database(name)
            .map(|(runtime, _)| runtime)
    }

    fn runtime_and_group_for_database(
        &self,
        name: &str,
    ) -> anyhow::Result<(OrionSqliteRuntime, String)> {
        if name == ORION_CATALOG_DATABASE || name == ORION_SYSTEM_DATABASE {
            return Ok((
                self.replication_groups.default_runtime()?,
                DEFAULT_REPLICATION_GROUP_ID.to_string(),
            ));
        }
        let conn = open_system_catalog_connection(self)?;
        let Some(record) = read_database_catalog_record_from_conn(&conn, name)? else {
            anyhow::bail!("database {name} has not been created");
        };
        if let Some(operation) =
            read_running_placement_operation_for_database(&conn, &record.database_id)?
        {
            anyhow::bail!(
                "database {name} is fenced for placement operation {} at phase {}",
                operation.operation_id,
                operation.phase
            );
        }
        let runtime = self
            .replication_groups
            .runtime(&record.replication_group_id)
            .with_context(|| {
                format!(
                    "resolving runtime for database {name} replication group {}",
                    record.replication_group_id
                )
            })?;
        Ok((runtime, record.replication_group_id))
    }

    fn session(
        &self,
        database: &str,
        baton: Option<String>,
    ) -> anyhow::Result<(String, Arc<Mutex<LibsqlSession>>)> {
        validate_database_name(database)?;
        if let Some(baton) = baton {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow!("libSQL session map mutex poisoned"))?;
            if let Some(session) = sessions.get(&baton) {
                {
                    let mut session_guard = session
                        .lock()
                        .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                    ensure!(
                        session_guard.database == database,
                        "baton belongs to database {}, not {}",
                        session_guard.database,
                        database
                    );
                    session_guard.last_used_at = Instant::now();
                }
                return Ok((baton, Arc::clone(session)));
            }
            anyhow::bail!("unknown or expired baton");
        }

        let db = self.database(database)?;
        let conn = db.connect()?;
        let catalog_db = if database == ORION_SYSTEM_DATABASE {
            self.database(ORION_CATALOG_DATABASE).ok()
        } else {
            None
        };
        let system_context = (database == ORION_SYSTEM_DATABASE).then(|| SystemQueryContext {
            metrics_registry: self.metrics_registry.clone(),
            replication_groups: self.replication_groups.clone(),
            sqlite_state: self.sqlite_state.clone(),
            compaction_policy: self.compaction_policy.clone(),
            catalog_db,
            placement_nodes: self.placement_nodes.clone(),
            idempotency_config: self.idempotency_config.clone(),
            idempotency_metrics: self.idempotency_metrics.clone(),
            tokio_handle: tokio::runtime::Handle::current(),
        });
        let baton = format!(
            "{database}-{}",
            self.next_baton_id.fetch_add(1, Ordering::Relaxed)
        );
        let session = Arc::new(Mutex::new(LibsqlSession {
            database: database.to_string(),
            conn,
            system_context,
            stored_sql: HashMap::new(),
            blob_handles: HashMap::new(),
            next_blob_id: 1,
            last_used_at: Instant::now(),
        }));
        self.sessions
            .lock()
            .map_err(|_| anyhow!("libSQL session map mutex poisoned"))?
            .insert(baton.clone(), Arc::clone(&session));
        Ok((baton, session))
    }

    fn close_session(&self, baton: &str) -> anyhow::Result<()> {
        self.sessions
            .lock()
            .map_err(|_| anyhow!("libSQL session map mutex poisoned"))?
            .remove(baton);
        Ok(())
    }

    fn close_database_sessions(&self, database: &str) -> anyhow::Result<usize> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow!("libSQL session map mutex poisoned"))?;
        let before = sessions.len();
        sessions.retain(|_, session| {
            session
                .lock()
                .map(|session| session.database != database)
                .unwrap_or(false)
        });
        Ok(before.saturating_sub(sessions.len()))
    }

    fn active_database_sessions(&self, database: &str) -> anyhow::Result<usize> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow!("libSQL session map mutex poisoned"))?;
        let mut active = 0;
        for session in sessions.values() {
            if session
                .lock()
                .map_err(|_| anyhow!("libSQL session mutex poisoned"))?
                .database
                == database
            {
                active += 1;
            }
        }
        Ok(active)
    }

    fn evict_database(&self, database: &str) -> anyhow::Result<()> {
        self.databases
            .lock()
            .map_err(|_| anyhow!("libSQL database cache mutex poisoned"))?
            .remove(database);
        let _ = clear_orion_vfs_shared_state(&self.sqlite_cache_root, database)?;
        Ok(())
    }

    fn blob_metrics_snapshot(&self) -> BlobApiMetricsSnapshot {
        let (sessions, current_open_handles) = self
            .sessions
            .lock()
            .map(|sessions| {
                let mut current_open_handles = 0;
                for session in sessions.values() {
                    if let Ok(session) = session.lock() {
                        current_open_handles += session.blob_handles.len() as u64;
                    }
                }
                (sessions.len() as u64, current_open_handles)
            })
            .unwrap_or_default();
        self.blob_metrics.snapshot(
            self.blob_max_chunk_bytes as u64,
            MAX_OPEN_BLOB_HANDLES_PER_SESSION as u64,
            sessions,
            current_open_handles,
        )
    }

    fn idempotency_metrics_snapshot(&self) -> IdempotencyMetricsSnapshot {
        self.idempotency_metrics.snapshot(
            &self.idempotency_config,
            self.active_session_database_count() as u64,
        )
    }

    fn active_session_database_count(&self) -> usize {
        let Ok(sessions) = self.sessions.lock() else {
            return 0;
        };
        sessions
            .values()
            .filter_map(|session| session.lock().ok().map(|session| session.database.clone()))
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }

    fn authorize(&self, headers: &HeaderMap, database: &str) -> anyhow::Result<()> {
        if self.auth.tokens.is_empty() {
            return Ok(());
        }
        let Some(token) = bearer_token(headers) else {
            anyhow::bail!("missing bearer token");
        };
        let Some(rule) = self.auth.tokens.iter().find(|rule| rule.token == token) else {
            anyhow::bail!("invalid bearer token");
        };
        if database == ORION_SYSTEM_DATABASE {
            ensure!(
                rule.has_system_read(),
                "bearer token is not authorized for system namespace {database}"
            );
            return Ok(());
        }
        ensure!(
            rule.database_prefixes
                .iter()
                .any(|prefix| database.starts_with(prefix)),
            "bearer token is not authorized for database {database}"
        );
        Ok(())
    }

    fn authorize_system_admin(&self, headers: &HeaderMap) -> anyhow::Result<()> {
        if self.auth.tokens.is_empty() {
            return Ok(());
        }
        let Some(token) = bearer_token(headers) else {
            anyhow::bail!("missing bearer token");
        };
        let Some(rule) = self.auth.tokens.iter().find(|rule| rule.token == token) else {
            anyhow::bail!("invalid bearer token");
        };
        ensure!(
            rule.has_system_admin(),
            "bearer token is not authorized for system admin operations"
        );
        Ok(())
    }

    fn internal_system_admin_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let Some(token) = self
            .auth
            .tokens
            .iter()
            .find(|token| token.has_system_admin())
        else {
            return headers;
        };
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", token.token)) {
            headers.insert(axum::http::header::AUTHORIZATION, value);
        }
        headers
    }

    async fn reap_idle_sessions(self) {
        let mut interval = tokio::time::interval(SESSION_GC_INTERVAL);
        loop {
            interval.tick().await;
            self.reap_idle_sessions_once();
        }
    }

    async fn collect_idempotency_garbage(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.idempotency_config.gc_interval_ms,
        ));
        loop {
            interval.tick().await;
            self.collect_idempotency_garbage_once();
        }
    }

    fn collect_idempotency_garbage_once(&self) {
        if !self.idempotency_config.enabled {
            return;
        }
        let sessions = match self.sessions.lock() {
            Ok(sessions) => sessions.values().cloned().collect::<Vec<_>>(),
            Err(_) => return,
        };
        let mut remaining = self.idempotency_config.gc_max_records_per_pass;
        for session in sessions {
            if remaining == 0 {
                break;
            }
            let Ok(session) = session.lock() else {
                continue;
            };
            match collect_idempotency_garbage_for_connection(
                &session.conn,
                &self.idempotency_config,
                remaining,
            ) {
                Ok(stats) => {
                    remaining = remaining.saturating_sub(stats.deleted_total());
                    self.idempotency_metrics.record_gc(stats);
                }
                Err(_) => {
                    self.idempotency_metrics
                        .gc_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn reconcile_database_lifecycle(self) {
        let mut interval = tokio::time::interval(DATABASE_LIFECYCLE_RECONCILE_INTERVAL);
        loop {
            interval.tick().await;
            let _ = self.reconcile_database_lifecycle_once().await;
        }
    }

    async fn reconcile_database_lifecycle_once(&self) -> anyhow::Result<usize> {
        let records = list_database_catalog_records(self, true)?;
        let mut reconciled = 0;
        for record in records {
            match record.state.as_str() {
                "creating" => {
                    let conn = open_system_catalog_connection(self)?;
                    match self.database(&record.name).and_then(|db| {
                        db.connect()
                            .map(|_| ())
                            .with_context(|| format!("opening database {}", record.name))
                    }) {
                        Ok(()) => {
                            mark_database_state(&conn, &record.name, "ready", None, None)?;
                        }
                        Err(error) => {
                            mark_database_state(&conn, &record.name, "failed", None, Some(&error))?;
                        }
                    }
                    reconciled += 1;
                }
                "deleting" => {
                    let conn = open_system_catalog_connection(self)?;
                    let _ = self.close_database_sessions(&record.name);
                    let _ = self.evict_database(&record.name);
                    mark_database_state(
                        &conn,
                        &record.name,
                        "deleted",
                        Some(current_time_millis()),
                        None,
                    )?;
                    reconciled += 1;
                }
                "deleted" if record.purged_at_ms.is_none() => {
                    let Some(deleted_at_ms) = record.deleted_at_ms else {
                        continue;
                    };
                    let conn = open_system_catalog_connection(self)?;
                    if read_catalog_schema_version(&conn)?
                        .unwrap_or(infer_database_catalog_schema_version(&conn)?)
                        < 3
                    {
                        continue;
                    }
                    let policy = SqliteDatabasePurgePolicy::default();
                    match purge_tombstoned_sqlite_database(
                        &self.sqlite_state,
                        &record.name,
                        deleted_at_ms,
                        current_time_millis(),
                        &policy,
                    )
                    .await
                    {
                        Ok(metrics) if metrics.skipped_for_retention => {}
                        Ok(metrics) => {
                            let conn = open_system_catalog_connection(self)?;
                            mark_database_purge_progress(&conn, &record.name, &metrics, None)?;
                            reconciled += 1;
                        }
                        Err(error) => {
                            let conn = open_system_catalog_connection(self)?;
                            let empty = SqliteDatabasePurgeMetrics {
                                database: record.name.clone(),
                                ..SqliteDatabasePurgeMetrics::default()
                            };
                            mark_database_purge_progress(
                                &conn,
                                &record.name,
                                &empty,
                                Some(&error),
                            )?;
                            reconciled += 1;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(reconciled)
    }

    async fn reconcile_standby_refreshes(self) {
        let mut interval = tokio::time::interval(STANDBY_REFRESH_RECONCILE_INTERVAL);
        loop {
            interval.tick().await;
            let _ = self.reconcile_standby_refreshes_once().await;
        }
    }

    async fn reconcile_standby_refreshes_once(
        &self,
    ) -> anyhow::Result<PlacementStandbyRefreshSummary> {
        let checked_at = current_time_millis();
        let groups = list_replication_group_records(self)?;
        let target_groups = groups
            .iter()
            .filter(|group| {
                group.state == "active"
                    && group.group_id != DEFAULT_REPLICATION_GROUP_ID
                    && group.failover_automatic
                    && group.runtime.loaded
                    && group.runtime.ready_for_linearizable_reads
                    && group.runtime.current_leader == Some(self.node_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let source_groups = groups
            .iter()
            .filter(|group| {
                group.state == "active"
                    && group.group_id != DEFAULT_REPLICATION_GROUP_ID
                    && group.failover_automatic
            })
            .cloned()
            .collect::<Vec<_>>();
        let conn = open_system_catalog_connection(self)?;
        require_database_catalog_schema(&conn, 7)?;
        let mut plans = Vec::new();
        let mut skipped = 0_u64;
        for source_group in source_groups {
            let target_group_ids =
                standby_refresh_target_group_ids(&conn, &target_groups, &source_group.group_id)?;
            if target_group_ids.is_empty() {
                skipped += 1;
                continue;
            }
            let refresh_after_ms = standby_refresh_due_after_ms(&source_group);
            for database in database_records_for_group(&conn, &source_group.group_id)? {
                if read_running_placement_operation_for_database(&conn, &database.database_id)?
                    .is_some()
                {
                    skipped += 1;
                    continue;
                }
                for target_group_id in &target_group_ids {
                    if !standby_refresh_due(
                        &conn,
                        &database,
                        &source_group.group_id,
                        target_group_id,
                        refresh_after_ms,
                        checked_at,
                    )? {
                        skipped += 1;
                        continue;
                    }
                    plans.push(PlacementStandbyRefreshPlan {
                        database_name: database.name.clone(),
                        source_group_id: source_group.group_id.clone(),
                        target_group_id: target_group_id.clone(),
                    });
                }
            }
        }
        drop(conn);

        let attempted = plans.len();
        let mut refreshed = 0_u64;
        let mut actions = Vec::new();
        let mut risks = Vec::new();
        let headers = self.internal_system_admin_headers();
        for plan in plans {
            let Some(_guard) =
                self.try_begin_standby_refresh(&plan.database_name, &plan.target_group_id)?
            else {
                skipped += 1;
                continue;
            };
            match refresh_database_placement_standby_for_endpoint_inner(
                self,
                &headers,
                &plan.database_name,
                Some(&plan.source_group_id),
                &plan.target_group_id,
            )
            .await
            {
                Ok(record) => {
                    refreshed += 1;
                    actions.push(format!(
                        "automatic_standby_refresh:{}:{}:{}",
                        record.standby.database_name,
                        record.standby.source_group_id,
                        record.standby.target_group_id
                    ));
                }
                Err(error) => risks.push(format!(
                    "automatic standby refresh for database {} from {} to {} failed: {}",
                    plan.database_name,
                    plan.source_group_id,
                    plan.target_group_id,
                    error_chain_message(&error)
                )),
            }
        }

        Ok(PlacementStandbyRefreshSummary {
            checked_at_ms: checked_at,
            attempted: attempted as u64,
            refreshed,
            skipped,
            errors: risks.len() as u64,
            actions,
            risks,
        })
    }

    fn try_begin_standby_refresh(
        &self,
        database: &str,
        target_group_id: &str,
    ) -> anyhow::Result<Option<StandbyRefreshGuard>> {
        let key = format!("{database}\u{0}{target_group_id}");
        let mut active = self
            .standby_refreshes
            .lock()
            .map_err(|_| anyhow!("standby refresh mutex poisoned"))?;
        if !active.insert(key.clone()) {
            return Ok(None);
        }
        Ok(Some(StandbyRefreshGuard {
            key,
            active: Arc::clone(&self.standby_refreshes),
        }))
    }

    fn reap_idle_sessions_once(&self) {
        let Ok(mut sessions) = self.sessions.lock() else {
            return;
        };
        let now = Instant::now();
        sessions.retain(|_, session| {
            let Ok(session) = session.lock() else {
                return false;
            };
            now.duration_since(session.last_used_at) <= self.session_idle_timeout
        });
    }
}

impl LibsqlHttpAuthTokenConfig {
    fn has_system_read(&self) -> bool {
        self.system_permissions.iter().any(|permission| {
            matches!(
                permission,
                LibsqlHttpSystemPermission::Read | LibsqlHttpSystemPermission::Admin
            )
        })
    }

    fn has_system_admin(&self) -> bool {
        self.system_permissions
            .iter()
            .any(|permission| matches!(permission, LibsqlHttpSystemPermission::Admin))
    }
}

struct LibsqlSession {
    database: String,
    conn: Connection,
    system_context: Option<SystemQueryContext>,
    stored_sql: HashMap<i64, String>,
    blob_handles: HashMap<String, BlobHandle>,
    next_blob_id: u64,
    last_used_at: Instant,
}

#[derive(Debug, Clone)]
struct BlobHandle {
    schema: String,
    table: String,
    column: String,
    rowid: i64,
    read_only: bool,
}

#[derive(Debug)]
struct OrionBlobTooManyOpenHandlesError {
    max_open_handles: usize,
}

impl fmt::Display for OrionBlobTooManyOpenHandlesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "too many open blob handles in session: max_open_handles={}",
            self.max_open_handles
        )
    }
}

impl Error for OrionBlobTooManyOpenHandlesError {}

#[derive(Clone)]
struct SystemQueryContext {
    metrics_registry: ClusterRaftMetricsRegistry,
    replication_groups: ReplicationGroupRegistry,
    sqlite_state: SlateDbStateStore,
    compaction_policy: SqlitePageCompactionPolicy,
    catalog_db: Option<Arc<OrionSqliteDb>>,
    placement_nodes: Arc<BTreeMap<u64, PlacementNodeConfig>>,
    idempotency_config: LibsqlHttpIdempotencyConfig,
    idempotency_metrics: Arc<IdempotencyMetrics>,
    tokio_handle: tokio::runtime::Handle,
}

impl SystemQueryContext {
    fn placement_nodes(&self) -> Vec<PlacementNodeConfig> {
        self.placement_nodes.values().cloned().collect()
    }
}

pub async fn serve_libsql_http_with_shutdown<F>(
    listener: TcpListener,
    raft: OrionRaft,
    state: SlateDbStateStore,
    config: LibsqlHttpConfig,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let runtime = OrionSqliteRuntime::new(
        raft,
        state,
        OrionSqliteRuntimeConfig::new(config.sqlite_cache_root.clone()),
    );
    let state = LibsqlHttpState::new(runtime, &config);
    initialize_system_catalog_for_service(&state)?;
    let session_gc = tokio::spawn(state.clone().reap_idle_sessions());
    let idempotency_gc = tokio::spawn(state.clone().collect_idempotency_garbage());
    let lifecycle_reconciler = tokio::spawn(state.clone().reconcile_database_lifecycle());
    let standby_refresh_reconciler = tokio::spawn(state.clone().reconcile_standby_refreshes());
    let result = axum::serve(listener, libsql_router(state))
        .with_graceful_shutdown(shutdown)
        .await
        .context("serving libSQL HTTP");
    session_gc.abort();
    let _ = session_gc.await;
    idempotency_gc.abort();
    let _ = idempotency_gc.await;
    lifecycle_reconciler.abort();
    let _ = lifecycle_reconciler.await;
    standby_refresh_reconciler.abort();
    let _ = standby_refresh_reconciler.await;
    result?;
    Ok(())
}

fn libsql_router(state: LibsqlHttpState) -> Router {
    Router::new()
        .route("/", get(default_v2))
        .route("/v2", get(default_v2))
        .route("/v2/pipeline", post(default_pipeline))
        .route("/v2/blob/open", post(default_blob_open))
        .route("/v2/blob/read", post(default_blob_read))
        .route("/v2/blob/write", post(default_blob_write))
        .route("/v2/blob/read-bytes", get(default_blob_read_bytes))
        .route("/v2/blob/write-bytes", post(default_blob_write_bytes))
        .route("/v2/blob/read-stream", get(default_blob_read_stream))
        .route("/v2/blob/write-stream", post(default_blob_write_stream))
        .route("/v2/blob/reopen", post(default_blob_reopen))
        .route("/v2/blob/close", post(default_blob_close))
        .route("/_orion/metrics/raft", get(raft_metrics_endpoint))
        .route("/_orion/metrics/storage", get(storage_metrics_endpoint))
        .route("/_orion/metrics/blob", get(blob_metrics_endpoint))
        .route(
            "/_orion/metrics/large-payload",
            get(large_payload_metrics_endpoint),
        )
        .route("/_orion/metrics/placement", get(placement_metrics_endpoint))
        .route(
            "/_orion/metrics/idempotency",
            get(idempotency_metrics_endpoint),
        )
        .route("/_orion/placement/nodes", get(placement_nodes_endpoint))
        .route(
            "/_orion/replication-groups",
            get(list_replication_groups_endpoint),
        )
        .route(
            "/_orion/replication-groups",
            post(create_replication_group_endpoint),
        )
        .route(
            "/_orion/replication-groups/runtime",
            get(replication_group_runtime_endpoint),
        )
        .route(
            "/_orion/placement/reconcile",
            post(reconcile_placement_endpoint),
        )
        .route(
            "/_orion/placement/standby/reconcile",
            post(reconcile_standby_refresh_endpoint),
        )
        .route("/_orion/placement/gc", post(placement_gc_endpoint))
        .route(
            "/_orion/replication-groups/{group_id}",
            get(get_replication_group_endpoint),
        )
        .route(
            "/_orion/replication-groups/{group_id}",
            axum::routing::delete(delete_replication_group_endpoint),
        )
        .route(
            "/_orion/replication-groups/{group_id}/drain",
            post(drain_replication_group_endpoint),
        )
        .route(
            "/_orion/replication-groups/{group_id}/members",
            post(add_replication_group_member_endpoint),
        )
        .route(
            "/_orion/replication-groups/{group_id}/members/{node_id}/{role}",
            axum::routing::delete(remove_replication_group_member_endpoint),
        )
        .route("/_orion/databases", get(list_databases_endpoint))
        .route("/_orion/databases", post(create_database_endpoint))
        .route(
            "/_orion/catalog/activate-schema",
            post(activate_catalog_schema_endpoint),
        )
        .route("/_orion/catalog/rollout", get(catalog_rollout_endpoint))
        .route("/_orion/databases/{database}", get(get_database_endpoint))
        .route(
            "/_orion/databases/{database}/placement",
            get(get_database_placement_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/plan",
            post(plan_database_placement_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/move",
            post(move_database_placement_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/standbys",
            get(list_database_placement_standbys_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/standby",
            post(refresh_database_placement_standby_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/promote",
            post(promote_database_placement_standby_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/export",
            get(export_database_placement_standby_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/checkpoint",
            get(export_database_placement_checkpoint_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/pages",
            get(export_database_placement_page_delta_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/files",
            get(export_database_placement_file_snapshot_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/checkpoint/object",
            get(export_database_placement_checkpoint_object_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/checkpoint/materialize",
            post(materialize_database_placement_checkpoint_endpoint),
        )
        .route(
            "/_orion/internal/databases/{database}/placement/pages/materialize",
            post(materialize_database_placement_page_delta_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/operations",
            get(list_database_placement_operations_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/operations/{operation_id}/cancel",
            post(cancel_database_placement_operation_endpoint),
        )
        .route(
            "/_orion/databases/{database}/placement/operations/{operation_id}/repair",
            post(repair_database_placement_operation_endpoint),
        )
        .route(
            "/_orion/databases/{database}",
            axum::routing::delete(drop_database_endpoint),
        )
        .route("/_orion/compaction", get(compaction_endpoint))
        .route("/_orion/compaction/pause", post(compaction_pause_endpoint))
        .route(
            "/_orion/compaction/resume",
            post(compaction_resume_endpoint),
        )
        .route("/_orion/compaction/force", post(compaction_force_endpoint))
        .route(
            "/_orion/compaction/retention-floor",
            post(compaction_set_retention_floor_endpoint),
        )
        .route(
            "/_orion/compaction/retention-floor/clear",
            post(compaction_clear_retention_floor_endpoint),
        )
        .route("/{database}", get(database_v2))
        .route("/{database}/v2", get(database_v2))
        .route("/{database}/v2/pipeline", post(database_pipeline))
        .route("/{database}/v2/blob/open", post(database_blob_open))
        .route("/{database}/v2/blob/read", post(database_blob_read))
        .route("/{database}/v2/blob/write", post(database_blob_write))
        .route(
            "/{database}/v2/blob/read-bytes",
            get(database_blob_read_bytes),
        )
        .route(
            "/{database}/v2/blob/write-bytes",
            post(database_blob_write_bytes),
        )
        .route(
            "/{database}/v2/blob/read-stream",
            get(database_blob_read_stream),
        )
        .route(
            "/{database}/v2/blob/write-stream",
            post(database_blob_write_stream),
        )
        .route("/{database}/v2/blob/reopen", post(database_blob_reopen))
        .route("/{database}/v2/blob/close", post(database_blob_close))
        .with_state(state)
}

async fn default_v2(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> impl IntoResponse {
    v2_endpoint(state, DEFAULT_DATABASE.to_string(), headers, ws).await
}

async fn database_v2(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> impl IntoResponse {
    v2_endpoint(state, database, headers, ws).await
}

async fn v2_endpoint(
    state: LibsqlHttpState,
    database: String,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> axum::response::Response {
    if let Ok(ws) = ws {
        if let Err(error) = validate_database_name(&database) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
        if let Err(error) = ensure_database_ready_for_client(&state, &database) {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
        return ws
            .protocols(["hrana3", "hrana2", "hrana1"])
            .on_upgrade(move |socket| handle_hrana_websocket(socket, state, database, headers))
            .into_response();
    }
    StatusCode::OK.into_response()
}

async fn raft_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    Json(serde_json::json!({
        "node_id": state.node_id,
        "stale_after_ms": RAFT_METRICS_STALE_AFTER_MS,
        "raft_metrics": operator_raft_metrics(state.metrics_registry.snapshot()),
    }))
    .into_response()
}

async fn storage_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match sqlite_storage_pressure(&state.sqlite_state, &state.compaction_policy).await {
        Ok(metrics) => Json(metrics).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn blob_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    Json(state.blob_metrics_snapshot()).into_response()
}

async fn large_payload_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match state.replication_groups.large_payload_metrics().await {
        Ok(rows) => Json(serde_json::json!({
            "node_id": state.node_id,
            "large_payload_metrics": rows,
        }))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn placement_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match placement_metrics(&state) {
        Ok(metrics) => Json(metrics).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn idempotency_metrics_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    Json(state.idempotency_metrics_snapshot()).into_response()
}

#[derive(Debug, Deserialize)]
struct ActivateCatalogSchemaRequest {
    target_version: u32,
}

#[derive(Debug, Clone, Serialize)]
struct CatalogRolloutStatus {
    target_version: u32,
    ready: bool,
    blockers: Vec<String>,
    voters: Vec<CatalogRolloutNodeStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct CatalogRolloutNodeStatus {
    node_id: u64,
    observed_age_ms: Option<u64>,
    stale: bool,
    catalog_min_read_schema_version: Option<u32>,
    catalog_max_read_schema_version: Option<u32>,
    catalog_max_write_schema_version: Option<u32>,
    ready: bool,
    reason: Option<String>,
}

async fn catalog_rollout_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    Json(catalog_rollout_status(
        &state,
        DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION,
    ))
    .into_response()
}

async fn activate_catalog_schema_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<ActivateCatalogSchemaRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let rollout = catalog_rollout_status(&state, request.target_version);
    if !rollout.ready {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "catalog schema activation is blocked because the cluster is not rollout-ready",
                "rollout": rollout
            })),
        )
            .into_response();
    }
    match activate_database_catalog_schema(&state, request.target_version) {
        Ok(version) => Json(serde_json::json!({
            "schema_version": version,
            "max_read_schema_version": DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION,
            "max_write_schema_version": DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION,
            "rollout": rollout
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct DatabaseListQuery {
    #[serde(default)]
    include_deleted: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CreateDatabaseRequest {
    name: String,
    #[serde(default)]
    placement: Option<CreateDatabasePlacementRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CreateDatabasePlacementRequest {
    #[serde(default = "default_placement_mode")]
    mode: String,
    #[serde(default)]
    write_home: Option<PlacementLocationRequest>,
    #[serde(default)]
    read_regions: Vec<PlacementLocationRequest>,
    #[serde(default)]
    durability: PlacementDurabilityRequest,
    #[serde(default)]
    failover: PlacementFailoverRequest,
}

impl Default for CreateDatabasePlacementRequest {
    fn default() -> Self {
        Self {
            mode: default_placement_mode(),
            write_home: None,
            read_regions: Vec::new(),
            durability: PlacementDurabilityRequest::default(),
            failover: PlacementFailoverRequest::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CreateReplicationGroupRequest {
    group_id: String,
    #[serde(default)]
    placement: CreateDatabasePlacementRequest,
    #[serde(default)]
    members: Vec<ReplicationGroupMemberRequest>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplicationGroupMemberRequest {
    node_id: u64,
    #[serde(default = "default_replication_group_member_role")]
    role: String,
    priority: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct MoveDatabasePlacementRequest {
    target_group_id: String,
    drain_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementStandbyRequest {
    target_group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StandbyExportQuery {
    source_group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StandbyCheckpointQuery {
    source_group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StandbyCheckpointObjectQuery {
    source_group_id: String,
    object_path: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StandbyPageDeltaQuery {
    source_group_id: String,
    min_exclusive_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementCheckpointMaterializeRequest {
    operation_id: String,
    target_group_id: String,
    source_endpoint: String,
    checkpoint: StandbyCheckpointExport,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementPageDeltaMaterializeRequest {
    operation_id: String,
    target_group_id: String,
    source_endpoint: String,
    source_group_id: String,
    min_exclusive_version: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementCheckpointMaterializeResponse {
    operation_id: String,
    node_id: u64,
    target_group_id: String,
    target_applied_index: Option<u64>,
    target_commit_ts: Option<HybridTimestamp>,
    checkpoint_objects_seen: u64,
    checkpoint_objects_copied: u64,
    checkpoint_objects_reused: u64,
    checkpoint_bytes_seen: u64,
    checkpoint_bytes_copied: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StandbyCheckpointObjectRef {
    path: String,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StandbyCheckpointExport {
    source_group_id: String,
    source_watermark: OrionSqliteRuntimeWatermark,
    artifact: SlateDbCheckpointArtifact,
    objects: Vec<StandbyCheckpointObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StandbyPageDeltaExport {
    source_group_id: String,
    source_watermark: OrionSqliteRuntimeWatermark,
    min_exclusive_version: u64,
    delta: SqliteDatabasePageSyncDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StandbyFileSnapshotExport {
    source_group_id: String,
    source_watermark: OrionSqliteRuntimeWatermark,
    snapshot: SqliteDatabaseFileSnapshot,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PromotePlacementStandbyRequest {
    target_group_id: String,
    max_staleness_ms: Option<u64>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct CancelPlacementOperationRequest {
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepairPlacementOperationRequest {
    phase: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PlacementOperationGcRequest {
    older_than_ms: Option<u64>,
    limit: Option<usize>,
    standby_older_than_ms: Option<u64>,
}

fn default_replication_group_member_role() -> String {
    "voter".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementLocationRequest {
    cloud: String,
    region: String,
    #[serde(default)]
    zone: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct PlacementDurabilityRequest {
    #[serde(default)]
    survive_cloud_outage: bool,
    #[serde(default)]
    survive_region_outage: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PlacementFailoverRequest {
    #[serde(default = "default_failover_automatic")]
    automatic: bool,
    #[serde(default = "default_failover_promote_after_ms")]
    promote_after_ms: u64,
    #[serde(default, alias = "standby_group_ids")]
    standby_targets: Vec<String>,
}

impl Default for PlacementFailoverRequest {
    fn default() -> Self {
        Self {
            automatic: default_failover_automatic(),
            promote_after_ms: default_failover_promote_after_ms(),
            standby_targets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DatabaseCatalogRecord {
    database_id: String,
    name: String,
    state: String,
    object_prefix: String,
    replication_group_id: String,
    generation: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    purged_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    purge_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementNodeRecord {
    node_id: u64,
    cloud: String,
    region: String,
    zone: String,
    raft_addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    libsql_http_addr: Option<String>,
    observed: bool,
    healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_seen_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raft_state: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ReplicationGroupRecord {
    group_id: String,
    state: String,
    placement_mode: String,
    object_prefix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_home_cloud: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_home_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_home_zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compaction_owner_node_id: Option<u64>,
    failover_automatic: bool,
    failover_promote_after_ms: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    members: Vec<ReplicationGroupMemberRecord>,
    runtime: ReplicationGroupRuntimeRecord,
}

#[derive(Debug, Clone, Serialize)]
struct ReplicationGroupRuntimeRecord {
    group_id: String,
    loaded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    loaded_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_leader: Option<u64>,
    voter_ids: Vec<u64>,
    learner_ids: Vec<u64>,
    ready_for_linearizable_reads: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LargePayloadMetricsRow {
    group_id: String,
    loaded_at_ms: u64,
    metrics: LargePayloadMetrics,
}

#[derive(Debug, Clone, Serialize)]
struct ReplicationGroupMemberRecord {
    group_id: String,
    node_id: u64,
    role: String,
    cloud: String,
    region: String,
    zone: String,
    priority: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct DatabasePlacementRecord {
    database: DatabaseCatalogRecord,
    group: ReplicationGroupRecord,
}

#[derive(Debug, Clone, Serialize)]
struct DatabasePlacementStandbyRecord {
    database_id: String,
    database_name: String,
    source_group_id: String,
    target_group_id: String,
    source_applied_index: Option<u64>,
    source_commit_ts: Option<HybridTimestamp>,
    target_applied_index: Option<u64>,
    target_commit_ts: Option<HybridTimestamp>,
    refreshed_at_ms: u64,
    updated_at_ms: u64,
    age_ms: u64,
    catalog_recorded: bool,
    target_group_available: bool,
    target_locally_openable: bool,
    promotable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementStandbyRefreshRecord {
    standby: DatabasePlacementStandbyRecord,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementStandbyPromotionRecord {
    database: DatabaseCatalogRecord,
    standby: DatabasePlacementStandbyRecord,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementStandbyRefreshSummary {
    checked_at_ms: u64,
    attempted: u64,
    refreshed: u64,
    skipped: u64,
    errors: u64,
    actions: Vec<String>,
    risks: Vec<String>,
}

#[derive(Debug, Clone)]
struct PlacementStandbyRefreshPlan {
    database_name: String,
    source_group_id: String,
    target_group_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementOperationRecord {
    operation_id: String,
    database_id: String,
    database_name: String,
    operation: String,
    status: String,
    phase: String,
    source_group_id: String,
    target_group_id: String,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_fence_applied_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_fence_commit_ts: Option<HybridTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_fence_observed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_clone_applied_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_clone_commit_ts: Option<HybridTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_epoch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_checkpoint_artifact: Option<SlateDbCheckpointArtifact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_source_applied_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_source_commit_ts: Option<HybridTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_created_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct DatabasePlacementPlan {
    database: String,
    valid: bool,
    selected_group_id: String,
    requested: CreateDatabasePlacementRequest,
    actions: Vec<String>,
    members: Vec<ReplicationGroupMemberRecord>,
    risks: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementMetricsRecord {
    checked_at_ms: u64,
    operations_total: u64,
    operations_running: u64,
    operations_completed: u64,
    operations_failed: u64,
    running_by_phase: BTreeMap<String, u64>,
    oldest_running_age_ms: Option<u64>,
    stale_running_operations: Vec<PlacementStaleOperationRecord>,
    databases_by_group: BTreeMap<String, u64>,
    groups_total: u64,
    groups_active: u64,
    groups_draining: u64,
    groups_deleted: u64,
    groups_failed: u64,
    groups_unloaded: u64,
    groups_not_ready: u64,
    standbys_total: u64,
    standbys_promotable: u64,
    standbys_stale: u64,
    standbys_errors: u64,
    standby_checkpoint: StandbyCheckpointMetricsSnapshot,
    standby_page_delta: StandbyPageDeltaMetricsSnapshot,
    placement_move_transfer: PlacementMoveTransferMetricsSnapshot,
    placement_transfer_voters: PlacementTransferVoterMetricsSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementStaleOperationRecord {
    operation_id: String,
    database_name: String,
    phase: String,
    age_ms: u64,
    updated_age_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct PlacementTransferVoterMetricsSnapshot {
    total: u64,
    ready: u64,
    failed: u64,
    pending: u64,
    checkpoint_objects_seen: u64,
    checkpoint_objects_copied: u64,
    checkpoint_objects_reused: u64,
    checkpoint_bytes_seen: u64,
    checkpoint_bytes_copied: u64,
}

#[derive(Debug, Clone, Default)]
struct PlacementStandbyMetrics {
    total: u64,
    promotable: u64,
    stale: u64,
    errors: u64,
}

#[derive(Debug, Clone, Serialize)]
struct PlacementOperationGcResult {
    checked_at_ms: u64,
    older_than_ms: u64,
    standby_older_than_ms: u64,
    limit: usize,
    deleted: usize,
    operations_deleted: usize,
    standbys_deleted: usize,
}

#[derive(Debug, Clone)]
struct PlacementMetricsPhaseRow {
    status: String,
    phase: String,
    operation_count: u64,
    oldest_age_ms: Option<u64>,
    newest_update_age_ms: Option<u64>,
}

const PLACEMENT_RUNNING_STALE_AFTER_MS: u64 = 5 * 60 * 1_000;

const PLACEMENT_OPERATION_SELECT_COLUMNS: &str = r#"
operation_id, database_id, database_name, operation, status, phase,
source_group_id, target_group_id, created_at_ms, updated_at_ms, completed_at_ms,
source_fence_applied_index, source_fence_commit_ts_physical_ms, source_fence_commit_ts_logical,
source_fence_observed_at_ms, target_clone_applied_index, target_clone_commit_ts_physical_ms,
target_clone_commit_ts_logical, error, transfer_epoch_id, transfer_kind,
transfer_checkpoint_artifact_json, transfer_source_applied_index,
transfer_source_commit_ts_physical_ms, transfer_source_commit_ts_logical,
transfer_created_at_ms
"#;

const DATABASE_STANDBY_SELECT_COLUMNS: &str = r#"
database_id, database_name, source_group_id, target_group_id,
source_applied_index, source_commit_ts_physical_ms, source_commit_ts_logical,
target_applied_index, target_commit_ts_physical_ms, target_commit_ts_logical,
refreshed_at_ms, updated_at_ms, error
"#;

fn default_placement_mode() -> String {
    "single_region".to_string()
}

fn default_failover_automatic() -> bool {
    true
}

fn default_failover_promote_after_ms() -> u64 {
    30_000
}

fn validate_placement_request(request: &CreateDatabasePlacementRequest) -> anyhow::Result<()> {
    ensure!(
        matches!(
            request.mode.as_str(),
            "single_region"
                | "regional_primary"
                | "dual_cloud_quorum"
                | "follow_the_tenant"
                | "read_global_write_home"
                | "manual"
        ),
        "unsupported placement mode {}",
        request.mode
    );
    if let Some(write_home) = &request.write_home {
        validate_placement_location(write_home, "placement.write_home")?;
    }
    for (index, location) in request.read_regions.iter().enumerate() {
        validate_placement_location(location, &format!("placement.read_regions[{index}]"))?;
    }
    ensure!(
        request.failover.promote_after_ms > 0,
        "placement.failover.promote_after_ms must be greater than zero"
    );
    Ok(())
}

fn validate_placement_location(
    location: &PlacementLocationRequest,
    path: &str,
) -> anyhow::Result<()> {
    ensure!(!location.cloud.is_empty(), "{path}.cloud must not be empty");
    ensure!(
        !location.region.is_empty(),
        "{path}.region must not be empty"
    );
    if let Some(zone) = &location.zone {
        ensure!(!zone.is_empty(), "{path}.zone must not be empty when set");
    }
    Ok(())
}

async fn list_databases_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Query(query): Query<DatabaseListQuery>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match list_database_catalog_records(&state, query.include_deleted) {
        Ok(databases) => Json(serde_json::json!({ "databases": databases })).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn get_database_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = validate_database_name(&database)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match read_database_catalog_record(&state, &database) {
        Ok(Some(database)) => Json(database).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("database {database} does not exist") })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn placement_nodes_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    Json(serde_json::json!({ "nodes": placement_node_records(&state) })).into_response()
}

async fn list_replication_groups_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match list_replication_group_records(&state) {
        Ok(groups) => Json(serde_json::json!({ "replication_groups": groups })).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn replication_group_runtime_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match replication_group_runtime_records(&state) {
        Ok(groups) => Json(serde_json::json!({ "runtime_groups": groups })).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn reconcile_placement_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match reconcile_placement(&state) {
        Ok(summary) => Json(summary).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn reconcile_standby_refresh_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match state.reconcile_standby_refreshes_once().await {
        Ok(summary) => Json(summary).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn placement_gc_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<PlacementOperationGcRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match collect_placement_garbage(
        &state,
        request.older_than_ms,
        request.standby_older_than_ms,
        request.limit,
    ) {
        Ok(result) => Json(result).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn get_replication_group_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(group_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = validate_replication_group_id(&group_id)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match read_replication_group_record(&state, &group_id) {
        Ok(Some(group)) => Json(group).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("replication group {group_id} does not exist") })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn create_replication_group_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<CreateReplicationGroupRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match create_replication_group(&state, request) {
        Ok(group) => (StatusCode::CREATED, Json(group)).into_response(),
        Err(error) if error.to_string().contains("already exists") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn drain_replication_group_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(group_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match set_replication_group_state(&state, &group_id, "draining") {
        Ok(group) => Json(group).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn delete_replication_group_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(group_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match delete_replication_group(&state, &group_id) {
        Ok(group) => Json(group).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("still has") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn add_replication_group_member_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(group_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReplicationGroupMemberRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match add_replication_group_member(&state, &group_id, request) {
        Ok(group) => Json(group).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn remove_replication_group_member_endpoint(
    State(state): State<LibsqlHttpState>,
    Path((group_id, node_id, role)): Path<(String, u64, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match remove_replication_group_member(&state, &group_id, node_id, &role) {
        Ok(group) => Json(group).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("last voter") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn get_database_placement_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = validate_database_name(&database)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match read_database_placement_record(&state, &database) {
        Ok(Some(placement)) => Json(placement).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("database {database} does not exist") })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn plan_database_placement_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CreateDatabasePlacementRequest>,
) -> impl IntoResponse {
    if let Err(error) = validate_database_name(&database)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match plan_database_placement(&state, &database, request) {
        Ok(plan) => Json(plan).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn move_database_placement_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<MoveDatabasePlacementRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let drain_timeout_ms = request.drain_timeout_ms.unwrap_or(0);
    let result = if drain_timeout_ms == 0 {
        create_database_move_operation(&state, &database, &request.target_group_id, false)
    } else {
        create_database_move_operation_with_session_drain(
            &state,
            &database,
            &request.target_group_id,
            drain_timeout_ms,
        )
        .await
    };
    match result {
        Ok(operation) => (StatusCode::ACCEPTED, Json(operation)).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error)
            if error.to_string().contains("not loaded")
                || error
                    .to_string()
                    .contains("already has a running placement operation") =>
        {
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn list_database_placement_standbys_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = validate_database_name(&database)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match list_database_placement_standbys(&state, &database) {
        Ok(standbys) => Json(serde_json::json!({ "standbys": standbys })).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn refresh_database_placement_standby_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PlacementStandbyRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match forward_standby_action_to_target_leader(
        &state,
        &headers,
        &database,
        &request.target_group_id,
        "standby",
        &request,
    )
    .await
    {
        Ok(Some(body)) => return Json(body).into_response(),
        Ok(None) => {}
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": error_chain_message(&error) })),
            )
                .into_response();
        }
    }
    match refresh_database_placement_standby_for_endpoint(
        &state,
        &headers,
        &database,
        &request.target_group_id,
    )
    .await
    {
        Ok(record) => Json(record).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn export_database_placement_standby_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    Query(query): Query<StandbyExportQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })).into_response(),
        )
            .into_response();
    }
    match export_database_placement_standby(&state, &database, &query.source_group_id) {
        Ok(export) => export.into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
    }
}

async fn export_database_placement_checkpoint_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    Query(query): Query<StandbyCheckpointQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match export_database_placement_checkpoint(&state, &database, &query.source_group_id).await {
        Ok(export) => Json(export).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn export_database_placement_page_delta_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    Query(query): Query<StandbyPageDeltaQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match export_database_placement_page_delta(
        &state,
        &database,
        &query.source_group_id,
        query.min_exclusive_version,
    )
    .await
    {
        Ok(export) => match encode_standby_page_delta_export(&export) {
            Ok(bytes) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                headers.insert(
                    axum::http::header::CONTENT_LENGTH,
                    HeaderValue::from_str(&bytes.len().to_string())
                        .expect("page delta byte count must be a valid header value"),
                );
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error_chain_message(&error) })),
            )
                .into_response(),
        },
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn export_database_placement_file_snapshot_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    Query(query): Query<StandbyExportQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match export_database_placement_file_snapshot(&state, &database, &query.source_group_id) {
        Ok(export) => match encode_standby_file_snapshot_export(&export) {
            Ok(bytes) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                headers.insert(
                    axum::http::header::CONTENT_LENGTH,
                    HeaderValue::from_str(&bytes.len().to_string())
                        .expect("file snapshot byte count must be a valid header value"),
                );
                (StatusCode::OK, headers, Body::from(bytes)).into_response()
            }
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error_chain_message(&error) })),
            )
                .into_response(),
        },
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn export_database_placement_checkpoint_object_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    Query(query): Query<StandbyCheckpointObjectQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })).into_response(),
        )
            .into_response();
    }
    match export_database_placement_checkpoint_object(
        &state,
        &database,
        &query.source_group_id,
        &query.object_path,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })).into_response(),
        )
            .into_response(),
    }
}

async fn materialize_database_placement_checkpoint_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PlacementCheckpointMaterializeRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match materialize_database_placement_checkpoint(&state, &headers, &database, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn promote_database_placement_standby_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PromotePlacementStandbyRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match forward_standby_action_to_target_leader(
        &state,
        &headers,
        &database,
        &request.target_group_id,
        "promote",
        &request,
    )
    .await
    {
        Ok(Some(body)) => return Json(body).into_response(),
        Ok(None) => {}
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": error_chain_message(&error) })),
            )
                .into_response();
        }
    }
    match promote_database_placement_standby(
        &state,
        &database,
        &request.target_group_id,
        request.max_staleness_ms,
        request.force,
    ) {
        Ok(record) => Json(record).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn list_database_placement_operations_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = validate_database_name(&database)
        .and_then(|_| state.authorize(&headers, ORION_SYSTEM_DATABASE))
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match list_placement_operations(&state, &database) {
        Ok(operations) => Json(serde_json::json!({ "operations": operations })).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn materialize_database_placement_page_delta_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PlacementPageDeltaMaterializeRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match materialize_database_placement_page_delta(&state, &headers, &database, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("not loaded") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn cancel_database_placement_operation_endpoint(
    State(state): State<LibsqlHttpState>,
    Path((database, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CancelPlacementOperationRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match cancel_placement_operation(&state, &database, &operation_id, request.reason.as_deref()) {
        Ok(operation) => Json(operation).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("is not running") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn repair_database_placement_operation_endpoint(
    State(state): State<LibsqlHttpState>,
    Path((database, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<RepairPlacementOperationRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match repair_placement_operation(
        &state,
        &database,
        &operation_id,
        request.phase.as_deref(),
        request.reason.as_deref(),
    ) {
        Ok(operation) => Json(operation).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("is still running") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn create_database_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<CreateDatabaseRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let idempotency =
        match lifecycle_idempotency_context_from_create_headers(&headers, &request, &state) {
            Ok(idempotency) => idempotency,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        };
    match create_database_lifecycle_idempotent(&state, request, idempotency) {
        Ok((status, record)) => (status, Json(record)).into_response(),
        Err(error) if error.to_string().contains("already exists") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("idempotency key conflict") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("idempotency key is pending") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error_chain_message(&error) })),
        )
            .into_response(),
    }
}

async fn drop_database_endpoint(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let idempotency =
        match lifecycle_idempotency_context_from_drop_headers(&headers, &database, &state) {
            Ok(idempotency) => idempotency,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": error.to_string() })),
                )
                    .into_response();
            }
        };
    match drop_database_lifecycle_idempotent(&state, &database, idempotency) {
        Ok((status, record)) => (status, Json(record)).into_response(),
        Err(error) if error.to_string().contains("does not exist") => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("idempotency key conflict") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) if error.to_string().contains("idempotency key is pending") => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn compaction_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize(&headers, ORION_SYSTEM_DATABASE) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let db = match state.database(ORION_SYSTEM_DATABASE) {
        Ok(db) => db,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };
    match read_compaction_summary(db) {
        Ok(summary) => Json(summary).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn compaction_pause_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    mutate_compaction_control(state, headers, |db| db.set_compaction_paused(true))
}

async fn compaction_resume_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    mutate_compaction_control(state, headers, |db| db.set_compaction_paused(false))
}

async fn compaction_force_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    mutate_compaction_control(state, headers, |db| db.request_compaction())
}

#[derive(Debug, Deserialize)]
struct SetRetentionFloorRequest {
    min_retained_version: u64,
    #[serde(default)]
    reason: Option<String>,
}

async fn compaction_set_retention_floor_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<SetRetentionFloorRequest>,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let db = match state.database(ORION_SYSTEM_DATABASE) {
        Ok(db) => db,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };
    match db.set_compaction_retention_floor(request.min_retained_version, request.reason.as_deref())
    {
        Ok(retention_floor) => Json(retention_floor).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn compaction_clear_retention_floor_endpoint(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let db = match state.database(ORION_SYSTEM_DATABASE) {
        Ok(db) => db,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };
    match db.clear_compaction_retention_floor() {
        Ok(retention_floor) => Json(retention_floor).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

fn mutate_compaction_control(
    state: LibsqlHttpState,
    headers: HeaderMap,
    mutate: impl FnOnce(&OrionSqliteDb) -> anyhow::Result<orion_raft::OrionCompactionControl>,
) -> axum::response::Response {
    if let Err(error) = state.authorize_system_admin(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    let db = match state.database(ORION_SYSTEM_DATABASE) {
        Ok(db) => db,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };
    match mutate(&db) {
        Ok(control) => Json(control).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Serialize)]
struct CompactionSummary {
    state: Option<serde_json::Value>,
    control: serde_json::Value,
    retention_floor: serde_json::Value,
    leases: Vec<serde_json::Value>,
    recent_runs: Vec<serde_json::Value>,
}

fn read_compaction_summary(db: Arc<OrionSqliteDb>) -> anyhow::Result<CompactionSummary> {
    let conn = db.connect()?;
    db.ensure_system_schema()?;
    let state = conn
        .query_row(
            r#"
            select json_object(
                'updated_at_ms', updated_at_ms,
                'last_status', last_status,
                'last_error', last_error,
                'last_duration_ms', last_duration_ms,
                'total_runs', total_runs,
                'total_errors', total_errors,
                'total_deleted_versions', total_deleted_versions,
                'total_deleted_bytes', total_deleted_bytes,
                'last_files_scanned', last_files_scanned,
                'last_files_compacted', last_files_compacted,
                'last_versions_scanned', last_versions_scanned,
                'last_obsolete_versions', last_obsolete_versions,
                'last_deleted_versions', last_deleted_versions,
                'last_bytes_scanned', last_bytes_scanned,
                'last_obsolete_bytes', last_obsolete_bytes,
                'last_deleted_bytes', last_deleted_bytes
            )
            from compaction_state where id = 1
            "#,
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    let mut stmt = conn.prepare(
        r#"
        select json_object(
            'id', id,
            'started_at_ms', started_at_ms,
            'finished_at_ms', finished_at_ms,
            'duration_ms', duration_ms,
            'status', status,
            'files_scanned', files_scanned,
            'files_compacted', files_compacted,
            'versions_scanned', versions_scanned,
            'obsolete_versions', obsolete_versions,
            'deleted_versions', deleted_versions,
            'bytes_scanned', bytes_scanned,
            'obsolete_bytes', obsolete_bytes,
            'deleted_bytes', deleted_bytes,
            'error', error
        )
        from compaction_runs
        order by id desc
        limit 20
        "#,
    )?;
    let recent_runs = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| {
            row.map_err(anyhow::Error::from)
                .and_then(|json| serde_json::from_str(&json).map_err(anyhow::Error::from))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let control = serde_json::to_value(db.compaction_control()?)?;
    let retention_floor = serde_json::to_value(db.compaction_retention_floor()?)?;
    let leases = db
        .compaction_leases()?
        .into_iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CompactionSummary {
        state,
        control,
        retention_floor,
        leases,
        recent_runs,
    })
}

#[derive(Debug, Clone)]
struct LifecycleIdempotencyContext {
    key: String,
    operation: String,
    database: String,
    request_hash: String,
}

#[derive(Debug)]
struct StoredLifecycleIdempotencyRecord {
    operation: String,
    database: String,
    request_hash: String,
    status: String,
    response_status: u16,
    response_json: String,
}

fn lifecycle_idempotency_context_from_create_headers(
    headers: &HeaderMap,
    request: &CreateDatabaseRequest,
    state: &LibsqlHttpState,
) -> anyhow::Result<Option<LifecycleIdempotencyContext>> {
    lifecycle_idempotency_context_from_headers(
        headers,
        "create",
        &request.name,
        request,
        &state.idempotency_config,
    )
}

fn lifecycle_idempotency_context_from_drop_headers(
    headers: &HeaderMap,
    database: &str,
    state: &LibsqlHttpState,
) -> anyhow::Result<Option<LifecycleIdempotencyContext>> {
    lifecycle_idempotency_context_from_headers(
        headers,
        "drop",
        database,
        &serde_json::json!({ "name": database }),
        &state.idempotency_config,
    )
}

fn lifecycle_idempotency_context_from_headers<T: Serialize>(
    headers: &HeaderMap,
    operation: &str,
    database: &str,
    body: &T,
    config: &LibsqlHttpIdempotencyConfig,
) -> anyhow::Result<Option<LifecycleIdempotencyContext>> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    ensure!(
        config.enabled,
        "idempotency keys are disabled by libsql_http.idempotency.enabled=false"
    );
    let key = value
        .to_str()
        .context("idempotency key must be valid UTF-8")?
        .trim()
        .to_string();
    ensure!(!key.is_empty(), "idempotency key must not be empty");
    ensure!(
        key.len() <= 512,
        "idempotency key is too long: max length is 512 bytes"
    );
    Ok(Some(LifecycleIdempotencyContext {
        key,
        operation: operation.to_string(),
        database: database.to_string(),
        request_hash: hash_lifecycle_request(operation, database, body)?,
    }))
}

fn hash_lifecycle_request<T: Serialize>(
    operation: &str,
    database: &str,
    body: &T,
) -> anyhow::Result<String> {
    let canonical = serde_json::to_vec(&serde_json::json!({
        "operation": operation,
        "database": database,
        "body": body,
    }))?;
    let digest = Sha256::digest(canonical);
    Ok(hex_lower(&digest))
}

fn create_database_lifecycle(
    state: &LibsqlHttpState,
    request: CreateDatabaseRequest,
) -> anyhow::Result<DatabaseCatalogRecord> {
    validate_database_name(&request.name)?;
    ensure!(
        request.name != ORION_SYSTEM_DATABASE && request.name != ORION_CATALOG_DATABASE,
        "database name {} is reserved for Orion service metadata",
        request.name
    );
    let placement = request.placement.unwrap_or_default();
    validate_placement_request(&placement)?;
    let group_id = DEFAULT_REPLICATION_GROUP_ID;
    let database_id = new_database_id(&request.name);
    let object_prefix = database_object_prefix(group_id, &database_id);
    validate_database_object_prefix(&object_prefix)?;
    let conn = open_system_catalog_write_connection(state)?;
    ensure_configured_replication_group(&conn, state, group_id, &placement)?;
    let creating =
        upsert_database_creating(&conn, &request.name, &database_id, &object_prefix, group_id)?;

    match state.database(&request.name).and_then(|db| {
        db.connect()
            .map(|_| ())
            .with_context(|| format!("opening database {}", request.name))
    }) {
        Ok(()) => {
            let ready = upsert_database_ready(
                &conn,
                &request.name,
                &database_id,
                &object_prefix,
                group_id,
            )?;
            checkpoint_catalog_connection(&conn)?;
            Ok(ready)
        }
        Err(error) => {
            let _ = mark_database_state(&conn, &request.name, "failed", None, Some(&error));
            let _ = checkpoint_catalog_connection(&conn);
            Err(error)
        }
    }
    .with_context(|| format!("creating database {}", request.name))?;

    read_database_catalog_record_from_conn(&conn, &request.name)?
        .or(Some(creating))
        .ok_or_else(|| anyhow!("database {} catalog record disappeared", request.name))
}

fn create_database_lifecycle_idempotent(
    state: &LibsqlHttpState,
    request: CreateDatabaseRequest,
    idempotency: Option<LifecycleIdempotencyContext>,
) -> anyhow::Result<(StatusCode, DatabaseCatalogRecord)> {
    let Some(idempotency) = idempotency else {
        return Ok((
            StatusCode::CREATED,
            create_database_lifecycle(state, request)?,
        ));
    };
    run_database_lifecycle_idempotent(state, idempotency, StatusCode::CREATED, || {
        create_database_lifecycle(state, request)
    })
}

fn drop_database_lifecycle(
    state: &LibsqlHttpState,
    database: &str,
) -> anyhow::Result<DatabaseCatalogRecord> {
    validate_database_name(database)?;
    ensure!(
        database != ORION_SYSTEM_DATABASE && database != ORION_CATALOG_DATABASE,
        "database name {database} is reserved for Orion service metadata"
    );
    let conn = open_system_catalog_write_connection(state)?;
    let Some(existing) = read_database_catalog_record_from_conn(&conn, database)? else {
        anyhow::bail!("database {database} does not exist");
    };
    if existing.state == "deleted" {
        return Ok(existing);
    }
    mark_database_state(&conn, database, "deleting", None, None)?;
    let _ = state.close_database_sessions(database);
    let _ = state.evict_database(database);
    mark_database_state(
        &conn,
        database,
        "deleted",
        Some(current_time_millis()),
        None,
    )?;
    read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} catalog record disappeared"))
}

fn drop_database_lifecycle_idempotent(
    state: &LibsqlHttpState,
    database: &str,
    idempotency: Option<LifecycleIdempotencyContext>,
) -> anyhow::Result<(StatusCode, DatabaseCatalogRecord)> {
    let Some(idempotency) = idempotency else {
        return Ok((StatusCode::OK, drop_database_lifecycle(state, database)?));
    };
    run_database_lifecycle_idempotent(state, idempotency, StatusCode::OK, || {
        drop_database_lifecycle(state, database)
    })
}

fn run_database_lifecycle_idempotent(
    state: &LibsqlHttpState,
    idempotency: LifecycleIdempotencyContext,
    response_status: StatusCode,
    run: impl FnOnce() -> anyhow::Result<DatabaseCatalogRecord>,
) -> anyhow::Result<(StatusCode, DatabaseCatalogRecord)> {
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 2)?;
    conn.execute_batch("begin immediate")?;
    let reservation = (|| {
        if let Some(stored) = read_lifecycle_idempotency_record(&conn, &idempotency.key)? {
            return anyhow::Ok(Some(stored));
        }
        insert_pending_lifecycle_idempotency_record(&conn, &idempotency)?;
        anyhow::Ok(None)
    })();
    match reservation {
        Ok(stored) => {
            conn.execute_batch("commit")?;
            if let Some(stored) = stored {
                return resolve_stored_lifecycle_idempotency_record(idempotency, stored);
            }
        }
        Err(error) => {
            let _ = conn.execute_batch("rollback");
            return Err(error);
        }
    }
    drop(conn);

    let record = match run() {
        Ok(record) => record,
        Err(error) => {
            let conn = open_system_catalog_write_connection(state)?;
            let _ = delete_lifecycle_idempotency_record(&conn, &idempotency.key);
            return Err(error);
        }
    };
    let response_json = serde_json::to_string(&record)?;
    let conn = open_system_catalog_write_connection(state)?;
    mark_lifecycle_idempotency_record_committed(
        &conn,
        &idempotency.key,
        response_status.as_u16(),
        &response_json,
    )?;
    Ok((response_status, record))
}

fn resolve_stored_lifecycle_idempotency_record(
    idempotency: LifecycleIdempotencyContext,
    stored: StoredLifecycleIdempotencyRecord,
) -> anyhow::Result<(StatusCode, DatabaseCatalogRecord)> {
    ensure!(
        stored.operation == idempotency.operation
            && stored.database == idempotency.database
            && stored.request_hash == idempotency.request_hash,
        "idempotency key conflict: key was already used with a different lifecycle request"
    );
    ensure!(
        stored.status == "committed",
        "idempotency key is pending; retry later with the same request"
    );
    let status = StatusCode::from_u16(stored.response_status).with_context(|| {
        format!(
            "stored lifecycle idempotency status {}",
            stored.response_status
        )
    })?;
    let record = serde_json::from_str(&stored.response_json)
        .context("stored lifecycle idempotency response is not valid JSON")?;
    Ok((status, record))
}

fn ensure_database_ready_for_client(state: &LibsqlHttpState, database: &str) -> anyhow::Result<()> {
    if database == ORION_SYSTEM_DATABASE {
        return Ok(());
    }
    ensure!(
        database != ORION_CATALOG_DATABASE,
        "database name {database} is reserved for Orion service metadata"
    );
    let conn = open_system_catalog_write_connection(state)?;
    let Some(record) = read_database_catalog_record_from_conn(&conn, database)? else {
        anyhow::bail!("database {database} has not been created");
    };
    if record.state == "creating" {
        state
            .database(database)
            .and_then(|db| {
                db.connect()
                    .map(|_| ())
                    .with_context(|| format!("opening database {database}"))
            })
            .with_context(|| format!("reconciling database {database} from creating to ready"))?;
        mark_database_state(&conn, database, "ready", None, None)?;
        checkpoint_catalog_connection(&conn)?;
        return Ok(());
    }
    ensure!(
        record.state == "ready",
        "database {database} is not ready; current state is {}",
        record.state
    );
    Ok(())
}

fn read_database_catalog_record(
    state: &LibsqlHttpState,
    database: &str,
) -> anyhow::Result<Option<DatabaseCatalogRecord>> {
    let conn = open_system_catalog_connection(state)?;
    read_database_catalog_record_from_conn(&conn, database)
}

fn list_database_catalog_records(
    state: &LibsqlHttpState,
    include_deleted: bool,
) -> anyhow::Result<Vec<DatabaseCatalogRecord>> {
    let conn = open_system_catalog_connection(state)?;
    list_database_catalog_records_from_conn(&conn, include_deleted)
}

fn list_database_catalog_records_from_conn(
    conn: &Connection,
    include_deleted: bool,
) -> anyhow::Result<Vec<DatabaseCatalogRecord>> {
    let sql = database_catalog_select_sql(
        conn,
        if include_deleted {
            None
        } else {
            Some("state != 'deleted'")
        },
    )?;
    let mut stmt = conn.prepare(&sql)?;
    Ok(stmt
        .query_map([], database_catalog_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?)
}

fn placement_node_records(state: &LibsqlHttpState) -> Vec<PlacementNodeRecord> {
    let entries = state
        .metrics_registry
        .snapshot()
        .into_iter()
        .map(|entry| (entry.metrics.node_id, entry))
        .collect::<BTreeMap<_, _>>();
    state
        .placement_nodes
        .values()
        .map(|node| {
            let entry = entries.get(&node.node_id);
            let observed_age_ms =
                entry.map(|entry| current_time_millis().saturating_sub(entry.observed_at_ms));
            PlacementNodeRecord {
                node_id: node.node_id,
                cloud: node.cloud.clone(),
                region: node.region.clone(),
                zone: node.zone.clone(),
                raft_addr: node.raft_addr.clone(),
                libsql_http_addr: node.libsql_http_addr.clone(),
                observed: entry.is_some(),
                healthy: observed_age_ms
                    .map(|age| age <= RAFT_METRICS_STALE_AFTER_MS)
                    .unwrap_or(false),
                last_seen_ms: entry.map(|entry| entry.observed_at_ms),
                raft_state: entry.map(|entry| entry.metrics.state.clone()),
            }
        })
        .collect()
}

fn replication_group_runtime_records(
    state: &LibsqlHttpState,
) -> anyhow::Result<Vec<ReplicationGroupRuntimeRecord>> {
    state.replication_groups.health_records()
}

fn replication_group_runtime_record(
    state: &LibsqlHttpState,
    group_id: &str,
) -> anyhow::Result<ReplicationGroupRuntimeRecord> {
    for record in state.replication_groups.health_records()? {
        if record.group_id == group_id {
            return Ok(record);
        }
    }
    Ok(ReplicationGroupRuntimeRecord {
        group_id: group_id.to_string(),
        loaded: false,
        loaded_at_ms: None,
        current_leader: None,
        voter_ids: Vec::new(),
        learner_ids: Vec::new(),
        ready_for_linearizable_reads: false,
        error: Some(format!(
            "replication group {group_id} is not loaded on this node"
        )),
    })
}

fn list_replication_group_records(
    state: &LibsqlHttpState,
) -> anyhow::Result<Vec<ReplicationGroupRecord>> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    let mut stmt = conn.prepare(
        r#"
        select group_id, state, placement_mode, object_prefix,
               write_home_cloud, write_home_region, write_home_zone,
               compaction_owner_node_id, failover_automatic, failover_promote_after_ms,
               created_at_ms, updated_at_ms, error
        from replication_groups
        order by group_id
        "#,
    )?;
    let groups = stmt
        .query_map([], replication_group_record_from_row_without_members)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    groups
        .into_iter()
        .map(|mut group| {
            group.members = list_replication_group_members_from_conn(&conn, &group.group_id)?;
            group.runtime = replication_group_runtime_record(state, &group.group_id)?;
            Ok(group)
        })
        .collect()
}

fn read_replication_group_record(
    state: &LibsqlHttpState,
    group_id: &str,
) -> anyhow::Result<Option<ReplicationGroupRecord>> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    let Some(mut group) = read_replication_group_record_from_conn(&conn, group_id)? else {
        return Ok(None);
    };
    group.members = list_replication_group_members_from_conn(&conn, group_id)?;
    group.runtime = replication_group_runtime_record(state, group_id)?;
    Ok(Some(group))
}

fn read_database_placement_record(
    state: &LibsqlHttpState,
    database: &str,
) -> anyhow::Result<Option<DatabasePlacementRecord>> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    let Some(database_record) = read_database_catalog_record_from_conn(&conn, database)? else {
        return Ok(None);
    };
    let mut group =
        read_replication_group_record_from_conn(&conn, &database_record.replication_group_id)?
            .ok_or_else(|| {
                anyhow!(
                    "database {} references missing replication group {}",
                    database_record.name,
                    database_record.replication_group_id
                )
            })?;
    group.members = list_replication_group_members_from_conn(&conn, &group.group_id)?;
    group.runtime = replication_group_runtime_record(state, &group.group_id)?;
    Ok(Some(DatabasePlacementRecord {
        database: database_record,
        group,
    }))
}

fn assigned_database_count_for_group(conn: &Connection, group_id: &str) -> anyhow::Result<i64> {
    conn.query_row(
        r#"
        select count(*)
        from database_replication_groups drg
        join database_catalog dc on dc.database_id = drg.database_id
        where drg.group_id = ? and dc.state != 'deleted'
        "#,
        [group_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn list_database_placement_standbys(
    state: &LibsqlHttpState,
    database: &str,
) -> anyhow::Result<Vec<DatabasePlacementStandbyRecord>> {
    validate_database_name(database)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    let sql = format!(
        "select {DATABASE_STANDBY_SELECT_COLUMNS} from database_standby_copies where database_id = ? order by target_group_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut standbys = stmt
        .query_map(
            [&record.database_id],
            database_placement_standby_record_from_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    for standby in &mut standbys {
        annotate_standby_record(state, &record, standby, None);
    }
    Ok(standbys)
}

fn standby_refresh_due_after_ms(source_group: &ReplicationGroupRecord) -> u64 {
    source_group
        .failover_promote_after_ms
        .saturating_div(2)
        .max(STANDBY_REFRESH_MIN_INTERVAL_MS)
}

fn standby_refresh_due(
    conn: &Connection,
    database: &DatabaseCatalogRecord,
    source_group_id: &str,
    target_group_id: &str,
    refresh_after_ms: u64,
    now: u64,
) -> anyhow::Result<bool> {
    let Some(standby) =
        read_database_placement_standby(conn, &database.database_id, target_group_id)?
    else {
        return Ok(true);
    };
    if standby.source_group_id != source_group_id || standby.error.is_some() {
        return Ok(true);
    }
    Ok(now.saturating_sub(standby.refreshed_at_ms) >= refresh_after_ms)
}

async fn refresh_database_placement_standby_for_endpoint(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    target_group_id: &str,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    let _guard = state
        .try_begin_standby_refresh(database, target_group_id)?
        .ok_or_else(|| {
            anyhow!(
                "standby refresh for database {database} into target group {target_group_id} is already running"
            )
        })?;
    refresh_database_placement_standby_for_endpoint_inner(
        state,
        headers,
        database,
        None,
        target_group_id,
    )
    .await
}

async fn refresh_database_placement_standby_for_endpoint_inner(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    expected_source_group_id: Option<&str>,
    target_group_id: &str,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    match refresh_database_placement_standby_with_source(
        state,
        database,
        expected_source_group_id,
        target_group_id,
    ) {
        Ok(record) => Ok(record),
        Err(error) if error_chain_contains(&error, "not loaded") => {
            refresh_database_placement_standby_from_peer(
                state,
                headers,
                database,
                expected_source_group_id,
                target_group_id,
            )
            .await
            .with_context(|| format!("refreshing standby for database {database} from source peer"))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
fn refresh_database_placement_standby(
    state: &LibsqlHttpState,
    database: &str,
    target_group_id: &str,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    refresh_database_placement_standby_with_source(state, database, None, target_group_id)
}

fn refresh_database_placement_standby_with_source(
    state: &LibsqlHttpState,
    database: &str,
    expected_source_group_id: Option<&str>,
    target_group_id: &str,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    validate_database_name(database)?;
    if let Some(source_group_id) = expected_source_group_id {
        validate_replication_group_id(source_group_id)?;
    }
    validate_replication_group_id(target_group_id)?;
    ensure!(
        state.replication_groups.contains(target_group_id)?,
        "target replication group {target_group_id} is not loaded by this node"
    );
    let conn = open_system_catalog_write_connection(state)
        .context("opening writable catalog for standby refresh")?;
    require_database_catalog_schema(&conn, 7).context("checking standby refresh catalog schema")?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready; current state is {}",
        database_record.state
    );
    ensure!(
        database_record.replication_group_id != target_group_id,
        "database {database} is already assigned to replication group {target_group_id}"
    );
    if let Some(source_group_id) = expected_source_group_id {
        ensure!(
            database_record.replication_group_id == source_group_id,
            "database {database} is assigned to replication group {}, not planned source group {source_group_id}",
            database_record.replication_group_id
        );
    }
    ensure!(
        read_running_placement_operation_for_database(&conn, &database_record.database_id)?
            .is_none(),
        "database {database} already has a running placement operation"
    );
    let target_group = read_replication_group_record_from_conn(&conn, target_group_id)?
        .ok_or_else(|| anyhow!("replication group {target_group_id} does not exist"))?;
    ensure!(
        target_group.state == "active",
        "replication group {target_group_id} is not active; current state is {}",
        target_group.state
    );
    let source_runtime = state
        .replication_groups
        .runtime(&database_record.replication_group_id)
        .with_context(|| {
            format!(
                "resolving source replication group {} for standby refresh",
                database_record.replication_group_id
            )
        })?;
    let target_runtime = state
        .replication_groups
        .runtime(target_group_id)
        .with_context(|| format!("resolving target replication group {target_group_id}"))?;
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for standby refresh")?;
    if source_runtime.state_store_path() != target_runtime.state_store_path() {
        let previous_standby =
            read_database_placement_standby(&conn, &database_record.database_id, target_group_id)?;
        let incremental_from_index = previous_standby.as_ref().and_then(|standby| {
            (standby.source_group_id == database_record.replication_group_id
                && standby.error.is_none()
                && target_runtime
                    .open_existing_database(database)
                    .ok()
                    .flatten()
                    .is_some())
            .then_some(standby.source_applied_index)
            .flatten()
        });
        if let Some(min_exclusive_version) = incremental_from_index {
            target_runtime
                .sync_database_pages_from(database, &source_runtime, min_exclusive_version)
                .with_context(|| {
                    format!(
                        "incrementally syncing database {database} from source group {} into target group {target_group_id} after index {min_exclusive_version}",
                        database_record.replication_group_id
                    )
                })?;
            if let Err(error) =
                target_runtime_mark_ready_and_verify(&target_runtime, database, target_group_id)
            {
                target_runtime.clone_database_from(database, &source_runtime).with_context(|| {
                    format!(
                        "incremental standby sync verification failed ({error}); recloning database {database} from source group {} into target group {target_group_id}",
                        database_record.replication_group_id
                    )
                })?;
            }
        } else {
            target_runtime
                .clone_database_from(database, &source_runtime)
                .with_context(|| {
                    format!(
                        "cloning database {database} from source group {} into target group {target_group_id}",
                        database_record.replication_group_id
                    )
                })?;
        }
    }
    target_runtime_mark_ready_and_verify(&target_runtime, database, target_group_id)?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark for standby refresh")?;
    let refreshed_at = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into database_standby_copies (
            database_id, database_name, source_group_id, target_group_id,
            source_applied_index, source_commit_ts_physical_ms, source_commit_ts_logical,
            target_applied_index, target_commit_ts_physical_ms, target_commit_ts_logical,
            refreshed_at_ms, updated_at_ms, error
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null)
        on conflict(database_id, target_group_id) do update set
            database_name = excluded.database_name,
            source_group_id = excluded.source_group_id,
            source_applied_index = excluded.source_applied_index,
            source_commit_ts_physical_ms = excluded.source_commit_ts_physical_ms,
            source_commit_ts_logical = excluded.source_commit_ts_logical,
            target_applied_index = excluded.target_applied_index,
            target_commit_ts_physical_ms = excluded.target_commit_ts_physical_ms,
            target_commit_ts_logical = excluded.target_commit_ts_logical,
            refreshed_at_ms = excluded.refreshed_at_ms,
            updated_at_ms = excluded.updated_at_ms,
            error = null
        "#,
        params![
            &database_record.database_id,
            &database_record.name,
            &database_record.replication_group_id,
            target_group_id,
            source_watermark.applied_index.map(sqlite_i64),
            source_watermark
                .applied_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            source_watermark
                .applied_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            target_watermark.applied_index.map(sqlite_i64),
            target_watermark
                .applied_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            target_watermark
                .applied_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            refreshed_at,
            refreshed_at,
        ],
    )
    .context("recording refreshed standby copy in catalog")?;
    let mut standby =
        read_database_placement_standby(&conn, &database_record.database_id, target_group_id)
            .context("reading refreshed standby copy from catalog")?
            .ok_or_else(|| anyhow!("standby copy for database {database} disappeared"))?;
    annotate_standby_record(state, &database_record, &mut standby, None);
    Ok(PlacementStandbyRefreshRecord { standby })
}

struct StandbyExportResponse {
    source_group_id: String,
    source_watermark: OrionSqliteRuntimeWatermark,
    bytes: u64,
    sha256: String,
    file: TempPath,
}

impl IntoResponse for StandbyExportResponse {
    fn into_response(self) -> axum::response::Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            HeaderValue::from_str(&self.bytes.to_string())
                .expect("snapshot byte count must be a valid header value"),
        );
        headers.insert(
            STANDBY_EXPORT_SOURCE_GROUP_HEADER,
            HeaderValue::from_str(&self.source_group_id)
                .expect("source group id must be a valid header value"),
        );
        headers.insert(
            STANDBY_EXPORT_SHA256_HEADER,
            HeaderValue::from_str(&self.sha256).expect("sha256 must be a valid header value"),
        );
        if let Some(index) = self.source_watermark.applied_index {
            headers.insert(
                STANDBY_EXPORT_APPLIED_INDEX_HEADER,
                HeaderValue::from_str(&index.to_string())
                    .expect("applied index must be a valid header value"),
            );
        }
        if let Some(timestamp) = self.source_watermark.applied_commit_ts {
            headers.insert(
                STANDBY_EXPORT_COMMIT_TS_PHYSICAL_MS_HEADER,
                HeaderValue::from_str(&timestamp.physical_ms.to_string())
                    .expect("commit timestamp physical ms must be a valid header value"),
            );
            headers.insert(
                STANDBY_EXPORT_COMMIT_TS_LOGICAL_HEADER,
                HeaderValue::from_str(&timestamp.logical.to_string())
                    .expect("commit timestamp logical value must be a valid header value"),
            );
        }
        (StatusCode::OK, headers, stream_temp_path(self.file)).into_response()
    }
}

#[derive(Debug, Clone)]
struct StandbyExportMetadata {
    source_group_id: String,
    source_applied_index: Option<u64>,
    source_commit_ts: Option<HybridTimestamp>,
    bytes: u64,
    sha256: String,
}

fn export_database_placement_standby(
    state: &LibsqlHttpState,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<StandbyExportResponse> {
    validate_database_name(database)?;
    validate_replication_group_id(source_group_id)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready; current state is {}",
        database_record.state
    );
    ensure!(
        database_record.replication_group_id == source_group_id,
        "database {database} is assigned to replication group {}, not requested source group {source_group_id}",
        database_record.replication_group_id
    );
    let source_runtime = state
        .replication_groups
        .runtime(source_group_id)
        .with_context(|| {
            format!("source replication group {source_group_id} is not loaded by this node")
        })?;
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for standby export")?;
    let mut file = NamedTempFile::new().context("creating standby export temp file")?;
    backup_database_to_file(&source_runtime, database, file.path())
        .with_context(|| format!("exporting SQLite backup for database {database}"))?;
    let (bytes, sha256) =
        file_sha256(file.as_file_mut()).context("hashing standby export snapshot")?;
    Ok(StandbyExportResponse {
        source_group_id: source_group_id.to_string(),
        source_watermark,
        bytes,
        sha256,
        file: file.into_temp_path(),
    })
}

async fn export_database_placement_checkpoint(
    state: &LibsqlHttpState,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<StandbyCheckpointExport> {
    validate_database_name(database)?;
    validate_replication_group_id(source_group_id)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready; current state is {}",
        database_record.state
    );
    ensure!(
        database_record.replication_group_id == source_group_id,
        "database {database} is assigned to replication group {}, not requested source group {source_group_id}",
        database_record.replication_group_id
    );
    let source_runtime = state
        .replication_groups
        .runtime(source_group_id)
        .with_context(|| {
            format!("source replication group {source_group_id} is not loaded by this node")
        })?;
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for standby checkpoint")?;
    let artifact = source_runtime
        .database_checkpoint_artifact(
            database,
            format!(
                "standby-checkpoint-{database}-{}",
                source_watermark.applied_index.unwrap_or_default()
            ),
        )
        .with_context(|| format!("creating standby checkpoint artifact for database {database}"))?;
    let objects = list_checkpoint_objects(&source_runtime.state_store().object_store(), &artifact)
        .await
        .with_context(|| format!("listing checkpoint objects for database {database}"))?;
    Ok(StandbyCheckpointExport {
        source_group_id: source_group_id.to_string(),
        source_watermark,
        artifact,
        objects,
    })
}

async fn export_database_placement_page_delta(
    state: &LibsqlHttpState,
    database: &str,
    source_group_id: &str,
    min_exclusive_version: u64,
) -> anyhow::Result<StandbyPageDeltaExport> {
    validate_database_name(database)?;
    validate_replication_group_id(source_group_id)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready; current state is {}",
        database_record.state
    );
    ensure!(
        database_record.replication_group_id == source_group_id,
        "database {database} is assigned to replication group {}, not requested source group {source_group_id}",
        database_record.replication_group_id
    );
    let source_runtime = state
        .replication_groups
        .runtime(source_group_id)
        .with_context(|| {
            format!("source replication group {source_group_id} is not loaded by this node")
        })?;
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for standby page delta")?;
    let delta = if min_exclusive_version == 0 {
        source_runtime
            .export_database_live_snapshot(database)
            .with_context(|| format!("exporting SQLite live snapshot for database {database}"))?
    } else {
        source_runtime
            .export_database_pages_since(database, min_exclusive_version)
            .with_context(|| {
                format!(
                    "exporting SQLite page delta for database {database} after index {min_exclusive_version}"
                )
            })?
    };
    Ok(StandbyPageDeltaExport {
        source_group_id: source_group_id.to_string(),
        source_watermark,
        min_exclusive_version,
        delta,
    })
}

fn export_database_placement_file_snapshot(
    state: &LibsqlHttpState,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<StandbyFileSnapshotExport> {
    validate_database_name(database)?;
    validate_replication_group_id(source_group_id)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready; current state is {}",
        database_record.state
    );
    ensure!(
        database_record.replication_group_id == source_group_id,
        "database {database} is assigned to replication group {}, not requested source group {source_group_id}",
        database_record.replication_group_id
    );
    let source_runtime = state
        .replication_groups
        .runtime(source_group_id)
        .with_context(|| {
            format!("source replication group {source_group_id} is not loaded by this node")
        })?;
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for file snapshot")?;
    let snapshot = source_runtime
        .export_database_file_snapshot(database, PLACEMENT_FILE_SNAPSHOT_CHUNK_BYTES)
        .with_context(|| format!("exporting SQLite file snapshot for database {database}"))?;
    Ok(StandbyFileSnapshotExport {
        source_group_id: source_group_id.to_string(),
        source_watermark,
        snapshot,
    })
}

fn encode_standby_page_delta_export(export: &StandbyPageDeltaExport) -> anyhow::Result<Vec<u8>> {
    postcard::to_stdvec(export).context("encoding standby page delta export")
}

fn decode_standby_page_delta_export(bytes: &[u8]) -> anyhow::Result<StandbyPageDeltaExport> {
    postcard::from_bytes(bytes).context("decoding standby page delta export")
}

fn encode_standby_file_snapshot_export(
    export: &StandbyFileSnapshotExport,
) -> anyhow::Result<Vec<u8>> {
    postcard::to_stdvec(export).context("encoding standby file snapshot export")
}

fn decode_standby_file_snapshot_export(bytes: &[u8]) -> anyhow::Result<StandbyFileSnapshotExport> {
    postcard::from_bytes(bytes).context("decoding standby file snapshot export")
}

fn standby_page_delta_stats(export: &StandbyPageDeltaExport) -> StandbyPageDeltaStats {
    let bytes_applied = export
        .delta
        .entries
        .iter()
        .map(|entry| entry.key.len() as u64 + entry.value.len() as u64)
        .sum();
    StandbyPageDeltaStats {
        entries_seen: export.delta.entries.len() as u64,
        entries_applied: export.delta.entries.len() as u64,
        deletes_applied: export.delta.metadata_deletes.len() as u64
            + export.delta.current_page_deletes.len() as u64,
        bytes_received: encode_standby_page_delta_export(export)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default(),
        bytes_applied,
    }
}

fn standby_file_snapshot_stats(export: &StandbyFileSnapshotExport) -> StandbyPageDeltaStats {
    let bytes_applied = export
        .snapshot
        .files
        .iter()
        .flat_map(|file| file.chunks.iter())
        .map(|chunk| chunk.bytes.len() as u64)
        .sum();
    let entries = export
        .snapshot
        .files
        .iter()
        .map(|file| file.chunks.len() as u64)
        .sum();
    StandbyPageDeltaStats {
        entries_seen: entries,
        entries_applied: entries,
        deletes_applied: 0,
        bytes_received: encode_standby_file_snapshot_export(export)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default(),
        bytes_applied,
    }
}

async fn export_database_placement_checkpoint_object(
    state: &LibsqlHttpState,
    database: &str,
    source_group_id: &str,
    object_path: &str,
) -> anyhow::Result<axum::response::Response> {
    validate_database_name(database)?;
    validate_replication_group_id(source_group_id)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.replication_group_id == source_group_id,
        "database {database} is assigned to replication group {}, not requested source group {source_group_id}",
        database_record.replication_group_id
    );
    let source_runtime = state
        .replication_groups
        .runtime(source_group_id)
        .with_context(|| {
            format!("source replication group {source_group_id} is not loaded by this node")
        })?;
    let prefix = source_runtime.state_store().sqlite_database_path(database);
    ensure_checkpoint_object_path_allowed(&prefix, object_path)?;
    let object_location = ObjectPath::parse(object_path)
        .with_context(|| format!("parsing checkpoint object path {object_path}"))?;
    let bytes = source_runtime
        .state_store()
        .object_store()
        .get(&object_location)
        .await
        .with_context(|| format!("reading checkpoint object {object_path}"))?
        .bytes()
        .await
        .with_context(|| format!("reading checkpoint object bytes {object_path}"))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&bytes.len().to_string())
            .expect("checkpoint object length must be a valid header value"),
    );
    Ok((StatusCode::OK, headers, Body::from(bytes)).into_response())
}

async fn list_checkpoint_objects(
    object_store: &Arc<dyn ObjectStore>,
    artifact: &SlateDbCheckpointArtifact,
) -> anyhow::Result<Vec<StandbyCheckpointObjectRef>> {
    list_slate_db_checkpoint_objects(object_store, artifact)
        .await
        .map(|objects| {
            objects
                .into_iter()
                .map(|object| StandbyCheckpointObjectRef {
                    path: object.path,
                    size: object.size,
                })
                .collect()
        })
}

fn ensure_checkpoint_object_path_allowed(prefix: &str, object_path: &str) -> anyhow::Result<()> {
    ensure!(
        object_path == prefix || object_path.starts_with(&format!("{prefix}/")),
        "checkpoint object path {object_path} is outside expected prefix {prefix}"
    );
    Ok(())
}

async fn refresh_database_placement_standby_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    expected_source_group_id: Option<&str>,
    target_group_id: &str,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    validate_database_name(database)?;
    if let Some(source_group_id) = expected_source_group_id {
        validate_replication_group_id(source_group_id)?;
    }
    validate_replication_group_id(target_group_id)?;
    ensure!(
        state.replication_groups.contains(target_group_id)?,
        "target replication group {target_group_id} is not loaded by this node"
    );
    let (database_record, source_endpoint, previous_standby) = {
        let conn = open_system_catalog_connection(state)?;
        require_database_catalog_schema(&conn, 7)?;
        let database_record = read_database_catalog_record_from_conn(&conn, database)?
            .ok_or_else(|| anyhow!("database {database} does not exist"))?;
        ensure!(
            database_record.state == "ready",
            "database {database} is not ready; current state is {}",
            database_record.state
        );
        ensure!(
            database_record.replication_group_id != target_group_id,
            "database {database} is already assigned to replication group {target_group_id}"
        );
        if let Some(source_group_id) = expected_source_group_id {
            ensure!(
                database_record.replication_group_id == source_group_id,
                "database {database} is assigned to replication group {}, not planned source group {source_group_id}",
                database_record.replication_group_id
            );
        }
        ensure!(
            read_running_placement_operation_for_database(&conn, &database_record.database_id)?
                .is_none(),
            "database {database} already has a running placement operation"
        );
        let target_group = read_replication_group_record_from_conn(&conn, target_group_id)?
            .ok_or_else(|| anyhow!("replication group {target_group_id} does not exist"))?;
        ensure!(
            target_group.state == "active",
            "replication group {target_group_id} is not active; current state is {}",
            target_group.state
        );
        let source_endpoint =
            source_peer_endpoint_for_group(state, &conn, &database_record.replication_group_id)?;
        let previous_standby =
            read_database_placement_standby(&conn, &database_record.database_id, target_group_id)?;
        (database_record, source_endpoint, previous_standby)
    };
    let target_runtime = state
        .replication_groups
        .runtime(target_group_id)
        .with_context(|| format!("resolving target replication group {target_group_id}"))?;
    let incremental_from_index = previous_standby.as_ref().and_then(|standby| {
        (standby.source_group_id == database_record.replication_group_id
            && standby.error.is_none()
            && target_runtime
                .open_existing_database(database)
                .ok()
                .flatten()
                .is_some())
        .then_some(standby.source_applied_index)
        .flatten()
    });
    if let Some(min_exclusive_version) = incremental_from_index {
        state.standby_page_delta_metrics.record_attempt();
        match refresh_database_placement_standby_from_peer_page_delta(
            state,
            headers,
            &source_endpoint,
            database,
            target_group_id,
            &database_record,
            &target_runtime,
            min_exclusive_version,
        )
        .await
        {
            Ok(record) => return Ok(record),
            Err(error) => {
                state.standby_page_delta_metrics.record_failure();
                state
                    .standby_page_delta_metrics
                    .record_fallback_to_checkpoint();
                eprintln!(
                    "incremental standby refresh for database {database} from peer failed ({}); falling back to checkpoint refresh",
                    error_chain_message(&error)
                );
            }
        }
    }
    state.standby_checkpoint_metrics.record_attempt();
    match refresh_database_placement_standby_from_peer_checkpoint(
        state,
        headers,
        &source_endpoint,
        database,
        target_group_id,
        &database_record,
        &target_runtime,
    )
    .await
    {
        Ok(record) => return Ok(record),
        Err(error) => {
            state.standby_checkpoint_metrics.record_failure();
            state.standby_checkpoint_metrics.record_fallback_to_backup();
            eprintln!(
                "checkpoint standby refresh for database {database} from peer failed ({}); falling back to SQLite backup export",
                error_chain_message(&error)
            );
        }
    }
    let (export_file, export_metadata) = fetch_standby_export_from_peer(
        state,
        headers,
        &source_endpoint,
        database,
        &database_record.replication_group_id,
    )
    .await?;
    import_database_backup_from_file(&target_runtime, database, export_file.path())
        .with_context(|| {
            format!(
                "importing standby snapshot for database {database} into target group {target_group_id}"
            )
        })?;
    target_runtime_mark_ready_and_verify(&target_runtime, database, target_group_id)?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark for standby import")?;
    record_database_placement_standby(
        state,
        &database_record,
        target_group_id,
        &export_metadata,
        &target_watermark,
    )
}

async fn refresh_database_placement_standby_from_peer_page_delta(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    target_group_id: &str,
    database_record: &DatabaseCatalogRecord,
    target_runtime: &OrionSqliteRuntime,
    min_exclusive_version: u64,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    let delta = fetch_standby_page_delta_from_peer(
        state,
        headers,
        source_endpoint,
        database,
        &database_record.replication_group_id,
        min_exclusive_version,
    )
    .await?;
    let stats = standby_page_delta_stats(&delta);
    ensure!(
        delta.source_group_id == database_record.replication_group_id,
        "standby page delta source group {} does not match database placement {}",
        delta.source_group_id,
        database_record.replication_group_id
    );
    ensure!(
        delta.min_exclusive_version == min_exclusive_version,
        "standby page delta min_exclusive_version {} does not match requested {}",
        delta.min_exclusive_version,
        min_exclusive_version
    );
    target_runtime
        .apply_database_page_delta(database, &delta.delta)
        .with_context(|| {
            format!(
                "applying standby page delta for database {database} into target group {target_group_id} after index {min_exclusive_version}"
            )
        })?;
    target_runtime_mark_ready_and_verify(target_runtime, database, target_group_id)?;
    state.standby_page_delta_metrics.record_success(stats);
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark for standby page delta import")?;
    let metadata = StandbyExportMetadata {
        source_group_id: delta.source_group_id,
        source_applied_index: delta.source_watermark.applied_index,
        source_commit_ts: delta.source_watermark.applied_commit_ts,
        bytes: delta
            .delta
            .entries
            .iter()
            .map(|entry| entry.value.len() as u64)
            .sum(),
        sha256: String::new(),
    };
    record_database_placement_standby(
        state,
        database_record,
        target_group_id,
        &metadata,
        &target_watermark,
    )
}

async fn refresh_database_placement_standby_from_peer_checkpoint(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    target_group_id: &str,
    database_record: &DatabaseCatalogRecord,
    target_runtime: &OrionSqliteRuntime,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    let checkpoint = fetch_standby_checkpoint_from_peer(
        state,
        headers,
        source_endpoint,
        database,
        &database_record.replication_group_id,
    )
    .await?;
    ensure!(
        checkpoint.source_group_id == database_record.replication_group_id,
        "standby checkpoint source group {} does not match database placement {}",
        checkpoint.source_group_id,
        database_record.replication_group_id
    );
    let fetch_stats = fetch_missing_checkpoint_objects_from_peer(
        state,
        headers,
        source_endpoint,
        database,
        &database_record.replication_group_id,
        target_runtime,
        &checkpoint,
    )
    .await?;
    target_runtime
        .clone_database_checkpoint_from_local_objects(database, &checkpoint.artifact)
        .with_context(|| {
            format!(
                "opening standby checkpoint clone for database {database} into target group {target_group_id}"
            )
        })?;
    target_runtime_mark_ready_and_verify(target_runtime, database, target_group_id)?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark for standby checkpoint import")?;
    let metadata = StandbyExportMetadata {
        source_group_id: checkpoint.source_group_id,
        source_applied_index: checkpoint.source_watermark.applied_index,
        source_commit_ts: checkpoint.source_watermark.applied_commit_ts,
        bytes: checkpoint.objects.iter().map(|object| object.size).sum(),
        sha256: String::new(),
    };
    let record = record_database_placement_standby(
        state,
        database_record,
        target_group_id,
        &metadata,
        &target_watermark,
    )?;
    state.standby_checkpoint_metrics.record_success(fetch_stats);
    Ok(record)
}

async fn materialize_database_placement_checkpoint(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    request: PlacementCheckpointMaterializeRequest,
) -> anyhow::Result<PlacementCheckpointMaterializeResponse> {
    validate_database_name(database)?;
    validate_replication_group_id(&request.target_group_id)?;
    validate_replication_group_id(&request.checkpoint.source_group_id)?;
    ensure_valid_placement_operation_id(&request.operation_id)?;
    ensure!(
        request.source_endpoint.starts_with("http://")
            || request.source_endpoint.starts_with("https://"),
        "source_endpoint must be an absolute HTTP(S) URL"
    );
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 9)?;
    let operation = read_placement_operation(&conn, &request.operation_id)?.ok_or_else(|| {
        anyhow!(
            "placement operation {} does not exist",
            request.operation_id
        )
    })?;
    ensure!(
        operation.database_name == database,
        "placement operation {} is for database {}, not {database}",
        operation.operation_id,
        operation.database_name
    );
    ensure!(
        operation.source_group_id == request.checkpoint.source_group_id,
        "placement operation {} source group {} does not match checkpoint source group {}",
        operation.operation_id,
        operation.source_group_id,
        request.checkpoint.source_group_id
    );
    ensure!(
        operation.target_group_id == request.target_group_id,
        "placement operation {} target group {} does not match request target group {}",
        operation.operation_id,
        operation.target_group_id,
        request.target_group_id
    );
    ensure_checkpoint_covers_placement_fence(&request.checkpoint, &operation)?;
    let target_runtime = state
        .replication_groups
        .runtime(&request.target_group_id)
        .with_context(|| {
            format!(
                "target replication group {} is not loaded by this node",
                request.target_group_id
            )
        })?;
    let target_metrics = target_runtime.metrics();
    ensure!(
        target_metrics.voter_ids.contains(&state.node_id),
        "node {} is not a voter for target replication group {}",
        state.node_id,
        request.target_group_id
    );
    let fetch_stats = fetch_missing_checkpoint_objects_from_peer(
        state,
        headers,
        &request.source_endpoint,
        database,
        &request.checkpoint.source_group_id,
        &target_runtime,
        &request.checkpoint,
    )
    .await?;
    target_runtime
        .clone_database_checkpoint_from_local_objects(database, &request.checkpoint.artifact)
        .with_context(|| {
            format!(
                "opening placement checkpoint clone for database {database} into target group {}",
                request.target_group_id
            )
        })?;
    target_runtime_mark_ready_and_verify(&target_runtime, database, &request.target_group_id)?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark after checkpoint materialization")?;
    let response = PlacementCheckpointMaterializeResponse {
        operation_id: request.operation_id,
        node_id: state.node_id,
        target_group_id: request.target_group_id,
        target_applied_index: target_watermark.applied_index,
        target_commit_ts: target_watermark.applied_commit_ts,
        checkpoint_objects_seen: fetch_stats.objects_seen,
        checkpoint_objects_copied: fetch_stats.objects_copied,
        checkpoint_objects_reused: fetch_stats.objects_reused,
        checkpoint_bytes_seen: fetch_stats.bytes_seen,
        checkpoint_bytes_copied: fetch_stats.bytes_copied,
    };
    let conn = open_system_catalog_write_connection(state)?;
    record_placement_transfer_voter_ready(
        &conn,
        &response,
        operation.transfer_epoch_id.as_deref(),
    )?;
    Ok(response)
}

async fn materialize_database_placement_page_delta(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    request: PlacementPageDeltaMaterializeRequest,
) -> anyhow::Result<PlacementCheckpointMaterializeResponse> {
    validate_database_name(database)?;
    validate_replication_group_id(&request.target_group_id)?;
    validate_replication_group_id(&request.source_group_id)?;
    ensure_valid_placement_operation_id(&request.operation_id)?;
    ensure!(
        request.source_endpoint.starts_with("http://")
            || request.source_endpoint.starts_with("https://"),
        "source_endpoint must be an absolute HTTP(S) URL"
    );
    ensure!(
        request.min_exclusive_version == 0,
        "placement live snapshot materialization requires min_exclusive_version 0"
    );
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 9)?;
    let operation = read_placement_operation(&conn, &request.operation_id)?.ok_or_else(|| {
        anyhow!(
            "placement operation {} does not exist",
            request.operation_id
        )
    })?;
    ensure!(
        operation.database_name == database,
        "placement operation {} is for database {}, not {database}",
        operation.operation_id,
        operation.database_name
    );
    ensure!(
        operation.source_group_id == request.source_group_id,
        "placement operation {} source group {} does not match request source group {}",
        operation.operation_id,
        operation.source_group_id,
        request.source_group_id
    );
    ensure!(
        operation.target_group_id == request.target_group_id,
        "placement operation {} target group {} does not match request target group {}",
        operation.operation_id,
        operation.target_group_id,
        request.target_group_id
    );
    let target_runtime = state
        .replication_groups
        .runtime(&request.target_group_id)
        .with_context(|| {
            format!(
                "target replication group {} is not loaded by this node",
                request.target_group_id
            )
        })?;
    let target_metrics = target_runtime.metrics();
    ensure!(
        target_metrics.voter_ids.contains(&state.node_id),
        "node {} is not a voter for target replication group {}",
        state.node_id,
        request.target_group_id
    );
    let export = fetch_standby_file_snapshot_from_peer(
        state,
        headers,
        &request.source_endpoint,
        database,
        &request.source_group_id,
    )
    .await?;
    ensure!(
        export.source_group_id == request.source_group_id,
        "placement file snapshot source group {} does not match request source group {}",
        export.source_group_id,
        request.source_group_id
    );
    let stats = standby_file_snapshot_stats(&export);
    target_runtime
        .materialize_database_file_snapshot(database, &export.snapshot)
        .with_context(|| {
            format!(
                "materializing placement file snapshot for database {database} into target group {}",
                request.target_group_id
            )
        })?;
    target_runtime_mark_ready_and_verify(&target_runtime, database, &request.target_group_id)?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark after live snapshot materialization")?;
    let response = PlacementCheckpointMaterializeResponse {
        operation_id: request.operation_id,
        node_id: state.node_id,
        target_group_id: request.target_group_id,
        target_applied_index: target_watermark.applied_index,
        target_commit_ts: target_watermark.applied_commit_ts,
        checkpoint_objects_seen: stats.entries_seen,
        checkpoint_objects_copied: stats.entries_applied,
        checkpoint_objects_reused: 0,
        checkpoint_bytes_seen: stats.bytes_received,
        checkpoint_bytes_copied: stats.bytes_applied,
    };
    let conn = open_system_catalog_write_connection(state)?;
    record_placement_transfer_voter_ready(
        &conn,
        &response,
        operation.transfer_epoch_id.as_deref(),
    )?;
    Ok(response)
}

fn source_peer_endpoint_for_group(
    state: &LibsqlHttpState,
    conn: &Connection,
    source_group_id: &str,
) -> anyhow::Result<String> {
    let members = list_replication_group_members_from_conn(conn, source_group_id)?;
    for member in members {
        if member.node_id == state.node_id {
            continue;
        }
        if let Some(endpoint) = http_endpoint_for_node(state, member.node_id) {
            return Ok(endpoint);
        }
    }
    anyhow::bail!(
        "no HTTP peer endpoint is configured for source replication group {source_group_id}"
    )
}

async fn fetch_standby_export_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<(NamedTempFile, StandbyExportMetadata)> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/export?source_group_id={}",
        source_endpoint.trim_end_matches('/'),
        database,
        source_group_id
    );
    let mut builder = state.http_client.get(url);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .context("requesting standby export from source peer")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable error body>".to_string());
        anyhow::bail!("source peer standby export HTTP {status}: {body}");
    }
    let metadata = standby_export_metadata_from_headers(response.headers())?;
    let mut file = NamedTempFile::new().context("creating standby import temp file")?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading standby export chunk from source peer")?;
        bytes = bytes.saturating_add(chunk.len() as u64);
        hasher.update(&chunk);
        io::Write::write_all(file.as_file_mut(), &chunk)
            .context("writing standby export chunk to temp file")?;
    }
    let sha256 = sha256_hex(hasher.finalize());
    ensure!(
        bytes == metadata.bytes,
        "standby export byte count mismatch: received {bytes}, expected {}",
        metadata.bytes
    );
    ensure!(
        sha256 == metadata.sha256,
        "standby export checksum mismatch: received {sha256}, expected {}",
        metadata.sha256
    );
    file.as_file()
        .sync_all()
        .context("syncing received standby export temp file")?;
    Ok((file, metadata))
}

async fn fetch_standby_checkpoint_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<StandbyCheckpointExport> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/checkpoint",
        source_endpoint.trim_end_matches('/'),
        database,
    );
    let mut builder = state
        .http_client
        .get(url)
        .query(&[("source_group_id", source_group_id)]);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .context("requesting standby checkpoint from source peer")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable error body>".to_string());
        anyhow::bail!("source peer standby checkpoint HTTP {status}: {body}");
    }
    response
        .json::<StandbyCheckpointExport>()
        .await
        .context("decoding standby checkpoint export from source peer")
}

async fn fetch_standby_page_delta_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
    min_exclusive_version: u64,
) -> anyhow::Result<StandbyPageDeltaExport> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/pages",
        source_endpoint.trim_end_matches('/'),
        database,
    );
    let min_exclusive_version = min_exclusive_version.to_string();
    let mut builder = state.http_client.get(url).query(&[
        ("source_group_id", source_group_id),
        ("min_exclusive_version", min_exclusive_version.as_str()),
    ]);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .context("requesting standby page delta from source peer")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable error body>".to_string());
        anyhow::bail!("source peer standby page delta HTTP {status}: {body}");
    }
    let bytes = response
        .bytes()
        .await
        .context("reading standby page delta export from source peer")?;
    decode_standby_page_delta_export(&bytes)
}

async fn fetch_standby_file_snapshot_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
) -> anyhow::Result<StandbyFileSnapshotExport> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/files",
        source_endpoint.trim_end_matches('/'),
        database,
    );
    let mut builder = state
        .http_client
        .get(url)
        .query(&[("source_group_id", source_group_id)]);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .context("requesting standby file snapshot from source peer")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable error body>".to_string());
        anyhow::bail!("source peer standby file snapshot HTTP {status}: {body}");
    }
    let bytes = response
        .bytes()
        .await
        .context("reading standby file snapshot export from source peer")?;
    decode_standby_file_snapshot_export(&bytes)
}

async fn fetch_missing_checkpoint_objects_from_peer(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
    target_runtime: &OrionSqliteRuntime,
    checkpoint: &StandbyCheckpointExport,
) -> anyhow::Result<StandbyCheckpointFetchStats> {
    let target_store = target_runtime.state_store().object_store();
    let mut stats = StandbyCheckpointFetchStats::default();
    for object in &checkpoint.objects {
        stats.objects_seen = stats.objects_seen.saturating_add(1);
        stats.bytes_seen = stats.bytes_seen.saturating_add(object.size);
        ensure_checkpoint_object_path_allowed(&checkpoint.artifact.object_prefix, &object.path)?;
        let location = ObjectPath::parse(&object.path)
            .with_context(|| format!("parsing checkpoint object path {}", object.path))?;
        if target_object_matches(target_store.as_ref(), &location, object.size).await? {
            stats.objects_reused = stats.objects_reused.saturating_add(1);
            continue;
        }
        let bytes_received = fetch_checkpoint_object_from_peer_into_store(
            state,
            headers,
            source_endpoint,
            database,
            source_group_id,
            &object.path,
            object.size,
            Arc::clone(&target_store),
            &location,
        )
        .await?;
        ensure!(
            bytes_received == object.size,
            "checkpoint object {} byte count mismatch: received {}, expected {}",
            object.path,
            bytes_received,
            object.size
        );
        stats.objects_copied = stats.objects_copied.saturating_add(1);
        stats.bytes_copied = stats.bytes_copied.saturating_add(object.size);
    }
    Ok(stats)
}

async fn target_object_matches(
    target_store: &dyn ObjectStore,
    location: &ObjectPath,
    expected_size: u64,
) -> anyhow::Result<bool> {
    match target_store.head(location).await {
        Ok(meta) => Ok(meta.size == expected_size),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn fetch_checkpoint_object_from_peer_into_store(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    source_endpoint: &str,
    database: &str,
    source_group_id: &str,
    object_path: &str,
    expected_size: u64,
    target_store: Arc<dyn ObjectStore>,
    target_location: &ObjectPath,
) -> anyhow::Result<u64> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/checkpoint/object",
        source_endpoint.trim_end_matches('/'),
        database,
    );
    let mut builder = state.http_client.get(url).query(&[
        ("source_group_id", source_group_id),
        ("object_path", object_path),
    ]);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .with_context(|| format!("requesting checkpoint object {object_path} from source peer"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable error body>".to_string());
        anyhow::bail!("source peer checkpoint object HTTP {status}: {body}");
    }
    if let Some(content_length) = response.content_length() {
        ensure!(
            content_length == expected_size,
            "checkpoint object {object_path} content length mismatch: source reported {content_length}, expected {expected_size}"
        );
    }

    let upload = target_store
        .put_multipart(target_location)
        .await
        .with_context(|| format!("starting checkpoint object upload {object_path}"))?;
    let mut writer = Some(WriteMultipart::new_with_chunk_size(
        upload,
        PLACEMENT_CHECKPOINT_OBJECT_CHUNK_BYTES,
    ));
    let mut bytes_received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                abort_checkpoint_object_upload(&mut writer).await;
                return Err(error)
                    .with_context(|| format!("reading checkpoint object {object_path}"));
            }
        };
        if chunk.is_empty() {
            continue;
        }
        bytes_received = bytes_received.saturating_add(chunk.len() as u64);
        if bytes_received > expected_size {
            abort_checkpoint_object_upload(&mut writer).await;
            anyhow::bail!(
                "checkpoint object {object_path} exceeded expected size: received {bytes_received}, expected {expected_size}"
            );
        }
        let Some(writer_ref) = writer.as_mut() else {
            anyhow::bail!("checkpoint object {object_path} upload already closed");
        };
        if let Err(error) = writer_ref
            .wait_for_capacity(PLACEMENT_CHECKPOINT_OBJECT_UPLOAD_CONCURRENCY)
            .await
        {
            abort_checkpoint_object_upload(&mut writer).await;
            return Err(error)
                .with_context(|| format!("checkpoint object {object_path} upload backpressure"));
        }
        writer_ref.put(chunk);
    }
    if bytes_received != expected_size {
        abort_checkpoint_object_upload(&mut writer).await;
        anyhow::bail!(
            "checkpoint object {object_path} byte count mismatch: received {bytes_received}, expected {expected_size}"
        );
    }
    let writer = writer
        .take()
        .expect("checkpoint object writer should still be present");
    writer
        .finish()
        .await
        .with_context(|| format!("writing checkpoint object {object_path}"))?;
    let meta = target_store
        .head(target_location)
        .await
        .with_context(|| format!("checking checkpoint object {object_path}"))?;
    ensure!(
        meta.size == expected_size,
        "checkpoint object {object_path} completed with size {}, expected {expected_size}",
        meta.size
    );
    Ok(bytes_received)
}

async fn abort_checkpoint_object_upload(writer: &mut Option<WriteMultipart>) {
    if let Some(writer) = writer.take() {
        let _ = writer.abort().await;
    }
}

fn standby_export_metadata_from_headers(
    headers: &reqwest::header::HeaderMap,
) -> anyhow::Result<StandbyExportMetadata> {
    let source_group_id = required_header(headers, STANDBY_EXPORT_SOURCE_GROUP_HEADER)?;
    let sha256 = required_header(headers, STANDBY_EXPORT_SHA256_HEADER)?;
    let bytes = required_header(headers, axum::http::header::CONTENT_LENGTH.as_str())?
        .parse::<u64>()
        .context("parsing standby export content length")?;
    let source_applied_index = optional_header_u64(headers, STANDBY_EXPORT_APPLIED_INDEX_HEADER)?;
    let physical_ms = optional_header_u64(headers, STANDBY_EXPORT_COMMIT_TS_PHYSICAL_MS_HEADER)?;
    let logical = optional_header_u64(headers, STANDBY_EXPORT_COMMIT_TS_LOGICAL_HEADER)?;
    let source_commit_ts = match (physical_ms, logical) {
        (Some(physical_ms), Some(logical)) => Some(HybridTimestamp {
            physical_ms,
            logical: logical as u32,
        }),
        _ => None,
    };
    Ok(StandbyExportMetadata {
        source_group_id,
        source_applied_index,
        source_commit_ts,
        bytes,
        sha256,
    })
}

fn record_database_placement_standby(
    state: &LibsqlHttpState,
    database_record: &DatabaseCatalogRecord,
    target_group_id: &str,
    source: &StandbyExportMetadata,
    target_watermark: &OrionSqliteRuntimeWatermark,
) -> anyhow::Result<PlacementStandbyRefreshRecord> {
    ensure!(
        source.source_group_id == database_record.replication_group_id,
        "standby export source group {} does not match database placement {}",
        source.source_group_id,
        database_record.replication_group_id
    );
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    let refreshed_at = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into database_standby_copies (
            database_id, database_name, source_group_id, target_group_id,
            source_applied_index, source_commit_ts_physical_ms, source_commit_ts_logical,
            target_applied_index, target_commit_ts_physical_ms, target_commit_ts_logical,
            refreshed_at_ms, updated_at_ms, error
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null)
        on conflict(database_id, target_group_id) do update set
            database_name = excluded.database_name,
            source_group_id = excluded.source_group_id,
            source_applied_index = excluded.source_applied_index,
            source_commit_ts_physical_ms = excluded.source_commit_ts_physical_ms,
            source_commit_ts_logical = excluded.source_commit_ts_logical,
            target_applied_index = excluded.target_applied_index,
            target_commit_ts_physical_ms = excluded.target_commit_ts_physical_ms,
            target_commit_ts_logical = excluded.target_commit_ts_logical,
            refreshed_at_ms = excluded.refreshed_at_ms,
            updated_at_ms = excluded.updated_at_ms,
            error = null
        "#,
        params![
            &database_record.database_id,
            &database_record.name,
            &database_record.replication_group_id,
            target_group_id,
            source.source_applied_index.map(sqlite_i64),
            source
                .source_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            source
                .source_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            target_watermark.applied_index.map(sqlite_i64),
            target_watermark
                .applied_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            target_watermark
                .applied_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            refreshed_at,
            refreshed_at,
        ],
    )
    .context("recording imported standby copy in catalog")?;
    let mut standby =
        read_database_placement_standby(&conn, &database_record.database_id, target_group_id)?
            .ok_or_else(|| {
                anyhow!(
                    "standby copy for database {} disappeared",
                    database_record.name
                )
            })?;
    annotate_standby_record(state, database_record, &mut standby, None);
    Ok(PlacementStandbyRefreshRecord { standby })
}

fn target_runtime_mark_ready_and_verify(
    target_runtime: &OrionSqliteRuntime,
    database: &str,
    target_group_id: &str,
) -> anyhow::Result<()> {
    target_runtime
        .mark_database_ready(database)
        .with_context(|| {
            format!("marking database {database} ready on target group {target_group_id}")
        })?;
    ensure_target_database_openable(target_runtime, database).with_context(|| {
        format!(
            "verifying standby database {database} is openable on target group {target_group_id}"
        )
    })
}

fn backup_database_to_file(
    runtime: &OrionSqliteRuntime,
    database: &str,
    path: &FsPath,
) -> anyhow::Result<()> {
    let source_db = runtime.open_database(database)?;
    let source_conn = source_db.connect()?;
    let mut target_conn =
        Connection::open(path).context("opening temporary SQLite backup target")?;
    let backup = Backup::new(&source_conn, &mut target_conn)?;
    backup.run_to_completion(128, Duration::from_millis(1), None)?;
    Ok(())
}

fn import_database_backup_from_file(
    runtime: &OrionSqliteRuntime,
    database: &str,
    path: &FsPath,
) -> anyhow::Result<()> {
    let source_conn = Connection::open(path).context("opening received SQLite backup")?;
    let target_db = runtime.open_database(database)?;
    let mut target_conn = target_db.connect()?;
    let backup = Backup::new(&source_conn, &mut target_conn)?;
    backup.run_to_completion(128, Duration::from_millis(1), None)?;
    Ok(())
}

fn file_sha256(file: &mut File) -> anyhow::Result<(u64, String)> {
    file.rewind()
        .context("rewinding snapshot file before hashing")?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; STANDBY_EXPORT_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).context("reading snapshot file")?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    file.rewind()
        .context("rewinding snapshot file after hashing")?;
    Ok((bytes, sha256_hex(hasher.finalize())))
}

fn stream_temp_path(path: TempPath) -> Body {
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(4);
    std::thread::spawn(move || {
        let result = (|| -> io::Result<()> {
            let mut file = File::open(&path)?;
            let mut buffer = vec![0_u8; STANDBY_EXPORT_CHUNK_BYTES];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                if tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = tx.blocking_send(Err(error));
        }
        drop(path);
    });
    Body::from_stream(ReceiverStream::new(rx))
}

fn required_header(headers: &reqwest::header::HeaderMap, name: &str) -> anyhow::Result<String> {
    Ok(headers
        .get(name)
        .ok_or_else(|| anyhow!("standby export missing header {name}"))?
        .to_str()
        .with_context(|| format!("reading standby export header {name}"))?
        .to_string())
}

fn optional_header_u64(
    headers: &reqwest::header::HeaderMap,
    name: &str,
) -> anyhow::Result<Option<u64>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .with_context(|| format!("reading standby export header {name}"))?
                .parse::<u64>()
                .with_context(|| format!("parsing standby export header {name}"))
        })
        .transpose()
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn promote_database_placement_standby(
    state: &LibsqlHttpState,
    database: &str,
    target_group_id: &str,
    max_staleness_ms: Option<u64>,
    force: bool,
) -> anyhow::Result<PlacementStandbyPromotionRecord> {
    validate_database_name(database)?;
    validate_replication_group_id(target_group_id)?;
    ensure!(
        state.replication_groups.contains(target_group_id)?,
        "target replication group {target_group_id} is not loaded by this node"
    );
    let conn = open_system_catalog_write_connection(state)
        .context("opening writable catalog for standby promotion")?;
    require_database_catalog_schema(&conn, 7)
        .context("checking standby promotion catalog schema")?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    ensure!(
        database_record.replication_group_id != target_group_id,
        "database {database} is already assigned to replication group {target_group_id}"
    );
    ensure!(
        read_running_placement_operation_for_database(&conn, &database_record.database_id)?
            .is_none(),
        "database {database} already has a running placement operation"
    );
    let mut standby =
        read_database_placement_standby(&conn, &database_record.database_id, target_group_id)?
            .ok_or_else(|| {
                anyhow!(
                    "standby copy for database {database} to group {target_group_id} does not exist"
                )
            })?;
    validate_standby_promotable(state, &database_record, &standby, max_staleness_ms, force)
        .context("validating standby copy before promotion")?;
    annotate_standby_record(state, &database_record, &mut standby, max_staleness_ms);
    let now = sqlite_i64(current_time_millis());
    let tx = conn
        .unchecked_transaction()
        .context("opening standby promotion catalog transaction")?;
    tx.execute(
        "update database_replication_groups set group_id = ?, updated_at_ms = ? where database_id = ?",
        params![target_group_id, now, &database_record.database_id],
    )
    .context("updating database placement to promoted standby group")?;
    tx.execute(
        "delete from database_standby_copies where database_id = ? and target_group_id = ?",
        params![&database_record.database_id, target_group_id],
    )
    .context("deleting consumed standby copy from catalog")?;
    tx.commit()
        .context("committing standby promotion catalog transaction")?;
    state
        .evict_database(database)
        .with_context(|| format!("evicting local cache for promoted database {database}"))?;
    let database = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} disappeared after standby promotion"))?;
    Ok(PlacementStandbyPromotionRecord { database, standby })
}

fn ensure_target_database_openable(
    target_runtime: &OrionSqliteRuntime,
    database: &str,
) -> anyhow::Result<()> {
    let Some(db) = target_runtime
        .open_existing_database(database)
        .with_context(|| format!("opening existing database {database} on target runtime"))?
    else {
        anyhow::bail!("database {database} is not present on this target runtime");
    };
    let conn = db
        .connect()
        .with_context(|| format!("opening SQLite connection for database {database}"))?;
    let _: i64 = conn
        .query_row("select 1", [], |row| row.get(0))
        .with_context(|| format!("executing SQLite smoke query for database {database}"))?;
    let _: i64 = conn
        .query_row("pragma schema_version", [], |row| row.get(0))
        .with_context(|| format!("reading SQLite schema_version for database {database}"))?;
    Ok(())
}

fn read_database_placement_standby(
    conn: &Connection,
    database_id: &str,
    target_group_id: &str,
) -> anyhow::Result<Option<DatabasePlacementStandbyRecord>> {
    let sql = format!(
        "select {DATABASE_STANDBY_SELECT_COLUMNS} from database_standby_copies where database_id = ? and target_group_id = ?"
    );
    conn.query_row(
        &sql,
        params![database_id, target_group_id],
        database_placement_standby_record_from_row,
    )
    .optional()
    .map_err(Into::into)
}

fn validate_standby_promotable(
    state: &LibsqlHttpState,
    database: &DatabaseCatalogRecord,
    standby: &DatabasePlacementStandbyRecord,
    max_staleness_ms: Option<u64>,
    force: bool,
) -> anyhow::Result<()> {
    ensure!(
        standby.database_id == database.database_id && standby.database_name == database.name,
        "standby copy does not match database {}",
        database.name
    );
    ensure!(
        standby.source_group_id == database.replication_group_id,
        "standby source group {} does not match current placement {}",
        standby.source_group_id,
        database.replication_group_id
    );
    let target_runtime = state
        .replication_groups
        .runtime(&standby.target_group_id)
        .with_context(|| {
            format!(
                "resolving target replication group {} for standby promotion",
                standby.target_group_id
            )
        })?;
    let target_metrics = target_runtime.metrics();
    ensure!(
        target_metrics.is_ready_for_linearizable_reads(),
        "target replication group {} is not ready for linearizable reads",
        standby.target_group_id
    );
    ensure_target_database_openable(&target_runtime, &database.name).with_context(|| {
        format!(
            "verifying standby database {} is openable on target group {}",
            database.name, standby.target_group_id
        )
    })?;
    let target_watermark = target_runtime
        .durability_watermark()
        .context("reading target durability watermark for standby promotion")?;
    if let (Some(current), Some(recorded)) =
        (target_watermark.applied_index, standby.target_applied_index)
    {
        ensure!(
            current >= recorded,
            "target replication group {} applied index regressed from standby index {} to {}",
            standby.target_group_id,
            recorded,
            current
        );
    }
    if let (Some(target_ts), Some(source_ts)) = (standby.target_commit_ts, standby.source_commit_ts)
    {
        ensure!(
            target_ts >= source_ts,
            "standby target commit timestamp {:?} is behind copied source timestamp {:?}",
            target_ts,
            source_ts
        );
    }
    if !force {
        let max_staleness_ms = max_staleness_ms.unwrap_or(default_failover_promote_after_ms());
        let age_ms = current_time_millis().saturating_sub(standby.refreshed_at_ms);
        ensure!(
            age_ms <= max_staleness_ms,
            "standby copy age {age_ms}ms exceeds max_staleness_ms={max_staleness_ms}"
        );
    }
    Ok(())
}

fn annotate_standby_record(
    state: &LibsqlHttpState,
    database: &DatabaseCatalogRecord,
    standby: &mut DatabasePlacementStandbyRecord,
    max_staleness_ms: Option<u64>,
) {
    standby.age_ms = current_time_millis().saturating_sub(standby.refreshed_at_ms);
    standby.catalog_recorded = true;
    if let Ok(target_runtime) = state.replication_groups.runtime(&standby.target_group_id) {
        standby.target_group_available = target_runtime.metrics().is_ready_for_linearizable_reads();
        standby.target_locally_openable = standby.target_group_available
            && ensure_target_database_openable(&target_runtime, &database.name).is_ok();
    }
    standby.promotable =
        validate_standby_promotable(state, database, standby, max_staleness_ms, false).is_ok();
}

fn create_replication_group(
    state: &LibsqlHttpState,
    request: CreateReplicationGroupRequest,
) -> anyhow::Result<ReplicationGroupRecord> {
    validate_replication_group_id(&request.group_id)?;
    validate_placement_request(&request.placement)?;
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_replication_group_record_from_conn(&conn, &request.group_id)?.is_none(),
        "replication group {} already exists",
        request.group_id
    );
    upsert_replication_group_catalog(
        &conn,
        state,
        &request.group_id,
        &request.placement,
        "active",
    )?;
    replace_replication_group_members(&conn, state, &request.group_id, &request.members)?;
    read_replication_group_record(state, &request.group_id)?
        .ok_or_else(|| anyhow!("replication group {} disappeared", request.group_id))
}

fn set_replication_group_state(
    state: &LibsqlHttpState,
    group_id: &str,
    next_state: &str,
) -> anyhow::Result<ReplicationGroupRecord> {
    validate_replication_group_id(group_id)?;
    ensure!(
        matches!(next_state, "active" | "draining" | "deleted" | "failed"),
        "unsupported replication group state {next_state}"
    );
    ensure!(
        group_id != DEFAULT_REPLICATION_GROUP_ID || next_state != "deleted",
        "default replication group cannot be deleted"
    );
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_replication_group_record_from_conn(&conn, group_id)?.is_some(),
        "replication group {group_id} does not exist"
    );
    if next_state == "draining" {
        let assigned = assigned_database_count_for_group(&conn, group_id)?;
        let group = read_replication_group_record_from_conn(&conn, group_id)?.unwrap();
        ensure!(
            assigned == 0 || group.failover_automatic,
            "replication group {group_id} has {assigned} assigned database(s); enable automatic failover or move them before draining"
        );
    }
    conn.execute(
        "update replication_groups set state = ?, updated_at_ms = ?, error = null where group_id = ?",
        params![next_state, sqlite_i64(current_time_millis()), group_id],
    )?;
    read_replication_group_record(state, group_id)?
        .ok_or_else(|| anyhow!("replication group {group_id} disappeared"))
}

fn delete_replication_group(
    state: &LibsqlHttpState,
    group_id: &str,
) -> anyhow::Result<ReplicationGroupRecord> {
    validate_replication_group_id(group_id)?;
    ensure!(
        group_id != DEFAULT_REPLICATION_GROUP_ID,
        "default replication group cannot be deleted"
    );
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_replication_group_record_from_conn(&conn, group_id)?.is_some(),
        "replication group {group_id} does not exist"
    );
    let assigned = assigned_database_count_for_group(&conn, group_id)?;
    ensure!(
        assigned == 0,
        "replication group {group_id} still has {assigned} assigned database(s)"
    );
    set_replication_group_state(state, group_id, "deleted")
}

fn add_replication_group_member(
    state: &LibsqlHttpState,
    group_id: &str,
    request: ReplicationGroupMemberRequest,
) -> anyhow::Result<ReplicationGroupRecord> {
    validate_replication_group_id(group_id)?;
    validate_replication_group_member_role(&request.role)?;
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_replication_group_record_from_conn(&conn, group_id)?.is_some(),
        "replication group {group_id} does not exist"
    );
    upsert_replication_group_member(&conn, state, group_id, &request)?;
    read_replication_group_record(state, group_id)?
        .ok_or_else(|| anyhow!("replication group {group_id} disappeared"))
}

fn remove_replication_group_member(
    state: &LibsqlHttpState,
    group_id: &str,
    node_id: u64,
    role: &str,
) -> anyhow::Result<ReplicationGroupRecord> {
    validate_replication_group_id(group_id)?;
    validate_replication_group_member_role(role)?;
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_replication_group_record_from_conn(&conn, group_id)?.is_some(),
        "replication group {group_id} does not exist"
    );
    let voter_count: i64 = conn.query_row(
        "select count(*) from replication_group_members where group_id = ? and role = 'voter'",
        [group_id],
        |row| row.get(0),
    )?;
    let assigned = assigned_database_count_for_group(&conn, group_id)?;
    ensure!(
        !(role == "voter" && voter_count <= 1),
        "cannot remove the last voter from replication group {group_id}"
    );
    ensure!(
        !(role == "voter" && assigned > 0 && voter_count <= 2),
        "cannot reduce assigned replication group {group_id} below 2 voters while it has {assigned} assigned database(s)"
    );
    let deleted = conn.execute(
        "delete from replication_group_members where group_id = ? and node_id = ? and role = ?",
        params![group_id, sqlite_i64(node_id), role],
    )?;
    ensure!(
        deleted > 0,
        "replication group member {group_id}/{node_id}/{role} does not exist"
    );
    read_replication_group_record(state, group_id)?
        .ok_or_else(|| anyhow!("replication group {group_id} disappeared"))
}

fn create_database_move_operation(
    state: &LibsqlHttpState,
    database: &str,
    target_group_id: &str,
    allow_active_sessions: bool,
) -> anyhow::Result<PlacementOperationRecord> {
    validate_database_name(database)?;
    validate_replication_group_id(target_group_id)?;
    ensure!(
        state.replication_groups.contains(target_group_id)?,
        "replication group {target_group_id} is not loaded by this node"
    );
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    let database_record = read_database_catalog_record_from_conn(&conn, database)?
        .ok_or_else(|| anyhow!("database {database} does not exist"))?;
    if !allow_active_sessions {
        let active_sessions = state.active_database_sessions(database)?;
        ensure!(
            active_sessions == 0,
            "database {database} has {active_sessions} active session(s); close sessions before placement move"
        );
    }
    ensure!(
        database_record.state == "ready",
        "database {database} is not ready for placement move; current state is {}",
        database_record.state
    );
    let group = read_replication_group_record_from_conn(&conn, target_group_id)?
        .ok_or_else(|| anyhow!("replication group {target_group_id} does not exist"))?;
    ensure!(
        group.state == "active",
        "replication group {target_group_id} is not active; current state is {}",
        group.state
    );
    ensure!(
        database_record.replication_group_id != target_group_id,
        "database {database} is already assigned to replication group {target_group_id}"
    );
    ensure!(
        read_running_placement_operation_for_database(&conn, &database_record.database_id)?
            .is_none(),
        "database {database} already has a running placement operation"
    );
    let operation_id = new_placement_operation_id(database, target_group_id);
    let tx = conn.unchecked_transaction()?;
    insert_placement_operation(
        &tx,
        &operation_id,
        &database_record,
        target_group_id,
        "running",
        "planned",
        None,
    )?;
    tx.commit()?;
    let _ = state.evict_database(database);
    read_placement_operation(&conn, &operation_id)?
        .ok_or_else(|| anyhow!("placement operation {operation_id} disappeared"))
}

async fn create_database_move_operation_with_session_drain(
    state: &LibsqlHttpState,
    database: &str,
    target_group_id: &str,
    drain_timeout_ms: u64,
) -> anyhow::Result<PlacementOperationRecord> {
    let operation = create_database_move_operation(state, database, target_group_id, true)?;
    let deadline = Instant::now() + Duration::from_millis(drain_timeout_ms);
    loop {
        if state.active_database_sessions(database)? == 0 {
            return Ok(operation);
        }
        if Instant::now() >= deadline {
            let _ = state.close_database_sessions(database);
            return Ok(operation);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn new_placement_operation_id(database: &str, target_group_id: &str) -> String {
    let now = current_time_millis();
    let digest = Sha256::digest(format!("{database}:{target_group_id}:{now}").as_bytes());
    format!("pmove_{}_{}", hex_lower(&digest[..8]), now)
}

fn insert_placement_operation(
    conn: &Connection,
    operation_id: &str,
    database_record: &DatabaseCatalogRecord,
    target_group_id: &str,
    status: &str,
    phase: &str,
    error: Option<&str>,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into placement_operations (
            operation_id, database_id, database_name, operation, status, phase,
            source_group_id, target_group_id, created_at_ms, updated_at_ms, completed_at_ms, error
        )
        values (?, ?, ?, 'move', ?, ?, ?, ?, ?, ?, null, ?)
        "#,
        params![
            operation_id,
            &database_record.database_id,
            &database_record.name,
            status,
            phase,
            &database_record.replication_group_id,
            target_group_id,
            now,
            now,
            error,
        ],
    )?;
    Ok(())
}

fn update_placement_operation_phase(
    conn: &Connection,
    operation_id: &str,
    phase: &str,
    status: &str,
    error: Option<&str>,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    let completed_at_ms = (status == "completed" || status == "failed").then_some(now);
    conn.execute(
        r#"
        update placement_operations
        set phase = ?, status = ?, updated_at_ms = ?, completed_at_ms = coalesce(?, completed_at_ms), error = ?
        where operation_id = ?
        "#,
        params![phase, status, now, completed_at_ms, error, operation_id],
    )?;
    Ok(())
}

fn record_placement_source_fence_watermark(
    conn: &Connection,
    operation_id: &str,
    applied_index: Option<u64>,
    commit_ts: Option<HybridTimestamp>,
    observed_at_ms: u64,
) -> anyhow::Result<()> {
    conn.execute(
        r#"
        update placement_operations
        set source_fence_applied_index = ?,
            source_fence_commit_ts_physical_ms = ?,
            source_fence_commit_ts_logical = ?,
            source_fence_observed_at_ms = ?,
            updated_at_ms = ?
        where operation_id = ?
        "#,
        params![
            applied_index.map(sqlite_i64),
            commit_ts.map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            commit_ts.map(|timestamp| i64::from(timestamp.logical)),
            sqlite_i64(observed_at_ms),
            sqlite_i64(current_time_millis()),
            operation_id,
        ],
    )?;
    Ok(())
}

fn record_placement_target_clone_watermark(
    conn: &Connection,
    operation_id: &str,
    applied_index: Option<u64>,
    commit_ts: Option<HybridTimestamp>,
) -> anyhow::Result<()> {
    conn.execute(
        r#"
        update placement_operations
        set target_clone_applied_index = ?,
            target_clone_commit_ts_physical_ms = ?,
            target_clone_commit_ts_logical = ?,
            updated_at_ms = ?
        where operation_id = ?
        "#,
        params![
            applied_index.map(sqlite_i64),
            commit_ts.map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            commit_ts.map(|timestamp| i64::from(timestamp.logical)),
            sqlite_i64(current_time_millis()),
            operation_id,
        ],
    )?;
    Ok(())
}

fn record_placement_checkpoint_transfer_epoch(
    conn: &Connection,
    operation_id: &str,
    epoch_id: &str,
    checkpoint: &StandbyCheckpointExport,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        update placement_operations
        set transfer_epoch_id = ?,
            transfer_kind = 'checkpoint',
            transfer_checkpoint_artifact_json = ?,
            transfer_source_applied_index = ?,
            transfer_source_commit_ts_physical_ms = ?,
            transfer_source_commit_ts_logical = ?,
            transfer_created_at_ms = coalesce(transfer_created_at_ms, ?),
            updated_at_ms = ?
        where operation_id = ?
        "#,
        params![
            epoch_id,
            serde_json::to_string(&checkpoint.artifact)?,
            checkpoint.source_watermark.applied_index.map(sqlite_i64),
            checkpoint
                .source_watermark
                .applied_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            checkpoint
                .source_watermark
                .applied_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            now,
            now,
            operation_id,
        ],
    )?;
    Ok(())
}

fn record_placement_transfer_voter_ready(
    conn: &Connection,
    response: &PlacementCheckpointMaterializeResponse,
    transfer_epoch_id: Option<&str>,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into placement_transfer_voter_status (
            operation_id, node_id, target_group_id, transfer_epoch_id, status,
            target_applied_index, target_commit_ts_physical_ms, target_commit_ts_logical,
            checkpoint_objects_seen, checkpoint_objects_copied, checkpoint_objects_reused,
            checkpoint_bytes_seen, checkpoint_bytes_copied, error, updated_at_ms
        )
        values (?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, null, ?)
        on conflict(operation_id, node_id) do update set
            target_group_id = excluded.target_group_id,
            transfer_epoch_id = excluded.transfer_epoch_id,
            status = 'ready',
            target_applied_index = excluded.target_applied_index,
            target_commit_ts_physical_ms = excluded.target_commit_ts_physical_ms,
            target_commit_ts_logical = excluded.target_commit_ts_logical,
            checkpoint_objects_seen = excluded.checkpoint_objects_seen,
            checkpoint_objects_copied = excluded.checkpoint_objects_copied,
            checkpoint_objects_reused = excluded.checkpoint_objects_reused,
            checkpoint_bytes_seen = excluded.checkpoint_bytes_seen,
            checkpoint_bytes_copied = excluded.checkpoint_bytes_copied,
            error = null,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            &response.operation_id,
            sqlite_i64(response.node_id),
            &response.target_group_id,
            transfer_epoch_id,
            response.target_applied_index.map(sqlite_i64),
            response
                .target_commit_ts
                .map(|timestamp| sqlite_i64(timestamp.physical_ms)),
            response
                .target_commit_ts
                .map(|timestamp| i64::from(timestamp.logical)),
            sqlite_i64(response.checkpoint_objects_seen),
            sqlite_i64(response.checkpoint_objects_copied),
            sqlite_i64(response.checkpoint_objects_reused),
            sqlite_i64(response.checkpoint_bytes_seen),
            sqlite_i64(response.checkpoint_bytes_copied),
            now,
        ],
    )?;
    Ok(())
}

fn ready_placement_transfer_voter_responses(
    conn: &Connection,
    operation_id: &str,
    transfer_epoch_id: &str,
) -> anyhow::Result<Vec<PlacementCheckpointMaterializeResponse>> {
    let mut stmt = conn.prepare(
        r#"
        select node_id, target_group_id, target_applied_index,
               target_commit_ts_physical_ms, target_commit_ts_logical,
               checkpoint_objects_seen, checkpoint_objects_copied,
               checkpoint_objects_reused, checkpoint_bytes_seen,
               checkpoint_bytes_copied
        from placement_transfer_voter_status
        where operation_id = ?
          and transfer_epoch_id = ?
          and status = 'ready'
        "#,
    )?;
    let rows = stmt.query_map(params![operation_id, transfer_epoch_id], |row| {
        let commit_ts_physical_ms = row.get::<_, Option<i64>>(3)?;
        let commit_ts_logical = row.get::<_, Option<i64>>(4)?;
        Ok(PlacementCheckpointMaterializeResponse {
            operation_id: operation_id.to_string(),
            node_id: row.get::<_, i64>(0)? as u64,
            target_group_id: row.get::<_, String>(1)?,
            target_applied_index: row.get::<_, Option<i64>>(2)?.map(|value| value as u64),
            target_commit_ts: match (commit_ts_physical_ms, commit_ts_logical) {
                (Some(physical_ms), Some(logical)) => Some(HybridTimestamp {
                    physical_ms: physical_ms as u64,
                    logical: logical as u32,
                }),
                _ => None,
            },
            checkpoint_objects_seen: row.get::<_, i64>(5)? as u64,
            checkpoint_objects_copied: row.get::<_, i64>(6)? as u64,
            checkpoint_objects_reused: row.get::<_, i64>(7)? as u64,
            checkpoint_bytes_seen: row.get::<_, i64>(8)? as u64,
            checkpoint_bytes_copied: row.get::<_, i64>(9)? as u64,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn plan_database_placement(
    state: &LibsqlHttpState,
    database: &str,
    request: CreateDatabasePlacementRequest,
) -> anyhow::Result<DatabasePlacementPlan> {
    validate_placement_request(&request)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 4)?;
    ensure!(
        read_database_catalog_record_from_conn(&conn, database)?.is_some(),
        "database {database} does not exist"
    );
    let members = planned_group_members(state, DEFAULT_REPLICATION_GROUP_ID);
    let mut risks = Vec::new();
    if request.durability.survive_cloud_outage {
        let clouds = members
            .iter()
            .map(|member| member.cloud.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if clouds.len() < 3 {
            risks.push(
                "survive_cloud_outage requested but fewer than 3 clouds are configured".to_string(),
            );
        }
    }
    Ok(DatabasePlacementPlan {
        database: database.to_string(),
        valid: risks.is_empty(),
        selected_group_id: DEFAULT_REPLICATION_GROUP_ID.to_string(),
        requested: request,
        actions: vec![
            "reuse_replication_group:rg_default".to_string(),
            "no_runtime_membership_change".to_string(),
        ],
        members,
        risks,
    })
}

fn list_placement_operations(
    state: &LibsqlHttpState,
    database: &str,
) -> anyhow::Result<Vec<PlacementOperationRecord>> {
    validate_database_name(database)?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    ensure!(
        read_database_catalog_record_from_conn(&conn, database)?.is_some(),
        "database {database} does not exist"
    );
    let sql = format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations where database_name = ? order by created_at_ms desc, operation_id desc"
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_map([database], placement_operation_record_from_row)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn cancel_placement_operation(
    state: &LibsqlHttpState,
    database: &str,
    operation_id: &str,
    reason: Option<&str>,
) -> anyhow::Result<PlacementOperationRecord> {
    validate_database_name(database)?;
    ensure_valid_placement_operation_id(operation_id)?;
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    let operation = read_placement_operation(&conn, operation_id)?
        .ok_or_else(|| anyhow!("placement operation {operation_id} does not exist"))?;
    ensure!(
        operation.database_name == database,
        "placement operation {operation_id} does not belong to database {database}"
    );
    ensure!(
        operation.status == "running",
        "placement operation {operation_id} is not running"
    );
    let reason = reason
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or("operator requested cancellation");
    update_placement_operation_phase(
        &conn,
        operation_id,
        "failed",
        "failed",
        Some(&format!("cancelled: {reason}")),
    )?;
    let _ = state.evict_database(database);
    read_placement_operation(&conn, operation_id)?
        .ok_or_else(|| anyhow!("placement operation {operation_id} disappeared"))
}

fn repair_placement_operation(
    state: &LibsqlHttpState,
    database: &str,
    operation_id: &str,
    phase: Option<&str>,
    reason: Option<&str>,
) -> anyhow::Result<PlacementOperationRecord> {
    validate_database_name(database)?;
    ensure_valid_placement_operation_id(operation_id)?;
    let requested_phase = phase
        .map(str::trim)
        .filter(|phase| !phase.is_empty())
        .unwrap_or("planned");
    validate_running_placement_phase(requested_phase)?;
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    let operation = read_placement_operation(&conn, operation_id)?
        .ok_or_else(|| anyhow!("placement operation {operation_id} does not exist"))?;
    ensure!(
        operation.database_name == database,
        "placement operation {operation_id} does not belong to database {database}"
    );
    ensure!(
        operation.status != "running",
        "placement operation {operation_id} is still running"
    );
    ensure!(
        operation.status != "completed",
        "placement operation {operation_id} is already completed"
    );
    validate_move_operation_can_progress(
        state,
        &PlacementOperationRecord {
            status: "running".to_string(),
            phase: requested_phase.to_string(),
            error: None,
            ..operation.clone()
        },
        placement_phase_requires_source_runtime(requested_phase),
    )?;
    let reason = reason
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .unwrap_or("operator requested repair");
    conn.execute(
        r#"
        update placement_operations
        set phase = ?, status = 'running', updated_at_ms = ?, completed_at_ms = null, error = ?
        where operation_id = ?
        "#,
        params![
            requested_phase,
            sqlite_i64(current_time_millis()),
            format!("repair: {reason}"),
            operation_id,
        ],
    )?;
    let _ = state.evict_database(database);
    read_placement_operation(&conn, operation_id)?
        .ok_or_else(|| anyhow!("placement operation {operation_id} disappeared"))
}

fn validate_running_placement_phase(phase: &str) -> anyhow::Result<()> {
    ensure!(
        matches!(
            phase,
            "planned" | "fenced" | "cloning" | "catching_up" | "switching"
        ),
        "unsupported placement repair phase {phase}"
    );
    Ok(())
}

fn placement_phase_requires_source_runtime(phase: &str) -> bool {
    !matches!(phase, "catching_up" | "switching")
}

fn read_placement_operation(
    conn: &Connection,
    operation_id: &str,
) -> anyhow::Result<Option<PlacementOperationRecord>> {
    let sql = format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations where operation_id = ?"
    );
    conn.query_row(&sql, [operation_id], placement_operation_record_from_row)
        .optional()
        .map_err(Into::into)
}

fn read_running_placement_operation_for_database(
    conn: &Connection,
    database_id: &str,
) -> anyhow::Result<Option<PlacementOperationRecord>> {
    let sql = format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations where database_id = ? and status = 'running' order by created_at_ms, operation_id limit 1"
    );
    conn.query_row(&sql, [database_id], placement_operation_record_from_row)
        .optional()
        .map_err(Into::into)
}

fn placement_metrics(state: &LibsqlHttpState) -> anyhow::Result<PlacementMetricsRecord> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 10)?;
    let now = current_time_millis();
    let mut stmt = conn.prepare(&format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations"
    ))?;
    let operations = stmt
        .query_map([], placement_operation_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut operations_running = 0;
    let mut operations_completed = 0;
    let mut operations_failed = 0;
    let mut running_by_phase = BTreeMap::new();
    let mut oldest_running_age_ms = None::<u64>;
    let mut stale_running_operations = Vec::new();
    for operation in &operations {
        match operation.status.as_str() {
            "running" => {
                operations_running += 1;
                *running_by_phase.entry(operation.phase.clone()).or_insert(0) += 1;
                let age_ms = now.saturating_sub(operation.created_at_ms);
                oldest_running_age_ms =
                    Some(oldest_running_age_ms.map_or(age_ms, |age| age.max(age_ms)));
                let updated_age_ms = now.saturating_sub(operation.updated_at_ms);
                if age_ms >= PLACEMENT_RUNNING_STALE_AFTER_MS
                    || updated_age_ms >= PLACEMENT_RUNNING_STALE_AFTER_MS
                {
                    stale_running_operations.push(PlacementStaleOperationRecord {
                        operation_id: operation.operation_id.clone(),
                        database_name: operation.database_name.clone(),
                        phase: operation.phase.clone(),
                        age_ms,
                        updated_age_ms,
                    });
                }
            }
            "completed" => operations_completed += 1,
            "failed" => operations_failed += 1,
            _ => {}
        }
    }

    let groups = list_replication_group_records(state)?;
    let mut groups_active = 0;
    let mut groups_draining = 0;
    let mut groups_deleted = 0;
    let mut groups_failed = 0;
    let mut groups_unloaded = 0;
    let mut groups_not_ready = 0;
    for group in &groups {
        match group.state.as_str() {
            "active" => groups_active += 1,
            "draining" => groups_draining += 1,
            "deleted" => groups_deleted += 1,
            "failed" => groups_failed += 1,
            _ => {}
        }
        if group.state == "active" && !group.runtime.loaded {
            groups_unloaded += 1;
        }
        if group.state == "active"
            && group.runtime.loaded
            && !group.runtime.ready_for_linearizable_reads
        {
            groups_not_ready += 1;
        }
    }
    let standby_metrics = placement_standby_metrics(state, &conn, &groups)?;
    let transfer_voter_metrics = placement_transfer_voter_metrics(&conn)?;

    Ok(PlacementMetricsRecord {
        checked_at_ms: now,
        operations_total: operations.len() as u64,
        operations_running,
        operations_completed,
        operations_failed,
        running_by_phase,
        oldest_running_age_ms,
        stale_running_operations,
        databases_by_group: placement_databases_by_group(&conn)?,
        groups_total: groups.len() as u64,
        groups_active,
        groups_draining,
        groups_deleted,
        groups_failed,
        groups_unloaded,
        groups_not_ready,
        standbys_total: standby_metrics.total,
        standbys_promotable: standby_metrics.promotable,
        standbys_stale: standby_metrics.stale,
        standbys_errors: standby_metrics.errors,
        standby_checkpoint: state.standby_checkpoint_metrics.snapshot(),
        standby_page_delta: state.standby_page_delta_metrics.snapshot(),
        placement_move_transfer: state.placement_move_transfer_metrics.snapshot(),
        placement_transfer_voters: transfer_voter_metrics,
    })
}

fn placement_transfer_voter_metrics(
    conn: &Connection,
) -> anyhow::Result<PlacementTransferVoterMetricsSnapshot> {
    if !table_exists(conn, "placement_transfer_voter_status")? {
        return Ok(PlacementTransferVoterMetricsSnapshot::default());
    }
    conn.query_row(
        r#"
        select
            count(*),
            coalesce(sum(case when status = 'ready' then 1 else 0 end), 0),
            coalesce(sum(case when status = 'failed' then 1 else 0 end), 0),
            coalesce(sum(case when status = 'pending' then 1 else 0 end), 0),
            coalesce(sum(checkpoint_objects_seen), 0),
            coalesce(sum(checkpoint_objects_copied), 0),
            coalesce(sum(checkpoint_objects_reused), 0),
            coalesce(sum(checkpoint_bytes_seen), 0),
            coalesce(sum(checkpoint_bytes_copied), 0)
        from placement_transfer_voter_status
        "#,
        [],
        |row| {
            Ok(PlacementTransferVoterMetricsSnapshot {
                total: row.get::<_, i64>(0)?.max(0) as u64,
                ready: row.get::<_, i64>(1)?.max(0) as u64,
                failed: row.get::<_, i64>(2)?.max(0) as u64,
                pending: row.get::<_, i64>(3)?.max(0) as u64,
                checkpoint_objects_seen: row.get::<_, i64>(4)?.max(0) as u64,
                checkpoint_objects_copied: row.get::<_, i64>(5)?.max(0) as u64,
                checkpoint_objects_reused: row.get::<_, i64>(6)?.max(0) as u64,
                checkpoint_bytes_seen: row.get::<_, i64>(7)?.max(0) as u64,
                checkpoint_bytes_copied: row.get::<_, i64>(8)?.max(0) as u64,
            })
        },
    )
    .map_err(Into::into)
}

fn placement_standby_metrics(
    state: &LibsqlHttpState,
    conn: &Connection,
    groups: &[ReplicationGroupRecord],
) -> anyhow::Result<PlacementStandbyMetrics> {
    let now = current_time_millis();
    let groups_by_id = groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<HashMap<_, _>>();
    let databases = list_database_catalog_records_from_conn(conn, true)?
        .into_iter()
        .map(|database| (database.database_id.clone(), database))
        .collect::<HashMap<_, _>>();
    let sql = format!("select {DATABASE_STANDBY_SELECT_COLUMNS} from database_standby_copies");
    let mut stmt = conn.prepare(&sql)?;
    let standbys = stmt
        .query_map([], database_placement_standby_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut metrics = PlacementStandbyMetrics {
        total: standbys.len() as u64,
        ..PlacementStandbyMetrics::default()
    };
    for mut standby in standbys {
        let Some(database) = databases.get(&standby.database_id) else {
            metrics.errors += 1;
            continue;
        };
        let max_staleness_ms = groups_by_id
            .get(standby.source_group_id.as_str())
            .map(|group| group.failover_promote_after_ms)
            .unwrap_or_else(default_failover_promote_after_ms);
        if now.saturating_sub(standby.refreshed_at_ms) > max_staleness_ms {
            metrics.stale += 1;
        }
        if standby.error.is_some() {
            metrics.errors += 1;
        }
        annotate_standby_record(state, database, &mut standby, Some(max_staleness_ms));
        if standby.promotable {
            metrics.promotable += 1;
        }
    }
    Ok(metrics)
}

fn placement_databases_by_group(conn: &Connection) -> anyhow::Result<BTreeMap<String, u64>> {
    let mut stmt = conn.prepare(
        r#"
        select group_id, count(*)
        from database_replication_groups
        group by group_id
        order by group_id
        "#,
    )?;
    let pairs = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(pairs
        .into_iter()
        .map(|(group_id, count)| (group_id, count.max(0) as u64))
        .collect())
}

fn collect_placement_garbage(
    state: &LibsqlHttpState,
    older_than: Option<u64>,
    standby_older_than: Option<u64>,
    limit: Option<usize>,
) -> anyhow::Result<PlacementOperationGcResult> {
    let older_than_ms = older_than.unwrap_or(PLACEMENT_OPERATION_GC_DEFAULT_TTL_MS);
    ensure!(
        older_than_ms > 0,
        "placement operation GC older_than_ms must be greater than zero"
    );
    let standby_older_than_ms = standby_older_than.unwrap_or(older_than_ms);
    ensure!(
        standby_older_than_ms > 0,
        "placement standby GC standby_older_than_ms must be greater than zero"
    );
    let limit = limit.unwrap_or(PLACEMENT_OPERATION_GC_DEFAULT_LIMIT);
    ensure!(
        limit > 0,
        "placement operation GC limit must be greater than zero"
    );
    let checked_at = current_time_millis();
    let cutoff = checked_at.saturating_sub(older_than_ms);
    let standby_cutoff = checked_at.saturating_sub(standby_older_than_ms);
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    let mut stmt = conn.prepare(
        r#"
        select operation_id
        from placement_operations
        where status in ('completed', 'failed')
          and completed_at_ms is not null
          and completed_at_ms < ?
        order by completed_at_ms, operation_id
        limit ?
        "#,
    )?;
    let ids = stmt
        .query_map(params![sqlite_i64(cutoff), sqlite_usize(limit)], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    let tx = conn.unchecked_transaction()?;
    let mut operations_deleted = 0;
    for id in ids {
        operations_deleted += tx.execute(
            "delete from placement_operations where operation_id = ?",
            [id],
        )?;
    }
    let standbys_deleted = if table_exists(&tx, "database_standby_copies")? {
        tx.execute(
            r#"
            delete from database_standby_copies
            where database_id not in (select database_id from database_catalog)
               or exists (
                    select 1
                    from database_replication_groups drg
                    where drg.database_id = database_standby_copies.database_id
                      and drg.group_id != database_standby_copies.source_group_id
               )
               or (error is not null and updated_at_ms < ?)
            "#,
            [sqlite_i64(standby_cutoff)],
        )?
    } else {
        0
    };
    tx.commit()?;
    Ok(PlacementOperationGcResult {
        checked_at_ms: checked_at,
        older_than_ms,
        standby_older_than_ms,
        limit,
        deleted: operations_deleted + standbys_deleted,
        operations_deleted,
        standbys_deleted,
    })
}

fn placement_metrics_phase_rows_from_catalog(
    catalog_db: Option<&OrionSqliteDb>,
) -> anyhow::Result<Vec<PlacementMetricsPhaseRow>> {
    let Some(catalog_db) = catalog_db else {
        return Ok(Vec::new());
    };
    let conn = catalog_db.connect()?;
    ensure_database_catalog_schema(&conn)?;
    require_database_catalog_schema(&conn, 6)?;
    placement_metrics_phase_rows_from_conn(&conn)
}

fn placement_metrics_phase_rows_from_conn(
    conn: &Connection,
) -> anyhow::Result<Vec<PlacementMetricsPhaseRow>> {
    let now = current_time_millis();
    let mut stmt = conn.prepare(
        r#"
        select status, phase, count(*), min(created_at_ms), max(updated_at_ms)
        from placement_operations
        group by status, phase
        order by status, phase
        "#,
    )?;
    let rows = stmt
        .query_map([], |row| {
            let oldest_created_at_ms = row.get::<_, Option<i64>>(3)?;
            let newest_updated_at_ms = row.get::<_, Option<i64>>(4)?;
            Ok(PlacementMetricsPhaseRow {
                status: row.get(0)?,
                phase: row.get(1)?,
                operation_count: row.get::<_, i64>(2)?.max(0) as u64,
                oldest_age_ms: oldest_created_at_ms
                    .map(|value| now.saturating_sub(value.max(0) as u64)),
                newest_update_age_ms: newest_updated_at_ms
                    .map(|value| now.saturating_sub(value.max(0) as u64)),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[derive(Debug, Serialize)]
struct PlacementReconcileSummary {
    checked_at_ms: u64,
    actions: Vec<String>,
    risks: Vec<String>,
    runtime_groups: Vec<ReplicationGroupRuntimeRecord>,
    open_operations: Vec<PlacementOperationRecord>,
}

fn reconcile_placement(state: &LibsqlHttpState) -> anyhow::Result<PlacementReconcileSummary> {
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    let runtime_groups = replication_group_runtime_records(state)?;
    let loaded_group_ids = runtime_groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let groups = list_replication_group_records(state)?;
    let mut actions = Vec::new();
    let mut risks = Vec::new();
    for group in &groups {
        let voter_count = group
            .members
            .iter()
            .filter(|member| member.role == "voter")
            .count();
        if group.state == "active" && !loaded_group_ids.contains(group.group_id.as_str()) {
            risks.push(format!(
                "replication group {} is active in catalog but not loaded on this node",
                group.group_id
            ));
            actions.push(format!("load_replication_group:{}", group.group_id));
        }
        if group.state == "active" && voter_count == 0 {
            risks.push(format!(
                "replication group {} has no voters and cannot make progress",
                group.group_id
            ));
        }
    }
    enqueue_automatic_draining_group_moves(state, &groups, &mut actions, &mut risks)?;
    enqueue_automatic_standby_promotions(state, &groups, &mut actions, &mut risks)?;
    let sql = format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations where status = 'running' order by created_at_ms"
    );
    let mut stmt = conn.prepare(&sql)?;
    let open_operations = stmt
        .query_map([], placement_operation_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    for operation in &open_operations {
        match advance_placement_operation(state, operation) {
            Ok(action) => actions.push(action),
            Err(error) if placement_operation_error_is_retryable(&error) => {
                risks.push(format!(
                    "placement operation {} for database {} is waiting at phase {}: {}",
                    operation.operation_id,
                    operation.database_name,
                    operation.phase,
                    error_chain_message(&error)
                ));
                actions.push(format!(
                    "retry_placement_operation:{}",
                    operation.operation_id
                ));
            }
            Err(error) => {
                risks.push(format!(
                    "placement operation {} for database {} failed at phase {}: {}",
                    operation.operation_id,
                    operation.database_name,
                    operation.phase,
                    error_chain_message(&error)
                ));
                let _ = fail_placement_operation(state, operation, &error);
            }
        }
    }
    drop(stmt);
    let sql = format!(
        "select {PLACEMENT_OPERATION_SELECT_COLUMNS} from placement_operations where status = 'running' order by created_at_ms"
    );
    let mut stmt = conn.prepare(&sql)?;
    let open_operations = stmt
        .query_map([], placement_operation_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    for operation in &open_operations {
        risks.push(format!(
            "placement operation {} for database {} is still running at phase {}",
            operation.operation_id, operation.database_name, operation.phase
        ));
    }
    Ok(PlacementReconcileSummary {
        checked_at_ms: current_time_millis(),
        actions,
        risks,
        runtime_groups,
        open_operations,
    })
}

fn placement_operation_error_is_retryable(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("is not loaded by this node")
        || message.contains("is not ready for linearizable reads")
        || message.contains("active session")
        || message.contains("placement clone must run on target leader")
        || message.contains("disk I/O error")
}

fn run_async_from_sync<F, T>(future: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("creating temporary runtime for placement async task")?;
            runtime.block_on(future)
        }
    }
}

fn enqueue_automatic_draining_group_moves(
    state: &LibsqlHttpState,
    groups: &[ReplicationGroupRecord],
    actions: &mut Vec<String>,
    risks: &mut Vec<String>,
) -> anyhow::Result<()> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 6)?;
    for group in groups {
        if group.state != "draining" || !group.failover_automatic {
            continue;
        }
        let Some(target_group_id) = automatic_placement_target_group(groups, &group.group_id)
        else {
            risks.push(format!(
                "replication group {} is draining but has no ready automatic failover target",
                group.group_id
            ));
            continue;
        };
        let databases = database_records_for_group(&conn, &group.group_id)?;
        for database in databases {
            if read_running_placement_operation_for_database(&conn, &database.database_id)?
                .is_some()
            {
                continue;
            }
            match create_database_move_operation(state, &database.name, &target_group_id, true) {
                Ok(operation) => actions.push(format!(
                    "automatic_placement_move:{}:{}",
                    database.name, operation.operation_id
                )),
                Err(error) if placement_operation_error_is_retryable(&error) => {
                    risks.push(format!(
                        "automatic placement move for database {} is waiting: {error}",
                        database.name
                    ))
                }
                Err(error) => risks.push(format!(
                    "automatic placement move for database {} failed to enqueue: {error}",
                    database.name
                )),
            }
        }
    }
    Ok(())
}

fn enqueue_automatic_standby_promotions(
    state: &LibsqlHttpState,
    groups: &[ReplicationGroupRecord],
    actions: &mut Vec<String>,
    risks: &mut Vec<String>,
) -> anyhow::Result<()> {
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    for group in groups {
        if !group.failover_automatic || replication_group_available(group) {
            continue;
        }
        let databases = database_records_for_group(&conn, &group.group_id)?;
        for database in databases {
            if read_running_placement_operation_for_database(&conn, &database.database_id)?
                .is_some()
            {
                continue;
            }
            let Some(standby) =
                best_promotable_standby_for_database(state, &conn, &database, group)?
            else {
                risks.push(format!(
                    "database {} is on unavailable replication group {} but has no promotable standby",
                    database.name, group.group_id
                ));
                continue;
            };
            match promote_database_placement_standby(
                state,
                &database.name,
                &standby.target_group_id,
                Some(group.failover_promote_after_ms),
                false,
            ) {
                Ok(record) => {
                    actions.push(format!(
                        "automatic_standby_promotion:{}:{}",
                        record.database.name, record.standby.target_group_id
                    ));
                    match ensure_replication_group_standby_target(
                        state,
                        &record.standby.target_group_id,
                        &record.standby.source_group_id,
                    ) {
                        Ok(true) => actions.push(format!(
                            "automatic_standby_failback_target:{}:{}",
                            record.standby.target_group_id, record.standby.source_group_id
                        )),
                        Ok(false) => {}
                        Err(error) => risks.push(format!(
                            "automatic standby failback target from {} to {} failed: {error}",
                            record.standby.target_group_id, record.standby.source_group_id
                        )),
                    }
                }
                Err(error) => risks.push(format!(
                    "automatic standby promotion for database {} failed: {error}",
                    database.name
                )),
            }
        }
    }
    Ok(())
}

fn replication_group_available(group: &ReplicationGroupRecord) -> bool {
    group.state == "active" && group.runtime.loaded && group.runtime.ready_for_linearizable_reads
}

fn best_promotable_standby_for_database(
    state: &LibsqlHttpState,
    conn: &Connection,
    database: &DatabaseCatalogRecord,
    source_group: &ReplicationGroupRecord,
) -> anyhow::Result<Option<DatabasePlacementStandbyRecord>> {
    let sql = format!(
        "select {DATABASE_STANDBY_SELECT_COLUMNS} from database_standby_copies where database_id = ? and source_group_id = ? order by refreshed_at_ms desc, target_group_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let standbys = stmt
        .query_map(
            params![&database.database_id, &source_group.group_id],
            database_placement_standby_record_from_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    for mut standby in standbys {
        annotate_standby_record(
            state,
            database,
            &mut standby,
            Some(source_group.failover_promote_after_ms),
        );
        if standby.promotable {
            return Ok(Some(standby));
        }
    }
    Ok(None)
}

fn automatic_placement_target_group(
    groups: &[ReplicationGroupRecord],
    source_group_id: &str,
) -> Option<String> {
    groups
        .iter()
        .filter(|group| {
            group.group_id != source_group_id
                && group.state == "active"
                && group.runtime.loaded
                && group.runtime.ready_for_linearizable_reads
        })
        .min_by_key(|group| group.group_id.as_str())
        .map(|group| group.group_id.clone())
}

fn standby_refresh_target_group_ids(
    conn: &Connection,
    target_groups: &[ReplicationGroupRecord],
    source_group_id: &str,
) -> anyhow::Result<Vec<String>> {
    let target_groups_by_id = target_groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.group_id.clone()))
        .collect::<HashMap<_, _>>();
    let explicit = list_replication_group_standby_targets(conn, source_group_id)?;
    Ok(explicit
        .into_iter()
        .filter_map(|target_group_id| target_groups_by_id.get(target_group_id.as_str()).cloned())
        .collect())
}

fn list_replication_group_standby_targets(
    conn: &Connection,
    source_group_id: &str,
) -> anyhow::Result<Vec<String>> {
    if !table_exists(conn, "replication_group_standby_targets")? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        r#"
        select target_group_id
        from replication_group_standby_targets
        where source_group_id = ?
        order by priority, target_group_id
        "#,
    )?;
    stmt.query_map([source_group_id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn ensure_replication_group_standby_target(
    state: &LibsqlHttpState,
    source_group_id: &str,
    target_group_id: &str,
) -> anyhow::Result<bool> {
    validate_replication_group_id(source_group_id)?;
    validate_replication_group_id(target_group_id)?;
    ensure!(
        source_group_id != target_group_id,
        "standby target group {target_group_id} must differ from source group {source_group_id}"
    );
    let conn = open_system_catalog_write_connection(state)?;
    require_database_catalog_schema(&conn, 7)?;
    if !table_exists(&conn, "replication_group_standby_targets")? {
        return Ok(false);
    }
    let exists = conn.query_row(
        r#"
        select 1
        from replication_group_standby_targets
        where source_group_id = ? and target_group_id = ?
        "#,
        params![source_group_id, target_group_id],
        |_| Ok(()),
    );
    match exists {
        Ok(()) => Ok(false),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            let priority = conn
                .query_row(
                    r#"
                    select coalesce(max(priority) + 1, 0)
                    from replication_group_standby_targets
                    where source_group_id = ?
                    "#,
                    [source_group_id],
                    |row| row.get::<_, i64>(0),
                )?
                .max(0);
            let now = sqlite_i64(current_time_millis());
            conn.execute(
                r#"
                insert into replication_group_standby_targets (
                    source_group_id, target_group_id, priority, created_at_ms, updated_at_ms
                )
                values (?, ?, ?, ?, ?)
                "#,
                params![source_group_id, target_group_id, priority, now, now],
            )?;
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

fn database_records_for_group(
    conn: &Connection,
    group_id: &str,
) -> anyhow::Result<Vec<DatabaseCatalogRecord>> {
    let sql = database_catalog_select_sql(
        conn,
        Some("database_catalog.state = 'ready' and database_replication_groups.group_id = ?"),
    )?;
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_map([group_id], database_catalog_record_from_row)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn advance_placement_operation(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    match operation.phase.as_str() {
        "planned" => advance_move_planned_to_fenced(state, operation),
        "fenced" => advance_move_fenced_to_cloning(state, operation),
        "cloning" => advance_move_cloning_to_catching_up(state, operation),
        "catching_up" => advance_move_catching_up_to_switching(state, operation),
        "switching" => advance_move_switching_to_completed(state, operation),
        phase => anyhow::bail!(
            "placement operation {} has unsupported running phase {phase}",
            operation.operation_id
        ),
    }
}

fn advance_move_planned_to_fenced(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    validate_move_operation_can_progress(state, operation, true)?;
    let active_sessions = state.active_database_sessions(&operation.database_name)?;
    ensure!(
        active_sessions == 0,
        "database {} has {active_sessions} active session(s); close sessions before placement move",
        operation.database_name
    );
    let _ = state.close_database_sessions(&operation.database_name);
    let _ = state.evict_database(&operation.database_name);
    let source_runtime = state
        .replication_groups
        .runtime(&operation.source_group_id)?;
    let source_watermark = source_runtime.durability_watermark()?;
    let conn = open_system_catalog_write_connection(state)?;
    record_placement_source_fence_watermark(
        &conn,
        &operation.operation_id,
        source_watermark.applied_index,
        source_watermark.applied_commit_ts,
        current_time_millis(),
    )?;
    update_placement_operation_phase(&conn, &operation.operation_id, "fenced", "running", None)?;
    Ok(format!(
        "placement_operation:{}:fenced",
        operation.operation_id
    ))
}

fn advance_move_fenced_to_cloning(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    validate_move_operation_can_progress(state, operation, true)?;
    let conn = open_system_catalog_write_connection(state)?;
    update_placement_operation_phase(&conn, &operation.operation_id, "cloning", "running", None)?;
    Ok(format!(
        "placement_operation:{}:cloning",
        operation.operation_id
    ))
}

fn advance_move_cloning_to_catching_up(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    validate_move_operation_can_progress(state, operation, true)?;
    clone_database_for_placement_move(state, operation)?;
    let target_runtime = state
        .replication_groups
        .runtime(&operation.target_group_id)?;
    let target_watermark = target_runtime.durability_watermark().with_context(|| {
        format!(
            "reading target durability watermark for placement move database {} in group {}",
            operation.database_name, operation.target_group_id
        )
    })?;
    let conn = open_system_catalog_write_connection(state)?;
    record_placement_target_clone_watermark(
        &conn,
        &operation.operation_id,
        target_watermark.applied_index,
        target_watermark
            .applied_commit_ts
            .or(operation.source_fence_commit_ts),
    )?;
    update_placement_operation_phase(
        &conn,
        &operation.operation_id,
        "catching_up",
        "running",
        None,
    )?;
    Ok(format!(
        "placement_operation:{}:catching_up",
        operation.operation_id
    ))
}

fn advance_move_catching_up_to_switching(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    validate_move_operation_can_progress(state, operation, false)?;
    validate_move_watermarks_before_switch(state, operation)?;
    let conn = open_system_catalog_write_connection(state)?;
    update_placement_operation_phase(&conn, &operation.operation_id, "switching", "running", None)?;
    Ok(format!(
        "placement_operation:{}:switching",
        operation.operation_id
    ))
}

fn advance_move_switching_to_completed(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<String> {
    validate_move_operation_can_progress(state, operation, false)?;
    let now = sqlite_i64(current_time_millis());
    let conn = open_system_catalog_write_connection(state)?;
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "update database_replication_groups set group_id = ?, updated_at_ms = ? where database_id = ?",
        params![operation.target_group_id, now, operation.database_id],
    )?;
    update_placement_operation_phase(&tx, &operation.operation_id, "completed", "completed", None)?;
    tx.commit()?;
    let _ = state.evict_database(&operation.database_name);
    Ok(format!(
        "placement_operation:{}:completed",
        operation.operation_id
    ))
}

fn validate_move_operation_can_progress(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
    require_source_runtime: bool,
) -> anyhow::Result<()> {
    ensure!(
        operation.operation == "move" && operation.status == "running",
        "placement operation {} is not a running move",
        operation.operation_id
    );
    let conn = open_system_catalog_write_connection(state)?;
    let database_record = read_database_catalog_record_from_conn(&conn, &operation.database_name)?
        .ok_or_else(|| anyhow!("database {} does not exist", operation.database_name))?;
    ensure!(
        database_record.database_id == operation.database_id,
        "database {} id changed during placement operation {}",
        operation.database_name,
        operation.operation_id
    );
    ensure!(
        database_record.state == "ready",
        "database {} is not ready for placement move; current state is {}",
        operation.database_name,
        database_record.state
    );
    ensure!(
        database_record.replication_group_id == operation.source_group_id,
        "database {} is no longer assigned to source replication group {}",
        operation.database_name,
        operation.source_group_id
    );
    let target_group = read_replication_group_record_from_conn(&conn, &operation.target_group_id)?
        .ok_or_else(|| {
            anyhow!(
                "replication group {} does not exist",
                operation.target_group_id
            )
        })?;
    ensure!(
        target_group.state == "active",
        "replication group {} is not active; current state is {}",
        target_group.group_id,
        target_group.state
    );
    if require_source_runtime {
        ensure!(
            state
                .replication_groups
                .contains(&operation.source_group_id)?,
            "source replication group {} is not loaded by this node",
            operation.source_group_id
        );
    }
    ensure!(
        state
            .replication_groups
            .contains(&operation.target_group_id)?,
        "target replication group {} is not loaded by this node",
        operation.target_group_id
    );
    if operation.transfer_kind.as_deref() == Some("checkpoint")
        && matches!(operation.phase.as_str(), "catching_up" | "switching")
    {
        let target_runtime = state
            .replication_groups
            .runtime(&operation.target_group_id)?;
        let target_metrics = target_runtime.metrics();
        ensure!(
            target_metrics.current_leader == Some(state.node_id),
            "placement operation {} checkpoint cutover must run on target leader for group {}, current leader is {:?}, local node is {}",
            operation.operation_id,
            operation.target_group_id,
            target_metrics.current_leader,
            state.node_id
        );
    }
    Ok(())
}

fn validate_move_watermarks_before_switch(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<()> {
    ensure!(
        operation.source_fence_observed_at_ms.is_some(),
        "placement operation {} has no recorded source fence observation",
        operation.operation_id
    );
    ensure!(
        operation.source_fence_applied_index.is_some(),
        "placement operation {} has no recorded source fence applied index",
        operation.operation_id
    );
    let target_runtime = state
        .replication_groups
        .runtime(&operation.target_group_id)?;
    let target_metrics = target_runtime.metrics();
    ensure!(
        target_metrics.is_ready_for_linearizable_reads(),
        "target replication group {} is not ready for linearizable reads",
        operation.target_group_id
    );

    let target_watermark = target_runtime.durability_watermark()?;
    let recorded_target_index = operation.target_clone_applied_index.ok_or_else(|| {
        anyhow!(
            "placement operation {} has no recorded target clone applied index",
            operation.operation_id
        )
    })?;
    let current_target_index = target_watermark.applied_index.ok_or_else(|| {
        anyhow!(
            "target replication group {} has no applied index",
            operation.target_group_id
        )
    })?;
    ensure!(
        current_target_index >= recorded_target_index,
        "target replication group {} applied index regressed from recorded clone index {} to {}",
        operation.target_group_id,
        recorded_target_index,
        current_target_index
    );

    if let Some(source_ts) = operation.source_fence_commit_ts {
        let target_ts = operation.target_clone_commit_ts.ok_or_else(|| {
            anyhow!(
                "placement operation {} has no target clone commit timestamp",
                operation.operation_id
            )
        })?;
        ensure!(
            target_ts >= source_ts,
            "target clone commit timestamp {:?} is behind source fence timestamp {:?}",
            target_ts,
            source_ts
        );
    }
    validate_placement_transfer_covers_source_fence(operation)?;
    validate_placement_transfer_voters_ready(state, operation)?;
    Ok(())
}

fn validate_placement_transfer_covers_source_fence(
    operation: &PlacementOperationRecord,
) -> anyhow::Result<()> {
    if operation.transfer_kind.as_deref() != Some("checkpoint")
        && operation.transfer_epoch_id.is_none()
    {
        return Ok(());
    }
    let source_fence_index = operation.source_fence_applied_index.ok_or_else(|| {
        anyhow!(
            "placement operation {} has no source fence applied index",
            operation.operation_id
        )
    })?;
    let transfer_source_index = operation.transfer_source_applied_index.ok_or_else(|| {
        anyhow!(
            "placement operation {} transfer has no source applied index",
            operation.operation_id
        )
    })?;
    ensure!(
        transfer_source_index >= source_fence_index,
        "placement operation {} transfer source index {} is behind source fence index {}",
        operation.operation_id,
        transfer_source_index,
        source_fence_index
    );
    if let Some(source_fence_ts) = operation.source_fence_commit_ts
        && let Some(transfer_source_ts) = operation.transfer_source_commit_ts
    {
        ensure!(
            transfer_source_ts >= source_fence_ts,
            "placement operation {} transfer timestamp {:?} is behind source fence timestamp {:?}",
            operation.operation_id,
            transfer_source_ts,
            source_fence_ts
        );
    }
    Ok(())
}

fn validate_placement_transfer_voters_ready(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<()> {
    if operation.transfer_kind.as_deref() != Some("checkpoint")
        && operation.transfer_epoch_id.is_none()
    {
        return Ok(());
    }
    let transfer_epoch_id = operation.transfer_epoch_id.as_deref().ok_or_else(|| {
        anyhow!(
            "placement operation {} has voter transfer without transfer epoch",
            operation.operation_id
        )
    })?;
    let conn = open_system_catalog_connection(state)?;
    require_database_catalog_schema(&conn, 10)?;
    let voters = list_replication_group_members_from_conn(&conn, &operation.target_group_id)?
        .into_iter()
        .filter(|member| member.role == "voter")
        .map(|member| member.node_id)
        .collect::<Vec<_>>();
    ensure!(
        !voters.is_empty(),
        "target replication group {} has no voters",
        operation.target_group_id
    );
    let ready = voters
        .iter()
        .map(|node_id| {
            let status = conn
                .query_row(
                    r#"
                    select status
                    from placement_transfer_voter_status
                    where operation_id = ?
                      and node_id = ?
                      and transfer_epoch_id = ?
                    "#,
                    params![
                        &operation.operation_id,
                        sqlite_i64(*node_id),
                        transfer_epoch_id,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            Ok((*node_id, status))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let not_ready = ready
        .into_iter()
        .filter_map(|(node_id, status)| (status.as_deref() != Some("ready")).then_some(node_id))
        .collect::<Vec<_>>();
    ensure!(
        not_ready.is_empty(),
        "placement operation {} transfer epoch {} is not ready on target voter(s): {:?}",
        operation.operation_id,
        transfer_epoch_id,
        not_ready
    );
    Ok(())
}

fn clone_database_for_placement_move(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<()> {
    let source_runtime = state
        .replication_groups
        .runtime(&operation.source_group_id)?;
    let target_runtime = state
        .replication_groups
        .runtime(&operation.target_group_id)?;
    if source_runtime.state_store_path() == target_runtime.state_store_path() {
        return Ok(());
    }
    let target_metrics = target_runtime.metrics();
    ensure!(
        target_metrics.current_leader == Some(state.node_id),
        "target replication group {} is led by node {:?}, not local node {}; placement clone must run on target leader",
        operation.target_group_id,
        target_metrics.current_leader,
        state.node_id
    );

    let target_catalog_voter_count = {
        let conn = open_system_catalog_connection(state)?;
        list_replication_group_members_from_conn(&conn, &operation.target_group_id)?
            .into_iter()
            .filter(|member| member.role == "voter")
            .count()
    };
    ensure!(
        target_catalog_voter_count >= 1,
        "target replication group {} has no catalog voter(s)",
        operation.target_group_id
    );

    state
        .placement_move_transfer_metrics
        .record_checkpoint_attempt();
    match clone_database_for_placement_move_from_checkpoint_all_voters(
        state,
        &source_runtime,
        &target_runtime,
        operation,
    ) {
        Ok(stats) => {
            state
                .placement_move_transfer_metrics
                .record_checkpoint_success(stats);
            Ok(())
        }
        Err(error) => {
            state
                .placement_move_transfer_metrics
                .record_checkpoint_failure();
            Err(error.context(format!(
                "placement move checkpoint clone for database {} from group {} into group {}",
                operation.database_name, operation.source_group_id, operation.target_group_id
            )))
        }
    }
}

fn clone_database_for_placement_move_from_checkpoint_all_voters(
    state: &LibsqlHttpState,
    source_runtime: &OrionSqliteRuntime,
    target_runtime: &OrionSqliteRuntime,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<StandbyCheckpointFetchStats> {
    let state = state.clone();
    let source_runtime = source_runtime.clone();
    let target_runtime = target_runtime.clone();
    let operation = operation.clone();
    run_async_from_sync(async move {
        clone_database_for_placement_move_from_checkpoint_all_voters_async(
            &state,
            &source_runtime,
            &target_runtime,
            &operation,
        )
        .await
    })
}

async fn clone_database_for_placement_move_from_checkpoint_all_voters_async(
    state: &LibsqlHttpState,
    source_runtime: &OrionSqliteRuntime,
    target_runtime: &OrionSqliteRuntime,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<StandbyCheckpointFetchStats> {
    let conn = open_system_catalog_connection(state)?;
    let target_voters =
        list_replication_group_members_from_conn(&conn, &operation.target_group_id)?
            .into_iter()
            .filter(|member| member.role == "voter")
            .collect::<Vec<_>>();
    ensure!(
        !target_voters.is_empty(),
        "target replication group {} has no voters",
        operation.target_group_id
    );
    let source_voters =
        list_replication_group_members_from_conn(&conn, &operation.source_group_id)?
            .into_iter()
            .filter(|member| member.role == "voter")
            .collect::<Vec<_>>();
    ensure!(
        !source_voters.is_empty(),
        "source replication group {} has no voters",
        operation.source_group_id
    );
    let transfer_epoch_id = operation
        .transfer_epoch_id
        .clone()
        .unwrap_or_else(|| format!("{}:checkpoint", operation.operation_id));
    let mut aggregate = StandbyCheckpointFetchStats::default();
    let ready_voters = {
        let conn = open_system_catalog_connection(state)?;
        let ready = ready_placement_transfer_voter_responses(
            &conn,
            &operation.operation_id,
            &transfer_epoch_id,
        )?;
        for response in &ready {
            aggregate.add_checkpoint_response(response);
        }
        ready
            .into_iter()
            .map(|response| response.node_id)
            .collect::<HashSet<_>>()
    };
    let pending_voters = target_voters
        .iter()
        .filter(|member| !ready_voters.contains(&member.node_id))
        .collect::<Vec<_>>();
    if pending_voters.is_empty() {
        return Ok(aggregate);
    }
    let headers = state.internal_system_admin_headers();
    let mut source_by_target = BTreeMap::new();
    for target in &pending_voters {
        let source_node_id =
            select_checkpoint_source_for_target(&source_voters, target, state.node_id)?;
        source_by_target.insert(target.node_id, source_node_id);
    }
    let mut selected_source_node_ids = source_by_target.values().copied().collect::<Vec<_>>();
    selected_source_node_ids.sort_unstable();
    selected_source_node_ids.dedup();

    let mut checkpoints_by_source = HashMap::new();
    for source_node_id in selected_source_node_ids {
        let checkpoint = placement_move_checkpoint_for_source(
            state,
            &headers,
            source_runtime,
            operation,
            source_node_id,
        )
        .await
        .with_context(|| {
            format!(
                "creating placement move checkpoint for source node {} and database {}",
                source_node_id, operation.database_name
            )
        })?;
        checkpoints_by_source.insert(source_node_id, checkpoint);
    }
    let primary_checkpoint = source_by_target
        .get(&state.node_id)
        .and_then(|source_node_id| checkpoints_by_source.get(source_node_id))
        .or_else(|| checkpoints_by_source.values().next())
        .context("placement checkpoint source selection produced no checkpoints")?;
    {
        let conn = open_system_catalog_write_connection(state)?;
        record_placement_checkpoint_transfer_epoch(
            &conn,
            &operation.operation_id,
            &transfer_epoch_id,
            primary_checkpoint,
        )?;
    }

    let mut voter_tasks = Vec::new();
    for target in pending_voters {
        let source_node_id = source_by_target
            .get(&target.node_id)
            .copied()
            .context("missing selected checkpoint source for target voter")?;
        let checkpoint = checkpoints_by_source
            .get(&source_node_id)
            .cloned()
            .context("missing checkpoint for selected source voter")?;
        if target.node_id == state.node_id {
            let source_endpoint = if source_node_id == state.node_id {
                None
            } else {
                Some(checkpoint_source_endpoint_for_node(state, source_node_id)?)
            };
            voter_tasks.push(tokio::spawn(materialize_checkpoint_on_local_voter(
                state.clone(),
                headers.clone(),
                source_endpoint,
                target_runtime.clone(),
                operation.clone(),
                checkpoint,
                state.node_id,
            )));
        } else {
            let endpoint = http_endpoint_for_node(state, target.node_id).ok_or_else(|| {
                anyhow!(
                    "target voter node {} has no configured HTTP endpoint for checkpoint materialization",
                    target.node_id
                )
            })?;
            let source_endpoint = checkpoint_source_endpoint_for_node(state, source_node_id)?;
            voter_tasks.push(tokio::spawn(materialize_checkpoint_on_remote_voter(
                state.clone(),
                headers.clone(),
                endpoint,
                operation.database_name.clone(),
                PlacementCheckpointMaterializeRequest {
                    operation_id: operation.operation_id.clone(),
                    target_group_id: operation.target_group_id.clone(),
                    source_endpoint,
                    checkpoint,
                },
            )));
        }
    }
    for result in futures_util::future::join_all(voter_tasks).await {
        let response = result.context("joining placement checkpoint materialization task")??;
        let conn = open_system_catalog_write_connection(state)?;
        record_placement_transfer_voter_ready(&conn, &response, Some(&transfer_epoch_id))?;
        aggregate.add_checkpoint_response(&response);
    }

    Ok(aggregate)
}

fn select_checkpoint_source_for_target(
    source_voters: &[ReplicationGroupMemberRecord],
    target: &ReplicationGroupMemberRecord,
    preferred_node_id: u64,
) -> anyhow::Result<u64> {
    if source_voters
        .iter()
        .any(|source| source.node_id == target.node_id)
    {
        return Ok(target.node_id);
    }
    if let Some(source) = source_voters
        .iter()
        .filter(|source| same_cloud_region(source, target))
        .min_by_key(|source| {
            (
                source.node_id != preferred_node_id,
                source.priority,
                source.node_id,
            )
        })
    {
        return Ok(source.node_id);
    }
    if source_voters
        .iter()
        .any(|source| source.node_id == preferred_node_id)
    {
        return Ok(preferred_node_id);
    }
    source_voters
        .iter()
        .min_by_key(|source| (source.priority, source.node_id))
        .map(|source| source.node_id)
        .context("source replication group has no checkpoint-capable voters")
}

fn same_cloud_region(
    left: &ReplicationGroupMemberRecord,
    right: &ReplicationGroupMemberRecord,
) -> bool {
    left.cloud == right.cloud && left.region == right.region
}

async fn placement_move_checkpoint_for_source(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    local_source_runtime: &OrionSqliteRuntime,
    operation: &PlacementOperationRecord,
    source_node_id: u64,
) -> anyhow::Result<StandbyCheckpointExport> {
    let checkpoint = if source_node_id == state.node_id {
        local_placement_move_checkpoint(local_source_runtime, operation).await?
    } else {
        let source_endpoint = http_endpoint_for_node(state, source_node_id).ok_or_else(|| {
            anyhow!(
                "source voter node {} has no configured HTTP endpoint for checkpoint export",
                source_node_id
            )
        })?;
        fetch_standby_checkpoint_from_peer(
            state,
            headers,
            &source_endpoint,
            &operation.database_name,
            &operation.source_group_id,
        )
        .await?
    };
    ensure_checkpoint_covers_placement_fence(&checkpoint, operation)?;
    Ok(checkpoint)
}

async fn local_placement_move_checkpoint(
    source_runtime: &OrionSqliteRuntime,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<StandbyCheckpointExport> {
    let source_watermark = source_runtime
        .durability_watermark()
        .context("reading source durability watermark for placement move checkpoint")?;
    let artifact = source_runtime
        .database_checkpoint_artifact(
            &operation.database_name,
            format!(
                "placement-move-checkpoint-{}-{}",
                operation.database_name,
                source_watermark.applied_index.unwrap_or_default()
            ),
        )
        .with_context(|| {
            format!(
                "creating placement move checkpoint artifact for database {}",
                operation.database_name
            )
        })?;
    let objects = list_checkpoint_objects(&source_runtime.state_store().object_store(), &artifact)
        .await
        .with_context(|| {
            format!(
                "listing placement move checkpoint objects for database {}",
                operation.database_name
            )
        })?;
    Ok(StandbyCheckpointExport {
        source_group_id: operation.source_group_id.clone(),
        source_watermark,
        artifact,
        objects,
    })
}

fn ensure_checkpoint_covers_placement_fence(
    checkpoint: &StandbyCheckpointExport,
    operation: &PlacementOperationRecord,
) -> anyhow::Result<()> {
    if let Some(required_index) = operation.source_fence_applied_index {
        let checkpoint_index = checkpoint
            .source_watermark
            .applied_index
            .unwrap_or_default();
        ensure!(
            checkpoint_index >= required_index,
            "checkpoint for placement operation {} is behind source fence: checkpoint applied index {}, required {}",
            operation.operation_id,
            checkpoint_index,
            required_index
        );
    }
    Ok(())
}

async fn materialize_checkpoint_on_local_voter(
    state: LibsqlHttpState,
    headers: HeaderMap,
    source_endpoint: Option<String>,
    target_runtime: OrionSqliteRuntime,
    operation: PlacementOperationRecord,
    checkpoint: StandbyCheckpointExport,
    node_id: u64,
) -> anyhow::Result<PlacementCheckpointMaterializeResponse> {
    ensure_checkpoint_covers_placement_fence(&checkpoint, &operation)?;
    let fetch_stats = if let Some(source_endpoint) = source_endpoint {
        fetch_missing_checkpoint_objects_from_peer(
            &state,
            &headers,
            &source_endpoint,
            &operation.database_name,
            &operation.source_group_id,
            &target_runtime,
            &checkpoint,
        )
        .await?
    } else {
        StandbyCheckpointFetchStats {
            objects_seen: checkpoint.objects.len() as u64,
            objects_copied: 0,
            objects_reused: checkpoint.objects.len() as u64,
            bytes_seen: checkpoint.objects.iter().map(|object| object.size).sum(),
            bytes_copied: 0,
        }
    };
    let operation_id = operation.operation_id.clone();
    let database_name = operation.database_name.clone();
    let target_group_id = operation.target_group_id.clone();
    let clone_database_name = database_name.clone();
    let clone_target_group_id = target_group_id.clone();
    let artifact = checkpoint.artifact.clone();
    let target_watermark = tokio::task::spawn_blocking(move || {
        target_runtime
            .clone_database_checkpoint_from_local_objects(&clone_database_name, &artifact)
            .with_context(|| {
                format!(
                    "opening local placement checkpoint clone for database {} into target group {}",
                    clone_database_name, clone_target_group_id
                )
            })?;
        target_runtime_mark_ready_and_verify(
            &target_runtime,
            &clone_database_name,
            &clone_target_group_id,
        )?;
        target_runtime
            .durability_watermark()
            .context("reading target durability watermark after local checkpoint materialization")
    })
    .await
    .context("joining local placement checkpoint materialization task")??;
    Ok(PlacementCheckpointMaterializeResponse {
        operation_id,
        node_id,
        target_group_id,
        target_applied_index: target_watermark.applied_index,
        target_commit_ts: target_watermark.applied_commit_ts,
        checkpoint_objects_seen: fetch_stats.objects_seen,
        checkpoint_objects_copied: fetch_stats.objects_copied,
        checkpoint_objects_reused: fetch_stats.objects_reused,
        checkpoint_bytes_seen: fetch_stats.bytes_seen,
        checkpoint_bytes_copied: fetch_stats.bytes_copied,
    })
}

fn checkpoint_source_endpoint_for_node(
    state: &LibsqlHttpState,
    node_id: u64,
) -> anyhow::Result<String> {
    if node_id == state.node_id {
        externally_reachable_http_endpoint_for_node(state, node_id).ok_or_else(|| {
            anyhow!(
                "node {} has no configured HTTP endpoint for checkpoint serving",
                node_id
            )
        })
    } else {
        http_endpoint_for_node(state, node_id).ok_or_else(|| {
            anyhow!(
                "source voter node {} has no configured HTTP endpoint for checkpoint serving",
                node_id
            )
        })
    }
}

fn externally_reachable_http_endpoint_for_node(
    state: &LibsqlHttpState,
    node_id: u64,
) -> Option<String> {
    let endpoint = http_endpoint_for_node(state, node_id)?;
    if node_id != state.node_id || !endpoint.contains("0.0.0.0") {
        return Some(endpoint);
    }
    let node = state.placement_nodes.get(&node_id)?;
    let url = reqwest::Url::parse(&endpoint).ok()?;
    let port = url.port_or_known_default()?;
    let (host, _) = node.raft_addr.rsplit_once(':')?;
    Some(format!("{}://{}:{}", url.scheme(), host, port))
}

async fn materialize_checkpoint_on_remote_voter(
    state: LibsqlHttpState,
    headers: HeaderMap,
    endpoint: String,
    database: String,
    request: PlacementCheckpointMaterializeRequest,
) -> anyhow::Result<PlacementCheckpointMaterializeResponse> {
    let url = format!(
        "{}/_orion/internal/databases/{}/placement/checkpoint/materialize",
        endpoint.trim_end_matches('/'),
        database
    );
    let mut builder = state.http_client.post(url).json(&request);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .with_context(|| format!("requesting checkpoint materialization from {endpoint}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading checkpoint materialization response from {endpoint}"))?;
    if !status.is_success() {
        anyhow::bail!("checkpoint materialization HTTP {status} from {endpoint}: {body}");
    }
    serde_json::from_str(&body)
        .with_context(|| format!("decoding checkpoint materialization response from {endpoint}"))
}

fn fail_placement_operation(
    state: &LibsqlHttpState,
    operation: &PlacementOperationRecord,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let conn = open_system_catalog_connection(state)?;
    update_placement_operation_phase(
        &conn,
        &operation.operation_id,
        "failed",
        "failed",
        Some(&error_chain_message(error)),
    )
}

fn planned_group_members(
    state: &LibsqlHttpState,
    group_id: &str,
) -> Vec<ReplicationGroupMemberRecord> {
    let now = current_time_millis();
    state
        .placement_nodes
        .values()
        .enumerate()
        .map(|(priority, node)| ReplicationGroupMemberRecord {
            group_id: group_id.to_string(),
            node_id: node.node_id,
            role: "voter".to_string(),
            cloud: node.cloud.clone(),
            region: node.region.clone(),
            zone: node.zone.clone(),
            priority: priority as u64,
            created_at_ms: now,
            updated_at_ms: now,
        })
        .collect()
}

fn open_system_catalog_connection(state: &LibsqlHttpState) -> anyhow::Result<Connection> {
    match open_system_catalog_connection_once(state) {
        Ok(conn) => Ok(conn),
        Err(error) if is_sqlite_not_database_error(&error) => {
            let _ = state.evict_database(ORION_CATALOG_DATABASE);
            open_system_catalog_connection_once(state)
                .context("reopening Orion database catalog after evicting local handles")
        }
        Err(error) => Err(error),
    }
}

fn open_system_catalog_connection_once(state: &LibsqlHttpState) -> anyhow::Result<Connection> {
    let catalog_db = state.database(ORION_CATALOG_DATABASE)?;
    let conn = catalog_db.connect()?;
    if !state
        .replication_groups
        .default_runtime()?
        .metrics()
        .is_leader()
        && read_catalog_schema_version(&conn)?.is_none()
        && !table_exists(&conn, "database_catalog")?
    {
        anyhow::bail!("Orion database catalog schema is not initialized on the current leader");
    }
    ensure_database_catalog_schema(&conn)?;
    state
        .replication_groups
        .default_runtime()?
        .mark_database_ready(ORION_CATALOG_DATABASE)?;
    Ok(conn)
}

fn open_system_catalog_write_connection(state: &LibsqlHttpState) -> anyhow::Result<Connection> {
    let conn = open_system_catalog_connection(state)?;
    activate_database_catalog_schema_from_conn(&conn, DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION)?;
    state
        .replication_groups
        .default_runtime()?
        .mark_database_ready(ORION_CATALOG_DATABASE)?;
    Ok(conn)
}

fn initialize_system_catalog_for_service(state: &LibsqlHttpState) -> anyhow::Result<()> {
    if state
        .replication_groups
        .default_runtime()?
        .metrics()
        .is_leader()
    {
        let _ = open_system_catalog_write_connection(state)?;
    }
    Ok(())
}

pub fn list_runtime_group_catalog_snapshots(
    runtime: &OrionSqliteRuntime,
) -> anyhow::Result<Vec<RuntimeGroupCatalogSnapshot>> {
    if !runtime.database_ready(ORION_CATALOG_DATABASE)? {
        return Ok(Vec::new());
    }
    let Some(catalog_db) = runtime.open_existing_database(ORION_CATALOG_DATABASE)? else {
        return Ok(Vec::new());
    };
    let conn = catalog_db.connect_read_only()?;
    require_database_catalog_schema(&conn, 5)?;
    let mut groups_stmt = conn.prepare(
        r#"
        select group_id, state
        from replication_groups
        where state in ('active', 'draining')
        order by group_id
        "#,
    )?;
    let groups = groups_stmt
        .query_map([], |row| {
            Ok(RuntimeGroupCatalogSnapshot {
                group_id: row.get(0)?,
                state: row.get(1)?,
                members: Vec::new(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(groups_stmt);

    groups
        .into_iter()
        .map(|mut group| {
            let mut members_stmt = conn.prepare(
                r#"
                select node_id, role
                from replication_group_members
                where group_id = ?
                order by priority, node_id, role
                "#,
            )?;
            group.members = members_stmt
                .query_map([&group.group_id], |row| {
                    Ok(RuntimeGroupMemberSnapshot {
                        node_id: row.get::<_, i64>(0)?.max(0) as u64,
                        role: row.get(1)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(group)
        })
        .collect()
}

#[cfg(test)]
pub fn ensure_database_catalog_schema_for_runtime(
    runtime: &OrionSqliteRuntime,
) -> anyhow::Result<()> {
    let catalog_db = runtime.open_database(ORION_CATALOG_DATABASE)?;
    let conn = catalog_db.connect()?;
    ensure_database_catalog_schema(&conn)?;
    runtime.mark_database_ready(ORION_CATALOG_DATABASE)
}

fn is_sqlite_not_database_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("file is not a database")
        || message.contains("database disk image is malformed")
}

fn ensure_database_catalog_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table if not exists catalog_meta (
            key text primary key,
            value text not null,
            updated_at_ms integer not null
        );
        "#,
    )?;
    if read_catalog_schema_version(conn)?.is_none() && !table_exists(conn, "database_catalog")? {
        migrate_database_catalog_schema(conn, 0, DATABASE_CATALOG_BOOTSTRAP_SCHEMA_VERSION)?;
    }
    let version =
        read_catalog_schema_version(conn)?.unwrap_or(infer_database_catalog_schema_version(conn)?);
    ensure!(
        version <= DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION,
        "Orion database catalog schema version {version} is newer than this binary can read ({DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION})"
    );
    Ok(())
}

fn activate_database_catalog_schema(
    state: &LibsqlHttpState,
    target_version: u32,
) -> anyhow::Result<u32> {
    let conn = open_system_catalog_connection(state)?;
    activate_database_catalog_schema_from_conn(&conn, target_version)
}

fn activate_database_catalog_schema_from_conn(
    conn: &Connection,
    target_version: u32,
) -> anyhow::Result<u32> {
    ensure!(
        target_version <= DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION,
        "requested catalog schema version {target_version} is newer than this binary can write ({DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION})"
    );
    let version =
        read_catalog_schema_version(conn)?.unwrap_or(infer_database_catalog_schema_version(conn)?);
    ensure!(
        version <= DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION,
        "Orion database catalog schema version {version} is newer than this binary can read ({DATABASE_CATALOG_MAX_READ_SCHEMA_VERSION})"
    );
    ensure!(
        target_version >= version,
        "cannot activate catalog schema version {target_version}; current version is {version}"
    );
    migrate_database_catalog_schema(conn, version, target_version)?;
    Ok(target_version)
}

fn migrate_database_catalog_schema(
    conn: &Connection,
    from_version: u32,
    target_version: u32,
) -> anyhow::Result<()> {
    let mut version = from_version;
    if version < 1 && target_version >= 1 {
        conn.execute_batch(
            r#"
            create table if not exists database_catalog (
                name text primary key,
                state text not null check (state in ('creating', 'ready', 'deleting', 'deleted', 'failed')),
                object_prefix text not null,
                generation integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                deleted_at_ms integer,
                error text
            );
            create index if not exists database_catalog_state_idx on database_catalog(state);
            "#,
        )?;
        write_catalog_schema_version(conn, 1)?;
        version = 1;
    }
    if version < 2 && target_version >= 2 {
        conn.execute_batch(
            r#"
            create table if not exists database_lifecycle_idempotency (
                key text primary key,
                operation text not null,
                database text not null,
                request_hash text not null,
                status text not null check (status in ('pending', 'committed')),
                response_status integer not null default 0,
                response_json text not null default '{}',
                created_at_ms integer not null,
                updated_at_ms integer not null
            );
            create index if not exists database_lifecycle_idempotency_database_idx
                on database_lifecycle_idempotency(database);
            "#,
        )?;
        write_catalog_schema_version(conn, 2)?;
        version = 2;
    }
    if version < 3 && target_version >= 3 {
        if !table_column_exists(conn, "database_catalog", "purged_at_ms")? {
            conn.execute(
                "alter table database_catalog add column purged_at_ms integer",
                [],
            )?;
        }
        if !table_column_exists(conn, "database_catalog", "purge_error")? {
            conn.execute(
                "alter table database_catalog add column purge_error text",
                [],
            )?;
        }
        write_catalog_schema_version(conn, 3)?;
        version = 3;
    }
    if version < 4 && target_version >= 4 {
        if !table_column_exists(conn, "database_catalog", "database_id")? {
            conn.execute(
                "alter table database_catalog add column database_id text",
                [],
            )?;
        }
        backfill_database_ids(conn)?;
        conn.execute(
            "create unique index if not exists database_catalog_database_id_idx on database_catalog(database_id)",
            [],
        )?;
        conn.execute_batch(
            r#"
            create table if not exists replication_groups (
                group_id text primary key,
                state text not null check (state in ('creating', 'active', 'draining', 'deleted', 'failed')),
                placement_mode text not null,
                object_prefix text not null,
                write_home_cloud text,
                write_home_region text,
                write_home_zone text,
                compaction_owner_node_id integer,
                failover_automatic integer not null,
                failover_promote_after_ms integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                error text
            );
            create table if not exists replication_group_members (
                group_id text not null,
                node_id integer not null,
                role text not null check (role in ('voter', 'learner', 'read_replica')),
                cloud text not null,
                region text not null,
                zone text not null,
                priority integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                primary key (group_id, node_id, role)
            );
            create table if not exists database_replication_groups (
                database_id text primary key,
                database_name text not null unique,
                group_id text not null,
                role text not null check (role in ('primary')),
                created_at_ms integer not null,
                updated_at_ms integer not null
            );
            create index if not exists database_replication_groups_group_idx
                on database_replication_groups(group_id);
            "#,
        )?;
        ensure_default_replication_group_catalog(conn)?;
        backfill_database_replication_groups(conn)?;
        write_catalog_schema_version(conn, 4)?;
        version = 4;
    }
    if version < 5 && target_version >= 5 {
        conn.execute_batch(
            r#"
            create table if not exists placement_operations (
                operation_id text primary key,
                database_id text not null,
                database_name text not null,
                operation text not null check (operation in ('move')),
                status text not null check (status in ('running', 'completed', 'failed')),
                phase text not null check (phase in ('planned', 'fenced', 'cloning', 'catching_up', 'switching', 'completed', 'failed')),
                source_group_id text not null,
                target_group_id text not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                completed_at_ms integer,
                error text
            );
            create index if not exists placement_operations_database_idx
                on placement_operations(database_id, created_at_ms);
            create index if not exists placement_operations_status_idx
                on placement_operations(status, updated_at_ms);
            "#,
        )?;
        write_catalog_schema_version(conn, 5)?;
        version = 5;
    }
    if version < 6 && target_version >= 6 {
        add_column_if_missing(
            conn,
            "placement_operations",
            "source_fence_applied_index",
            "alter table placement_operations add column source_fence_applied_index integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "source_fence_commit_ts_physical_ms",
            "alter table placement_operations add column source_fence_commit_ts_physical_ms integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "source_fence_commit_ts_logical",
            "alter table placement_operations add column source_fence_commit_ts_logical integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "source_fence_observed_at_ms",
            "alter table placement_operations add column source_fence_observed_at_ms integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "target_clone_applied_index",
            "alter table placement_operations add column target_clone_applied_index integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "target_clone_commit_ts_physical_ms",
            "alter table placement_operations add column target_clone_commit_ts_physical_ms integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "target_clone_commit_ts_logical",
            "alter table placement_operations add column target_clone_commit_ts_logical integer",
        )?;
        write_catalog_schema_version(conn, 6)?;
        version = 6;
    }
    if version < 7 && target_version >= 7 {
        conn.execute_batch(
            r#"
            create table if not exists database_standby_copies (
                database_id text not null,
                database_name text not null,
                source_group_id text not null,
                target_group_id text not null,
                source_applied_index integer,
                source_commit_ts_physical_ms integer,
                source_commit_ts_logical integer,
                target_applied_index integer,
                target_commit_ts_physical_ms integer,
                target_commit_ts_logical integer,
                refreshed_at_ms integer not null,
                updated_at_ms integer not null,
                error text,
                primary key (database_id, target_group_id)
            );
            create index if not exists database_standby_copies_target_idx
                on database_standby_copies(target_group_id, refreshed_at_ms);
            "#,
        )?;
        write_catalog_schema_version(conn, 7)?;
        version = 7;
    }
    if version < 8 && target_version >= 8 {
        conn.execute_batch(
            r#"
            create table if not exists replication_group_standby_targets (
                source_group_id text not null,
                target_group_id text not null,
                priority integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                primary key (source_group_id, target_group_id)
            );
            create index if not exists replication_group_standby_targets_source_priority_idx
                on replication_group_standby_targets(source_group_id, priority, target_group_id);
            "#,
        )?;
        write_catalog_schema_version(conn, 8)?;
        version = 8;
    }
    if version < 9 && target_version >= 9 {
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_epoch_id",
            "alter table placement_operations add column transfer_epoch_id text",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_kind",
            "alter table placement_operations add column transfer_kind text",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_checkpoint_artifact_json",
            "alter table placement_operations add column transfer_checkpoint_artifact_json text",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_source_applied_index",
            "alter table placement_operations add column transfer_source_applied_index integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_source_commit_ts_physical_ms",
            "alter table placement_operations add column transfer_source_commit_ts_physical_ms integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_source_commit_ts_logical",
            "alter table placement_operations add column transfer_source_commit_ts_logical integer",
        )?;
        add_column_if_missing(
            conn,
            "placement_operations",
            "transfer_created_at_ms",
            "alter table placement_operations add column transfer_created_at_ms integer",
        )?;
        write_catalog_schema_version(conn, 9)?;
        version = 9;
    }
    if version < 10 && target_version >= 10 {
        conn.execute_batch(
            r#"
            create table if not exists placement_transfer_voter_status (
                operation_id text not null,
                node_id integer not null,
                target_group_id text not null,
                transfer_epoch_id text,
                status text not null check (status in ('pending', 'ready', 'failed')),
                target_applied_index integer,
                target_commit_ts_physical_ms integer,
                target_commit_ts_logical integer,
                checkpoint_objects_seen integer not null default 0,
                checkpoint_objects_copied integer not null default 0,
                checkpoint_objects_reused integer not null default 0,
                checkpoint_bytes_seen integer not null default 0,
                checkpoint_bytes_copied integer not null default 0,
                error text,
                updated_at_ms integer not null,
                primary key (operation_id, node_id)
            );
            create index if not exists placement_transfer_voter_status_operation_idx
                on placement_transfer_voter_status(operation_id, status, node_id);
            "#,
        )?;
        write_catalog_schema_version(conn, 10)?;
        version = 10;
    }
    ensure!(
        version == target_version,
        "Orion database catalog schema migration stopped at version {version}, expected {target_version}"
    );
    Ok(())
}

fn read_catalog_schema_version(conn: &Connection) -> anyhow::Result<Option<u32>> {
    if !table_exists(conn, "catalog_meta")? {
        return Ok(None);
    }
    conn.query_row(
        "select value from catalog_meta where key = 'schema_version'",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()?
    .map(|value| {
        value
            .parse::<u32>()
            .with_context(|| format!("invalid database catalog schema_version {value:?}"))
    })
    .transpose()
}

fn backfill_database_ids(conn: &Connection) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "select name from database_catalog where database_id is null or database_id = '' order by name",
    )?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    for name in names {
        conn.execute(
            "update database_catalog set database_id = ? where name = ?",
            params![database_id_from_name(&name), name],
        )?;
    }
    Ok(())
}

fn ensure_default_replication_group_catalog(conn: &Connection) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into replication_groups (
            group_id, state, placement_mode, object_prefix,
            write_home_cloud, write_home_region, write_home_zone,
            compaction_owner_node_id, failover_automatic, failover_promote_after_ms,
            created_at_ms, updated_at_ms, error
        )
        values (?, 'active', 'default', ?, null, null, null, null, 1, ?, ?, ?, null)
        on conflict(group_id) do nothing
        "#,
        params![
            DEFAULT_REPLICATION_GROUP_ID,
            replication_group_object_prefix(DEFAULT_REPLICATION_GROUP_ID),
            sqlite_i64(default_failover_promote_after_ms()),
            now,
            now
        ],
    )?;
    Ok(())
}

fn upsert_replication_group_standby_targets(
    conn: &Connection,
    source_group_id: &str,
    target_group_ids: &[String],
) -> anyhow::Result<()> {
    if !table_exists(conn, "replication_group_standby_targets")? {
        return Ok(());
    }
    validate_replication_group_id(source_group_id)?;
    let now = sqlite_i64(current_time_millis());
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "delete from replication_group_standby_targets where source_group_id = ?",
        [source_group_id],
    )?;
    let mut seen = HashSet::new();
    let mut insert = tx.prepare(
        r#"
        insert into replication_group_standby_targets (
            source_group_id, target_group_id, priority, created_at_ms, updated_at_ms
        )
        values (?, ?, ?, ?, ?)
        "#,
    )?;
    for (priority, target_group_id) in target_group_ids.iter().enumerate() {
        validate_replication_group_id(target_group_id)?;
        ensure!(
            target_group_id != source_group_id,
            "standby target group {target_group_id} must differ from source group {source_group_id}"
        );
        if !seen.insert(target_group_id) {
            continue;
        }
        insert.execute(params![
            source_group_id,
            target_group_id,
            sqlite_usize(priority),
            now,
            now
        ])?;
    }
    drop(insert);
    tx.commit()?;
    Ok(())
}

fn backfill_database_replication_groups(conn: &Connection) -> anyhow::Result<()> {
    let mut stmt = conn
        .prepare("select database_id, name, created_at_ms, updated_at_ms from database_catalog")?;
    let records = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    for (database_id, name, created_at_ms, updated_at_ms) in records {
        conn.execute(
            r#"
            insert into database_replication_groups (
                database_id, database_name, group_id, role, created_at_ms, updated_at_ms
            )
            values (?, ?, ?, 'primary', ?, ?)
            on conflict(database_id) do update set
                database_name = excluded.database_name,
                group_id = excluded.group_id,
                role = excluded.role,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![
                database_id,
                name,
                DEFAULT_REPLICATION_GROUP_ID,
                created_at_ms,
                updated_at_ms
            ],
        )?;
    }
    Ok(())
}

fn require_database_catalog_schema(conn: &Connection, min_version: u32) -> anyhow::Result<()> {
    let version =
        read_catalog_schema_version(conn)?.unwrap_or(infer_database_catalog_schema_version(conn)?);
    ensure!(
        version >= min_version,
        "Orion database catalog schema version {version} does not support this operation; activate schema version {min_version} first"
    );
    Ok(())
}

fn write_catalog_schema_version(conn: &Connection, version: u32) -> anyhow::Result<()> {
    conn.execute(
        r#"
        insert into catalog_meta (key, value, updated_at_ms)
        values ('schema_version', ?, ?)
        on conflict(key) do update set
            value = excluded.value,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![version.to_string(), sqlite_i64(current_time_millis())],
    )?;
    Ok(())
}

fn infer_database_catalog_schema_version(conn: &Connection) -> anyhow::Result<u32> {
    if !table_exists(conn, "database_catalog")? {
        return Ok(0);
    }
    if table_column_exists(conn, "database_catalog", "database_id")?
        && table_exists(conn, "replication_groups")?
        && table_exists(conn, "replication_group_members")?
        && table_exists(conn, "database_replication_groups")?
    {
        if table_exists(conn, "placement_transfer_voter_status")? {
            return Ok(10);
        }
        if table_exists(conn, "placement_operations")? {
            if table_column_exists(conn, "placement_operations", "transfer_epoch_id")? {
                return Ok(9);
            }
            if table_exists(conn, "replication_group_standby_targets")? {
                return Ok(8);
            }
            if table_exists(conn, "database_standby_copies")? {
                return Ok(7);
            }
            if table_column_exists(conn, "placement_operations", "source_fence_applied_index")? {
                return Ok(6);
            }
            return Ok(5);
        }
        return Ok(4);
    }
    if table_column_exists(conn, "database_catalog", "purged_at_ms")?
        && table_column_exists(conn, "database_catalog", "purge_error")?
        && table_exists(conn, LIFECYCLE_IDEMPOTENCY_TABLE)?
    {
        return Ok(3);
    }
    if table_exists(conn, LIFECYCLE_IDEMPOTENCY_TABLE)? {
        return Ok(2);
    }
    Ok(1)
}

fn table_exists(conn: &Connection, table: &str) -> anyhow::Result<bool> {
    Ok(conn
        .query_row(
            "select 1 from sqlite_master where type = 'table' and name = ?",
            [table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn table_column_exists(conn: &Connection, table: &str, column: &str) -> anyhow::Result<bool> {
    let mut stmt = conn.prepare(&format!("pragma table_info({})", quote_sqlite_ident(table)))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    alter_sql: &str,
) -> anyhow::Result<()> {
    if !table_column_exists(conn, table, column)? {
        conn.execute(alter_sql, [])?;
    }
    Ok(())
}

fn quote_sqlite_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn new_database_id(name: &str) -> String {
    let now = current_time_millis();
    let digest = Sha256::digest(format!("{name}:{now}").as_bytes());
    format!("db_{}_{}", hex_lower(&digest[..8]), now)
}

fn database_id_from_name(name: &str) -> String {
    let digest = Sha256::digest(name.as_bytes());
    format!("db_legacy_{}", hex_lower(&digest[..8]))
}

fn replication_group_object_prefix(group_id: &str) -> String {
    format!("replication-groups/{group_id}")
}

fn database_object_prefix(group_id: &str, database_id: &str) -> String {
    format!(
        "{}/databases/{database_id}",
        replication_group_object_prefix(group_id)
    )
}

fn read_lifecycle_idempotency_record(
    conn: &Connection,
    key: &str,
) -> anyhow::Result<Option<StoredLifecycleIdempotencyRecord>> {
    Ok(conn
        .query_row(
            &format!(
                "select operation, database, request_hash, status, response_status, response_json \
                 from {LIFECYCLE_IDEMPOTENCY_TABLE} where key = ?"
            ),
            [key],
            |row| {
                Ok(StoredLifecycleIdempotencyRecord {
                    operation: row.get(0)?,
                    database: row.get(1)?,
                    request_hash: row.get(2)?,
                    status: row.get(3)?,
                    response_status: row.get::<_, i64>(4)?.max(0) as u16,
                    response_json: row.get(5)?,
                })
            },
        )
        .optional()?)
}

fn insert_pending_lifecycle_idempotency_record(
    conn: &Connection,
    idempotency: &LifecycleIdempotencyContext,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        &format!(
            "insert into {LIFECYCLE_IDEMPOTENCY_TABLE} \
             (key, operation, database, request_hash, status, response_status, response_json, created_at_ms, updated_at_ms) \
             values (?, ?, ?, ?, 'pending', 0, '{{}}', ?, ?)"
        ),
        params![
            &idempotency.key,
            &idempotency.operation,
            &idempotency.database,
            &idempotency.request_hash,
            now,
            now
        ],
    )?;
    Ok(())
}

fn mark_lifecycle_idempotency_record_committed(
    conn: &Connection,
    key: &str,
    response_status: u16,
    response_json: &str,
) -> anyhow::Result<()> {
    conn.execute(
        &format!(
            "update {LIFECYCLE_IDEMPOTENCY_TABLE} \
             set status = 'committed', response_status = ?, response_json = ?, updated_at_ms = ? \
             where key = ?"
        ),
        params![
            i64::from(response_status),
            response_json,
            sqlite_i64(current_time_millis()),
            key
        ],
    )?;
    Ok(())
}

fn delete_lifecycle_idempotency_record(conn: &Connection, key: &str) -> anyhow::Result<()> {
    conn.execute(
        &format!("delete from {LIFECYCLE_IDEMPOTENCY_TABLE} where key = ?"),
        [key],
    )?;
    Ok(())
}

fn ensure_configured_replication_group(
    conn: &Connection,
    state: &LibsqlHttpState,
    group_id: &str,
    placement: &CreateDatabasePlacementRequest,
) -> anyhow::Result<()> {
    upsert_replication_group_catalog(conn, state, group_id, placement, "active")?;
    upsert_configured_replication_group_members(conn, state, group_id)?;
    Ok(())
}

fn upsert_replication_group_catalog(
    conn: &Connection,
    state: &LibsqlHttpState,
    group_id: &str,
    placement: &CreateDatabasePlacementRequest,
    group_state: &str,
) -> anyhow::Result<()> {
    validate_replication_group_id(group_id)?;
    ensure!(
        matches!(
            group_state,
            "creating" | "active" | "draining" | "deleted" | "failed"
        ),
        "unsupported replication group state {group_state}"
    );
    require_database_catalog_schema(conn, 4)?;
    let now = sqlite_i64(current_time_millis());
    let write_home = placement
        .write_home
        .clone()
        .or_else(|| preferred_write_home(state));
    conn.execute(
        r#"
        insert into replication_groups (
            group_id, state, placement_mode, object_prefix,
            write_home_cloud, write_home_region, write_home_zone,
            compaction_owner_node_id, failover_automatic, failover_promote_after_ms,
            created_at_ms, updated_at_ms, error
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null)
        on conflict(group_id) do update set
            state = excluded.state,
            placement_mode = excluded.placement_mode,
            write_home_cloud = excluded.write_home_cloud,
            write_home_region = excluded.write_home_region,
            write_home_zone = excluded.write_home_zone,
            compaction_owner_node_id = excluded.compaction_owner_node_id,
            failover_automatic = excluded.failover_automatic,
            failover_promote_after_ms = excluded.failover_promote_after_ms,
            updated_at_ms = excluded.updated_at_ms,
            error = null
        "#,
        params![
            group_id,
            group_state,
            &placement.mode,
            replication_group_object_prefix(group_id),
            write_home.as_ref().map(|location| location.cloud.as_str()),
            write_home.as_ref().map(|location| location.region.as_str()),
            write_home
                .as_ref()
                .and_then(|location| location.zone.as_deref()),
            sqlite_i64(state.node_id),
            i64::from(placement.failover.automatic),
            sqlite_i64(placement.failover.promote_after_ms),
            now,
            now
        ],
    )?;
    upsert_replication_group_standby_targets(conn, group_id, &placement.failover.standby_targets)?;
    Ok(())
}

fn replace_replication_group_members(
    conn: &Connection,
    state: &LibsqlHttpState,
    group_id: &str,
    members: &[ReplicationGroupMemberRequest],
) -> anyhow::Result<()> {
    conn.execute(
        "delete from replication_group_members where group_id = ?",
        [group_id],
    )?;
    if members.is_empty() {
        upsert_configured_replication_group_members(conn, state, group_id)?;
    } else {
        for member in members {
            upsert_replication_group_member(conn, state, group_id, member)?;
        }
    }
    let voters: i64 = conn.query_row(
        "select count(*) from replication_group_members where group_id = ? and role = 'voter'",
        [group_id],
        |row| row.get(0),
    )?;
    ensure!(
        voters > 0,
        "replication group {group_id} must have at least one voter"
    );
    Ok(())
}

fn upsert_replication_group_member(
    conn: &Connection,
    state: &LibsqlHttpState,
    group_id: &str,
    member: &ReplicationGroupMemberRequest,
) -> anyhow::Result<()> {
    validate_replication_group_member_role(&member.role)?;
    let Some(node) = state.placement_nodes.get(&member.node_id) else {
        anyhow::bail!("placement node {} does not exist", member.node_id);
    };
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        r#"
        insert into replication_group_members (
            group_id, node_id, role, cloud, region, zone, priority, created_at_ms, updated_at_ms
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(group_id, node_id, role) do update set
            cloud = excluded.cloud,
            region = excluded.region,
            zone = excluded.zone,
            priority = excluded.priority,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![
            group_id,
            sqlite_i64(node.node_id),
            &member.role,
            &node.cloud,
            &node.region,
            &node.zone,
            sqlite_i64(member.priority.unwrap_or(0)),
            now,
            now
        ],
    )?;
    Ok(())
}

fn preferred_write_home(state: &LibsqlHttpState) -> Option<PlacementLocationRequest> {
    state
        .placement_nodes
        .get(&state.node_id)
        .map(|node| PlacementLocationRequest {
            cloud: node.cloud.clone(),
            region: node.region.clone(),
            zone: Some(node.zone.clone()),
        })
}

fn upsert_configured_replication_group_members(
    conn: &Connection,
    state: &LibsqlHttpState,
    group_id: &str,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    for (priority, node) in state.placement_nodes.values().enumerate() {
        conn.execute(
            r#"
            insert into replication_group_members (
                group_id, node_id, role, cloud, region, zone, priority, created_at_ms, updated_at_ms
            )
            values (?, ?, 'voter', ?, ?, ?, ?, ?, ?)
            on conflict(group_id, node_id, role) do update set
                cloud = excluded.cloud,
                region = excluded.region,
                zone = excluded.zone,
                priority = excluded.priority,
                updated_at_ms = excluded.updated_at_ms
            "#,
            params![
                group_id,
                sqlite_i64(node.node_id),
                &node.cloud,
                &node.region,
                &node.zone,
                sqlite_usize(priority),
                now,
                now
            ],
        )?;
    }
    Ok(())
}

fn upsert_database_creating(
    conn: &Connection,
    name: &str,
    database_id: &str,
    object_prefix: &str,
    group_id: &str,
) -> anyhow::Result<DatabaseCatalogRecord> {
    upsert_database_lifecycle_state(conn, name, database_id, object_prefix, group_id, "creating")
}

fn upsert_database_ready(
    conn: &Connection,
    name: &str,
    database_id: &str,
    object_prefix: &str,
    group_id: &str,
) -> anyhow::Result<DatabaseCatalogRecord> {
    upsert_database_lifecycle_state(conn, name, database_id, object_prefix, group_id, "ready")
}

fn upsert_database_lifecycle_state(
    conn: &Connection,
    name: &str,
    database_id: &str,
    object_prefix: &str,
    group_id: &str,
    lifecycle_state: &str,
) -> anyhow::Result<DatabaseCatalogRecord> {
    ensure!(
        matches!(lifecycle_state, "creating" | "ready"),
        "unsupported database lifecycle upsert state {lifecycle_state}"
    );
    require_database_catalog_schema(conn, 4)?;
    let now = sqlite_i64(current_time_millis());
    let tx = conn.unchecked_transaction()?;
    let existing = read_database_catalog_record_from_conn(&tx, name)?;
    let effective_database_id = existing
        .as_ref()
        .map(|record| record.database_id.as_str())
        .unwrap_or(database_id);
    match existing.as_ref().map(|record| record.state.as_str()) {
        Some("ready") | Some("deleting") => {
            anyhow::bail!("database {name} already exists")
        }
        Some("creating") | Some("failed") | Some("deleted") => {
            tx.execute(
                r#"
                update database_catalog
                set state = ?,
                    database_id = ?,
                    object_prefix = ?,
                    generation = case
                        when state = 'creating' and ? = 'ready' then generation
                        else generation + 1
                    end,
                    updated_at_ms = ?,
                    deleted_at_ms = null,
                    purged_at_ms = null,
                    purge_error = null,
                    error = null
                where name = ?
                "#,
                params![
                    lifecycle_state,
                    effective_database_id,
                    object_prefix,
                    lifecycle_state,
                    now,
                    name
                ],
            )?;
        }
        Some(_) => anyhow::bail!("database {name} catalog record has invalid state"),
        None => {
            tx.execute(
                r#"
                insert into database_catalog (
                    database_id, name, state, object_prefix, generation, created_at_ms, updated_at_ms, deleted_at_ms, purged_at_ms, purge_error, error
                )
                values (?, ?, ?, ?, 1, ?, ?, null, null, null, null)
                "#,
                params![
                    effective_database_id,
                    name,
                    lifecycle_state,
                    object_prefix,
                    now,
                    now
                ],
            )?;
        }
    }
    tx.execute(
        r#"
        insert into database_replication_groups (
            database_id, database_name, group_id, role, created_at_ms, updated_at_ms
        )
        values (?, ?, ?, 'primary', ?, ?)
        on conflict(database_id) do update set
            database_name = excluded.database_name,
            group_id = excluded.group_id,
            role = excluded.role,
            updated_at_ms = excluded.updated_at_ms
        "#,
        params![effective_database_id, name, group_id, now, now],
    )?;
    tx.commit()?;
    read_database_catalog_record_from_conn(conn, name)?
        .ok_or_else(|| anyhow!("database {name} catalog record disappeared"))
}

fn mark_database_state(
    conn: &Connection,
    name: &str,
    state: &str,
    deleted_at_ms: Option<u64>,
    error: Option<&anyhow::Error>,
) -> anyhow::Result<DatabaseCatalogRecord> {
    let now = sqlite_i64(current_time_millis());
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        r#"
        update database_catalog
        set state = ?, updated_at_ms = ?, deleted_at_ms = coalesce(?, deleted_at_ms), error = ?
        where name = ?
        "#,
        params![
            state,
            now,
            deleted_at_ms.map(sqlite_i64),
            error.map(ToString::to_string),
            name
        ],
    )?;
    tx.commit()?;
    read_database_catalog_record_from_conn(conn, name)?
        .ok_or_else(|| anyhow!("database {name} catalog record disappeared"))
}

fn checkpoint_catalog_connection(conn: &Connection) -> anyhow::Result<()> {
    let journal_mode: String = conn.query_row("pragma journal_mode", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Ok(());
    }
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
        conn.query_row("pragma wal_checkpoint(full)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    ensure!(
        busy == 0 && checkpointed_frames >= log_frames,
        "catalog checkpoint did not complete: busy={busy} log_frames={log_frames} checkpointed_frames={checkpointed_frames}"
    );
    Ok(())
}

fn mark_database_purge_progress(
    conn: &Connection,
    name: &str,
    metrics: &SqliteDatabasePurgeMetrics,
    error: Option<&anyhow::Error>,
) -> anyhow::Result<DatabaseCatalogRecord> {
    let now = sqlite_i64(current_time_millis());
    let purged_at_ms = metrics.complete.then_some(now);
    conn.execute(
        r#"
        update database_catalog
        set updated_at_ms = ?,
            purged_at_ms = coalesce(?, purged_at_ms),
            purge_error = ?
        where name = ?
        "#,
        params![now, purged_at_ms, error.map(ToString::to_string), name],
    )?;
    read_database_catalog_record_from_conn(conn, name)?
        .ok_or_else(|| anyhow!("database {name} catalog record disappeared"))
}

fn read_database_catalog_record_from_conn(
    conn: &Connection,
    database: &str,
) -> anyhow::Result<Option<DatabaseCatalogRecord>> {
    let sql = database_catalog_select_sql(conn, Some("name = ?"))?;
    Ok(conn
        .query_row(&sql, [database], database_catalog_record_from_row)
        .optional()?)
}

fn database_catalog_select_sql(
    conn: &Connection,
    where_clause: Option<&str>,
) -> anyhow::Result<String> {
    let has_database_id = table_column_exists(conn, "database_catalog", "database_id")?;
    let has_purge_columns = table_column_exists(conn, "database_catalog", "purged_at_ms")?
        && table_column_exists(conn, "database_catalog", "purge_error")?;
    let has_group_mapping = table_exists(conn, "database_replication_groups")?;
    let database_id_projection = if has_database_id {
        "database_catalog.database_id"
    } else {
        "null as database_id"
    };
    let purge_projection = if has_purge_columns {
        "database_catalog.purged_at_ms, database_catalog.purge_error"
    } else {
        "null as purged_at_ms, null as purge_error"
    };
    let group_projection = if has_group_mapping {
        "coalesce(database_replication_groups.group_id, 'rg_default') as replication_group_id"
    } else {
        "'rg_default' as replication_group_id"
    };
    let join = if has_group_mapping {
        " left join database_replication_groups on database_replication_groups.database_id = database_catalog.database_id"
    } else {
        ""
    };
    let mut sql = format!(
        "select {database_id_projection}, database_catalog.name, database_catalog.state, database_catalog.object_prefix, {group_projection}, database_catalog.generation, database_catalog.created_at_ms, database_catalog.updated_at_ms, database_catalog.deleted_at_ms, {purge_projection}, database_catalog.error from database_catalog{join}"
    );
    if let Some(where_clause) = where_clause {
        sql.push_str(" where ");
        sql.push_str(where_clause);
    }
    sql.push_str(" order by database_catalog.name");
    Ok(sql)
}

fn database_catalog_record_from_row(row: &Row<'_>) -> rusqlite::Result<DatabaseCatalogRecord> {
    let name: String = row.get(1)?;
    let database_id = row
        .get::<_, Option<String>>(0)?
        .unwrap_or_else(|| database_id_from_name(&name));
    Ok(DatabaseCatalogRecord {
        database_id,
        name,
        state: row.get(2)?,
        object_prefix: row.get(3)?,
        replication_group_id: row.get(4)?,
        generation: row.get::<_, i64>(5)?.max(0) as u64,
        created_at_ms: row.get::<_, i64>(6)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(7)?.max(0) as u64,
        deleted_at_ms: row
            .get::<_, Option<i64>>(8)?
            .map(|value| value.max(0) as u64),
        purged_at_ms: row
            .get::<_, Option<i64>>(9)?
            .map(|value| value.max(0) as u64),
        purge_error: row.get(10)?,
        error: row.get(11)?,
    })
}

fn replication_group_record_from_row_without_members(
    row: &Row<'_>,
) -> rusqlite::Result<ReplicationGroupRecord> {
    Ok(ReplicationGroupRecord {
        group_id: row.get(0)?,
        state: row.get(1)?,
        placement_mode: row.get(2)?,
        object_prefix: row.get(3)?,
        write_home_cloud: row.get(4)?,
        write_home_region: row.get(5)?,
        write_home_zone: row.get(6)?,
        compaction_owner_node_id: row
            .get::<_, Option<i64>>(7)?
            .map(|value| value.max(0) as u64),
        failover_automatic: row.get::<_, i64>(8)? != 0,
        failover_promote_after_ms: row.get::<_, i64>(9)?.max(0) as u64,
        created_at_ms: row.get::<_, i64>(10)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(11)?.max(0) as u64,
        error: row.get(12)?,
        members: Vec::new(),
        runtime: ReplicationGroupRuntimeRecord {
            group_id: row.get(0)?,
            loaded: false,
            loaded_at_ms: None,
            current_leader: None,
            voter_ids: Vec::new(),
            learner_ids: Vec::new(),
            ready_for_linearizable_reads: false,
            error: None,
        },
    })
}

fn read_replication_group_record_from_conn(
    conn: &Connection,
    group_id: &str,
) -> anyhow::Result<Option<ReplicationGroupRecord>> {
    Ok(conn
        .query_row(
            r#"
            select group_id, state, placement_mode, object_prefix,
                   write_home_cloud, write_home_region, write_home_zone,
                   compaction_owner_node_id, failover_automatic, failover_promote_after_ms,
                   created_at_ms, updated_at_ms, error
            from replication_groups
            where group_id = ?
            "#,
            [group_id],
            replication_group_record_from_row_without_members,
        )
        .optional()?)
}

fn list_replication_group_members_from_conn(
    conn: &Connection,
    group_id: &str,
) -> anyhow::Result<Vec<ReplicationGroupMemberRecord>> {
    let mut stmt = conn.prepare(
        r#"
        select group_id, node_id, role, cloud, region, zone, priority, created_at_ms, updated_at_ms
        from replication_group_members
        where group_id = ?
        order by priority, node_id, role
        "#,
    )?;
    Ok(stmt
        .query_map([group_id], replication_group_member_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?)
}

fn replication_group_member_record_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<ReplicationGroupMemberRecord> {
    Ok(ReplicationGroupMemberRecord {
        group_id: row.get(0)?,
        node_id: row.get::<_, i64>(1)?.max(0) as u64,
        role: row.get(2)?,
        cloud: row.get(3)?,
        region: row.get(4)?,
        zone: row.get(5)?,
        priority: row.get::<_, i64>(6)?.max(0) as u64,
        created_at_ms: row.get::<_, i64>(7)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(8)?.max(0) as u64,
    })
}

fn placement_operation_record_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<PlacementOperationRecord> {
    let transfer_checkpoint_artifact_json = row.get::<_, Option<String>>(21)?;
    let transfer_checkpoint_artifact = transfer_checkpoint_artifact_json
        .map(|json| {
            serde_json::from_str::<SlateDbCheckpointArtifact>(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(21, SqliteType::Text, Box::new(error))
            })
        })
        .transpose()?;
    Ok(PlacementOperationRecord {
        operation_id: row.get(0)?,
        database_id: row.get(1)?,
        database_name: row.get(2)?,
        operation: row.get(3)?,
        status: row.get(4)?,
        phase: row.get(5)?,
        source_group_id: row.get(6)?,
        target_group_id: row.get(7)?,
        created_at_ms: row.get::<_, i64>(8)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(9)?.max(0) as u64,
        completed_at_ms: row
            .get::<_, Option<i64>>(10)?
            .map(|value| value.max(0) as u64),
        source_fence_applied_index: row
            .get::<_, Option<i64>>(11)?
            .map(|value| value.max(0) as u64),
        source_fence_commit_ts: optional_hybrid_timestamp_from_row(row, 12, 13)?,
        source_fence_observed_at_ms: row
            .get::<_, Option<i64>>(14)?
            .map(|value| value.max(0) as u64),
        target_clone_applied_index: row
            .get::<_, Option<i64>>(15)?
            .map(|value| value.max(0) as u64),
        target_clone_commit_ts: optional_hybrid_timestamp_from_row(row, 16, 17)?,
        error: row.get(18)?,
        transfer_epoch_id: row.get(19)?,
        transfer_kind: row.get(20)?,
        transfer_checkpoint_artifact,
        transfer_source_applied_index: row
            .get::<_, Option<i64>>(22)?
            .map(|value| value.max(0) as u64),
        transfer_source_commit_ts: optional_hybrid_timestamp_from_row(row, 23, 24)?,
        transfer_created_at_ms: row
            .get::<_, Option<i64>>(25)?
            .map(|value| value.max(0) as u64),
    })
}

fn database_placement_standby_record_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<DatabasePlacementStandbyRecord> {
    Ok(DatabasePlacementStandbyRecord {
        database_id: row.get(0)?,
        database_name: row.get(1)?,
        source_group_id: row.get(2)?,
        target_group_id: row.get(3)?,
        source_applied_index: row
            .get::<_, Option<i64>>(4)?
            .map(|value| value.max(0) as u64),
        source_commit_ts: optional_hybrid_timestamp_from_row(row, 5, 6)?,
        target_applied_index: row
            .get::<_, Option<i64>>(7)?
            .map(|value| value.max(0) as u64),
        target_commit_ts: optional_hybrid_timestamp_from_row(row, 8, 9)?,
        refreshed_at_ms: row.get::<_, i64>(10)?.max(0) as u64,
        updated_at_ms: row.get::<_, i64>(11)?.max(0) as u64,
        age_ms: 0,
        catalog_recorded: false,
        target_group_available: false,
        target_locally_openable: false,
        promotable: false,
        error: row.get(12)?,
    })
}

fn optional_hybrid_timestamp_from_row(
    row: &Row<'_>,
    physical_ms_index: usize,
    logical_index: usize,
) -> rusqlite::Result<Option<HybridTimestamp>> {
    let physical_ms = row.get::<_, Option<i64>>(physical_ms_index)?;
    let logical = row.get::<_, Option<i64>>(logical_index)?;
    Ok(match (physical_ms, logical) {
        (Some(physical_ms), Some(logical)) => Some(HybridTimestamp {
            physical_ms: physical_ms.max(0) as u64,
            logical: logical.max(0) as u32,
        }),
        _ => None,
    })
}

fn validate_database_object_prefix(prefix: &str) -> anyhow::Result<()> {
    ensure!(
        !prefix.is_empty(),
        "database object_prefix must not be empty"
    );
    ensure!(
        !prefix.starts_with('/') && !prefix.contains("..") && !prefix.contains('\\'),
        "database object_prefix must be a relative object-store prefix without parent traversal"
    );
    Ok(())
}

fn catalog_rollout_status(state: &LibsqlHttpState, target_version: u32) -> CatalogRolloutStatus {
    catalog_rollout_status_from_entries(
        state.metrics_registry.snapshot(),
        target_version,
        current_time_millis(),
    )
}

fn catalog_rollout_status_from_entries(
    entries: Vec<ClusterRaftMetricsEntry>,
    target_version: u32,
    now_ms: u64,
) -> CatalogRolloutStatus {
    let newest_membership = entries
        .iter()
        .max_by_key(|entry| entry.observed_at_ms)
        .map(|entry| entry.metrics.voter_ids.clone())
        .unwrap_or_default();
    let mut voters = Vec::new();
    let mut blockers = Vec::new();
    if newest_membership.is_empty() {
        blockers.push("no observed Raft voter membership".to_string());
    }

    for node_id in newest_membership {
        let entry = entries
            .iter()
            .filter(|entry| entry.metrics.node_id == node_id)
            .max_by_key(|entry| entry.observed_at_ms);
        let Some(entry) = entry else {
            let reason = format!("node {node_id} has no observed metrics");
            blockers.push(reason.clone());
            voters.push(CatalogRolloutNodeStatus {
                node_id,
                observed_age_ms: None,
                stale: true,
                catalog_min_read_schema_version: None,
                catalog_max_read_schema_version: None,
                catalog_max_write_schema_version: None,
                ready: false,
                reason: Some(reason),
            });
            continue;
        };
        let observed_age_ms = now_ms.saturating_sub(entry.observed_at_ms);
        let stale = observed_age_ms > RAFT_METRICS_STALE_AFTER_MS;
        let capabilities = entry.metrics.capabilities.as_ref();
        let reason = if stale {
            Some(format!(
                "node {node_id} metrics are stale: observed_age_ms={observed_age_ms}, stale_after_ms={RAFT_METRICS_STALE_AFTER_MS}"
            ))
        } else if capabilities.is_none() {
            Some(format!(
                "node {node_id} has not advertised software capabilities"
            ))
        } else if capabilities.is_some_and(|capabilities| {
            capabilities.catalog_max_read_schema_version < target_version
        }) {
            Some(format!(
                "node {node_id} cannot read catalog schema version {target_version}"
            ))
        } else if capabilities.is_some_and(|capabilities| {
            capabilities.catalog_max_write_schema_version < target_version
        }) {
            Some(format!(
                "node {node_id} cannot write catalog schema version {target_version}"
            ))
        } else {
            None
        };
        if let Some(reason) = &reason {
            blockers.push(reason.clone());
        }
        voters.push(CatalogRolloutNodeStatus {
            node_id,
            observed_age_ms: Some(observed_age_ms),
            stale,
            catalog_min_read_schema_version: capabilities
                .map(|capabilities| capabilities.catalog_min_read_schema_version),
            catalog_max_read_schema_version: capabilities
                .map(|capabilities| capabilities.catalog_max_read_schema_version),
            catalog_max_write_schema_version: capabilities
                .map(|capabilities| capabilities.catalog_max_write_schema_version),
            ready: reason.is_none(),
            reason,
        });
    }

    CatalogRolloutStatus {
        target_version,
        ready: blockers.is_empty(),
        blockers,
        voters,
    }
}

fn operator_raft_metrics(entries: Vec<ClusterRaftMetricsEntry>) -> Vec<serde_json::Value> {
    let now = current_time_millis();
    entries
        .into_iter()
        .map(|entry| {
            let observed_age_ms = now.saturating_sub(entry.observed_at_ms);
            serde_json::json!({
                "observed_at_ms": entry.observed_at_ms,
                "observed_age_ms": observed_age_ms,
                "stale_after_ms": RAFT_METRICS_STALE_AFTER_MS,
                "stale": observed_age_ms > RAFT_METRICS_STALE_AFTER_MS,
                "metrics": entry.metrics,
            })
        })
        .collect()
}

async fn default_pipeline(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<PipelineReqBody>,
) -> Json<PipelineRespBody> {
    Json(run_pipeline(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_pipeline(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PipelineReqBody>,
) -> Json<PipelineRespBody> {
    Json(run_pipeline(state, headers, database, request).await)
}

async fn default_blob_open(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<BlobOpenReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_open(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_blob_open(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<BlobOpenReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_open(state, headers, database, request).await)
}

async fn default_blob_read(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<BlobReadReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_read(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_blob_read(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<BlobReadReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_read(state, headers, database, request).await)
}

async fn default_blob_write(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<BlobWriteReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_write(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_blob_write(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<BlobWriteReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_write(state, headers, database, request).await)
}

async fn default_blob_read_bytes(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Query(request): Query<BlobReadReqBody>,
) -> impl IntoResponse {
    run_blob_read_bytes(state, headers, DEFAULT_DATABASE.to_string(), request).await
}

async fn database_blob_read_bytes(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Query(request): Query<BlobReadReqBody>,
) -> impl IntoResponse {
    run_blob_read_bytes(state, headers, database, request).await
}

async fn default_blob_write_bytes(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Query(request): Query<BlobWriteBytesReqQuery>,
    body: Body,
) -> impl IntoResponse {
    run_blob_write_bytes(state, headers, DEFAULT_DATABASE.to_string(), request, body).await
}

async fn database_blob_write_bytes(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Query(request): Query<BlobWriteBytesReqQuery>,
    body: Body,
) -> impl IntoResponse {
    run_blob_write_bytes(state, headers, database, request, body).await
}

async fn default_blob_read_stream(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Query(request): Query<BlobReadReqBody>,
) -> impl IntoResponse {
    run_blob_read_stream(state, headers, DEFAULT_DATABASE.to_string(), request).await
}

async fn database_blob_read_stream(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Query(request): Query<BlobReadReqBody>,
) -> impl IntoResponse {
    run_blob_read_stream(state, headers, database, request).await
}

async fn default_blob_write_stream(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Query(request): Query<BlobWriteStreamReqQuery>,
    body: Body,
) -> impl IntoResponse {
    run_blob_write_stream(state, headers, DEFAULT_DATABASE.to_string(), request, body).await
}

async fn database_blob_write_stream(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Query(request): Query<BlobWriteStreamReqQuery>,
    body: Body,
) -> impl IntoResponse {
    run_blob_write_stream(state, headers, database, request, body).await
}

async fn default_blob_reopen(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<BlobReopenReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_reopen(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_blob_reopen(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<BlobReopenReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_reopen(state, headers, database, request).await)
}

async fn default_blob_close(
    State(state): State<LibsqlHttpState>,
    headers: HeaderMap,
    Json(request): Json<BlobCloseReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_close(state, headers, DEFAULT_DATABASE.to_string(), request).await)
}

async fn database_blob_close(
    State(state): State<LibsqlHttpState>,
    Path(database): Path<String>,
    headers: HeaderMap,
    Json(request): Json<BlobCloseReqBody>,
) -> Json<BlobRespBody> {
    Json(run_blob_close(state, headers, database, request).await)
}

async fn handle_hrana_websocket(
    mut socket: WebSocket,
    state: LibsqlHttpState,
    database: String,
    headers: HeaderMap,
) {
    let mut connection = HranaWsConnection::new(state, database, headers);
    while let Some(message) = socket.recv().await {
        let message = match message {
            Ok(message) => message,
            Err(_) => break,
        };
        match message {
            Message::Text(text) => match connection.handle_text(text.as_str()).await {
                Ok(responses) => {
                    for response in responses {
                        if socket.send(response).await.is_err() {
                            break;
                        }
                    }
                }
                Err(error) => {
                    let response =
                        serde_json::to_string(&HranaWsServerMsg::HelloError { error }).unwrap();
                    let _ = socket.send(Message::Text(response.into())).await;
                    break;
                }
            },
            Message::Binary(bytes) => {
                if let Some(response) = connection.handle_binary(bytes.to_vec()).await
                    && socket.send(Message::Text(response.into())).await.is_err()
                {
                    break;
                }
            }
            Message::Close(_) => break,
            Message::Ping(payload) => {
                if socket.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
        }
    }
    connection.close_all_streams();
}

struct HranaWsConnection {
    state: LibsqlHttpState,
    database: String,
    headers: HeaderMap,
    authenticated: bool,
    streams: HashMap<i64, HranaWsStreamState>,
    stored_sql: HashMap<i64, String>,
    pending_binary_write: Option<PendingWsBlobWrite>,
}

struct HranaWsStreamState {
    baton: String,
    session: Arc<Mutex<LibsqlSession>>,
}

struct PendingWsBlobWrite {
    request_id: i64,
    stream_id: i64,
    blob_id: String,
    offset: usize,
}

impl HranaWsConnection {
    fn new(state: LibsqlHttpState, database: String, headers: HeaderMap) -> Self {
        Self {
            state,
            database,
            headers,
            authenticated: false,
            streams: HashMap::new(),
            stored_sql: HashMap::new(),
            pending_binary_write: None,
        }
    }

    async fn handle_text(&mut self, text: &str) -> Result<Vec<Message>, HranaWsErrorBody> {
        let message: HranaWsClientMsg =
            serde_json::from_str(text).map_err(|error| HranaWsErrorBody {
                message: format!("invalid Hrana WebSocket JSON message: {error}"),
                code: Some("HRANA_PROTO_ERROR".to_string()),
            })?;
        match message {
            HranaWsClientMsg::Hello { jwt } => {
                let headers = headers_with_optional_bearer(&self.headers, jwt.as_deref());
                if let Err(error) = self
                    .state
                    .authorize(&headers, &self.database)
                    .and_then(|_| validate_database_name(&self.database))
                {
                    return Ok(vec![ws_text(HranaWsServerMsg::HelloError {
                        error: HranaWsErrorBody {
                            message: error.to_string(),
                            code: Some("SQLITE_AUTH".to_string()),
                        },
                    })]);
                }
                self.headers = headers;
                self.authenticated = true;
                Ok(vec![ws_text(HranaWsServerMsg::HelloOk)])
            }
            HranaWsClientMsg::Request {
                request_id,
                request,
            } => {
                if !self.authenticated {
                    return Ok(vec![ws_text(HranaWsServerMsg::ResponseError {
                        request_id,
                        error: HranaWsErrorBody {
                            message: "Hrana WebSocket hello is required before requests"
                                .to_string(),
                            code: Some("HRANA_PROTO_ERROR".to_string()),
                        },
                    })]);
                }

                match request {
                    HranaWsRequest::BlobWriteBytes {
                        stream_id,
                        blob_id,
                        offset,
                    } => match self
                        .prepare_binary_blob_write(request_id, stream_id, blob_id, offset)
                        .await
                    {
                        Ok(()) => Ok(Vec::new()),
                        Err(error) => Ok(vec![ws_text(HranaWsServerMsg::ResponseError {
                            request_id,
                            error,
                        })]),
                    },
                    HranaWsRequest::BlobReadBytes {
                        stream_id,
                        blob_id,
                        offset,
                        length,
                    } => match self
                        .handle_blob_read_bytes(stream_id, blob_id, offset, length)
                        .await
                    {
                        Ok(response) => Ok(vec![
                            ws_text(HranaWsServerMsg::ResponseOk {
                                request_id,
                                response: HranaWsResponse::BlobReadBytes {
                                    result: response.metadata(),
                                },
                            }),
                            Message::Binary(response.bytes.into()),
                        ]),
                        Err(error) => Ok(vec![ws_text(HranaWsServerMsg::ResponseError {
                            request_id,
                            error,
                        })]),
                    },
                    request => {
                        let result = self.handle_request(request).await;
                        let response = match result {
                            Ok(response) => HranaWsServerMsg::ResponseOk {
                                request_id,
                                response,
                            },
                            Err(error) => HranaWsServerMsg::ResponseError { request_id, error },
                        };
                        Ok(vec![ws_text(response)])
                    }
                }
            }
        }
    }

    async fn handle_binary(&mut self, bytes: Vec<u8>) -> Option<String> {
        let Some(pending) = self.pending_binary_write.take() else {
            return Some(
                serde_json::to_string(&HranaWsServerMsg::ResponseError {
                    request_id: -1,
                    error: HranaWsErrorBody::protocol(
                        "binary blob frame received without a pending blob_write_bytes request",
                    ),
                })
                .unwrap(),
            );
        };
        let result = self
            .handle_blob_write_bytes(pending.stream_id, pending.blob_id, pending.offset, bytes)
            .await;
        let response = match result {
            Ok(result) => HranaWsServerMsg::ResponseOk {
                request_id: pending.request_id,
                response: HranaWsResponse::BlobWriteBytes {
                    result: result.metadata(),
                },
            },
            Err(error) => HranaWsServerMsg::ResponseError {
                request_id: pending.request_id,
                error,
            },
        };
        Some(serde_json::to_string(&response).unwrap())
    }

    async fn prepare_binary_blob_write(
        &mut self,
        request_id: i64,
        stream_id: i64,
        blob_id: String,
        offset: usize,
    ) -> Result<(), HranaWsErrorBody> {
        if self.pending_binary_write.is_some() {
            return Err(HranaWsErrorBody::protocol(
                "another blob_write_bytes request is already waiting for a binary frame",
            ));
        }
        self.stream(stream_id)?;
        self.pending_binary_write = Some(PendingWsBlobWrite {
            request_id,
            stream_id,
            blob_id,
            offset,
        });
        Ok(())
    }

    async fn handle_blob_read_bytes(
        &self,
        stream_id: i64,
        blob_id: String,
        offset: usize,
        length: usize,
    ) -> Result<BlobBytesReadResponse, HranaWsErrorBody> {
        enforce_orion_read_policy(&self.state, &self.database, &OrionReadPolicy::Strong)
            .await
            .map_err(|error| HranaWsErrorBody {
                message: error.to_string(),
                code: Some("SQLITE_BUSY".to_string()),
            })?;
        let request = BlobReadReqBody {
            baton: None,
            blob_id,
            offset,
            length,
        };
        let max_chunk_bytes = self.state.blob_max_chunk_bytes;
        run_ws_blob_op(
            self.state.blob_metrics.clone(),
            BlobApiOp::Read,
            self.stream(stream_id)?,
            move |session| {
                let response = blob_read_bytes_session(session, request, max_chunk_bytes)?;
                let stats = BlobRequestStats::from_blob_read(&response, session.blob_handles.len());
                Ok((response, stats))
            },
        )
        .await
    }

    async fn handle_blob_write_bytes(
        &self,
        stream_id: i64,
        blob_id: String,
        offset: usize,
        bytes: Vec<u8>,
    ) -> Result<BlobBytesWriteResponse, HranaWsErrorBody> {
        let max_chunk_bytes = self.state.blob_max_chunk_bytes;
        run_ws_blob_op(
            self.state.blob_metrics.clone(),
            BlobApiOp::Write,
            self.stream(stream_id)?,
            move |session| {
                let response =
                    blob_write_bytes_session(session, blob_id, offset, bytes, max_chunk_bytes)?;
                let stats =
                    BlobRequestStats::from_blob_write(&response, session.blob_handles.len());
                Ok((response, stats))
            },
        )
        .await
    }

    async fn handle_request(
        &mut self,
        request: HranaWsRequest,
    ) -> Result<HranaWsResponse, HranaWsErrorBody> {
        match request {
            HranaWsRequest::OpenStream { stream_id } => {
                if self.streams.contains_key(&stream_id) {
                    return Err(HranaWsErrorBody::protocol(format!(
                        "stream {stream_id} already exists"
                    )));
                }
                let (baton, session) = self
                    .state
                    .session(&self.database, None)
                    .map_err(HranaWsErrorBody::sqlite)?;
                {
                    let mut session = session.lock().map_err(|_| {
                        HranaWsErrorBody::io("libSQL session mutex poisoned".to_string())
                    })?;
                    session.stored_sql.extend(self.stored_sql.clone());
                }
                self.streams
                    .insert(stream_id, HranaWsStreamState { baton, session });
                Ok(HranaWsResponse::OpenStream)
            }
            HranaWsRequest::CloseStream { stream_id } => {
                if let Some(stream) = self.streams.remove(&stream_id) {
                    let _ = self.state.close_session(&stream.baton);
                }
                Ok(HranaWsResponse::CloseStream)
            }
            HranaWsRequest::Execute { stream_id, stmt } => {
                let request = StreamRequest::Execute { stmt };
                self.enforce_ws_read_policy(&request).await?;
                let result = run_stream_request(self.stream(stream_id)?, request).await;
                match result {
                    Ok(StreamResult::Ok {
                        response: StreamResponse::Execute { result },
                    }) => Ok(HranaWsResponse::Execute { result }),
                    Ok(StreamResult::Error { error }) => Err(error.into()),
                    Ok(_) => Err(HranaWsErrorBody::protocol(
                        "unexpected response type for execute",
                    )),
                    Err(error) => Err(HranaWsErrorBody::from_anyhow(error)),
                }
            }
            HranaWsRequest::Batch { stream_id, batch } => {
                let request = StreamRequest::Batch { batch };
                self.enforce_ws_read_policy(&request).await?;
                let result = run_stream_request(self.stream(stream_id)?, request).await;
                match result {
                    Ok(StreamResult::Ok {
                        response: StreamResponse::Batch { result },
                    }) => Ok(HranaWsResponse::Batch { result }),
                    Ok(StreamResult::Error { error }) => Err(error.into()),
                    Ok(_) => Err(HranaWsErrorBody::protocol(
                        "unexpected response type for batch",
                    )),
                    Err(error) => Err(HranaWsErrorBody::from_anyhow(error)),
                }
            }
            HranaWsRequest::Sequence {
                stream_id,
                sql,
                sql_id,
            } => {
                let request = StreamRequest::Sequence { sql, sql_id };
                self.enforce_ws_read_policy(&request).await?;
                let result = run_stream_request(self.stream(stream_id)?, request).await;
                match result {
                    Ok(StreamResult::Ok {
                        response: StreamResponse::Sequence,
                    }) => Ok(HranaWsResponse::Sequence),
                    Ok(StreamResult::Error { error }) => Err(error.into()),
                    Ok(_) => Err(HranaWsErrorBody::protocol(
                        "unexpected response type for sequence",
                    )),
                    Err(error) => Err(HranaWsErrorBody::from_anyhow(error)),
                }
            }
            HranaWsRequest::Describe {
                stream_id,
                sql,
                sql_id,
            } => {
                let request = StreamRequest::Describe { sql, sql_id };
                self.enforce_ws_read_policy(&request).await?;
                let result = run_stream_request(self.stream(stream_id)?, request).await;
                match result {
                    Ok(StreamResult::Ok {
                        response: StreamResponse::Describe { result },
                    }) => Ok(HranaWsResponse::Describe { result }),
                    Ok(StreamResult::Error { error }) => Err(error.into()),
                    Ok(_) => Err(HranaWsErrorBody::protocol(
                        "unexpected response type for describe",
                    )),
                    Err(error) => Err(HranaWsErrorBody::from_anyhow(error)),
                }
            }
            HranaWsRequest::StoreSql { sql_id, sql } => {
                self.stored_sql.insert(sql_id, sql.clone());
                for stream in self.streams.values() {
                    let mut session = stream.session.lock().map_err(|_| {
                        HranaWsErrorBody::io("libSQL session mutex poisoned".to_string())
                    })?;
                    session.stored_sql.insert(sql_id, sql.clone());
                }
                Ok(HranaWsResponse::StoreSql)
            }
            HranaWsRequest::CloseSql { sql_id } => {
                self.stored_sql.remove(&sql_id);
                for stream in self.streams.values() {
                    let mut session = stream.session.lock().map_err(|_| {
                        HranaWsErrorBody::io("libSQL session mutex poisoned".to_string())
                    })?;
                    session.stored_sql.remove(&sql_id);
                }
                Ok(HranaWsResponse::CloseSql)
            }
            HranaWsRequest::GetAutocommit { stream_id } => {
                let request = StreamRequest::GetAutocommit;
                let result = run_stream_request(self.stream(stream_id)?, request).await;
                match result {
                    Ok(StreamResult::Ok {
                        response: StreamResponse::GetAutocommit { is_autocommit },
                    }) => Ok(HranaWsResponse::GetAutocommit { is_autocommit }),
                    Ok(StreamResult::Error { error }) => Err(error.into()),
                    Ok(_) => Err(HranaWsErrorBody::protocol(
                        "unexpected response type for get_autocommit",
                    )),
                    Err(error) => Err(HranaWsErrorBody::from_anyhow(error)),
                }
            }
            HranaWsRequest::BlobOpen {
                stream_id,
                schema,
                table,
                column,
                rowid,
                read_only,
            } => {
                let request = BlobOpenReqBody {
                    baton: None,
                    schema,
                    table,
                    column,
                    rowid,
                    read_only,
                };
                let result = run_ws_blob_op(
                    self.state.blob_metrics.clone(),
                    BlobApiOp::Open,
                    self.stream(stream_id)?,
                    move |session| {
                        let response = blob_open_session(session, request)?;
                        let stats =
                            BlobRequestStats::from_response(&response, session.blob_handles.len());
                        Ok((response, stats))
                    },
                )
                .await?;
                Ok(HranaWsResponse::BlobOpen { result })
            }
            HranaWsRequest::BlobRead {
                stream_id,
                blob_id,
                offset,
                length,
            } => {
                enforce_orion_read_policy(&self.state, &self.database, &OrionReadPolicy::Strong)
                    .await
                    .map_err(|error| HranaWsErrorBody {
                        message: error.to_string(),
                        code: Some("SQLITE_BUSY".to_string()),
                    })?;
                let request = BlobReadReqBody {
                    baton: None,
                    blob_id,
                    offset,
                    length,
                };
                let max_chunk_bytes = self.state.blob_max_chunk_bytes;
                let result = run_ws_blob_op(
                    self.state.blob_metrics.clone(),
                    BlobApiOp::Read,
                    self.stream(stream_id)?,
                    move |session| {
                        let response = blob_read_session(session, request, max_chunk_bytes)?;
                        let stats =
                            BlobRequestStats::from_response(&response, session.blob_handles.len());
                        Ok((response, stats))
                    },
                )
                .await?;
                Ok(HranaWsResponse::BlobRead { result })
            }
            HranaWsRequest::BlobWrite {
                stream_id,
                blob_id,
                offset,
                base64,
            } => {
                let request = BlobWriteReqBody {
                    baton: None,
                    blob_id,
                    offset,
                    base64,
                };
                let max_chunk_bytes = self.state.blob_max_chunk_bytes;
                let result = run_ws_blob_op(
                    self.state.blob_metrics.clone(),
                    BlobApiOp::Write,
                    self.stream(stream_id)?,
                    move |session| {
                        let response = blob_write_session(session, request, max_chunk_bytes)?;
                        let stats =
                            BlobRequestStats::from_response(&response, session.blob_handles.len());
                        Ok((response, stats))
                    },
                )
                .await?;
                Ok(HranaWsResponse::BlobWrite { result })
            }
            HranaWsRequest::BlobReopen {
                stream_id,
                blob_id,
                rowid,
            } => {
                let request = BlobReopenReqBody {
                    baton: None,
                    blob_id,
                    rowid,
                };
                let result = run_ws_blob_op(
                    self.state.blob_metrics.clone(),
                    BlobApiOp::Reopen,
                    self.stream(stream_id)?,
                    move |session| {
                        let response = blob_reopen_session(session, request)?;
                        let stats =
                            BlobRequestStats::from_response(&response, session.blob_handles.len());
                        Ok((response, stats))
                    },
                )
                .await?;
                Ok(HranaWsResponse::BlobReopen { result })
            }
            HranaWsRequest::BlobClose { stream_id, blob_id } => {
                let request = BlobCloseReqBody {
                    baton: None,
                    blob_id,
                };
                let result = run_ws_blob_op(
                    self.state.blob_metrics.clone(),
                    BlobApiOp::Close,
                    self.stream(stream_id)?,
                    move |session| {
                        let response = blob_close_session(session, request)?;
                        let stats =
                            BlobRequestStats::from_response(&response, session.blob_handles.len());
                        Ok((response, stats))
                    },
                )
                .await?;
                Ok(HranaWsResponse::BlobClose { result })
            }
            HranaWsRequest::BlobReadBytes { .. } | HranaWsRequest::BlobWriteBytes { .. } => {
                Err(HranaWsErrorBody::protocol(
                    "binary blob requests must be handled by the WebSocket frame coordinator",
                ))
            }
            HranaWsRequest::OpenCursor { .. }
            | HranaWsRequest::CloseCursor { .. }
            | HranaWsRequest::FetchCursor { .. } => Err(HranaWsErrorBody {
                message: "Hrana WebSocket cursors are not supported yet".to_string(),
                code: Some("HRANA_PROTO_ERROR".to_string()),
            }),
        }
    }

    async fn enforce_ws_read_policy(
        &self,
        request: &StreamRequest,
    ) -> Result<(), HranaWsErrorBody> {
        if stream_request_requires_fresh_read(request) {
            enforce_orion_read_policy(&self.state, &self.database, &OrionReadPolicy::Strong)
                .await
                .map_err(|error| HranaWsErrorBody {
                    message: error.to_string(),
                    code: Some("SQLITE_BUSY".to_string()),
                })?;
        }
        Ok(())
    }

    fn stream(&self, stream_id: i64) -> Result<Arc<Mutex<LibsqlSession>>, HranaWsErrorBody> {
        self.streams
            .get(&stream_id)
            .map(|stream| Arc::clone(&stream.session))
            .ok_or_else(|| HranaWsErrorBody::protocol(format!("stream {stream_id} does not exist")))
    }

    fn close_all_streams(&mut self) {
        for (_, stream) in self.streams.drain() {
            let _ = self.state.close_session(&stream.baton);
        }
    }
}

fn ws_text(message: HranaWsServerMsg) -> Message {
    Message::Text(serde_json::to_string(&message).unwrap().into())
}

async fn run_ws_blob_op<T>(
    metrics: Arc<BlobApiMetrics>,
    op_name: BlobApiOp,
    session: Arc<Mutex<LibsqlSession>>,
    op: impl FnOnce(&mut LibsqlSession) -> anyhow::Result<(T, BlobRequestStats)> + Send + 'static,
) -> Result<T, HranaWsErrorBody>
where
    T: Send + 'static,
{
    let start = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        op(&mut session)
    })
    .await
    .map_err(|error| HranaWsErrorBody::io(format!("joining libSQL blob task: {error}")))?;
    match result {
        Ok((response, stats)) => {
            metrics.record_request(op_name, start.elapsed(), Ok(stats));
            Ok(response)
        }
        Err(error) => {
            metrics.record_request(
                op_name,
                start.elapsed(),
                Err(error.is::<OrionBlobTooManyOpenHandlesError>()),
            );
            Err(HranaWsErrorBody::from_anyhow(error))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaWsClientMsg {
    Hello {
        #[serde(default)]
        jwt: Option<String>,
    },
    Request {
        request_id: i64,
        request: HranaWsRequest,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaWsRequest {
    OpenStream {
        stream_id: i64,
    },
    CloseStream {
        stream_id: i64,
    },
    Execute {
        stream_id: i64,
        stmt: Stmt,
    },
    Batch {
        stream_id: i64,
        batch: Batch,
    },
    Sequence {
        stream_id: i64,
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i64>,
    },
    Describe {
        stream_id: i64,
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i64>,
    },
    StoreSql {
        sql_id: i64,
        sql: String,
    },
    CloseSql {
        sql_id: i64,
    },
    GetAutocommit {
        stream_id: i64,
    },
    BlobOpen {
        stream_id: i64,
        #[serde(default)]
        schema: Option<String>,
        table: String,
        column: String,
        rowid: i64,
        #[serde(default = "default_blob_read_only")]
        read_only: bool,
    },
    BlobRead {
        stream_id: i64,
        blob_id: String,
        offset: usize,
        length: usize,
    },
    BlobWrite {
        stream_id: i64,
        blob_id: String,
        offset: usize,
        base64: String,
    },
    BlobReadBytes {
        stream_id: i64,
        blob_id: String,
        offset: usize,
        length: usize,
    },
    BlobWriteBytes {
        stream_id: i64,
        blob_id: String,
        offset: usize,
    },
    BlobReopen {
        stream_id: i64,
        blob_id: String,
        rowid: i64,
    },
    BlobClose {
        stream_id: i64,
        blob_id: String,
    },
    OpenCursor {
        stream_id: i64,
        cursor_id: i64,
        batch: Batch,
    },
    CloseCursor {
        cursor_id: i64,
    },
    FetchCursor {
        cursor_id: i64,
        max_count: i64,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaWsServerMsg {
    HelloOk,
    HelloError {
        error: HranaWsErrorBody,
    },
    ResponseOk {
        request_id: i64,
        response: HranaWsResponse,
    },
    ResponseError {
        request_id: i64,
        error: HranaWsErrorBody,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaWsResponse {
    OpenStream,
    CloseStream,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    Describe { result: DescribeResult },
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
    BlobOpen { result: BlobResponseKind },
    BlobRead { result: BlobResponseKind },
    BlobWrite { result: BlobResponseKind },
    BlobReadBytes { result: HranaWsBlobReadBytesResult },
    BlobWriteBytes { result: HranaWsBlobWriteBytesResult },
    BlobReopen { result: BlobResponseKind },
    BlobClose { result: BlobResponseKind },
}

#[derive(Debug, Serialize)]
struct HranaWsBlobReadBytesResult {
    blob_id: String,
    offset: usize,
    bytes_read: usize,
    size: usize,
}

#[derive(Debug, Serialize)]
struct HranaWsBlobWriteBytesResult {
    blob_id: String,
    offset: usize,
    bytes_written: usize,
    size: usize,
}

#[derive(Debug, Serialize)]
struct HranaWsErrorBody {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

impl HranaWsErrorBody {
    fn protocol(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some("HRANA_PROTO_ERROR".to_string()),
        }
    }

    fn sqlite(error: anyhow::Error) -> Self {
        Self {
            message: error.to_string(),
            code: Some(sqlite_error_code(error.as_ref()).to_string()),
        }
    }

    fn io(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some("SQLITE_IOERR".to_string()),
        }
    }

    fn from_anyhow(error: anyhow::Error) -> Self {
        Self {
            message: error.to_string(),
            code: Some(sqlite_error_code(error.as_ref()).to_string()),
        }
    }
}

impl From<ErrorBody> for HranaWsErrorBody {
    fn from(value: ErrorBody) -> Self {
        Self {
            message: value.message,
            code: value.code,
        }
    }
}

fn headers_with_optional_bearer(headers: &HeaderMap, jwt: Option<&str>) -> HeaderMap {
    let mut headers = headers.clone();
    if let Some(jwt) = jwt.filter(|jwt| !jwt.is_empty())
        && let Ok(value) = HeaderValue::from_str(&format!("Bearer {jwt}"))
    {
        headers.insert(axum::http::header::AUTHORIZATION, value);
    }
    headers
}

#[derive(Debug, Clone)]
struct IdempotencyContext {
    key: String,
    request_hash: String,
}

#[derive(Debug, Clone)]
struct IdempotentPipelineOutcome {
    results: Vec<StreamResult>,
    outcome: OrionIdempotencyMetadata,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrionIdempotencyMetadata {
    key: String,
    request_hash: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reused: Option<bool>,
}

fn idempotency_context_from_headers(
    headers: &HeaderMap,
    request: &PipelineReqBody,
    config: &LibsqlHttpIdempotencyConfig,
) -> anyhow::Result<Option<IdempotencyContext>> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    ensure!(
        config.enabled,
        "idempotency keys are disabled by libsql_http.idempotency.enabled=false"
    );
    let key = value
        .to_str()
        .context("idempotency key must be valid UTF-8")?
        .trim()
        .to_string();
    ensure!(!key.is_empty(), "idempotency key must not be empty");
    ensure!(
        key.len() <= 512,
        "idempotency key is too long: max length is 512 bytes"
    );
    ensure!(
        pipeline_may_write(request),
        "idempotency keys are only supported for write pipelines"
    );
    ensure!(
        request.baton.is_none(),
        "idempotency keys are only supported for standalone pipelines, not baton sessions"
    );
    ensure!(
        !pipeline_has_transaction_control(request),
        "idempotency keys are only supported when Orion can own the transaction boundary"
    );
    Ok(Some(IdempotencyContext {
        key,
        request_hash: hash_pipeline_request(request)?,
    }))
}

fn hash_pipeline_request(request: &PipelineReqBody) -> anyhow::Result<String> {
    let canonical = serde_json::to_vec(request)?;
    let digest = Sha256::digest(canonical);
    Ok(hex_lower(&digest))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

async fn run_pipeline(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: PipelineReqBody,
) -> PipelineRespBody {
    if let Err(error) =
        validate_database_name(&database).and_then(|_| state.authorize(&headers, &database))
    {
        return pipeline_error_response(
            request.baton,
            &request.requests,
            error.to_string(),
            "SQLITE_AUTH",
        );
    }
    let read_policy = match orion_read_policy_from_headers(&headers) {
        Ok(read_policy) => read_policy,
        Err(error) => {
            return pipeline_error_response(
                request.baton,
                &request.requests,
                error.to_string(),
                "HRANA_PROTO_ERROR",
            );
        }
    };
    let idempotency =
        match idempotency_context_from_headers(&headers, &request, &state.idempotency_config) {
            Ok(idempotency) => idempotency,
            Err(error) => {
                state
                    .idempotency_metrics
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                return pipeline_error_response(
                    request.baton,
                    &request.requests,
                    error.to_string(),
                    "HRANA_PROTO_ERROR",
                );
            }
        };

    if pipeline_may_write(&request) {
        match forward_pipeline_to_leader(&state, &headers, &database, &request).await {
            Ok(Some(response)) => return response,
            Ok(None) => {}
            Err(error) => {
                return pipeline_error_response(
                    request.baton,
                    &request.requests,
                    error.to_string(),
                    "SQLITE_BUSY",
                );
            }
        }
    }

    if pipeline_should_enforce_read_policy(&request, &read_policy)
        && let Err(error) =
            satisfy_orion_read_policy(&state, &headers, &database, &request, &read_policy).await
    {
        match error {
            ReadPolicySatisfaction::Forwarded(response) => return response,
            ReadPolicySatisfaction::Failed(error) => {
                return pipeline_error_response(
                    request.baton,
                    &request.requests,
                    error.to_string(),
                    "SQLITE_BUSY",
                );
            }
        }
    }

    if let Err(error) = ensure_database_ready_for_client(&state, &database) {
        return pipeline_error_response(
            request.baton,
            &request.requests,
            error.to_string(),
            "SQLITE_AUTH",
        );
    }

    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => {
            if request.baton.is_some() {
                match forward_pipeline_to_leader(&state, &headers, &database, &request).await {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(_) => {}
                }
            }
            return pipeline_error_response(
                request.baton,
                &request.requests,
                error.to_string(),
                "SQLITE_IOERR",
            );
        }
    };
    let close_after_pipeline = request
        .requests
        .iter()
        .any(|request| matches!(request, StreamRequest::Close));

    let mut idempotency_outcome = None;
    let results = if let Some(idempotency) = idempotency.clone() {
        state
            .idempotency_metrics
            .requests
            .fetch_add(1, Ordering::Relaxed);
        match run_idempotent_pipeline(Arc::clone(&session), request.requests, idempotency).await {
            Ok(outcome) => {
                if outcome.outcome.reused == Some(true) {
                    state
                        .idempotency_metrics
                        .committed_reused
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    state
                        .idempotency_metrics
                        .committed_new
                        .fetch_add(1, Ordering::Relaxed);
                }
                idempotency_outcome = Some(outcome.outcome);
                outcome.results
            }
            Err(error) => {
                let code = idempotency_or_sqlite_error_code(error.as_ref());
                match code {
                    "ORION_IDEMPOTENCY_CONFLICT" => {
                        state
                            .idempotency_metrics
                            .conflicts
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "ORION_COMMIT_UNKNOWN" => {
                        state
                            .idempotency_metrics
                            .commit_unknown
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
                vec![StreamResult::error(error.to_string(), code)]
            }
        }
    } else {
        let mut results = Vec::with_capacity(request.requests.len());
        for stream_request in request.requests {
            let result = run_stream_request(Arc::clone(&session), stream_request).await;
            results.push(result.unwrap_or_else(|error| {
                StreamResult::error(error.to_string(), sqlite_error_code(error.as_ref()))
            }));
        }
        results
    };
    if close_after_pipeline {
        let _ = state.close_session(&baton);
    }

    let freshness = match state.runtime_for_database(&database) {
        Ok(runtime) => runtime.replica_freshness().await.ok(),
        Err(_) => None,
    };

    PipelineRespBody {
        baton: if close_after_pipeline {
            None
        } else {
            Some(baton)
        },
        base_url: None,
        orion: freshness.map(|freshness| {
            let mut metadata = OrionPipelineMetadata::local(&state, &read_policy, freshness);
            metadata.idempotency = idempotency_outcome;
            metadata
        }),
        results,
    }
}

fn pipeline_error_response(
    baton: Option<String>,
    requests: &[StreamRequest],
    message: impl Into<String>,
    code: &str,
) -> PipelineRespBody {
    let message = message.into();
    PipelineRespBody {
        baton,
        base_url: None,
        orion: None,
        results: requests
            .iter()
            .map(|_| StreamResult::error(message.clone(), code))
            .collect(),
    }
}

fn idempotency_or_sqlite_error_code(error: &(dyn Error + 'static)) -> &'static str {
    let message = error.to_string();
    if message.contains("idempotency key conflict") {
        return "ORION_IDEMPOTENCY_CONFLICT";
    }
    if message.contains("idempotency key is pending") {
        return "ORION_COMMIT_UNKNOWN";
    }
    sqlite_error_code(error)
}

async fn run_blob_open(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobOpenReqBody,
) -> BlobRespBody {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_AUTH"),
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_IOERR"),
    };
    run_blob_session_op(
        &state,
        BlobApiOp::Open,
        &database,
        &read_policy,
        Some(baton),
        session,
        move |session| blob_open_session(session, request),
    )
    .await
}

async fn run_blob_read(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobReadReqBody,
) -> BlobRespBody {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_AUTH"),
    };
    if let Err(error) = enforce_orion_read_policy(&state, &database, &read_policy).await {
        return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_BUSY");
    }
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_IOERR"),
    };
    let max_chunk_bytes = state.blob_max_chunk_bytes;
    run_blob_session_op(
        &state,
        BlobApiOp::Read,
        &database,
        &read_policy,
        Some(baton),
        session,
        move |session| blob_read_session(session, request, max_chunk_bytes),
    )
    .await
}

async fn run_blob_write(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobWriteReqBody,
) -> BlobRespBody {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_AUTH"),
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_IOERR"),
    };
    let max_chunk_bytes = state.blob_max_chunk_bytes;
    run_blob_session_op(
        &state,
        BlobApiOp::Write,
        &database,
        &read_policy,
        Some(baton),
        session,
        move |session| blob_write_session(session, request, max_chunk_bytes),
    )
    .await
}

async fn run_blob_read_bytes(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobReadReqBody,
) -> axum::response::Response {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_AUTH",
            ))
            .into_response();
        }
    };
    if let Err(error) = enforce_orion_read_policy(&state, &database, &read_policy).await {
        return Json(BlobRespBody::error(
            request.baton,
            error.to_string(),
            "SQLITE_BUSY",
        ))
        .into_response();
    }
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };
    let max_chunk_bytes = state.blob_max_chunk_bytes;
    let start = Instant::now();
    let metrics = state.blob_metrics.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        let response = blob_read_bytes_session(&mut session, request, max_chunk_bytes)?;
        let stats = BlobRequestStats::from_blob_read(&response, session.blob_handles.len());
        Ok::<_, anyhow::Error>((response, stats))
    })
    .await
    .context("joining libSQL binary blob read task");

    match result {
        Ok(Ok((response, stats))) => {
            metrics.record_request(BlobApiOp::Read, start.elapsed(), Ok(stats));
            blob_read_bytes_http_response(Some(baton), response)
        }
        Ok(Err(error)) => {
            metrics.record_request(
                BlobApiOp::Read,
                start.elapsed(),
                Err(error.is::<OrionBlobTooManyOpenHandlesError>()),
            );
            Json(BlobRespBody::error(
                Some(baton),
                error.to_string(),
                sqlite_error_code(error.as_ref()).to_string(),
            ))
            .into_response()
        }
        Err(error) => {
            metrics.record_request(BlobApiOp::Read, start.elapsed(), Err(false));
            Json(BlobRespBody::error(
                Some(baton),
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response()
        }
    }
}

async fn run_blob_write_bytes(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobWriteBytesReqQuery,
    body: Body,
) -> axum::response::Response {
    let _read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_AUTH",
            ))
            .into_response();
        }
    };
    let max_chunk_bytes = state.blob_max_chunk_bytes;
    if let Err(error) = ensure_content_length_within_limit(&headers, max_chunk_bytes) {
        return Json(BlobRespBody::error(
            request.baton,
            error.to_string(),
            "SQLITE_IOERR",
        ))
        .into_response();
    }
    let bytes = match to_bytes(body, max_chunk_bytes).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                format!(
                    "blob binary payload is too large or invalid: {error}; max_chunk_bytes={max_chunk_bytes}"
                ),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };
    let start = Instant::now();
    let metrics = state.blob_metrics.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        let response = blob_write_bytes_session(
            &mut session,
            request.blob_id,
            request.offset,
            bytes.to_vec(),
            max_chunk_bytes,
        )?;
        let stats = BlobRequestStats::from_blob_write(&response, session.blob_handles.len());
        Ok::<_, anyhow::Error>((response, stats))
    })
    .await
    .context("joining libSQL binary blob write task");

    match result {
        Ok(Ok((response, stats))) => {
            metrics.record_request(BlobApiOp::Write, start.elapsed(), Ok(stats));
            blob_write_bytes_http_response(Some(baton), response)
        }
        Ok(Err(error)) => {
            metrics.record_request(
                BlobApiOp::Write,
                start.elapsed(),
                Err(error.is::<OrionBlobTooManyOpenHandlesError>()),
            );
            Json(BlobRespBody::error(
                Some(baton),
                error.to_string(),
                sqlite_error_code(error.as_ref()).to_string(),
            ))
            .into_response()
        }
        Err(error) => {
            metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
            Json(BlobRespBody::error(
                Some(baton),
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response()
        }
    }
}

async fn run_blob_read_stream(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobReadReqBody,
) -> axum::response::Response {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_AUTH",
            ))
            .into_response();
        }
    };
    if let Err(error) = enforce_orion_read_policy(&state, &database, &read_policy).await {
        return Json(BlobRespBody::error(
            request.baton,
            error.to_string(),
            "SQLITE_BUSY",
        ))
        .into_response();
    }
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };

    let plan = match blob_read_stream_plan(Arc::clone(&session), request).await {
        Ok(plan) => plan,
        Err(error) => {
            return Json(BlobRespBody::error(
                Some(baton),
                error.to_string(),
                sqlite_error_code(error.as_ref()).to_string(),
            ))
            .into_response();
        }
    };

    let metrics = state.blob_metrics.clone();
    let start = Instant::now();
    let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(2);
    spawn_blob_read_stream_task(
        session,
        plan.clone(),
        state.blob_max_chunk_bytes,
        tx,
        metrics,
        start,
    );
    blob_read_stream_http_response(Some(baton), plan, ReceiverStream::new(rx))
}

async fn run_blob_write_stream(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobWriteStreamReqQuery,
    body: Body,
) -> axum::response::Response {
    let _read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_AUTH",
            ))
            .into_response();
        }
    };
    let expected_len = match expected_stream_length(&headers, request.length) {
        Ok(length) => length,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => {
            return Json(BlobRespBody::error(
                request.baton,
                error.to_string(),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
    };
    let plan = BlobWriteStreamPlan {
        blob_id: request.blob_id,
        offset: request.offset,
        expected_len,
    };
    if let Err(error) = blob_write_stream_validate(Arc::clone(&session), &plan).await {
        return Json(BlobRespBody::error(
            Some(baton),
            error.to_string(),
            sqlite_error_code(error.as_ref()).to_string(),
        ))
        .into_response();
    }

    let start = Instant::now();
    let metrics = state.blob_metrics.clone();
    let (tx, rx) = mpsc::channel::<Bytes>(2);
    let writer = spawn_blob_write_stream_task(
        session,
        plan.clone(),
        state.blob_max_chunk_bytes,
        rx,
        metrics.clone(),
        start,
    );

    let mut stream = body.into_data_stream();
    let mut received = 0usize;
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                drop(tx);
                let _ = writer.await;
                metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
                return Json(BlobRespBody::error(
                    Some(baton),
                    format!("blob stream upload failed: {error}"),
                    "SQLITE_IOERR",
                ))
                .into_response();
            }
        };
        if chunk.is_empty() {
            continue;
        }
        if received.saturating_add(chunk.len()) > expected_len {
            drop(tx);
            let _ = writer.await;
            metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
            return Json(BlobRespBody::error(
                Some(baton),
                format!(
                    "blob stream body exceeds declared length: received more than {expected_len} bytes"
                ),
                "SQLITE_IOERR",
            ))
            .into_response();
        }
        received += chunk.len();
        for chunk in split_bytes(chunk, state.blob_max_chunk_bytes) {
            if tx.send(chunk).await.is_err() {
                metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
                return Json(BlobRespBody::error(
                    Some(baton),
                    "blob stream writer stopped before upload completed",
                    "SQLITE_IOERR",
                ))
                .into_response();
            }
        }
    }
    drop(tx);
    if received != expected_len {
        let _ = writer.await;
        metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
        return Json(BlobRespBody::error(
            Some(baton),
            format!(
                "blob stream body length mismatch: expected {expected_len} bytes, received {received}"
            ),
            "SQLITE_IOERR",
        ))
        .into_response();
    }

    match writer.await {
        Ok(Ok(response)) => blob_write_bytes_http_response(Some(baton), response),
        Ok(Err(error)) => Json(BlobRespBody::error(
            Some(baton),
            error.to_string(),
            sqlite_error_code(error.as_ref()).to_string(),
        ))
        .into_response(),
        Err(error) => {
            metrics.record_request(BlobApiOp::Write, start.elapsed(), Err(false));
            Json(BlobRespBody::error(
                Some(baton),
                format!("joining blob stream writer failed: {error}"),
                "SQLITE_IOERR",
            ))
            .into_response()
        }
    }
}

async fn run_blob_reopen(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobReopenReqBody,
) -> BlobRespBody {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_AUTH"),
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_IOERR"),
    };
    run_blob_session_op(
        &state,
        BlobApiOp::Reopen,
        &database,
        &read_policy,
        Some(baton),
        session,
        move |session| blob_reopen_session(session, request),
    )
    .await
}

async fn run_blob_close(
    state: LibsqlHttpState,
    headers: HeaderMap,
    database: String,
    request: BlobCloseReqBody,
) -> BlobRespBody {
    let read_policy = match prepare_blob_request(&state, &headers, &database).await {
        Ok(read_policy) => read_policy,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_AUTH"),
    };
    let (baton, session) = match state.session(&database, request.baton.clone()) {
        Ok(session) => session,
        Err(error) => return BlobRespBody::error(request.baton, error.to_string(), "SQLITE_IOERR"),
    };
    run_blob_session_op(
        &state,
        BlobApiOp::Close,
        &database,
        &read_policy,
        Some(baton),
        session,
        move |session| blob_close_session(session, request),
    )
    .await
}

async fn prepare_blob_request(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
) -> anyhow::Result<OrionReadPolicy> {
    validate_database_name(database).and_then(|_| state.authorize(headers, database))?;
    ensure_database_ready_for_client(state, database)?;
    orion_read_policy_from_headers(headers)
}

async fn run_blob_session_op(
    state: &LibsqlHttpState,
    op_name: BlobApiOp,
    database: &str,
    read_policy: &OrionReadPolicy,
    baton: Option<String>,
    session: Arc<Mutex<LibsqlSession>>,
    op: impl FnOnce(&mut LibsqlSession) -> anyhow::Result<BlobResponseKind> + Send + 'static,
) -> BlobRespBody {
    let start = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        let response = op(&mut session)?;
        let stats = BlobRequestStats::from_response(&response, session.blob_handles.len());
        Ok::<_, anyhow::Error>((response, stats))
    })
    .await
    .context("joining libSQL blob task");

    match result {
        Ok(Ok((response, stats))) => {
            state
                .blob_metrics
                .record_request(op_name, start.elapsed(), Ok(stats));
            let freshness = match state.runtime_for_database(database) {
                Ok(runtime) => runtime.replica_freshness().await.ok(),
                Err(_) => None,
            };
            BlobRespBody {
                baton,
                orion: freshness
                    .map(|freshness| OrionPipelineMetadata::local(state, read_policy, freshness)),
                result: Some(response),
                error: None,
            }
        }
        Ok(Err(error)) => {
            state.blob_metrics.record_request(
                op_name,
                start.elapsed(),
                Err(error.is::<OrionBlobTooManyOpenHandlesError>()),
            );
            BlobRespBody::error(
                baton,
                error.to_string(),
                sqlite_error_code(error.as_ref()).to_string(),
            )
        }
        Err(error) => {
            state
                .blob_metrics
                .record_request(op_name, start.elapsed(), Err(false));
            BlobRespBody::error(baton, error.to_string(), "SQLITE_IOERR")
        }
    }
}

fn blob_open_session(
    session: &mut LibsqlSession,
    request: BlobOpenReqBody,
) -> anyhow::Result<BlobResponseKind> {
    blob_open_session_with_limit(session, request, MAX_OPEN_BLOB_HANDLES_PER_SESSION)
}

fn blob_open_session_with_limit(
    session: &mut LibsqlSession,
    request: BlobOpenReqBody,
    max_open_handles: usize,
) -> anyhow::Result<BlobResponseKind> {
    if session.blob_handles.len() >= max_open_handles {
        return Err(OrionBlobTooManyOpenHandlesError { max_open_handles }.into());
    }
    let schema = request.schema.unwrap_or_else(|| "main".to_string());
    validate_blob_name("schema", &schema)?;
    validate_blob_name("table", &request.table)?;
    validate_blob_name("column", &request.column)?;
    let handle = BlobHandle {
        schema,
        table: request.table,
        column: request.column,
        rowid: request.rowid,
        read_only: request.read_only,
    };
    let size = blob_size(&session.conn, &handle)?;
    let blob_id = format!("blob-{}", session.next_blob_id);
    session.next_blob_id += 1;
    session.blob_handles.insert(blob_id.clone(), handle.clone());
    Ok(BlobResponseKind::Open {
        blob_id,
        size,
        read_only: handle.read_only,
    })
}

fn blob_read_session(
    session: &mut LibsqlSession,
    request: BlobReadReqBody,
    max_chunk_bytes: usize,
) -> anyhow::Result<BlobResponseKind> {
    let response = blob_read_bytes_session(session, request, max_chunk_bytes)?;
    Ok(BlobResponseKind::Read {
        blob_id: response.blob_id,
        offset: response.offset,
        bytes_read: response.bytes_read,
        base64: BASE64.encode(response.bytes),
        size: response.size,
    })
}

fn blob_read_bytes_session(
    session: &mut LibsqlSession,
    request: BlobReadReqBody,
    max_chunk_bytes: usize,
) -> anyhow::Result<BlobBytesReadResponse> {
    let handle = session
        .blob_handles
        .get(&request.blob_id)
        .cloned()
        .ok_or_else(|| {
            HranaProtocolError::new(format!("blob id {} does not exist", request.blob_id))
        })?;
    let blob = open_blob(&session.conn, &handle)?;
    let size = blob_size_i32(blob.size())?;
    ensure_blob_offset(request.offset)?;
    ensure_blob_length(request.length, max_chunk_bytes)?;
    let mut bytes = vec![0; request.length];
    let bytes_read = blob.read_at(&mut bytes, request.offset)?;
    bytes.truncate(bytes_read);
    Ok(BlobBytesReadResponse {
        blob_id: request.blob_id,
        offset: request.offset,
        bytes_read,
        bytes,
        size,
    })
}

fn blob_write_session(
    session: &mut LibsqlSession,
    request: BlobWriteReqBody,
    max_chunk_bytes: usize,
) -> anyhow::Result<BlobResponseKind> {
    ensure_base64_blob_length(request.base64.len(), max_chunk_bytes)?;
    let bytes = BASE64.decode(request.base64.as_bytes())?;
    let response = blob_write_bytes_session(
        session,
        request.blob_id,
        request.offset,
        bytes,
        max_chunk_bytes,
    )?;
    Ok(BlobResponseKind::Write {
        blob_id: response.blob_id,
        offset: response.offset,
        bytes_written: response.bytes_written,
        size: response.size,
    })
}

fn blob_write_bytes_session(
    session: &mut LibsqlSession,
    blob_id: String,
    offset: usize,
    bytes: Vec<u8>,
    max_chunk_bytes: usize,
) -> anyhow::Result<BlobBytesWriteResponse> {
    let handle = session
        .blob_handles
        .get(&blob_id)
        .cloned()
        .ok_or_else(|| HranaProtocolError::new(format!("blob id {blob_id} does not exist")))?;
    if handle.read_only {
        return Err(OrionBlobReadOnlyError::new(blob_id).into());
    }
    ensure_blob_offset(offset)?;
    ensure_blob_length(bytes.len(), max_chunk_bytes)?;
    let mut blob = open_blob(&session.conn, &handle)?;
    blob.write_all_at(&bytes, offset)?;
    let size = blob_size_i32(blob.size())?;
    Ok(BlobBytesWriteResponse {
        blob_id,
        offset,
        bytes_written: bytes.len(),
        size,
    })
}

fn blob_reopen_session(
    session: &mut LibsqlSession,
    request: BlobReopenReqBody,
) -> anyhow::Result<BlobResponseKind> {
    {
        let handle = session
            .blob_handles
            .get_mut(&request.blob_id)
            .ok_or_else(|| {
                HranaProtocolError::new(format!("blob id {} does not exist", request.blob_id))
            })?;
        handle.rowid = request.rowid;
    }
    let handle = session.blob_handles.get(&request.blob_id).unwrap();
    let size = blob_size(&session.conn, handle)?;
    Ok(BlobResponseKind::Reopen {
        blob_id: request.blob_id,
        rowid: request.rowid,
        size,
    })
}

fn blob_close_session(
    session: &mut LibsqlSession,
    request: BlobCloseReqBody,
) -> anyhow::Result<BlobResponseKind> {
    session
        .blob_handles
        .remove(&request.blob_id)
        .ok_or_else(|| {
            HranaProtocolError::new(format!("blob id {} does not exist", request.blob_id))
        })?;
    Ok(BlobResponseKind::Close {
        blob_id: request.blob_id,
    })
}

struct BlobBytesReadResponse {
    blob_id: String,
    offset: usize,
    bytes_read: usize,
    bytes: Vec<u8>,
    size: usize,
}

impl BlobBytesReadResponse {
    fn metadata(&self) -> HranaWsBlobReadBytesResult {
        HranaWsBlobReadBytesResult {
            blob_id: self.blob_id.clone(),
            offset: self.offset,
            bytes_read: self.bytes_read,
            size: self.size,
        }
    }
}

#[derive(Clone)]
struct BlobReadStreamPlan {
    blob_id: String,
    offset: usize,
    bytes_read: usize,
    size: usize,
}

#[derive(Clone)]
struct BlobWriteStreamPlan {
    blob_id: String,
    offset: usize,
    expected_len: usize,
}

struct BlobBytesWriteResponse {
    blob_id: String,
    offset: usize,
    bytes_written: usize,
    size: usize,
}

impl BlobBytesWriteResponse {
    fn metadata(&self) -> HranaWsBlobWriteBytesResult {
        HranaWsBlobWriteBytesResult {
            blob_id: self.blob_id.clone(),
            offset: self.offset,
            bytes_written: self.bytes_written,
            size: self.size,
        }
    }
}

async fn blob_read_stream_plan(
    session: Arc<Mutex<LibsqlSession>>,
    request: BlobReadReqBody,
) -> anyhow::Result<BlobReadStreamPlan> {
    tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        blob_read_stream_plan_session(&mut session, request)
    })
    .await
    .context("joining libSQL blob stream planning task")?
}

fn blob_read_stream_plan_session(
    session: &mut LibsqlSession,
    request: BlobReadReqBody,
) -> anyhow::Result<BlobReadStreamPlan> {
    let handle = session
        .blob_handles
        .get(&request.blob_id)
        .cloned()
        .ok_or_else(|| {
            HranaProtocolError::new(format!("blob id {} does not exist", request.blob_id))
        })?;
    let blob = open_blob(&session.conn, &handle)?;
    let size = blob_size_i32(blob.size())?;
    ensure_blob_offset(request.offset)?;
    ensure_blob_i32_length(request.length)?;
    ensure_blob_range(request.offset, request.length, size)?;
    Ok(BlobReadStreamPlan {
        blob_id: request.blob_id,
        offset: request.offset,
        bytes_read: request.length,
        size,
    })
}

fn spawn_blob_read_stream_task(
    session: Arc<Mutex<LibsqlSession>>,
    plan: BlobReadStreamPlan,
    chunk_bytes: usize,
    tx: mpsc::Sender<Result<Bytes, io::Error>>,
    metrics: Arc<BlobApiMetrics>,
    start: Instant,
) {
    tokio::task::spawn_blocking(move || {
        let mut bytes_sent = 0usize;
        let result = (|| -> anyhow::Result<()> {
            let session = session
                .lock()
                .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
            let handle = session
                .blob_handles
                .get(&plan.blob_id)
                .cloned()
                .ok_or_else(|| {
                    HranaProtocolError::new(format!("blob id {} does not exist", plan.blob_id))
                })?;
            let blob = open_blob(&session.conn, &handle)?;
            let mut remaining = plan.bytes_read;
            let mut offset = plan.offset;
            let chunk_bytes = chunk_bytes.max(1);
            while remaining > 0 {
                let len = remaining.min(chunk_bytes);
                let mut bytes = vec![0; len];
                let read = blob.read_at(&mut bytes, offset)?;
                if read == 0 {
                    break;
                }
                bytes.truncate(read);
                bytes_sent += read;
                offset += read;
                remaining -= read;
                if tx.blocking_send(Ok(Bytes::from(bytes))).is_err() {
                    break;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => metrics.record_request(
                BlobApiOp::Read,
                start.elapsed(),
                Ok(BlobRequestStats {
                    bytes_read: bytes_sent,
                    bytes_written: 0,
                    open_handles: 0,
                }),
            ),
            Err(error) => {
                let _ = tx.blocking_send(Err(io::Error::other(error.to_string())));
                metrics.record_request(
                    BlobApiOp::Read,
                    start.elapsed(),
                    Err(error.is::<OrionBlobTooManyOpenHandlesError>()),
                );
            }
        }
    });
}

async fn blob_write_stream_validate(
    session: Arc<Mutex<LibsqlSession>>,
    plan: &BlobWriteStreamPlan,
) -> anyhow::Result<()> {
    let plan = plan.clone();
    tokio::task::spawn_blocking(move || {
        let session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        let handle = session
            .blob_handles
            .get(&plan.blob_id)
            .cloned()
            .ok_or_else(|| {
                HranaProtocolError::new(format!("blob id {} does not exist", plan.blob_id))
            })?;
        if handle.read_only {
            return Err(OrionBlobReadOnlyError::new(plan.blob_id).into());
        }
        let blob = open_blob(&session.conn, &handle)?;
        let size = blob_size_i32(blob.size())?;
        ensure_blob_offset(plan.offset)?;
        ensure_blob_i32_length(plan.expected_len)?;
        ensure_blob_range(plan.offset, plan.expected_len, size)
    })
    .await
    .context("joining libSQL blob stream validation task")?
}

fn spawn_blob_write_stream_task(
    session: Arc<Mutex<LibsqlSession>>,
    plan: BlobWriteStreamPlan,
    chunk_bytes: usize,
    mut rx: mpsc::Receiver<Bytes>,
    metrics: Arc<BlobApiMetrics>,
    start: Instant,
) -> tokio::task::JoinHandle<anyhow::Result<BlobBytesWriteResponse>> {
    tokio::task::spawn_blocking(move || {
        let session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        let handle = session
            .blob_handles
            .get(&plan.blob_id)
            .cloned()
            .ok_or_else(|| {
                HranaProtocolError::new(format!("blob id {} does not exist", plan.blob_id))
            })?;
        if handle.read_only {
            return Err(OrionBlobReadOnlyError::new(plan.blob_id).into());
        }
        let mut blob = open_blob(&session.conn, &handle)?;
        let size = blob_size_i32(blob.size())?;
        ensure_blob_offset(plan.offset)?;
        ensure_blob_i32_length(plan.expected_len)?;
        ensure_blob_range(plan.offset, plan.expected_len, size)?;

        let mut written = 0usize;
        let chunk_bytes = chunk_bytes.max(1);
        while let Some(bytes) = rx.blocking_recv() {
            ensure_blob_length(bytes.len(), chunk_bytes)?;
            blob.write_all_at(&bytes, plan.offset + written)?;
            written += bytes.len();
        }
        ensure!(
            written == plan.expected_len,
            "blob stream body length mismatch: expected {} bytes, received {}",
            plan.expected_len,
            written
        );
        let response = BlobBytesWriteResponse {
            blob_id: plan.blob_id,
            offset: plan.offset,
            bytes_written: written,
            size,
        };
        let stats = BlobRequestStats::from_blob_write(&response, session.blob_handles.len());
        metrics.record_request(BlobApiOp::Write, start.elapsed(), Ok(stats));
        Ok(response)
    })
}

fn blob_read_bytes_http_response(
    baton: Option<String>,
    response: BlobBytesReadResponse,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        axum::http::header::CONTENT_TYPE.as_str(),
        "application/octet-stream",
    );
    insert_optional_header(&mut headers, SESSION_TOKEN_HEADER, baton.as_deref());
    insert_header(&mut headers, BLOB_ID_HEADER, &response.blob_id);
    insert_header(
        &mut headers,
        BLOB_OFFSET_HEADER,
        response.offset.to_string(),
    );
    insert_header(
        &mut headers,
        BLOB_BYTES_READ_HEADER,
        response.bytes_read.to_string(),
    );
    insert_header(&mut headers, BLOB_SIZE_HEADER, response.size.to_string());
    (StatusCode::OK, headers, response.bytes).into_response()
}

fn blob_read_stream_http_response(
    baton: Option<String>,
    plan: BlobReadStreamPlan,
    stream: ReceiverStream<Result<Bytes, io::Error>>,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        axum::http::header::CONTENT_TYPE.as_str(),
        "application/octet-stream",
    );
    insert_optional_header(&mut headers, SESSION_TOKEN_HEADER, baton.as_deref());
    insert_header(&mut headers, BLOB_ID_HEADER, &plan.blob_id);
    insert_header(&mut headers, BLOB_OFFSET_HEADER, plan.offset.to_string());
    insert_header(
        &mut headers,
        BLOB_BYTES_READ_HEADER,
        plan.bytes_read.to_string(),
    );
    insert_header(&mut headers, BLOB_SIZE_HEADER, plan.size.to_string());
    (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
}

fn blob_write_bytes_http_response(
    baton: Option<String>,
    response: BlobBytesWriteResponse,
) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    insert_optional_header(&mut headers, SESSION_TOKEN_HEADER, baton.as_deref());
    insert_header(&mut headers, BLOB_ID_HEADER, &response.blob_id);
    insert_header(
        &mut headers,
        BLOB_OFFSET_HEADER,
        response.offset.to_string(),
    );
    insert_header(
        &mut headers,
        BLOB_BYTES_WRITTEN_HEADER,
        response.bytes_written.to_string(),
    );
    insert_header(&mut headers, BLOB_SIZE_HEADER, response.size.to_string());
    (StatusCode::OK, headers, Bytes::new()).into_response()
}

fn insert_optional_header(headers: &mut HeaderMap, name: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        insert_header(headers, name, value);
    }
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: impl AsRef<str>) {
    if let Ok(value) = HeaderValue::from_str(value.as_ref()) {
        headers.insert(name, value);
    }
}

fn open_blob<'conn>(
    conn: &'conn Connection,
    handle: &BlobHandle,
) -> rusqlite::Result<rusqlite::blob::Blob<'conn>> {
    conn.blob_open(
        handle.schema.as_str(),
        handle.table.as_str(),
        handle.column.as_str(),
        handle.rowid,
        handle.read_only,
    )
}

fn blob_size(conn: &Connection, handle: &BlobHandle) -> anyhow::Result<usize> {
    let blob = open_blob(conn, handle)?;
    blob_size_i32(blob.size())
}

fn blob_size_i32(size: i32) -> anyhow::Result<usize> {
    usize::try_from(size).map_err(|_| anyhow!("negative SQLite blob size {size}"))
}

fn validate_blob_name(label: &str, value: &str) -> anyhow::Result<()> {
    ensure!(!value.is_empty(), "blob {label} must not be empty");
    ensure!(
        !value.as_bytes().contains(&0),
        "blob {label} must not contain NUL bytes"
    );
    Ok(())
}

fn ensure_blob_offset(offset: usize) -> anyhow::Result<()> {
    ensure!(i32::try_from(offset).is_ok(), "blob offset is too large");
    Ok(())
}

fn ensure_base64_blob_length(encoded_length: usize, max_chunk_bytes: usize) -> anyhow::Result<()> {
    let max_encoded_length = max_base64_len(max_chunk_bytes);
    ensure!(
        encoded_length <= max_encoded_length,
        "blob base64 payload is too large: {encoded_length} bytes exceeds encoded limit {max_encoded_length} for max_chunk_bytes={max_chunk_bytes}"
    );
    Ok(())
}

fn ensure_content_length_within_limit(
    headers: &HeaderMap,
    max_chunk_bytes: usize,
) -> anyhow::Result<()> {
    let Some(content_length) = headers.get(axum::http::header::CONTENT_LENGTH) else {
        return Ok(());
    };
    let content_length = content_length
        .to_str()
        .context("blob binary content-length is not valid UTF-8")?
        .parse::<usize>()
        .context("blob binary content-length is not a valid usize")?;
    ensure_blob_length(content_length, max_chunk_bytes)
}

fn expected_stream_length(
    headers: &HeaderMap,
    query_length: Option<usize>,
) -> anyhow::Result<usize> {
    let header_length = optional_content_length(headers)?;
    match (query_length, header_length) {
        (Some(query_length), Some(header_length)) => {
            ensure!(
                query_length == header_length,
                "blob stream length mismatch: query length {query_length} does not match content-length {header_length}"
            );
            ensure_blob_i32_length(query_length)?;
            Ok(query_length)
        }
        (Some(query_length), None) => {
            ensure_blob_i32_length(query_length)?;
            Ok(query_length)
        }
        (None, Some(header_length)) => {
            ensure_blob_i32_length(header_length)?;
            Ok(header_length)
        }
        (None, None) => anyhow::bail!(
            "blob stream writes require either a length query parameter or content-length"
        ),
    }
}

fn optional_content_length(headers: &HeaderMap) -> anyhow::Result<Option<usize>> {
    let Some(content_length) = headers.get(axum::http::header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let content_length = content_length
        .to_str()
        .context("blob binary content-length is not valid UTF-8")?
        .parse::<usize>()
        .context("blob binary content-length is not a valid usize")?;
    Ok(Some(content_length))
}

fn ensure_blob_length(length: usize, max_chunk_bytes: usize) -> anyhow::Result<()> {
    ensure_blob_i32_length(length)?;
    ensure!(
        length <= max_chunk_bytes,
        "blob chunk is too large: {length} bytes exceeds max_chunk_bytes={max_chunk_bytes}"
    );
    Ok(())
}

fn ensure_blob_i32_length(length: usize) -> anyhow::Result<()> {
    ensure!(i32::try_from(length).is_ok(), "blob chunk is too large");
    Ok(())
}

fn ensure_blob_range(offset: usize, length: usize, size: usize) -> anyhow::Result<()> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| anyhow!("blob range is too large"))?;
    ensure!(end <= size, "Blob size is insufficient");
    Ok(())
}

fn split_bytes(bytes: Bytes, max_chunk_bytes: usize) -> impl Iterator<Item = Bytes> {
    let max_chunk_bytes = max_chunk_bytes.max(1);
    let len = bytes.len();
    (0..len)
        .step_by(max_chunk_bytes)
        .map(move |start| bytes.slice(start..(start + max_chunk_bytes).min(len)))
}

fn max_base64_len(max_chunk_bytes: usize) -> usize {
    max_chunk_bytes.div_ceil(3).saturating_mul(4)
}

enum ReadPolicySatisfaction {
    Forwarded(PipelineRespBody),
    Failed(anyhow::Error),
}

async fn satisfy_orion_read_policy(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    request: &PipelineReqBody,
    policy: &OrionReadPolicy,
) -> Result<(), ReadPolicySatisfaction> {
    if should_prefer_leader_for_read_policy(policy)
        && let Some(response) = forward_pipeline_to_leader(state, headers, database, request)
            .await
            .map_err(ReadPolicySatisfaction::Failed)?
    {
        return Err(ReadPolicySatisfaction::Forwarded(response));
    }

    match enforce_orion_read_policy(state, database, policy).await {
        Ok(_) => Ok(()),
        Err(error) => {
            if should_route_read_to_leader(policy)
                && let Some(response) =
                    forward_pipeline_to_leader(state, headers, database, request)
                        .await
                        .map_err(ReadPolicySatisfaction::Failed)?
            {
                return Err(ReadPolicySatisfaction::Forwarded(response));
            }
            Err(ReadPolicySatisfaction::Failed(error))
        }
    }
}

async fn enforce_orion_read_policy(
    state: &LibsqlHttpState,
    database: &str,
    policy: &OrionReadPolicy,
) -> anyhow::Result<OrionSqliteReplicaFreshness> {
    let runtime = state
        .runtime_for_database(database)
        .or_else(|_| state.replication_groups.default_runtime())?;
    let freshness = runtime.replica_freshness().await?;
    match policy {
        OrionReadPolicy::Strong | OrionReadPolicy::RevocationSafe => {
            runtime.ensure_linearizable_read().await?;
        }
        OrionReadPolicy::Session {
            min_applied_index,
            timeout_ms,
        } => {
            if let Some(min_applied_index) = min_applied_index {
                runtime
                    .wait_for_applied_index(
                        *min_applied_index,
                        Duration::from_millis((*timeout_ms).max(1)),
                    )
                    .await?;
            }
        }
        OrionReadPolicy::BoundedStaleness { max_staleness_ms } => {
            if *max_staleness_ms == 0 {
                runtime.ensure_linearizable_read().await?;
                return runtime.replica_freshness().await;
            }
            if !freshness.raft.is_ready_for_linearizable_reads() {
                runtime.ensure_linearizable_read().await?;
                return runtime.replica_freshness().await;
            }
            let Some(closed_commit_ts) = freshness.closed_commit_ts else {
                runtime.ensure_linearizable_read().await?;
                return runtime.replica_freshness().await;
            };
            let read_ts = bounded_staleness_read_timestamp(*max_staleness_ms);
            ensure!(
                freshness.can_serve_at_timestamp(read_ts),
                "local replica closed timestamp {:?} is behind bounded staleness read timestamp {:?}",
                closed_commit_ts,
                read_ts
            );
            let staleness_ms = current_time_millis().saturating_sub(closed_commit_ts.physical_ms);
            ensure!(
                staleness_ms <= *max_staleness_ms,
                "local replica staleness {staleness_ms}ms exceeds bounded staleness read policy \
                 max_staleness_ms={max_staleness_ms}"
            );
        }
        OrionReadPolicy::Local => {}
    }
    runtime.replica_freshness().await
}

fn should_route_read_to_leader(policy: &OrionReadPolicy) -> bool {
    !matches!(policy, OrionReadPolicy::Local)
}

fn should_prefer_leader_for_read_policy(policy: &OrionReadPolicy) -> bool {
    matches!(
        policy,
        OrionReadPolicy::Strong | OrionReadPolicy::RevocationSafe
    )
}

async fn forward_pipeline_to_leader(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    request: &PipelineReqBody,
) -> anyhow::Result<Option<PipelineRespBody>> {
    let runtime = state
        .runtime_for_database(database)
        .or_else(|_| state.replication_groups.default_runtime())?;
    let metrics = runtime.metrics();
    let Some(leader_id) = metrics.current_leader else {
        return Ok(None);
    };
    if leader_id == state.node_id {
        return Ok(None);
    }
    let Some(endpoint) = state.peer_http_endpoints.get(&leader_id) else {
        return Ok(None);
    };

    let url = format!(
        "{}/{}/v2/pipeline",
        endpoint.trim_end_matches('/'),
        database
    );
    let mut builder = state.http_client.post(url).json(request);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    for header in [
        READ_POLICY_HEADER,
        MIN_APPLIED_INDEX_HEADER,
        SESSION_TOKEN_HEADER,
        READ_TIMEOUT_MS_HEADER,
        MAX_STALENESS_MS_HEADER,
        IDEMPOTENCY_KEY_HEADER,
    ] {
        if let Some(value) = headers.get(header)
            && let Ok(value) = value.to_str()
        {
            builder = builder.header(header, value);
        }
    }

    let mut response: PipelineRespBody = builder.send().await?.error_for_status()?.json().await?;
    if let Some(orion) = response.orion.as_mut() {
        orion.forwarded_from_node_id = Some(state.node_id);
    }
    Ok(Some(response))
}

async fn forward_standby_action_to_target_leader<T>(
    state: &LibsqlHttpState,
    headers: &HeaderMap,
    database: &str,
    target_group_id: &str,
    action: &str,
    request: &T,
) -> anyhow::Result<Option<serde_json::Value>>
where
    T: Serialize + ?Sized,
{
    validate_database_name(database)?;
    validate_replication_group_id(target_group_id)?;
    let runtime = match state.replication_groups.runtime(target_group_id) {
        Ok(runtime) => runtime,
        Err(_) => return Ok(None),
    };
    let Some(leader_id) = runtime.metrics().current_leader else {
        return Ok(None);
    };
    if leader_id == state.node_id {
        return Ok(None);
    }
    let target_group = match read_replication_group_record(state, target_group_id)? {
        Some(group) => group,
        None => return Ok(None),
    };
    let local_is_target_member = target_group
        .members
        .iter()
        .any(|member| member.node_id == state.node_id);
    let leader_is_target_member = target_group
        .members
        .iter()
        .any(|member| member.node_id == leader_id);
    if !local_is_target_member || !leader_is_target_member {
        return Ok(None);
    }
    let Some(endpoint) = http_endpoint_for_node(state, leader_id) else {
        return Ok(None);
    };
    let url = format!(
        "{}/_orion/databases/{}/placement/{}",
        endpoint.trim_end_matches('/'),
        database,
        action
    );
    let mut builder = state.http_client.post(url).json(request);
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(auth) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    let response = builder
        .send()
        .await
        .with_context(|| format!("forwarding standby {action} to node {leader_id}"))?;
    let status = response.status();
    let body = response
        .json::<serde_json::Value>()
        .await
        .with_context(|| {
            format!("reading forwarded standby {action} response from node {leader_id}")
        })?;
    if !status.is_success() {
        anyhow::bail!("forwarded standby {action} HTTP {status}: {body}");
    }
    Ok(Some(body))
}

fn http_endpoint_for_node(state: &LibsqlHttpState, node_id: u64) -> Option<String> {
    state
        .peer_http_endpoints
        .get(&node_id)
        .cloned()
        .or_else(|| {
            state
                .placement_nodes
                .get(&node_id)
                .and_then(|node| node.libsql_http_addr.clone())
        })
}

fn bounded_staleness_read_timestamp(max_staleness_ms: u64) -> HybridTimestamp {
    HybridTimestamp {
        physical_ms: current_time_millis().saturating_sub(max_staleness_ms),
        logical: 0,
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

fn orion_read_policy_from_headers(headers: &HeaderMap) -> anyhow::Result<OrionReadPolicy> {
    let policy = optional_header_str(headers, READ_POLICY_HEADER)?;
    match policy.unwrap_or("strong").to_ascii_lowercase().as_str() {
        "strong" => Ok(OrionReadPolicy::Strong),
        "revocation_safe" | "revocation-safe" => Ok(OrionReadPolicy::RevocationSafe),
        "session" => Ok(OrionReadPolicy::Session {
            min_applied_index: optional_u64_header(headers, MIN_APPLIED_INDEX_HEADER)?
                .or(optional_session_token_header(headers)?.map(|token| token.applied_index)),
            timeout_ms: optional_u64_header(headers, READ_TIMEOUT_MS_HEADER)?
                .unwrap_or(DEFAULT_READ_TIMEOUT_MS),
        }),
        "bounded_staleness" | "bounded-staleness" => {
            let max_staleness_ms =
                optional_u64_header(headers, MAX_STALENESS_MS_HEADER)?.ok_or_else(|| {
                    HranaProtocolError::new(format!(
                        "{MAX_STALENESS_MS_HEADER} is required when {READ_POLICY_HEADER} is bounded_staleness"
                    ))
                })?;
            Ok(OrionReadPolicy::BoundedStaleness { max_staleness_ms })
        }
        "local" => Ok(OrionReadPolicy::Local),
        other => Err(HranaProtocolError::new(format!(
            "unsupported {READ_POLICY_HEADER} value {other:?}"
        ))
        .into()),
    }
}

fn optional_header_str<'a>(headers: &'a HeaderMap, name: &str) -> anyhow::Result<Option<&'a str>> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map_err(|_| {
                HranaProtocolError::new(format!("{name} must contain visible ASCII")).into()
            })
        })
        .transpose()
}

fn optional_u64_header(headers: &HeaderMap, name: &str) -> anyhow::Result<Option<u64>> {
    optional_header_str(headers, name)?
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                HranaProtocolError::new(format!("{name} must be an unsigned integer")).into()
            })
        })
        .transpose()
}

fn optional_session_token_header(headers: &HeaderMap) -> anyhow::Result<Option<OrionSessionToken>> {
    let Some(value) = optional_header_str(headers, SESSION_TOKEN_HEADER)? else {
        return Ok(None);
    };
    let mut parts = value.split(':');
    let applied_index = parts
        .next()
        .ok_or_else(|| HranaProtocolError::new("session token missing applied index"))?
        .parse::<u64>()
        .map_err(|_| {
            HranaProtocolError::new("session token applied index must be an unsigned integer")
        })?;
    let physical_ms = parts
        .next()
        .ok_or_else(|| HranaProtocolError::new("session token missing physical timestamp"))?
        .parse::<u64>()
        .map_err(|_| {
            HranaProtocolError::new("session token physical timestamp must be an unsigned integer")
        })?;
    let logical = parts
        .next()
        .ok_or_else(|| HranaProtocolError::new("session token missing logical timestamp"))?
        .parse::<u32>()
        .map_err(|_| {
            HranaProtocolError::new("session token logical timestamp must be an unsigned integer")
        })?;
    if parts.next().is_some() {
        return Err(HranaProtocolError::new(format!(
            "{SESSION_TOKEN_HEADER} must be formatted as applied_index:physical_ms:logical"
        ))
        .into());
    }
    Ok(Some(OrionSessionToken {
        applied_index,
        commit_ts: HybridTimestamp {
            physical_ms,
            logical,
        },
        token: value.to_string(),
    }))
}

fn pipeline_requires_fresh_read(request: &PipelineReqBody) -> bool {
    request
        .requests
        .iter()
        .any(stream_request_requires_fresh_read)
}

fn pipeline_should_enforce_read_policy(
    request: &PipelineReqBody,
    policy: &OrionReadPolicy,
) -> bool {
    pipeline_requires_fresh_read(request)
        || (!matches!(policy, OrionReadPolicy::Local) && pipeline_may_observe_database(request))
}

fn pipeline_may_observe_database(request: &PipelineReqBody) -> bool {
    request
        .requests
        .iter()
        .any(stream_request_may_observe_database)
}

fn pipeline_may_write(request: &PipelineReqBody) -> bool {
    request.requests.iter().any(stream_request_may_write)
}

fn pipeline_has_transaction_control(request: &PipelineReqBody) -> bool {
    request
        .requests
        .iter()
        .any(stream_request_has_transaction_control)
}

fn stream_request_has_transaction_control(request: &StreamRequest) -> bool {
    match request {
        StreamRequest::Execute { stmt } => stmt_has_transaction_control(stmt),
        StreamRequest::Batch { batch } => batch
            .steps
            .iter()
            .any(|step| stmt_has_transaction_control(&step.stmt)),
        StreamRequest::Sequence { sql, sql_id } => {
            sql_id.is_some() || sql_has_transaction_control(sql.as_deref())
        }
        StreamRequest::Close
        | StreamRequest::Describe { .. }
        | StreamRequest::StoreSql { .. }
        | StreamRequest::CloseSql { .. }
        | StreamRequest::GetAutocommit => false,
    }
}

fn stmt_has_transaction_control(stmt: &Stmt) -> bool {
    stmt.sql_id.is_some() || sql_has_transaction_control(stmt.sql.as_deref())
}

fn sql_has_transaction_control(sql: Option<&str>) -> bool {
    let Some(sql) = sql else {
        return true;
    };
    let token = first_sql_token(sql);
    matches!(
        token.as_str(),
        "begin" | "commit" | "rollback" | "savepoint" | "release"
    )
}

fn stream_request_may_write(request: &StreamRequest) -> bool {
    match request {
        StreamRequest::Execute { stmt } => stmt_may_write(stmt),
        StreamRequest::Batch { batch } => batch.steps.iter().any(|step| stmt_may_write(&step.stmt)),
        StreamRequest::Sequence { sql, sql_id } => {
            sql_id.is_some() || sql_may_write(sql.as_deref())
        }
        StreamRequest::Close
        | StreamRequest::Describe { .. }
        | StreamRequest::StoreSql { .. }
        | StreamRequest::CloseSql { .. }
        | StreamRequest::GetAutocommit => false,
    }
}

fn stream_request_may_observe_database(request: &StreamRequest) -> bool {
    matches!(
        request,
        StreamRequest::Execute { .. }
            | StreamRequest::Batch { .. }
            | StreamRequest::Sequence { .. }
            | StreamRequest::Describe { .. }
    )
}

fn stmt_may_write(stmt: &Stmt) -> bool {
    stmt.sql_id.is_some() || sql_may_write(stmt.sql.as_deref())
}

fn stream_request_requires_fresh_read(request: &StreamRequest) -> bool {
    match request {
        StreamRequest::Execute { stmt } => stmt_requires_fresh_read(stmt),
        StreamRequest::Batch { batch } => batch
            .steps
            .iter()
            .any(|step| stmt_requires_fresh_read(&step.stmt)),
        StreamRequest::Describe { .. } => true,
        StreamRequest::Sequence { sql, sql_id } => sql_id.is_some() || sql_may_read(sql.as_deref()),
        StreamRequest::Close
        | StreamRequest::StoreSql { .. }
        | StreamRequest::CloseSql { .. }
        | StreamRequest::GetAutocommit => false,
    }
}

fn stmt_requires_fresh_read(stmt: &Stmt) -> bool {
    stmt.want_rows || stmt.sql_id.is_some() || sql_may_read(stmt.sql.as_deref())
}

fn sql_may_read(sql: Option<&str>) -> bool {
    let Some(sql) = sql else {
        return true;
    };
    let token = first_sql_token(sql);
    matches!(
        token.as_str(),
        "select" | "with" | "pragma" | "explain" | "values"
    )
}

fn sql_may_write(sql: Option<&str>) -> bool {
    !sql_may_read(sql)
}

fn first_sql_token(sql: &str) -> String {
    sql.trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

async fn run_stream_request(
    session: Arc<Mutex<LibsqlSession>>,
    request: StreamRequest,
) -> anyhow::Result<StreamResult> {
    match request {
        StreamRequest::Close => Ok(StreamResult::ok(StreamResponse::Close)),
        StreamRequest::Execute { stmt } => {
            let result = tokio::task::spawn_blocking(move || {
                let mut session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                execute_stmt(&mut session, stmt)
            })
            .await
            .context("joining libSQL execute task")??;
            Ok(StreamResult::ok(StreamResponse::Execute { result }))
        }
        StreamRequest::Batch { batch } => {
            let result = tokio::task::spawn_blocking(move || {
                let mut session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                execute_batch(&mut session, batch)
            })
            .await
            .context("joining libSQL batch task")??;
            Ok(StreamResult::ok(StreamResponse::Batch { result }))
        }
        StreamRequest::Sequence { sql, sql_id } => {
            tokio::task::spawn_blocking(move || {
                let session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                let sql = resolve_sql(&session, StmtSql { sql, sql_id })?;
                reject_denied_sql_surface(&sql)?;
                session.conn.execute_batch(&sql)?;
                anyhow::Ok(())
            })
            .await
            .context("joining libSQL sequence task")??;
            Ok(StreamResult::ok(StreamResponse::Sequence))
        }
        StreamRequest::Describe { sql, sql_id } => {
            let result = tokio::task::spawn_blocking(move || {
                let session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                let sql = resolve_sql(&session, StmtSql { sql, sql_id })?;
                describe_sql(&session.conn, &sql)
            })
            .await
            .context("joining libSQL describe task")??;
            Ok(StreamResult::ok(StreamResponse::Describe { result }))
        }
        StreamRequest::StoreSql { sql_id, sql } => {
            tokio::task::spawn_blocking(move || {
                let mut session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                session.stored_sql.insert(sql_id, sql);
                anyhow::Ok(())
            })
            .await
            .context("joining libSQL store_sql task")??;
            Ok(StreamResult::ok(StreamResponse::StoreSql))
        }
        StreamRequest::CloseSql { sql_id } => {
            tokio::task::spawn_blocking(move || {
                let mut session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                session.stored_sql.remove(&sql_id).ok_or_else(|| {
                    HranaProtocolError::new(format!("stored SQL id {sql_id} does not exist"))
                })?;
                anyhow::Ok(())
            })
            .await
            .context("joining libSQL close_sql task")??;
            Ok(StreamResult::ok(StreamResponse::CloseSql))
        }
        StreamRequest::GetAutocommit => Ok(StreamResult::ok(StreamResponse::GetAutocommit {
            is_autocommit: tokio::task::spawn_blocking(move || {
                let session = session
                    .lock()
                    .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
                Ok::<_, anyhow::Error>(session.conn.is_autocommit())
            })
            .await
            .context("joining libSQL autocommit task")??,
        })),
    }
}

async fn run_idempotent_pipeline(
    session: Arc<Mutex<LibsqlSession>>,
    requests: Vec<StreamRequest>,
    idempotency: IdempotencyContext,
) -> anyhow::Result<IdempotentPipelineOutcome> {
    tokio::task::spawn_blocking(move || {
        let mut session = session
            .lock()
            .map_err(|_| anyhow!("libSQL session mutex poisoned"))?;
        run_idempotent_pipeline_locked(&mut session, requests, idempotency)
    })
    .await
    .context("joining libSQL idempotent pipeline task")?
}

fn run_idempotent_pipeline_locked(
    session: &mut LibsqlSession,
    requests: Vec<StreamRequest>,
    idempotency: IdempotencyContext,
) -> anyhow::Result<IdempotentPipelineOutcome> {
    ensure_idempotency_schema(&session.conn)?;
    if let Some(stored) = read_idempotency_record(&session.conn, &idempotency.key)? {
        return resolve_stored_idempotency_record(
            &session.conn,
            idempotency,
            stored,
            IDEMPOTENCY_PENDING_RECONCILE_TIMEOUT,
        );
    }

    session.conn.execute_batch("begin immediate")?;
    let outcome = (|| {
        insert_pending_idempotency_record(&session.conn, &idempotency)?;
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let result = run_stream_request_locked(session, request);
            results.push(result.unwrap_or_else(|error| {
                StreamResult::error(error.to_string(), sqlite_error_code(error.as_ref()))
            }));
        }
        let results_json = serde_json::to_string(&results)?;
        mark_idempotency_record_committed(&session.conn, &idempotency.key, &results_json)?;
        anyhow::Ok(results)
    })();

    match outcome {
        Ok(results) => {
            session.conn.execute_batch("commit")?;
            Ok(IdempotentPipelineOutcome {
                results,
                outcome: OrionIdempotencyMetadata {
                    key: idempotency.key,
                    request_hash: idempotency.request_hash,
                    status: "committed".to_string(),
                    reused: Some(false),
                },
            })
        }
        Err(error) => {
            let _ = session.conn.execute_batch("rollback");
            Err(error)
        }
    }
}

fn resolve_stored_idempotency_record(
    conn: &Connection,
    idempotency: IdempotencyContext,
    mut stored: StoredIdempotencyRecord,
    pending_reconcile_timeout: Duration,
) -> anyhow::Result<IdempotentPipelineOutcome> {
    let started = Instant::now();
    loop {
        ensure!(
            stored.request_hash == idempotency.request_hash,
            "idempotency key conflict: key was already used with a different request"
        );
        if stored.status == "committed" {
            let results = serde_json::from_str(&stored.results_json)
                .context("stored idempotency response is not valid JSON")?;
            return Ok(IdempotentPipelineOutcome {
                results,
                outcome: OrionIdempotencyMetadata {
                    key: idempotency.key,
                    request_hash: idempotency.request_hash,
                    status: "committed".to_string(),
                    reused: Some(true),
                },
            });
        }
        ensure!(
            stored.status == "pending",
            "idempotency record has unknown status '{}'",
            stored.status
        );
        if started.elapsed() >= pending_reconcile_timeout {
            return Err(anyhow!(
                "idempotency key is pending; retry later with the same request"
            ));
        }
        std::thread::sleep(IDEMPOTENCY_PENDING_RECONCILE_POLL);
        stored = read_idempotency_record(conn, &idempotency.key)?.ok_or_else(|| {
            anyhow!("idempotency key is pending; retry later with the same request")
        })?;
    }
}

fn run_stream_request_locked(
    session: &mut LibsqlSession,
    request: StreamRequest,
) -> anyhow::Result<StreamResult> {
    match request {
        StreamRequest::Close => Ok(StreamResult::ok(StreamResponse::Close)),
        StreamRequest::Execute { stmt } => Ok(StreamResult::ok(StreamResponse::Execute {
            result: execute_stmt(session, stmt)?,
        })),
        StreamRequest::Batch { batch } => Ok(StreamResult::ok(StreamResponse::Batch {
            result: execute_batch(session, batch)?,
        })),
        StreamRequest::Sequence { sql, sql_id } => {
            let sql = resolve_sql(session, StmtSql { sql, sql_id })?;
            reject_denied_sql_surface(&sql)?;
            session.conn.execute_batch(&sql)?;
            Ok(StreamResult::ok(StreamResponse::Sequence))
        }
        StreamRequest::Describe { sql, sql_id } => {
            let sql = resolve_sql(session, StmtSql { sql, sql_id })?;
            Ok(StreamResult::ok(StreamResponse::Describe {
                result: describe_sql(&session.conn, &sql)?,
            }))
        }
        StreamRequest::StoreSql { sql_id, sql } => {
            session.stored_sql.insert(sql_id, sql);
            Ok(StreamResult::ok(StreamResponse::StoreSql))
        }
        StreamRequest::CloseSql { sql_id } => {
            session.stored_sql.remove(&sql_id).ok_or_else(|| {
                HranaProtocolError::new(format!("stored SQL id {sql_id} does not exist"))
            })?;
            Ok(StreamResult::ok(StreamResponse::CloseSql))
        }
        StreamRequest::GetAutocommit => Ok(StreamResult::ok(StreamResponse::GetAutocommit {
            is_autocommit: session.conn.is_autocommit(),
        })),
    }
}

#[derive(Debug)]
struct StoredIdempotencyRecord {
    request_hash: String,
    status: String,
    results_json: String,
}

fn ensure_idempotency_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(&format!(
        r#"
        create table if not exists {IDEMPOTENCY_TABLE} (
            key text primary key,
            request_hash text not null,
            status text not null check (status in ('pending', 'committed')),
            results_json text not null default '[]',
            created_at_ms integer not null,
            updated_at_ms integer not null
        );
        "#
    ))?;
    Ok(())
}

fn read_idempotency_record(
    conn: &Connection,
    key: &str,
) -> anyhow::Result<Option<StoredIdempotencyRecord>> {
    Ok(conn
        .query_row(
            &format!(
                "select request_hash, status, results_json from {IDEMPOTENCY_TABLE} where key = ?"
            ),
            [key],
            |row| {
                Ok(StoredIdempotencyRecord {
                    request_hash: row.get(0)?,
                    status: row.get(1)?,
                    results_json: row.get(2)?,
                })
            },
        )
        .optional()?)
}

fn insert_pending_idempotency_record(
    conn: &Connection,
    idempotency: &IdempotencyContext,
) -> anyhow::Result<()> {
    let now = sqlite_i64(current_time_millis());
    conn.execute(
        &format!(
            "insert into {IDEMPOTENCY_TABLE} \
             (key, request_hash, status, results_json, created_at_ms, updated_at_ms) \
             values (?, ?, 'pending', '[]', ?, ?)"
        ),
        params![&idempotency.key, &idempotency.request_hash, now, now],
    )?;
    Ok(())
}

fn mark_idempotency_record_committed(
    conn: &Connection,
    key: &str,
    results_json: &str,
) -> anyhow::Result<()> {
    conn.execute(
        &format!(
            "update {IDEMPOTENCY_TABLE} \
             set status = 'committed', results_json = ?, updated_at_ms = ? \
             where key = ?"
        ),
        params![results_json, sqlite_i64(current_time_millis()), key],
    )?;
    Ok(())
}

fn collect_idempotency_garbage_for_connection(
    conn: &Connection,
    config: &LibsqlHttpIdempotencyConfig,
    max_records: usize,
) -> anyhow::Result<IdempotencyGcStats> {
    if max_records == 0 {
        return Ok(IdempotencyGcStats::default());
    }
    ensure_idempotency_schema(conn)?;
    let now = current_time_millis();
    let committed_before = sqlite_i64(now.saturating_sub(config.committed_ttl_ms));
    let pending_before = sqlite_i64(now.saturating_sub(config.pending_ttl_ms));
    let deleted_committed =
        delete_idempotency_records(conn, "committed", committed_before, max_records)?;
    let remaining = max_records.saturating_sub(deleted_committed);
    let deleted_pending = delete_idempotency_records(conn, "pending", pending_before, remaining)?;
    Ok(IdempotencyGcStats {
        deleted_committed,
        deleted_pending,
    })
}

fn delete_idempotency_records(
    conn: &Connection,
    status: &str,
    updated_before_ms: i64,
    limit: usize,
) -> anyhow::Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let rowids = {
        let mut stmt = conn.prepare(&format!(
            "select rowid from {IDEMPOTENCY_TABLE} \
             where status = ? and updated_at_ms < ? \
             order by updated_at_ms asc \
             limit ?"
        ))?;
        stmt.query_map(
            params![status, updated_before_ms, sqlite_usize(limit)],
            |row| row.get::<_, i64>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?
    };
    let tx = conn.unchecked_transaction()?;
    for rowid in &rowids {
        tx.execute(
            &format!("delete from {IDEMPOTENCY_TABLE} where rowid = ?"),
            [rowid],
        )?;
    }
    tx.commit()?;
    Ok(rowids.len())
}

fn execute_batch(session: &mut LibsqlSession, batch: Batch) -> anyhow::Result<BatchResult> {
    let mut step_results = Vec::with_capacity(batch.steps.len());
    let mut step_errors = Vec::with_capacity(batch.steps.len());
    for step in batch.steps {
        if !batch_condition_matches(
            step.condition.as_ref(),
            &step_results,
            &step_errors,
            session,
        ) {
            step_results.push(None);
            step_errors.push(None);
            continue;
        }
        match execute_stmt(session, step.stmt) {
            Ok(result) => {
                step_results.push(Some(result));
                step_errors.push(None);
            }
            Err(error) => {
                step_results.push(None);
                step_errors.push(Some(ErrorBody {
                    message: error.to_string(),
                    code: Some(sqlite_error_code(error.as_ref()).to_string()),
                }));
            }
        }
    }
    Ok(BatchResult {
        step_results,
        step_errors,
    })
}

fn execute_stmt(session: &mut LibsqlSession, stmt: Stmt) -> anyhow::Result<StmtResult> {
    let sql = resolve_sql(session, StmtSql::from(&stmt))?;
    reject_denied_sql_surface(&sql)?;
    let want_rows = stmt.want_rows;
    let args = stmt_to_params(stmt)?;
    if let Some(result) = system_query_result(session.system_context.as_ref(), &sql, &args)? {
        return Ok(result);
    }
    let mut prepared = session.conn.prepare(&sql)?;
    let cols = columns_for_statement(&prepared);

    if want_rows || prepared.column_count() > 0 {
        let mut rows = query_statement(&mut prepared, &args)?;
        let mut out_rows = Vec::new();
        while let Some(row) = rows.next()? {
            out_rows.push(row_to_values(row, cols.len())?);
        }
        Ok(StmtResult {
            cols,
            rows: out_rows,
            affected_row_count: session.conn.changes(),
            last_insert_rowid: Some(session.conn.last_insert_rowid().to_string()),
        })
    } else {
        let affected = execute_statement(&mut prepared, &args)?;
        Ok(StmtResult {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count: affected as u64,
            last_insert_rowid: Some(session.conn.last_insert_rowid().to_string()),
        })
    }
}

fn system_query_result(
    context: Option<&SystemQueryContext>,
    sql: &str,
    args: &StatementParams,
) -> anyhow::Result<Option<StmtResult>> {
    let Some(context) = context else {
        return Ok(None);
    };
    let normalized = normalize_system_sql(sql);
    let needs_raft_metrics = references_virtual_table(&normalized, "raft_metrics");
    let needs_storage_pressure = references_virtual_table(&normalized, "storage_pressure");
    let needs_large_payload_metrics =
        references_virtual_table(&normalized, "large_payload_metrics");
    let needs_idempotency_metrics = references_virtual_table(&normalized, "idempotency_metrics");
    let needs_database_catalog = references_virtual_table(&normalized, "database_catalog");
    let needs_placement_nodes = references_virtual_table(&normalized, "placement_nodes");
    let needs_database_placement = references_virtual_table(&normalized, "database_placement");
    let needs_database_standbys = references_virtual_table(&normalized, "database_standbys");
    let needs_placement_metrics = references_virtual_table(&normalized, "placement_metrics");
    if !needs_raft_metrics
        && !needs_storage_pressure
        && !needs_large_payload_metrics
        && !needs_idempotency_metrics
        && !needs_database_catalog
        && !needs_placement_nodes
        && !needs_database_placement
        && !needs_database_standbys
        && !needs_placement_metrics
    {
        return Ok(None);
    }

    let conn = Connection::open_in_memory()?;
    if needs_raft_metrics {
        create_virtual_raft_metrics_table(&conn)?;
        populate_virtual_raft_metrics_table(&conn, context.metrics_registry.snapshot())?;
    }
    if needs_storage_pressure {
        create_virtual_storage_pressure_table(&conn)?;
        let metrics = context.tokio_handle.block_on(sqlite_storage_pressure(
            &context.sqlite_state,
            &context.compaction_policy,
        ))?;
        populate_virtual_storage_pressure_table(&conn, metrics)?;
    }
    if needs_large_payload_metrics {
        create_virtual_large_payload_metrics_table(&conn)?;
        let rows = context
            .tokio_handle
            .block_on(context.replication_groups.large_payload_metrics())?;
        populate_virtual_large_payload_metrics_table(&conn, rows)?;
    }
    if needs_idempotency_metrics {
        create_virtual_idempotency_metrics_table(&conn)?;
        populate_virtual_idempotency_metrics_table(
            &conn,
            context
                .idempotency_metrics
                .snapshot(&context.idempotency_config, 0),
        )?;
    }
    if needs_database_catalog {
        create_virtual_database_catalog_table(&conn)?;
        populate_virtual_database_catalog_table(&conn, context.catalog_db.as_deref())?;
    }
    if needs_placement_nodes {
        create_virtual_placement_nodes_table(&conn)?;
        populate_virtual_placement_nodes_table(&conn, context.placement_nodes())?;
    }
    if needs_database_placement {
        create_virtual_database_placement_table(&conn)?;
        populate_virtual_database_placement_table(&conn, context.catalog_db.as_deref())?;
    }
    if needs_database_standbys {
        create_virtual_database_standbys_table(&conn)?;
        populate_virtual_database_standbys_table(&conn, context.catalog_db.as_deref())?;
    }
    if needs_placement_metrics {
        create_virtual_placement_metrics_table(&conn)?;
        let rows = placement_metrics_phase_rows_from_catalog(context.catalog_db.as_deref())?;
        populate_virtual_placement_metrics_table(&conn, rows)?;
    }
    Ok(Some(query_virtual_system_table(
        &conn,
        &rewrite_virtual_system_sql(sql),
        args,
    )?))
}

fn normalize_system_sql(sql: &str) -> String {
    strip_sql_trailing_semicolon(sql)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn strip_sql_trailing_semicolon(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

fn references_virtual_table(normalized_sql: &str, table: &str) -> bool {
    normalized_sql.contains(&format!(" from {table}"))
        || normalized_sql.contains(&format!(" join {table}"))
        || normalized_sql.contains(&format!(" from _orion.{table}"))
        || normalized_sql.contains(&format!(" join _orion.{table}"))
}

fn rewrite_virtual_system_sql(sql: &str) -> String {
    strip_sql_trailing_semicolon(sql)
        .replace("_orion.raft_metrics", "raft_metrics")
        .replace("_orion.storage_pressure", "storage_pressure")
        .replace("_orion.large_payload_metrics", "large_payload_metrics")
        .replace("_orion.idempotency_metrics", "idempotency_metrics")
        .replace("_orion.database_catalog", "database_catalog")
        .replace("_orion.placement_nodes", "placement_nodes")
        .replace("_orion.database_placement", "database_placement")
        .replace("_orion.database_standbys", "database_standbys")
        .replace("_orion.placement_metrics", "placement_metrics")
}

fn create_virtual_raft_metrics_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table raft_metrics (
            node_id integer not null,
            observed_at_ms integer not null,
            observed_age_ms integer not null,
            stale_after_ms integer not null,
            stale integer not null,
            state text not null,
            running integer not null,
            current_term integer not null,
            current_leader integer,
            last_log_index integer,
            committed_index integer,
            applied_index integer,
            snapshot_index integer,
            purged_index integer,
            ready_for_linearizable_reads integer not null,
            voters_json text not null,
            learners_json text not null,
            replication_json text not null
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_raft_metrics_table(
    conn: &Connection,
    entries: Vec<ClusterRaftMetricsEntry>,
) -> anyhow::Result<()> {
    let now = current_time_millis();
    let mut stmt = conn.prepare(
        r#"
        insert into raft_metrics values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        "#,
    )?;
    for entry in entries {
        let metrics = entry.metrics;
        let observed_age_ms = now.saturating_sub(entry.observed_at_ms);
        let ready_for_linearizable_reads = metrics.is_ready_for_linearizable_reads();
        stmt.execute(params![
            sqlite_i64(metrics.node_id),
            sqlite_i64(entry.observed_at_ms),
            sqlite_i64(observed_age_ms),
            sqlite_i64(RAFT_METRICS_STALE_AFTER_MS),
            i64::from(observed_age_ms > RAFT_METRICS_STALE_AFTER_MS),
            metrics.state,
            i64::from(metrics.running),
            sqlite_i64(metrics.current_term),
            metrics.current_leader.map(sqlite_i64),
            metrics.last_log_index.map(sqlite_i64),
            metrics.committed_index.map(sqlite_i64),
            metrics.applied_index.map(sqlite_i64),
            metrics.snapshot_index.map(sqlite_i64),
            metrics.purged_index.map(sqlite_i64),
            i64::from(ready_for_linearizable_reads),
            serde_json::to_string(&metrics.voter_ids)?,
            serde_json::to_string(&metrics.learner_ids)?,
            serde_json::to_string(&metrics.replication)?,
        ])?;
    }
    Ok(())
}

fn create_virtual_storage_pressure_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table storage_pressure (
            databases integer not null,
            files integer not null,
            current_pages integer not null,
            obsolete_page_versions integer not null,
            obsolete_versions integer not null,
            bytes_scanned integer not null,
            obsolete_bytes integer not null,
            compaction_eligible_files integer not null
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_storage_pressure_table(
    conn: &Connection,
    metrics: SqliteStoragePressureMetrics,
) -> anyhow::Result<()> {
    conn.execute(
        "insert into storage_pressure values (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            sqlite_usize(metrics.databases),
            sqlite_usize(metrics.files),
            sqlite_usize(metrics.current_pages),
            sqlite_usize(metrics.obsolete_page_versions),
            sqlite_usize(metrics.obsolete_versions),
            sqlite_i64(metrics.bytes_scanned),
            sqlite_i64(metrics.obsolete_bytes),
            sqlite_usize(metrics.compaction_eligible_files),
        ],
    )?;
    Ok(())
}

fn create_virtual_large_payload_metrics_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table large_payload_metrics (
            group_id text not null,
            loaded_at_ms integer not null,
            uploads_started integer not null,
            chunks_staged integer not null,
            bytes_staged integer not null,
            uploads_committed integer not null,
            bytes_committed integer not null,
            uploads_aborted integer not null,
            uploads_rejected integer not null,
            cleanup_runs integer not null,
            cleanup_uploads integer not null,
            active_uploads integer not null,
            active_bytes integer not null
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_large_payload_metrics_table(
    conn: &Connection,
    rows: Vec<LargePayloadMetricsRow>,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        r#"
        insert into large_payload_metrics values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        "#,
    )?;
    for row in rows {
        let metrics = row.metrics;
        stmt.execute(params![
            row.group_id,
            sqlite_i64(row.loaded_at_ms),
            sqlite_i64(metrics.uploads_started),
            sqlite_i64(metrics.chunks_staged),
            sqlite_i64(metrics.bytes_staged),
            sqlite_i64(metrics.uploads_committed),
            sqlite_i64(metrics.bytes_committed),
            sqlite_i64(metrics.uploads_aborted),
            sqlite_i64(metrics.uploads_rejected),
            sqlite_i64(metrics.cleanup_runs),
            sqlite_i64(metrics.cleanup_uploads),
            sqlite_i64(metrics.active_uploads),
            sqlite_i64(metrics.active_bytes),
        ])?;
    }
    Ok(())
}

fn create_virtual_idempotency_metrics_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table idempotency_metrics (
            enabled integer not null,
            committed_ttl_ms integer not null,
            pending_ttl_ms integer not null,
            gc_interval_ms integer not null,
            gc_max_records_per_pass integer not null,
            requests integer not null,
            committed_new integer not null,
            committed_reused integer not null,
            conflicts integer not null,
            commit_unknown integer not null,
            rejected integer not null,
            gc_runs integer not null,
            gc_failures integer not null,
            gc_deleted_committed integer not null,
            gc_deleted_pending integer not null
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_idempotency_metrics_table(
    conn: &Connection,
    metrics: IdempotencyMetricsSnapshot,
) -> anyhow::Result<()> {
    conn.execute(
        r#"
        insert into idempotency_metrics values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        "#,
        params![
            i64::from(metrics.enabled),
            sqlite_i64(metrics.committed_ttl_ms),
            sqlite_i64(metrics.pending_ttl_ms),
            sqlite_i64(metrics.gc_interval_ms),
            sqlite_i64(metrics.gc_max_records_per_pass),
            sqlite_i64(metrics.requests),
            sqlite_i64(metrics.committed_new),
            sqlite_i64(metrics.committed_reused),
            sqlite_i64(metrics.conflicts),
            sqlite_i64(metrics.commit_unknown),
            sqlite_i64(metrics.rejected),
            sqlite_i64(metrics.gc.runs),
            sqlite_i64(metrics.gc.failures),
            sqlite_i64(metrics.gc.deleted_committed),
            sqlite_i64(metrics.gc.deleted_pending),
        ],
    )?;
    Ok(())
}

fn create_virtual_database_catalog_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table database_catalog (
            database_id text not null,
            name text not null,
            state text not null,
            object_prefix text not null,
            replication_group_id text not null,
            generation integer not null,
            created_at_ms integer not null,
            updated_at_ms integer not null,
            deleted_at_ms integer,
            purged_at_ms integer,
            purge_error text,
            error text
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_database_catalog_table(
    conn: &Connection,
    catalog_db: Option<&OrionSqliteDb>,
) -> anyhow::Result<()> {
    let Some(catalog_db) = catalog_db else {
        return Ok(());
    };
    let catalog_conn = catalog_db.connect()?;
    ensure_database_catalog_schema(&catalog_conn)?;
    let sql = database_catalog_select_sql(&catalog_conn, None)?;
    let mut rows = catalog_conn.prepare(&sql)?;
    let records = rows
        .query_map([], database_catalog_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert = conn.prepare(
        r#"
        insert into database_catalog values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )?;
    for record in records {
        insert.execute(params![
            record.database_id,
            record.name,
            record.state,
            record.object_prefix,
            record.replication_group_id,
            sqlite_i64(record.generation),
            sqlite_i64(record.created_at_ms),
            sqlite_i64(record.updated_at_ms),
            record.deleted_at_ms.map(sqlite_i64),
            record.purged_at_ms.map(sqlite_i64),
            record.purge_error,
            record.error,
        ])?;
    }
    Ok(())
}

fn create_virtual_placement_nodes_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table placement_nodes (
            node_id integer not null,
            cloud text not null,
            region text not null,
            zone text not null,
            raft_addr text not null,
            libsql_http_addr text
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_placement_nodes_table(
    conn: &Connection,
    nodes: Vec<PlacementNodeConfig>,
) -> anyhow::Result<()> {
    let mut insert = conn.prepare("insert into placement_nodes values (?, ?, ?, ?, ?, ?)")?;
    for node in nodes {
        insert.execute(params![
            sqlite_i64(node.node_id),
            node.cloud,
            node.region,
            node.zone,
            node.raft_addr,
            node.libsql_http_addr,
        ])?;
    }
    Ok(())
}

fn create_virtual_database_placement_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table database_placement (
            database_id text not null,
            database_name text not null,
            group_id text not null,
            group_state text not null,
            placement_mode text not null,
            member_node_id integer,
            member_role text,
            cloud text,
            region text,
            zone text,
            compaction_owner_node_id integer,
            failover_automatic integer not null,
            failover_promote_after_ms integer not null
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_database_placement_table(
    conn: &Connection,
    catalog_db: Option<&OrionSqliteDb>,
) -> anyhow::Result<()> {
    let Some(catalog_db) = catalog_db else {
        return Ok(());
    };
    let catalog_conn = catalog_db.connect()?;
    ensure_database_catalog_schema(&catalog_conn)?;
    require_database_catalog_schema(&catalog_conn, 4)?;
    let mut rows = catalog_conn.prepare(
        r#"
        select dc.database_id, dc.name, rg.group_id, rg.state, rg.placement_mode,
               rgm.node_id, rgm.role, rgm.cloud, rgm.region, rgm.zone,
               rg.compaction_owner_node_id, rg.failover_automatic, rg.failover_promote_after_ms
        from database_catalog dc
        join database_replication_groups drg on drg.database_id = dc.database_id
        join replication_groups rg on rg.group_id = drg.group_id
        left join replication_group_members rgm on rgm.group_id = rg.group_id
        order by dc.name, rgm.priority, rgm.node_id, rgm.role
        "#,
    )?;
    let records = rows
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert = conn.prepare(
        r#"
        insert into database_placement values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )?;
    for record in records {
        insert.execute(params![
            record.0, record.1, record.2, record.3, record.4, record.5, record.6, record.7,
            record.8, record.9, record.10, record.11, record.12,
        ])?;
    }
    Ok(())
}

fn create_virtual_database_standbys_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table database_standbys (
            database_id text not null,
            database_name text not null,
            source_group_id text not null,
            target_group_id text not null,
            source_applied_index integer,
            target_applied_index integer,
            refreshed_at_ms integer not null,
            age_ms integer not null,
            updated_at_ms integer not null,
            error text
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_database_standbys_table(
    conn: &Connection,
    catalog_db: Option<&OrionSqliteDb>,
) -> anyhow::Result<()> {
    let Some(catalog_db) = catalog_db else {
        return Ok(());
    };
    let catalog_conn = catalog_db.connect()?;
    ensure_database_catalog_schema(&catalog_conn)?;
    if read_catalog_schema_version(&catalog_conn)?
        .unwrap_or(infer_database_catalog_schema_version(&catalog_conn)?)
        < 7
    {
        return Ok(());
    }
    let now = current_time_millis();
    let mut rows = catalog_conn.prepare(&format!(
        "select {DATABASE_STANDBY_SELECT_COLUMNS} from database_standby_copies order by database_name, target_group_id"
    ))?;
    let records = rows
        .query_map([], database_placement_standby_record_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert = conn.prepare(
        r#"
        insert into database_standbys values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )?;
    for record in records {
        insert.execute(params![
            record.database_id,
            record.database_name,
            record.source_group_id,
            record.target_group_id,
            record.source_applied_index.map(sqlite_i64),
            record.target_applied_index.map(sqlite_i64),
            sqlite_i64(record.refreshed_at_ms),
            sqlite_i64(now.saturating_sub(record.refreshed_at_ms)),
            sqlite_i64(record.updated_at_ms),
            record.error,
        ])?;
    }
    Ok(())
}

fn create_virtual_placement_metrics_table(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        create table placement_metrics (
            status text not null,
            phase text not null,
            operation_count integer not null,
            oldest_age_ms integer,
            newest_update_age_ms integer
        );
        "#,
    )?;
    Ok(())
}

fn populate_virtual_placement_metrics_table(
    conn: &Connection,
    rows: Vec<PlacementMetricsPhaseRow>,
) -> anyhow::Result<()> {
    let mut insert = conn.prepare("insert into placement_metrics values (?, ?, ?, ?, ?)")?;
    for row in rows {
        insert.execute(params![
            row.status,
            row.phase,
            sqlite_i64(row.operation_count),
            row.oldest_age_ms.map(sqlite_i64),
            row.newest_update_age_ms.map(sqlite_i64),
        ])?;
    }
    Ok(())
}

fn query_virtual_system_table(
    conn: &Connection,
    sql: &str,
    args: &StatementParams,
) -> anyhow::Result<StmtResult> {
    let mut prepared = conn.prepare(sql)?;
    let cols = columns_for_statement(&prepared);
    if prepared.column_count() == 0 {
        let affected = execute_statement(&mut prepared, args)?;
        return Ok(StmtResult {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count: affected as u64,
            last_insert_rowid: Some(conn.last_insert_rowid().to_string()),
        });
    }

    let column_count = cols.len();
    let mut rows = query_statement(&mut prepared, args)?;
    let mut out_rows = Vec::new();
    while let Some(row) = rows.next()? {
        out_rows.push(row_to_values(row, column_count)?);
    }
    Ok(StmtResult {
        cols,
        rows: out_rows,
        affected_row_count: 0,
        last_insert_rowid: Some(conn.last_insert_rowid().to_string()),
    })
}

fn sqlite_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn batch_condition_matches(
    condition: Option<&BatchCond>,
    step_results: &[Option<StmtResult>],
    step_errors: &[Option<ErrorBody>],
    session: &LibsqlSession,
) -> bool {
    let Some(condition) = condition else {
        return true;
    };
    match condition {
        BatchCond::Ok { step } => step_results
            .get(*step as usize)
            .is_some_and(|result| result.is_some()),
        BatchCond::Error { step } => step_errors
            .get(*step as usize)
            .is_some_and(|error| error.is_some()),
        BatchCond::Not { cond } => {
            !batch_condition_matches(Some(cond), step_results, step_errors, session)
        }
        BatchCond::And { conds } => conds
            .iter()
            .all(|cond| batch_condition_matches(Some(cond), step_results, step_errors, session)),
        BatchCond::Or { conds } => conds
            .iter()
            .any(|cond| batch_condition_matches(Some(cond), step_results, step_errors, session)),
        BatchCond::IsAutocommit => session.conn.is_autocommit(),
    }
}

struct StatementParams {
    positional: Vec<SqliteValue>,
    named: Vec<(String, SqliteValue)>,
}

fn stmt_to_params(stmt: Stmt) -> anyhow::Result<StatementParams> {
    let positional = stmt
        .args
        .into_iter()
        .map(hrana_to_sqlite_value)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let named = stmt
        .named_args
        .into_iter()
        .map(|arg| Ok((arg.name, hrana_to_sqlite_value(arg.value)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(StatementParams { positional, named })
}

fn query_statement<'stmt>(
    stmt: &'stmt mut rusqlite::Statement<'_>,
    args: &'stmt StatementParams,
) -> anyhow::Result<rusqlite::Rows<'stmt>> {
    if args.named.is_empty() {
        Ok(stmt.query(params_from_iter(args.positional.iter()))?)
    } else {
        let named_names = resolved_named_arg_names(stmt, args)?;
        let named = named_param_refs(&named_names, args);
        Ok(stmt.query(named.as_slice())?)
    }
}

fn execute_statement(
    stmt: &mut rusqlite::Statement<'_>,
    args: &StatementParams,
) -> anyhow::Result<usize> {
    if args.named.is_empty() {
        Ok(stmt.execute(params_from_iter(args.positional.iter()))?)
    } else {
        let named_names = resolved_named_arg_names(stmt, args)?;
        let named = named_param_refs(&named_names, args);
        Ok(stmt.execute(named.as_slice())?)
    }
}

fn named_param_refs<'a>(
    names: &'a [String],
    args: &'a StatementParams,
) -> Vec<(&'a str, &'a dyn ToSql)> {
    args.named
        .iter()
        .zip(names)
        .map(|((_, value), name)| (name.as_str(), value as &dyn ToSql))
        .collect()
}

fn resolved_named_arg_names(
    stmt: &rusqlite::Statement<'_>,
    args: &StatementParams,
) -> anyhow::Result<Vec<String>> {
    args.named
        .iter()
        .map(|(name, _)| resolve_named_arg_name(stmt, name))
        .collect()
}

fn resolve_named_arg_name(stmt: &rusqlite::Statement<'_>, name: &str) -> anyhow::Result<String> {
    if matches!(name.as_bytes().first(), Some(b':' | b'$' | b'@')) {
        return Ok(name.to_string());
    }

    for prefix in [":", "$", "@"] {
        let candidate = format!("{prefix}{name}");
        if stmt.parameter_index(&candidate)?.is_some() {
            return Ok(candidate);
        }
    }

    Ok(format!(":{name}"))
}

fn describe_sql(conn: &Connection, sql: &str) -> anyhow::Result<DescribeResult> {
    reject_denied_sql_surface(sql)?;
    let prepared = conn.prepare(sql)?;
    let params = (1..=prepared.parameter_count())
        .map(|index| DescribeParam {
            name: prepared.parameter_name(index).map(str::to_string),
        })
        .collect();
    let is_explain = prepared.is_explain() != 0;
    let is_readonly = prepared.readonly();
    Ok(DescribeResult {
        params,
        cols: columns_for_statement(&prepared)
            .into_iter()
            .map(|col| DescribeCol {
                name: col.name.unwrap_or_default(),
                decltype: col.decltype,
            })
            .collect(),
        is_explain,
        is_readonly,
    })
}

#[derive(Debug, Clone)]
struct StmtSql {
    sql: Option<String>,
    sql_id: Option<i64>,
}

impl From<&Stmt> for StmtSql {
    fn from(stmt: &Stmt) -> Self {
        Self {
            sql: stmt.sql.clone(),
            sql_id: stmt.sql_id,
        }
    }
}

fn resolve_sql(session: &LibsqlSession, sql: StmtSql) -> anyhow::Result<String> {
    if let Some(sql) = sql.sql {
        return Ok(sql);
    }
    let sql_id = sql
        .sql_id
        .ok_or_else(|| HranaProtocolError::new("statement sql or sql_id is required"))?;
    session
        .stored_sql
        .get(&sql_id)
        .cloned()
        .ok_or_else(|| HranaProtocolError::new(format!("stored SQL id {sql_id} does not exist")))
        .map_err(Into::into)
}

fn columns_for_statement(stmt: &rusqlite::Statement<'_>) -> Vec<Column> {
    stmt.columns()
        .into_iter()
        .map(|column| Column {
            name: Some(column.name().to_string()),
            decltype: column.decl_type().map(str::to_string),
        })
        .collect()
}

fn row_to_values(row: &Row<'_>, column_count: usize) -> rusqlite::Result<Vec<HranaValue>> {
    let mut values = Vec::with_capacity(column_count);
    for index in 0..column_count {
        values.push(sqlite_to_hrana_value(row.get_ref(index)?));
    }
    Ok(values)
}

fn sqlite_to_hrana_value(value: ValueRef<'_>) -> HranaValue {
    match value {
        ValueRef::Null => HranaValue::Null,
        ValueRef::Integer(value) => HranaValue::Integer {
            value: value.to_string(),
        },
        ValueRef::Real(value) => HranaValue::Float { value },
        ValueRef::Text(value) => HranaValue::Text {
            value: String::from_utf8_lossy(value).into_owned(),
        },
        ValueRef::Blob(value) => HranaValue::Blob {
            base64: BASE64.encode(value),
        },
    }
}

fn hrana_to_sqlite_value(value: HranaValue) -> anyhow::Result<SqliteValue> {
    Ok(match value {
        HranaValue::Null => SqliteValue::Null,
        HranaValue::Integer { value } => SqliteValue::Integer(value.parse().map_err(|_| {
            HranaProtocolError::new(format!("invalid Hrana integer value {value:?}"))
        })?),
        HranaValue::Float { value } => SqliteValue::Real(value),
        HranaValue::Text { value } => SqliteValue::Text(value),
        HranaValue::Blob { base64 } => SqliteValue::Blob(
            BASE64
                .decode(&base64)
                .map_err(|_| HranaProtocolError::new("invalid Hrana blob base64 value"))?,
        ),
    })
}

fn sqlite_error_code(error: &(dyn std::error::Error + 'static)) -> &'static str {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.downcast_ref::<HranaProtocolError>().is_some() {
            return "HRANA_PROTO_ERROR";
        }
        if error
            .downcast_ref::<OrionSqliteAuthorizationError>()
            .is_some()
        {
            return "SQLITE_AUTH";
        }
        if error.downcast_ref::<OrionBlobReadOnlyError>().is_some() {
            return "SQLITE_READONLY";
        }
        if let Some(error) = error.downcast_ref::<rusqlite::Error>() {
            return rusqlite_error_code(error);
        }
        current = error.source();
    }
    "SQLITE_IOERR"
}

#[derive(Debug)]
struct HranaProtocolError {
    message: String,
}

impl HranaProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for HranaProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for HranaProtocolError {}

#[derive(Debug)]
struct OrionSqliteAuthorizationError {
    message: String,
}

impl OrionSqliteAuthorizationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for OrionSqliteAuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for OrionSqliteAuthorizationError {}

#[derive(Debug)]
struct OrionBlobReadOnlyError {
    blob_id: String,
}

impl OrionBlobReadOnlyError {
    fn new(blob_id: impl Into<String>) -> Self {
        Self {
            blob_id: blob_id.into(),
        }
    }
}

impl fmt::Display for OrionBlobReadOnlyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blob {} was opened read-only", self.blob_id)
    }
}

impl Error for OrionBlobReadOnlyError {}

fn reject_denied_sql_surface(sql: &str) -> anyhow::Result<()> {
    let first_token = first_sql_token(sql);
    if first_token.eq_ignore_ascii_case("vacuum") {
        return Err(OrionSqliteAuthorizationError::new(
            "VACUUM is not authorized by Orion; use a service-owned compaction or backup operation",
        )
        .into());
    }
    if first_token.eq_ignore_ascii_case("pragma") && unsafe_pragma_sql(sql) {
        return Err(
            OrionSqliteAuthorizationError::new("this PRAGMA is not authorized by Orion").into(),
        );
    }
    if sql
        .to_ascii_lowercase()
        .contains(&IDEMPOTENCY_TABLE.to_ascii_lowercase())
    {
        return Err(OrionSqliteAuthorizationError::new(
            "direct access to Orion idempotency records is not authorized",
        )
        .into());
    }
    Ok(())
}

fn unsafe_pragma_sql(sql: &str) -> bool {
    let normalized = strip_sql_trailing_semicolon(sql)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    [
        "pragma journal_mode",
        "pragma synchronous",
        "pragma locking_mode",
        "pragma writable_schema",
        "pragma temp_store_directory",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
}

fn rusqlite_error_code(error: &rusqlite::Error) -> &'static str {
    if let rusqlite::Error::SqliteFailure(_, Some(message)) = error
        && message.to_ascii_lowercase().contains("not authorized")
    {
        return "SQLITE_AUTH";
    }
    if let rusqlite::Error::SqlInputError { msg, .. } = error
        && msg.to_ascii_lowercase().contains("not authorized")
    {
        return "SQLITE_AUTH";
    }
    match error {
        rusqlite::Error::SqliteFailure(error, _) => match error.code {
            SqliteErrorCode::InternalMalfunction => "SQLITE_INTERNAL",
            SqliteErrorCode::PermissionDenied => "SQLITE_PERM",
            SqliteErrorCode::OperationAborted => "SQLITE_ABORT",
            SqliteErrorCode::DatabaseBusy => "SQLITE_BUSY",
            SqliteErrorCode::DatabaseLocked => "SQLITE_LOCKED",
            SqliteErrorCode::OutOfMemory => "SQLITE_NOMEM",
            SqliteErrorCode::ReadOnly => "SQLITE_READONLY",
            SqliteErrorCode::OperationInterrupted => "SQLITE_INTERRUPT",
            SqliteErrorCode::SystemIoFailure => "SQLITE_IOERR",
            SqliteErrorCode::DatabaseCorrupt => "SQLITE_CORRUPT",
            SqliteErrorCode::NotFound => "SQLITE_NOTFOUND",
            SqliteErrorCode::DiskFull => "SQLITE_FULL",
            SqliteErrorCode::CannotOpen => "SQLITE_CANTOPEN",
            SqliteErrorCode::FileLockingProtocolFailed => "SQLITE_PROTOCOL",
            SqliteErrorCode::SchemaChanged => "SQLITE_SCHEMA",
            SqliteErrorCode::TooBig => "SQLITE_TOOBIG",
            SqliteErrorCode::ConstraintViolation => "SQLITE_CONSTRAINT",
            SqliteErrorCode::TypeMismatch => "SQLITE_MISMATCH",
            SqliteErrorCode::ApiMisuse => "SQLITE_MISUSE",
            SqliteErrorCode::NoLargeFileSupport => "SQLITE_NOLFS",
            SqliteErrorCode::AuthorizationForStatementDenied => "SQLITE_AUTH",
            SqliteErrorCode::ParameterOutOfRange => "SQLITE_RANGE",
            SqliteErrorCode::NotADatabase => "SQLITE_NOTADB",
            SqliteErrorCode::Unknown => "SQLITE_ERROR",
            _ => "SQLITE_ERROR",
        },
        rusqlite::Error::InvalidParameterName(_) | rusqlite::Error::InvalidParameterCount(_, _) => {
            "SQLITE_RANGE"
        }
        rusqlite::Error::InvalidColumnType(_, _, _)
        | rusqlite::Error::IntegralValueOutOfRange(_, _)
        | rusqlite::Error::FromSqlConversionFailure(_, _, _) => "SQLITE_MISMATCH",
        rusqlite::Error::ExecuteReturnedResults
        | rusqlite::Error::QueryReturnedNoRows
        | rusqlite::Error::QueryReturnedMoreThanOneRow
        | rusqlite::Error::InvalidQuery
        | rusqlite::Error::MultipleStatement => "SQLITE_MISUSE",
        _ => "SQLITE_ERROR",
    }
}

fn validate_database_name(database: &str) -> anyhow::Result<()> {
    ensure!(!database.is_empty(), "database name must not be empty");
    ensure!(
        database.len() <= 128,
        "database name must be at most 128 bytes"
    );
    ensure!(
        database != "." && database != ".." && !database.contains(".."),
        "database name must not contain parent directory segments"
    );
    ensure!(
        database
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "database name may only contain ASCII letters, digits, dots, hyphens, and underscores"
    );
    Ok(())
}

fn validate_replication_group_id(group_id: &str) -> anyhow::Result<()> {
    ensure_valid_runtime_group_id(group_id)
}

fn ensure_valid_placement_operation_id(operation_id: &str) -> anyhow::Result<()> {
    ensure!(
        !operation_id.is_empty(),
        "placement operation id must not be empty"
    );
    ensure!(
        operation_id.len() <= 160,
        "placement operation id must be at most 160 bytes"
    );
    ensure!(
        operation_id != "." && operation_id != ".." && !operation_id.contains(".."),
        "placement operation id must not contain parent directory segments"
    );
    ensure!(
        operation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "placement operation id may only contain ASCII letters, digits, dots, hyphens, and underscores"
    );
    Ok(())
}

pub fn ensure_valid_runtime_group_id(group_id: &str) -> anyhow::Result<()> {
    ensure!(
        !group_id.is_empty(),
        "replication group id must not be empty"
    );
    ensure!(
        group_id.len() <= 128,
        "replication group id must be at most 128 bytes"
    );
    ensure!(
        group_id != "." && group_id != ".." && !group_id.contains(".."),
        "replication group id must not contain parent directory segments"
    );
    ensure!(
        group_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "replication group id may only contain ASCII letters, digits, dots, hyphens, and underscores"
    );
    Ok(())
}

fn validate_replication_group_member_role(role: &str) -> anyhow::Result<()> {
    ensure!(
        matches!(role, "voter" | "learner" | "read_replica"),
        "unsupported replication group member role {role}"
    );
    Ok(())
}

fn is_valid_database_prefix(prefix: &str) -> bool {
    !prefix.is_empty()
        && prefix.len() <= 128
        && !prefix.contains("..")
        && prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    header.strip_prefix("Bearer ")
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PipelineReqBody {
    #[serde(default)]
    baton: Option<String>,
    requests: Vec<StreamRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobOpenReqBody {
    #[serde(default)]
    baton: Option<String>,
    #[serde(default)]
    schema: Option<String>,
    table: String,
    column: String,
    rowid: i64,
    #[serde(default = "default_blob_read_only")]
    read_only: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobReadReqBody {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
    offset: usize,
    length: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobWriteReqBody {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
    offset: usize,
    base64: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobWriteBytesReqQuery {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
    offset: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobWriteStreamReqQuery {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
    offset: usize,
    #[serde(default)]
    length: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobReopenReqBody {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
    rowid: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobCloseReqBody {
    #[serde(default)]
    baton: Option<String>,
    blob_id: String,
}

fn default_blob_read_only() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlobRespBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    baton: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    orion: Option<OrionPipelineMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<BlobResponseKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorBody>,
}

impl BlobRespBody {
    fn error(baton: Option<String>, message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            baton,
            orion: None,
            result: None,
            error: Some(ErrorBody {
                message: message.into(),
                code: Some(code.into()),
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BlobResponseKind {
    Open {
        blob_id: String,
        size: usize,
        read_only: bool,
    },
    Read {
        blob_id: String,
        offset: usize,
        bytes_read: usize,
        base64: String,
        size: usize,
    },
    Write {
        blob_id: String,
        offset: usize,
        bytes_written: usize,
        size: usize,
    },
    Reopen {
        blob_id: String,
        rowid: i64,
        size: usize,
    },
    Close {
        blob_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamRequest {
    Close,
    Execute {
        stmt: Stmt,
    },
    Batch {
        batch: Batch,
    },
    Sequence {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i64>,
    },
    Describe {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i64>,
    },
    StoreSql {
        sql_id: i64,
        sql: String,
    },
    CloseSql {
        sql_id: i64,
    },
    GetAutocommit,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Stmt {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    sql_id: Option<i64>,
    #[serde(default)]
    args: Vec<HranaValue>,
    #[serde(default)]
    named_args: Vec<NamedArg>,
    #[serde(default)]
    want_rows: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct NamedArg {
    name: String,
    value: HranaValue,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Batch {
    steps: Vec<BatchStep>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BatchStep {
    #[serde(default)]
    condition: Option<BatchCond>,
    stmt: Stmt,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BatchCond {
    Ok { step: u64 },
    Error { step: u64 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PipelineRespBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    baton: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    orion: Option<OrionPipelineMetadata>,
    results: Vec<StreamResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrionPipelineMetadata {
    node_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    forwarded_from_node_id: Option<u64>,
    read_policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency: Option<OrionIdempotencyMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_token: Option<OrionSessionToken>,
    freshness: OrionFreshnessMetadata,
}

impl OrionPipelineMetadata {
    fn local(
        state: &LibsqlHttpState,
        read_policy: &OrionReadPolicy,
        freshness: OrionSqliteReplicaFreshness,
    ) -> Self {
        Self {
            node_id: state.node_id,
            forwarded_from_node_id: None,
            read_policy: read_policy.as_str().to_string(),
            idempotency: None,
            session_token: freshness
                .raft
                .applied_index
                .zip(freshness.applied_commit_ts)
                .map(|(applied_index, commit_ts)| OrionSessionToken {
                    applied_index,
                    commit_ts,
                    token: format!(
                        "{}:{}:{}",
                        applied_index, commit_ts.physical_ms, commit_ts.logical
                    ),
                }),
            freshness: freshness.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrionSessionToken {
    applied_index: u64,
    commit_ts: HybridTimestamp,
    token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrionFreshnessMetadata {
    raft: orion_raft::RaftMetricsSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    applied_commit_ts: Option<HybridTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_commit_ts: Option<HybridTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    staleness_ms: Option<u64>,
}

impl From<OrionSqliteReplicaFreshness> for OrionFreshnessMetadata {
    fn from(value: OrionSqliteReplicaFreshness) -> Self {
        Self {
            raft: value.raft,
            applied_commit_ts: value.applied_commit_ts,
            closed_commit_ts: value.closed_commit_ts,
            staleness_ms: value.staleness_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: ErrorBody },
}

impl StreamResult {
    fn ok(response: StreamResponse) -> Self {
        Self::Ok { response }
    }

    fn error(message: impl Into<String>, code: &str) -> Self {
        Self::Error {
            error: ErrorBody {
                message: message.into(),
                code: Some(code.to_string()),
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamResponse {
    Close,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    Describe { result: DescribeResult },
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ErrorBody {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StmtResult {
    cols: Vec<Column>,
    rows: Vec<Vec<HranaValue>>,
    affected_row_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_insert_rowid: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BatchResult {
    step_results: Vec<Option<StmtResult>>,
    step_errors: Vec<Option<ErrorBody>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DescribeResult {
    params: Vec<DescribeParam>,
    cols: Vec<DescribeCol>,
    is_explain: bool,
    is_readonly: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DescribeParam {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct DescribeCol {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    decltype: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Column {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decltype: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HranaValue {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use openraft::{BasicNode, Config, Raft};
    use orion_raft::tonic_transport::bind_raft_transport;
    use orion_raft::{OrionRaftLogStore, OrionRaftStateMachine, TonicRaftNetwork};
    use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn memory_session() -> LibsqlSession {
        LibsqlSession {
            database: "test".to_string(),
            conn: Connection::open_in_memory().unwrap(),
            system_context: None,
            stored_sql: HashMap::new(),
            blob_handles: HashMap::new(),
            next_blob_id: 1,
            last_used_at: Instant::now(),
        }
    }

    fn catalog_rollout_test_entry(
        node_id: u64,
        observed_at_ms: u64,
        voter_ids: Vec<u64>,
        capabilities: Option<NodeSoftwareCapabilities>,
    ) -> ClusterRaftMetricsEntry {
        ClusterRaftMetricsEntry {
            observed_at_ms,
            metrics: catalog_rollout_test_metrics(node_id, voter_ids, capabilities),
        }
    }

    fn catalog_rollout_test_metrics(
        node_id: u64,
        voter_ids: Vec<u64>,
        capabilities: Option<NodeSoftwareCapabilities>,
    ) -> orion_raft::RaftMetricsSnapshot {
        orion_raft::RaftMetricsSnapshot {
            node_id,
            state: "Leader".to_string(),
            running: true,
            current_term: 1,
            current_leader: Some(1),
            last_log_index: Some(1),
            committed_index: Some(1),
            applied_index: Some(1),
            snapshot_index: None,
            purged_index: None,
            voter_ids,
            learner_ids: Vec::new(),
            replication: Vec::new(),
            snapshot_transfer: Default::default(),
            capabilities,
        }
    }

    fn stmt(sql: impl Into<String>) -> Stmt {
        Stmt {
            sql: Some(sql.into()),
            sql_id: None,
            args: Vec::new(),
            named_args: Vec::new(),
            want_rows: false,
        }
    }

    fn query_stmt(sql: impl Into<String>) -> Stmt {
        Stmt {
            want_rows: true,
            ..stmt(sql)
        }
    }

    fn shared_memory_session() -> Arc<Mutex<LibsqlSession>> {
        Arc::new(Mutex::new(memory_session()))
    }

    fn system_read_and_admin_auth_config() -> LibsqlHttpAuthConfig {
        LibsqlHttpAuthConfig {
            tokens: vec![
                LibsqlHttpAuthTokenConfig {
                    token: "operator-read".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Read],
                },
                LibsqlHttpAuthTokenConfig {
                    token: "operator-admin".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Admin],
                },
            ],
        }
    }

    async fn run_fixture_requests(requests: Vec<StreamRequest>) -> Vec<serde_json::Value> {
        let session = shared_memory_session();
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let result = run_stream_request(Arc::clone(&session), request)
                .await
                .unwrap_or_else(|error| {
                    StreamResult::error(error.to_string(), sqlite_error_code(error.as_ref()))
                });
            results.push(serde_json::to_value(result).unwrap());
        }
        results
    }

    fn ws_message_json(message: &Message) -> serde_json::Value {
        match message {
            Message::Text(text) => serde_json::from_str(text.as_str()).unwrap(),
            other => panic!("expected text WebSocket message, got {other:?}"),
        }
    }

    struct RouterFixture {
        router: Router,
        state: LibsqlHttpState,
        _cache_dir: TempDir,
        _log_dir: TempDir,
    }

    impl RouterFixture {
        async fn new(auth: LibsqlHttpAuthConfig) -> Self {
            Self::new_with_session_idle_timeout(auth, Duration::from_secs(60)).await
        }

        async fn new_with_blob_max_chunk_bytes(
            auth: LibsqlHttpAuthConfig,
            blob_max_chunk_bytes: usize,
        ) -> Self {
            Self::new_with_session_idle_timeout_and_blob_max_chunk_bytes(
                auth,
                Duration::from_secs(60),
                blob_max_chunk_bytes,
            )
            .await
        }

        async fn new_with_session_idle_timeout(
            auth: LibsqlHttpAuthConfig,
            session_idle_timeout: Duration,
        ) -> Self {
            Self::new_with_session_idle_timeout_and_blob_max_chunk_bytes(
                auth,
                session_idle_timeout,
                512 * 1024,
            )
            .await
        }

        async fn new_with_session_idle_timeout_and_blob_max_chunk_bytes(
            auth: LibsqlHttpAuthConfig,
            session_idle_timeout: Duration,
            blob_max_chunk_bytes: usize,
        ) -> Self {
            Self::new_with_config(
                auth,
                session_idle_timeout,
                blob_max_chunk_bytes,
                LibsqlHttpIdempotencyConfig::default(),
            )
            .await
        }

        async fn new_with_idempotency_config(
            auth: LibsqlHttpAuthConfig,
            idempotency: LibsqlHttpIdempotencyConfig,
        ) -> Self {
            Self::new_with_config(auth, Duration::from_secs(60), 512 * 1024, idempotency).await
        }

        async fn new_with_config(
            auth: LibsqlHttpAuthConfig,
            session_idle_timeout: Duration,
            blob_max_chunk_bytes: usize,
            idempotency: LibsqlHttpIdempotencyConfig,
        ) -> Self {
            static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);
            let fixture_id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
            let cluster_name = format!("libsql-http-router-test-{fixture_id}");
            let cache_dir = TempDir::new().unwrap();
            let log_dir = TempDir::new().unwrap();
            let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
            let state = SlateDbStateStore::open_in_memory(&cluster_name)
                .await
                .unwrap();
            let state_machine = OrionRaftStateMachine::new(state.clone());
            let raft_config = Arc::new(
                Config {
                    cluster_name,
                    heartbeat_interval: 50,
                    election_timeout_min: 150,
                    election_timeout_max: 300,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );
            let raft = Raft::new(
                1,
                raft_config,
                TonicRaftNetwork::new(),
                log_store,
                state_machine,
            )
            .await
            .unwrap();
            let mut members = BTreeMap::new();
            members.insert(
                1,
                BasicNode {
                    addr: "127.0.0.1:0".to_string(),
                },
            );
            raft.initialize(members).await.unwrap();
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "libsql http router fixture leader")
                .await
                .unwrap();

            let metrics_registry = ClusterRaftMetricsRegistry::default();
            metrics_registry.set_local_capabilities(node_software_capabilities());
            metrics_registry.record_observed(&raft);
            let config = LibsqlHttpConfig {
                sqlite_cache_root: cache_dir.path().to_path_buf(),
                session_idle_timeout,
                blob_max_chunk_bytes,
                idempotency,
                auth,
                node_id: 1,
                peer_http_endpoints: BTreeMap::new(),
                placement_nodes: BTreeMap::from([(
                    1,
                    PlacementNodeConfig {
                        node_id: 1,
                        cloud: "local".to_string(),
                        region: "local".to_string(),
                        zone: "local".to_string(),
                        raft_addr: "127.0.0.1:0".to_string(),
                        libsql_http_addr: None,
                    },
                )]),
                metrics_registry: metrics_registry.clone(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: None,
            };
            let runtime = OrionSqliteRuntime::new(
                raft,
                state,
                OrionSqliteRuntimeConfig::new(config.sqlite_cache_root.clone()),
            );
            let state = LibsqlHttpState::new(runtime, &config);

            Self {
                router: libsql_router(state.clone()),
                state,
                _cache_dir: cache_dir,
                _log_dir: log_dir,
            }
        }

        async fn pipeline(
            &self,
            path: &str,
            body: serde_json::Value,
            bearer_token: Option<&str>,
        ) -> (StatusCode, serde_json::Value) {
            self.pipeline_with_headers(path, body, bearer_token, &[])
                .await
        }

        async fn pipeline_with_headers(
            &self,
            path: &str,
            body: serde_json::Value,
            bearer_token: Option<&str>,
            headers: &[(&str, &str)],
        ) -> (StatusCode, serde_json::Value) {
            self.ensure_test_database_for_path(path);
            self.pipeline_with_headers_raw(path, body, bearer_token, headers)
                .await
        }

        async fn pipeline_with_headers_raw(
            &self,
            path: &str,
            body: serde_json::Value,
            bearer_token: Option<&str>,
            headers: &[(&str, &str)],
        ) -> (StatusCode, serde_json::Value) {
            let mut builder = Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(axum::http::header::CONTENT_TYPE, "application/json");
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }

            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::from(body.to_string())).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json = serde_json::from_slice(&body).unwrap();

            (status, json)
        }

        fn ensure_test_database_for_path(&self, path: &str) {
            let Some(database) = test_database_from_path(path) else {
                return;
            };
            let _ = create_database_lifecycle(
                &self.state,
                CreateDatabaseRequest {
                    name: database,
                    placement: None,
                },
            );
        }

        async fn get(
            &self,
            path: &str,
            bearer_token: Option<&str>,
        ) -> (StatusCode, serde_json::Value) {
            let mut builder = Request::builder().method(Method::GET).uri(path);
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json = serde_json::from_slice(&body).unwrap();
            (status, json)
        }

        async fn post(
            &self,
            path: &str,
            body: serde_json::Value,
            bearer_token: Option<&str>,
        ) -> (StatusCode, serde_json::Value) {
            self.post_with_headers(path, body, bearer_token, &[]).await
        }

        async fn post_with_headers(
            &self,
            path: &str,
            body: serde_json::Value,
            bearer_token: Option<&str>,
            headers: &[(&str, &str)],
        ) -> (StatusCode, serde_json::Value) {
            let mut builder = Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(axum::http::header::CONTENT_TYPE, "application/json");
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::from(body.to_string())).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json = serde_json::from_slice(&body).unwrap();
            (status, json)
        }

        async fn delete(
            &self,
            path: &str,
            bearer_token: Option<&str>,
        ) -> (StatusCode, serde_json::Value) {
            self.delete_with_headers(path, bearer_token, &[]).await
        }

        async fn delete_with_headers(
            &self,
            path: &str,
            bearer_token: Option<&str>,
            headers: &[(&str, &str)],
        ) -> (StatusCode, serde_json::Value) {
            let mut builder = Request::builder().method(Method::DELETE).uri(path);
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json = serde_json::from_slice(&body).unwrap();
            (status, json)
        }

        async fn get_bytes(
            &self,
            path: &str,
            bearer_token: Option<&str>,
        ) -> (StatusCode, HeaderMap, Vec<u8>) {
            let mut builder = Request::builder().method(Method::GET).uri(path);
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, headers, body.to_vec())
        }

        async fn post_bytes(
            &self,
            path: &str,
            body: impl Into<Vec<u8>>,
            bearer_token: Option<&str>,
        ) -> (StatusCode, HeaderMap, Vec<u8>) {
            let mut builder = Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(axum::http::header::CONTENT_TYPE, "application/octet-stream");
            if let Some(token) = bearer_token {
                builder =
                    builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = self
                .router
                .clone()
                .oneshot(builder.body(Body::from(body.into())).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, headers, body.to_vec())
        }
    }

    fn one_query_pipeline(sql: &str) -> serde_json::Value {
        serde_json::json!({
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "want_rows": true
                    }
                }
            ]
        })
    }

    fn execute_request(sql: &str, want_rows: bool) -> serde_json::Value {
        serde_json::json!({
            "type": "execute",
            "stmt": {
                "sql": sql,
                "want_rows": want_rows
            }
        })
    }

    fn sql_pipeline(requests: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({ "requests": requests })
    }

    fn test_database_from_path(path: &str) -> Option<String> {
        let path = path.trim_start_matches('/');
        if path.is_empty() || path.starts_with("_orion") {
            return None;
        }
        if path == "v2/pipeline" || path.starts_with("v2/blob/") {
            return Some(DEFAULT_DATABASE.to_string());
        }
        let mut segments = path.split('/');
        let database = segments.next()?;
        if database.is_empty() || database == "_orion" {
            return None;
        }
        Some(database.to_string())
    }

    #[test]
    fn serializes_hrana_execute_response_shape() {
        let body = PipelineRespBody {
            baton: Some("orion".to_string()),
            base_url: None,
            orion: None,
            results: vec![StreamResult::ok(StreamResponse::Execute {
                result: StmtResult {
                    cols: vec![Column {
                        name: Some("answer".to_string()),
                        decltype: None,
                    }],
                    rows: vec![vec![HranaValue::Integer {
                        value: "42".to_string(),
                    }]],
                    affected_row_count: 0,
                    last_insert_rowid: Some("0".to_string()),
                },
            })],
        };

        let json = serde_json::to_value(body).unwrap();
        assert_eq!(json["baton"], "orion");
        assert_eq!(json["results"][0]["type"], "ok");
        assert_eq!(json["results"][0]["response"]["type"], "execute");
        assert_eq!(
            json["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "42"
        );
    }

    #[tokio::test]
    async fn raw_fixture_store_execute_by_sql_id_and_close_sql_missing_id_error() {
        let body: PipelineReqBody = serde_json::from_value(serde_json::json!({
            "requests": [
                {
                    "type": "store_sql",
                    "sql_id": 7,
                    "sql": "select 42 as answer"
                },
                {
                    "type": "execute",
                    "stmt": {
                        "sql_id": 7,
                        "want_rows": true
                    }
                },
                {
                    "type": "close_sql",
                    "sql_id": 7
                },
                {
                    "type": "close_sql",
                    "sql_id": 7
                }
            ]
        }))
        .unwrap();

        let results = run_fixture_requests(body.requests).await;

        assert_eq!(results[0]["type"], "ok");
        assert_eq!(results[0]["response"]["type"], "store_sql");
        assert_eq!(results[1]["type"], "ok");
        assert_eq!(results[1]["response"]["type"], "execute");
        assert_eq!(
            results[1]["response"]["result"]["rows"][0][0],
            serde_json::json!({
                "type": "integer",
                "value": "42"
            })
        );
        assert_eq!(results[2]["type"], "ok");
        assert_eq!(results[2]["response"]["type"], "close_sql");
        assert_eq!(results[3]["type"], "error");
        assert_eq!(results[3]["error"]["code"], "HRANA_PROTO_ERROR");
        assert_eq!(
            results[3]["error"]["message"],
            "stored SQL id 7 does not exist"
        );
    }

    #[tokio::test]
    async fn raw_fixture_get_autocommit_tracks_begin_and_commit() {
        let body: PipelineReqBody = serde_json::from_value(serde_json::json!({
            "requests": [
                { "type": "get_autocommit" },
                { "type": "execute", "stmt": { "sql": "begin" } },
                { "type": "get_autocommit" },
                { "type": "execute", "stmt": { "sql": "commit" } },
                { "type": "get_autocommit" }
            ]
        }))
        .unwrap();

        let results = run_fixture_requests(body.requests).await;

        assert_eq!(results[0]["response"]["type"], "get_autocommit");
        assert_eq!(results[0]["response"]["is_autocommit"], true);
        assert_eq!(results[2]["response"]["type"], "get_autocommit");
        assert_eq!(results[2]["response"]["is_autocommit"], false);
        assert_eq!(results[4]["response"]["type"], "get_autocommit");
        assert_eq!(results[4]["response"]["is_autocommit"], true);
    }

    #[tokio::test]
    async fn raw_fixture_close_response_shape() {
        let body: PipelineReqBody = serde_json::from_value(serde_json::json!({
            "requests": [
                { "type": "close" }
            ]
        }))
        .unwrap();

        let results = run_fixture_requests(body.requests).await;

        assert_eq!(
            results[0],
            serde_json::json!({
                "type": "ok",
                "response": {
                    "type": "close"
                }
            })
        );
    }

    #[tokio::test]
    async fn raw_fixture_missing_sql_and_unknown_sql_id_are_protocol_errors() {
        let body: PipelineReqBody = serde_json::from_value(serde_json::json!({
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "want_rows": true
                    }
                },
                {
                    "type": "execute",
                    "stmt": {
                        "sql_id": 404,
                        "want_rows": true
                    }
                }
            ]
        }))
        .unwrap();

        let results = run_fixture_requests(body.requests).await;

        assert_eq!(results[0]["type"], "error");
        assert_eq!(results[0]["error"]["code"], "HRANA_PROTO_ERROR");
        assert_eq!(
            results[0]["error"]["message"],
            "statement sql or sql_id is required"
        );
        assert_eq!(results[1]["type"], "error");
        assert_eq!(results[1]["error"]["code"], "HRANA_PROTO_ERROR");
        assert_eq!(
            results[1]["error"]["message"],
            "stored SQL id 404 does not exist"
        );
    }

    #[test]
    fn raw_fixture_rejects_execute_request_missing_stmt() {
        let error = serde_json::from_value::<PipelineReqBody>(serde_json::json!({
            "requests": [
                {
                    "type": "execute"
                }
            ]
        }))
        .unwrap_err();

        assert!(error.to_string().contains("missing field `stmt`"));
    }

    #[tokio::test]
    async fn raw_fixture_batch_preserves_result_and_error_ordering() {
        let body: PipelineReqBody = serde_json::from_value(serde_json::json!({
            "requests": [
                {
                    "type": "batch",
                    "batch": {
                        "steps": [
                            {
                                "stmt": {
                                    "sql": "create table services (id integer primary key)"
                                }
                            },
                            {
                                "stmt": {
                                    "sql": "insert into services values (1)"
                                }
                            },
                            {
                                "stmt": {
                                    "sql": "insert into services values (1)"
                                }
                            },
                            {
                                "condition": {
                                    "type": "error",
                                    "step": 2
                                },
                                "stmt": {
                                    "sql": "select count(*) as count from services",
                                    "want_rows": true
                                }
                            },
                            {
                                "condition": {
                                    "type": "ok",
                                    "step": 2
                                },
                                "stmt": {
                                    "sql": "select 99",
                                    "want_rows": true
                                }
                            }
                        ]
                    }
                }
            ]
        }))
        .unwrap();

        let results = run_fixture_requests(body.requests).await;
        let batch = &results[0]["response"]["result"];

        assert_eq!(results[0]["type"], "ok");
        assert_eq!(results[0]["response"]["type"], "batch");
        assert!(batch["step_results"][0].is_object());
        assert!(batch["step_results"][1].is_object());
        assert!(batch["step_results"][2].is_null());
        assert_eq!(batch["step_errors"][2]["code"], "SQLITE_CONSTRAINT");
        assert_eq!(
            batch["step_results"][3]["rows"][0][0],
            serde_json::json!({
                "type": "integer",
                "value": "1"
            })
        );
        assert!(batch["step_results"][4].is_null());
        assert!(batch["step_errors"][4].is_null());
    }

    #[test]
    fn rejects_unsafe_database_names() {
        for database in ["", ".", "..", "../tenant", "tenant/name", "tenant name"] {
            assert!(
                validate_database_name(database).is_err(),
                "expected {database:?} to be rejected"
            );
        }
        validate_database_name("tenant.prod-1").unwrap();
    }

    #[test]
    fn parses_orion_read_policy_headers() {
        let headers = HeaderMap::new();
        assert_eq!(
            orion_read_policy_from_headers(&headers).unwrap(),
            OrionReadPolicy::Strong
        );

        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "local".parse().unwrap());
        assert_eq!(
            orion_read_policy_from_headers(&headers).unwrap(),
            OrionReadPolicy::Local
        );

        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "session".parse().unwrap());
        headers.insert(MIN_APPLIED_INDEX_HEADER, "7".parse().unwrap());
        headers.insert(READ_TIMEOUT_MS_HEADER, "250".parse().unwrap());
        assert_eq!(
            orion_read_policy_from_headers(&headers).unwrap(),
            OrionReadPolicy::Session {
                min_applied_index: Some(7),
                timeout_ms: 250
            }
        );

        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "bounded_staleness".parse().unwrap());
        headers.insert(MAX_STALENESS_MS_HEADER, "100".parse().unwrap());
        assert_eq!(
            orion_read_policy_from_headers(&headers).unwrap(),
            OrionReadPolicy::BoundedStaleness {
                max_staleness_ms: 100
            }
        );
    }

    #[test]
    fn rejects_invalid_orion_read_policy_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "eventual".parse().unwrap());
        let error = orion_read_policy_from_headers(&headers).unwrap_err();
        assert_eq!(sqlite_error_code(error.as_ref()), "HRANA_PROTO_ERROR");

        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "session".parse().unwrap());
        headers.insert(MIN_APPLIED_INDEX_HEADER, "not-a-number".parse().unwrap());
        let error = orion_read_policy_from_headers(&headers).unwrap_err();
        assert_eq!(sqlite_error_code(error.as_ref()), "HRANA_PROTO_ERROR");

        let mut headers = HeaderMap::new();
        headers.insert(READ_POLICY_HEADER, "bounded_staleness".parse().unwrap());
        let error = orion_read_policy_from_headers(&headers).unwrap_err();
        assert_eq!(sqlite_error_code(error.as_ref()), "HRANA_PROTO_ERROR");
    }

    #[test]
    fn classifies_fresh_read_requirements() {
        let read = PipelineReqBody {
            baton: None,
            requests: vec![StreamRequest::Execute {
                stmt: query_stmt("select * from services"),
            }],
        };
        assert!(pipeline_requires_fresh_read(&read));

        let write = PipelineReqBody {
            baton: None,
            requests: vec![StreamRequest::Execute {
                stmt: stmt("insert into services values (1)"),
            }],
        };
        assert!(!pipeline_requires_fresh_read(&write));

        let stored_sql = PipelineReqBody {
            baton: None,
            requests: vec![StreamRequest::Execute {
                stmt: Stmt {
                    sql: None,
                    sql_id: Some(1),
                    args: Vec::new(),
                    named_args: Vec::new(),
                    want_rows: false,
                },
            }],
        };
        assert!(pipeline_requires_fresh_read(&stored_sql));
    }

    #[test]
    fn validates_static_auth_prefixes() {
        LibsqlHttpAuthConfig {
            tokens: vec![LibsqlHttpAuthTokenConfig {
                token: "secret".to_string(),
                database_prefixes: vec!["tenant_".to_string()],
                system_permissions: Vec::new(),
            }],
        }
        .validate()
        .unwrap();

        assert!(
            LibsqlHttpAuthConfig {
                tokens: vec![LibsqlHttpAuthTokenConfig {
                    token: String::new(),
                    database_prefixes: vec!["tenant".to_string()],
                    system_permissions: Vec::new(),
                }],
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_reports_ok_status_and_pipeline_body_for_successful_query() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/tenant_ok/v2/pipeline",
                one_query_pipeline("select 42 as answer"),
                None,
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert!(body["baton"].as_str().unwrap().starts_with("tenant_ok-"));
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][0]["response"]["type"], "execute");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({
                "type": "integer",
                "value": "42"
            })
        );
        assert_eq!(body["orion"]["node_id"], 1);
        assert_eq!(body["orion"]["read_policy"], "strong");
        assert!(body["orion"]["freshness"]["raft"]["applied_index"].is_number());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_reuses_committed_idempotent_write_response() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let request = sql_pipeline(vec![
            execute_request(
                "create table if not exists idem_items (id integer primary key, value text not null)",
                false,
            ),
            execute_request("insert into idem_items values (1, 'created-once')", false),
        ]);

        let (first_status, first_body) = fixture
            .pipeline_with_headers(
                "/idem_reuse/v2/pipeline",
                request.clone(),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "idem-create-1")],
            )
            .await;
        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(first_body["results"][0]["type"], "ok", "{first_body:?}");
        assert_eq!(first_body["orion"]["idempotency"]["status"], "committed");
        assert_eq!(first_body["orion"]["idempotency"]["reused"], false);

        let (second_status, second_body) = fixture
            .pipeline_with_headers(
                "/idem_reuse/v2/pipeline",
                request,
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "idem-create-1")],
            )
            .await;
        assert_eq!(second_status, StatusCode::OK);
        assert_eq!(second_body["results"], first_body["results"]);
        assert_eq!(second_body["orion"]["idempotency"]["reused"], true);

        let (read_status, read_body) = fixture
            .pipeline(
                "/idem_reuse/v2/pipeline",
                one_query_pipeline("select count(*) from idem_items"),
                None,
            )
            .await;
        assert_eq!(read_status, StatusCode::OK);
        assert_eq!(
            read_body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "1"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_rejects_idempotency_key_conflicts_and_transaction_control() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let first = sql_pipeline(vec![
            execute_request(
                "create table if not exists idem_conflicts (id integer primary key, value text not null)",
                false,
            ),
            execute_request("insert into idem_conflicts values (1, 'first')", false),
        ]);
        let second = sql_pipeline(vec![execute_request(
            "insert into idem_conflicts values (2, 'second')",
            false,
        )]);

        let (status, body) = fixture
            .pipeline_with_headers(
                "/idem_conflict/v2/pipeline",
                first,
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "same-key")],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");

        let (status, body) = fixture
            .pipeline_with_headers(
                "/idem_conflict/v2/pipeline",
                second,
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "same-key")],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(
            body["results"][0]["error"]["code"],
            "ORION_IDEMPOTENCY_CONFLICT"
        );

        let (status, body) = fixture
            .pipeline_with_headers(
                "/idem_conflict/v2/pipeline",
                sql_pipeline(vec![execute_request("begin immediate", false)]),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "txn-key")],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "HRANA_PROTO_ERROR");
    }

    #[test]
    fn pending_idempotency_record_reports_commit_unknown() {
        let session = memory_session();
        let idempotency = IdempotencyContext {
            key: "pending-key".to_string(),
            request_hash: "hash-1".to_string(),
        };
        ensure_idempotency_schema(&session.conn).unwrap();
        insert_pending_idempotency_record(&session.conn, &idempotency).unwrap();

        let stored = read_idempotency_record(&session.conn, &idempotency.key)
            .unwrap()
            .unwrap();
        let error =
            resolve_stored_idempotency_record(&session.conn, idempotency, stored, Duration::ZERO)
                .unwrap_err();

        assert_eq!(
            idempotency_or_sqlite_error_code(error.as_ref()),
            "ORION_COMMIT_UNKNOWN"
        );
        assert!(
            error
                .to_string()
                .contains("retry later with the same request")
        );
    }

    #[test]
    fn idempotency_gc_respects_ttls_and_record_budget() {
        let session = memory_session();
        ensure_idempotency_schema(&session.conn).unwrap();
        let now = sqlite_i64(current_time_millis());
        let old = now - 10_000;
        let fresh = now;
        for key in ["committed-1", "committed-2", "committed-3"] {
            session
                .conn
                .execute(
                    &format!(
                        "insert into {IDEMPOTENCY_TABLE} \
                         (key, request_hash, status, results_json, created_at_ms, updated_at_ms) \
                         values (?, 'hash', 'committed', '[]', ?, ?)"
                    ),
                    params![key, old, old],
                )
                .unwrap();
        }
        for key in ["pending-1", "pending-2"] {
            session
                .conn
                .execute(
                    &format!(
                        "insert into {IDEMPOTENCY_TABLE} \
                         (key, request_hash, status, results_json, created_at_ms, updated_at_ms) \
                         values (?, 'hash', 'pending', '[]', ?, ?)"
                    ),
                    params![key, old, old],
                )
                .unwrap();
        }
        session
            .conn
            .execute(
                &format!(
                    "insert into {IDEMPOTENCY_TABLE} \
                     (key, request_hash, status, results_json, created_at_ms, updated_at_ms) \
                     values ('fresh', 'hash', 'committed', '[]', ?, ?)"
                ),
                params![fresh, fresh],
            )
            .unwrap();

        let config = LibsqlHttpIdempotencyConfig {
            committed_ttl_ms: 1_000,
            pending_ttl_ms: 1_000,
            gc_max_records_per_pass: 3,
            ..LibsqlHttpIdempotencyConfig::default()
        };
        let stats = collect_idempotency_garbage_for_connection(&session.conn, &config, 3).unwrap();
        assert_eq!(stats.deleted_committed, 3);
        assert_eq!(stats.deleted_pending, 0);

        let stats = collect_idempotency_garbage_for_connection(&session.conn, &config, 10).unwrap();
        assert_eq!(stats.deleted_committed, 0);
        assert_eq!(stats.deleted_pending, 2);

        let remaining: i64 = session
            .conn
            .query_row(
                &format!("select count(*) from {IDEMPOTENCY_TABLE}"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_rejects_idempotency_header_when_policy_is_disabled() {
        let fixture = RouterFixture::new_with_idempotency_config(
            LibsqlHttpAuthConfig::default(),
            LibsqlHttpIdempotencyConfig {
                enabled: false,
                ..LibsqlHttpIdempotencyConfig::default()
            },
        )
        .await;

        let (status, body) = fixture
            .pipeline_with_headers(
                "/idem_disabled/v2/pipeline",
                sql_pipeline(vec![execute_request(
                    "create table idem_disabled (id integer)",
                    false,
                )]),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "disabled-key")],
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "HRANA_PROTO_ERROR");
        assert!(
            body["results"][0]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("idempotency keys are disabled")
        );

        let (status, body) = fixture.get("/_orion/metrics/idempotency", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enabled"], false);
        assert_eq!(body["rejected"], 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_exposes_virtual_system_tables_without_sqlite_storage() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select node_id, stale, observed_age_ms from raft_metrics where node_id = 1 order by node_id limit 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][0]["name"],
            "node_id"
        );
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "1"
        );
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][1]["name"],
            "stale"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select databases from storage_pressure where compaction_eligible_files >= 0 limit 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][0]["name"],
            "databases"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select group_id, active_uploads from large_payload_metrics order by group_id limit 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][0]["name"],
            "group_id"
        );
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][1]["name"],
            "active_uploads"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select enabled, requests, gc_runs from idempotency_metrics limit 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][0]["name"],
            "enabled"
        );

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "catalog_visible" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["state"], "ready");

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select name, state, generation from _orion.database_catalog where name = 'catalog_visible'",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "catalog_visible"
        );
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][1]["value"],
            "ready"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select database_name, group_id, member_role, cloud from _orion.database_placement where database_name = 'catalog_visible' order by member_node_id limit 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][1]["value"],
            DEFAULT_REPLICATION_GROUP_ID
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select node_id, cloud, region, zone from _orion.placement_nodes where node_id = 1",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][1]["value"],
            "local"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline("select count(*) from _orion.placement_metrics"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["cols"][0]["name"],
            "count(*)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_exposes_operator_json_endpoints() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture.get("/_orion/metrics/raft", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node_id"], 1);
        assert_eq!(body["stale_after_ms"], RAFT_METRICS_STALE_AFTER_MS);
        assert!(
            body["raft_metrics"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        );
        assert!(body["raft_metrics"][0]["observed_age_ms"].is_number());
        assert!(body["raft_metrics"][0]["stale"].is_boolean());

        let (status, body) = fixture.get("/_orion/metrics/storage", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["databases"].is_number());

        let (status, body) = fixture.get("/_orion/metrics/blob", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["max_chunk_bytes"], 512 * 1024);
        assert_eq!(
            body["max_open_handles_per_session"],
            MAX_OPEN_BLOB_HANDLES_PER_SESSION
        );
        assert_eq!(body["requests"]["total"], 0);
        assert_eq!(body["bytes"]["read"], 0);
        assert_eq!(body["bytes"]["written"], 0);
        assert!(body["latency"]["total_ns"].is_number());

        let (status, body) = fixture.get("/_orion/metrics/large-payload", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node_id"], 1);
        assert!(
            body["large_payload_metrics"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        );
        assert_eq!(
            body["large_payload_metrics"][0]["metrics"]["active_uploads"],
            0
        );

        let (status, body) = fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["operations_total"], 0);
        assert_eq!(body["groups_active"], 1);
        assert_eq!(body["groups_unloaded"], 0);

        let (status, body) = fixture.get("/_orion/metrics/idempotency", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enabled"], true);
        assert_eq!(body["committed_ttl_ms"], 86_400_000);

        let (status, body) = fixture.get("/_orion/placement/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["nodes"][0]["node_id"], 1);
        assert_eq!(body["nodes"][0]["cloud"], "local");

        let (status, _body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({
                    "name": "placed_endpoint",
                    "placement": {
                        "mode": "single_region",
                        "failover": {
                            "automatic": true,
                            "promote_after_ms": 1000
                        }
                    }
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, body) = fixture.get("/_orion/replication-groups", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["replication_groups"][0]["group_id"],
            DEFAULT_REPLICATION_GROUP_ID
        );

        let (status, body) = fixture
            .get("/_orion/databases/placed_endpoint/placement", None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["group"]["group_id"], DEFAULT_REPLICATION_GROUP_ID);
        assert_eq!(body["group"]["members"][0]["cloud"], "local");

        let (status, body) = fixture
            .post(
                "/_orion/databases/placed_endpoint/placement/plan",
                serde_json::json!({
                    "mode": "dual_cloud_quorum",
                    "durability": {
                        "survive_cloud_outage": true
                    }
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["selected_group_id"], DEFAULT_REPLICATION_GROUP_ID);
        assert_eq!(body["valid"], false);

        let (status, body) = fixture.get("/_orion/compaction", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["recent_runs"].is_array());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_manages_operator_replication_group_catalog() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_dedicated_test",
                    "placement": {
                        "mode": "single_region",
                        "failover": {
                            "automatic": true,
                            "promote_after_ms": 5000
                        }
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        assert_eq!(body["group_id"], "rg_dedicated_test");
        assert_eq!(body["state"], "active");
        assert_eq!(body["members"][0]["node_id"], 1);
        assert_eq!(body["runtime"]["loaded"], false);

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups/rg_dedicated_test/members",
                serde_json::json!({
                    "node_id": 1,
                    "role": "learner",
                    "priority": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["members"].as_array().unwrap().len(), 2);

        let (status, body) = fixture
            .get("/_orion/replication-groups/runtime", None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["runtime_groups"][0]["group_id"],
            DEFAULT_REPLICATION_GROUP_ID
        );
        assert_eq!(body["runtime_groups"][0]["loaded"], true);

        let (status, body) = fixture
            .post("/_orion/placement/reconcile", serde_json::json!({}), None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert!(
            body["actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action == "load_replication_group:rg_dedicated_test")
        );

        let (status, body) = fixture
            .delete(
                "/_orion/replication-groups/rg_dedicated_test/members/1/voter",
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("last voter"));

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups/rg_dedicated_test/drain",
                serde_json::json!({}),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["state"], "draining");

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "move_guard_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");

        let (status, body) = fixture
            .post(
                "/_orion/databases/move_guard_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_dedicated_test" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("not loaded"));

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_loaded_test",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        fixture
            .state
            .replication_groups
            .register_for_test("rg_loaded_test", loaded_runtime)
            .unwrap();

        let (open_baton, _session) = fixture.state.session("move_guard_db", None).unwrap();
        let (status, body) = fixture
            .post(
                "/_orion/databases/move_guard_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_loaded_test" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("active session"));
        fixture.state.close_session(&open_baton).unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases/move_guard_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_loaded_test" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        assert_eq!(body["status"], "running");
        assert_eq!(body["phase"], "planned");
        assert_eq!(body["target_group_id"], "rg_loaded_test");
        let operation_id = body["operation_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/_orion/databases/move_guard_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_loaded_test" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("running placement operation")
        );

        let (status, body) = fixture
            .pipeline(
                "/move_guard_db/v2/pipeline",
                one_query_pipeline("select 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error", "{body:?}");
        assert!(
            body["results"][0]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("fenced"),
            "{body:?}"
        );

        for expected_action in [":fenced", ":cloning", ":catching_up"] {
            let (status, body) = fixture
                .post("/_orion/placement/reconcile", serde_json::json!({}), None)
                .await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
            assert!(
                body["actions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|action| action.as_str().unwrap().contains(expected_action)),
                "{body:?}"
            );
        }

        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        let running_operation = read_placement_operation(&conn, &operation_id)
            .unwrap()
            .unwrap();
        assert!(running_operation.source_fence_applied_index.is_some());
        assert!(running_operation.source_fence_commit_ts.is_some());
        assert!(running_operation.source_fence_observed_at_ms.is_some());
        assert!(running_operation.target_clone_applied_index.is_some());
        assert!(running_operation.target_clone_commit_ts.is_some());
        assert_eq!(running_operation.phase, "catching_up");
        drop(conn);

        for expected_action in [":switching", ":completed"] {
            let (status, body) = fixture
                .post("/_orion/placement/reconcile", serde_json::json!({}), None)
                .await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
            assert!(
                body["actions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|action| action.as_str().unwrap().contains(expected_action)),
                "{body:?}"
            );
        }

        let placement = read_database_placement_record(&fixture.state, "move_guard_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_loaded_test");
        assert!(placement.group.runtime.loaded);

        let (status, body) = fixture
            .get("/_orion/databases/move_guard_db/placement/operations", None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["operations"][0]["status"], "completed");
        assert_eq!(body["operations"][0]["phase"], "completed");
        assert_eq!(
            body["operations"][0]["source_group_id"],
            DEFAULT_REPLICATION_GROUP_ID
        );
        assert_eq!(body["operations"][0]["target_group_id"], "rg_loaded_test");

        let (status, body) = fixture
            .delete("/_orion/replication-groups/rg_loaded_test", None)
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("still has"));

        let (status, body) = fixture
            .delete("/_orion/replication-groups/rg_dedicated_test", None)
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["state"], "deleted");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_operations_can_be_cancelled_and_repaired() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_repair_test",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        fixture
            .state
            .replication_groups
            .register_for_test("rg_repair_test", loaded_runtime)
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "repair_move_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");

        let (status, body) = fixture
            .post(
                "/_orion/databases/repair_move_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_repair_test" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        let operation_id = body["operation_id"].as_str().unwrap().to_string();

        let (status, body) = fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["operations_running"], 1);
        assert_eq!(body["running_by_phase"]["planned"], 1);

        let cancel_path =
            format!("/_orion/databases/repair_move_db/placement/operations/{operation_id}/cancel");
        let (status, body) = fixture
            .post(
                &cancel_path,
                serde_json::json!({ "reason": "test cancellation" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["status"], "failed");
        assert_eq!(body["phase"], "failed");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("cancelled: test cancellation")
        );

        let (status, body) = fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["operations_failed"], 1);
        assert_eq!(body["operations_running"], 0);

        let repair_path =
            format!("/_orion/databases/repair_move_db/placement/operations/{operation_id}/repair");
        let (status, body) = fixture
            .post(
                &repair_path,
                serde_json::json!({
                    "phase": "planned",
                    "reason": "resume after operator check"
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["status"], "running");
        assert_eq!(body["phase"], "planned");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("repair: resume after operator check")
        );

        for expected_action in [
            ":fenced",
            ":cloning",
            ":catching_up",
            ":switching",
            ":completed",
        ] {
            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );
        }

        let placement = read_database_placement_record(&fixture.state, "repair_move_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_repair_test");
        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        let operation = read_placement_operation(&conn, &operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(operation.status, "completed");
        assert_eq!(operation.phase, "completed");
        let database = read_database_catalog_record_from_conn(&conn, "repair_move_db")
            .unwrap()
            .unwrap();
        conn.execute(
            "update placement_operations set completed_at_ms = 0 where operation_id = ?",
            [&operation_id],
        )
        .unwrap();
        conn.execute(
            r#"
            insert into database_standby_copies (
                database_id, database_name, source_group_id, target_group_id,
                source_applied_index, source_commit_ts_physical_ms, source_commit_ts_logical,
                target_applied_index, target_commit_ts_physical_ms, target_commit_ts_logical,
                refreshed_at_ms, updated_at_ms, error
            )
            values (?, ?, ?, ?, 1, null, null, 1, null, null, 0, 0, null)
            "#,
            params![
                database.database_id,
                database.name,
                DEFAULT_REPLICATION_GROUP_ID,
                "rg_repair_test"
            ],
        )
        .unwrap();
        drop(conn);

        let (status, body) = fixture
            .post(
                "/_orion/placement/gc",
                serde_json::json!({
                    "older_than_ms": 1,
                    "limit": 10
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["deleted"], 2);
        assert_eq!(body["operations_deleted"], 1);
        assert_eq!(body["standbys_deleted"], 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_move_can_drain_active_sessions() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_drain_sessions",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        fixture
            .state
            .replication_groups
            .register_for_test("rg_drain_sessions", loaded_runtime)
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "session_drain_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (_baton, _session) = fixture.state.session("session_drain_db", None).unwrap();
        assert_eq!(
            fixture
                .state
                .active_database_sessions("session_drain_db")
                .unwrap(),
            1
        );

        let (status, body) = fixture
            .post(
                "/_orion/databases/session_drain_db/placement/move",
                serde_json::json!({
                    "target_group_id": "rg_drain_sessions",
                    "drain_timeout_ms": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        assert_eq!(body["status"], "running");
        assert_eq!(
            fixture
                .state
                .active_database_sessions("session_drain_db")
                .unwrap(),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_reconcile_enqueues_automatic_moves_for_draining_groups() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for group_id in ["rg_auto_source", "rg_auto_target"] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 1
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "auto_drain_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let operation = create_database_move_operation(
            &fixture.state,
            "auto_drain_db",
            "rg_auto_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary
                .actions
                .iter()
                .any(|action| action.ends_with(":completed"))
            {
                break;
            }
        }
        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        let operation = read_placement_operation(&conn, &operation.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(operation.status, "completed");
        drop(conn);

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups/rg_auto_source/drain",
                serde_json::json!({}),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["state"], "draining");

        let summary = reconcile_placement(&fixture.state).unwrap();
        assert!(
            summary
                .actions
                .iter()
                .any(|action| action.starts_with("automatic_placement_move:auto_drain_db:")),
            "{summary:?}"
        );
        let operations = list_placement_operations(&fixture.state, "auto_drain_db").unwrap();
        assert_eq!(operations[0].status, "running");
        assert_eq!(operations[0].target_group_id, "rg_auto_target");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_move_promotes_post_clone_target_when_source_runtime_is_dead() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for group_id in ["rg_dead_source", "rg_dead_target"] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region"
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "dead_source_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let initial = create_database_move_operation(
            &fixture.state,
            "dead_source_db",
            "rg_dead_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", initial.operation_id)
            }) {
                break;
            }
        }
        let placement = read_database_placement_record(&fixture.state, "dead_source_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_dead_source");

        let operation = create_database_move_operation(
            &fixture.state,
            "dead_source_db",
            "rg_dead_target",
            false,
        )
        .unwrap();
        for expected_action in [":fenced", ":cloning", ":catching_up"] {
            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );
        }

        fixture
            .state
            .replication_groups
            .unregister_for_test("rg_dead_source")
            .unwrap();

        for expected_action in [":switching", ":completed"] {
            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );
        }
        let placement = read_database_placement_record(&fixture.state, "dead_source_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_dead_target");
        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        let operation = read_placement_operation(&conn, &operation.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(operation.status, "completed");
        assert_eq!(operation.phase, "completed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_standby_promotes_when_source_runtime_is_dead_before_move() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for group_id in ["rg_standby_source", "rg_standby_target"] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region"
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "standby_failover_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "standby_failover_db",
            "rg_standby_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }
        let placement = read_database_placement_record(&fixture.state, "standby_failover_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_standby_source");

        let (status, body) = fixture
            .post(
                "/_orion/databases/standby_failover_db/placement/standby",
                serde_json::json!({ "target_group_id": "rg_standby_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["standby"]["source_group_id"], "rg_standby_source");
        assert_eq!(body["standby"]["target_group_id"], "rg_standby_target");

        fixture
            .state
            .replication_groups
            .unregister_for_test("rg_standby_source")
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases/standby_failover_db/placement/promote",
                serde_json::json!({
                    "target_group_id": "rg_standby_target",
                    "max_staleness_ms": 60_000
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["database"]["replication_group_id"],
            "rg_standby_target"
        );

        let placement = read_database_placement_record(&fixture.state, "standby_failover_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_standby_target");

        let (status, body) = fixture
            .get(
                "/_orion/databases/standby_failover_db/placement/standbys",
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["standbys"].as_array().unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_standby_refresh_can_materialize_checkpoint_from_source_peer() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let default_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        let source_registry = ReplicationGroupRegistry::empty();
        source_registry
            .register_for_test(DEFAULT_REPLICATION_GROUP_ID, default_runtime.clone())
            .unwrap();
        let (source_runtime, _source_log_dir, _source_cache_dir) =
            isolated_test_runtime("remote-standby-source").await;
        source_registry
            .register_for_test("rg_remote_standby_source", source_runtime)
            .unwrap();
        let target_registry = ReplicationGroupRegistry::empty();
        target_registry
            .register_for_test(DEFAULT_REPLICATION_GROUP_ID, default_runtime)
            .unwrap();
        let (target_runtime, _target_log_dir, _target_cache_dir) =
            isolated_test_runtime("remote-standby-target").await;
        target_registry
            .register_for_test("rg_remote_standby_target", target_runtime)
            .unwrap();

        let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_addr = source_listener.local_addr().unwrap();
        let source_http_cache_dir = TempDir::new().unwrap();
        let target_http_cache_dir = TempDir::new().unwrap();
        let placement_nodes = BTreeMap::from([
            (
                1,
                PlacementNodeConfig {
                    node_id: 1,
                    cloud: "local".to_string(),
                    region: "source".to_string(),
                    zone: "source-a".to_string(),
                    raft_addr: "127.0.0.1:0".to_string(),
                    libsql_http_addr: Some(format!("http://{source_addr}")),
                },
            ),
            (
                2,
                PlacementNodeConfig {
                    node_id: 2,
                    cloud: "local".to_string(),
                    region: "target".to_string(),
                    zone: "target-a".to_string(),
                    raft_addr: "127.0.0.1:0".to_string(),
                    libsql_http_addr: None,
                },
            ),
        ]);
        let source_state = LibsqlHttpState::new(
            fixture.state.replication_groups.default_runtime().unwrap(),
            &LibsqlHttpConfig {
                sqlite_cache_root: source_http_cache_dir.path().to_path_buf(),
                session_idle_timeout: Duration::from_secs(60),
                blob_max_chunk_bytes: 512 * 1024,
                idempotency: LibsqlHttpIdempotencyConfig::default(),
                auth: LibsqlHttpAuthConfig::default(),
                node_id: 1,
                peer_http_endpoints: BTreeMap::new(),
                placement_nodes: placement_nodes.clone(),
                metrics_registry: ClusterRaftMetricsRegistry::default(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: Some(source_registry),
            },
        );
        let source_server = tokio::spawn({
            let source_state = source_state.clone();
            async move {
                axum::serve(source_listener, libsql_router(source_state))
                    .await
                    .unwrap();
            }
        });
        let target_state = LibsqlHttpState::new(
            fixture.state.replication_groups.default_runtime().unwrap(),
            &LibsqlHttpConfig {
                sqlite_cache_root: target_http_cache_dir.path().to_path_buf(),
                session_idle_timeout: Duration::from_secs(60),
                blob_max_chunk_bytes: 512 * 1024,
                idempotency: LibsqlHttpIdempotencyConfig::default(),
                auth: LibsqlHttpAuthConfig::default(),
                node_id: 2,
                peer_http_endpoints: BTreeMap::from([(1, format!("http://{source_addr}"))]),
                placement_nodes: placement_nodes.clone(),
                metrics_registry: ClusterRaftMetricsRegistry::default(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: Some(target_registry),
            },
        );
        let source_router = libsql_router(source_state.clone());
        let target_router = libsql_router(target_state.clone());

        for (group_id, node_id) in [
            ("rg_remote_standby_source", 1_u64),
            ("rg_remote_standby_target", 2_u64),
        ] {
            let (status, body) = RouterFixture {
                router: source_router.clone(),
                state: source_state.clone(),
                _cache_dir: TempDir::new().unwrap(),
                _log_dir: TempDir::new().unwrap(),
            }
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": group_id,
                    "placement": { "mode": "manual" },
                    "members": [{ "node_id": node_id, "role": "voter", "priority": 0 }]
                }),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
        }

        let source_fixture = RouterFixture {
            router: source_router.clone(),
            state: source_state.clone(),
            _cache_dir: TempDir::new().unwrap(),
            _log_dir: TempDir::new().unwrap(),
        };
        let (status, body) = source_fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "remote_standby_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (status, body) = source_fixture
            .pipeline(
                "/remote_standby_db/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table items(id integer primary key, value text not null)",
                        false,
                    ),
                    execute_request("insert into items values (1, 'remote-copy')", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        source_state
            .close_database_sessions("remote_standby_db")
            .unwrap();
        let (status, body) = source_fixture
            .post(
                "/_orion/databases/remote_standby_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_remote_standby_source" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        for _ in 0..5 {
            reconcile_placement(&source_state).unwrap();
        }
        let placement = read_database_placement_record(&source_state, "remote_standby_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_remote_standby_source");
        let (status, body) = source_fixture
            .pipeline(
                "/remote_standby_db/v2/pipeline",
                one_query_pipeline("select value from items where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"], "remote-copy",
            "{body:?}"
        );
        let (status, checkpoint) = source_fixture
            .get(
                "/_orion/internal/databases/remote_standby_db/placement/checkpoint?source_group_id=rg_remote_standby_source",
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{checkpoint:?}");
        assert_eq!(checkpoint["source_group_id"], "rg_remote_standby_source");
        assert!(
            checkpoint["objects"]
                .as_array()
                .is_some_and(|objects| !objects.is_empty()),
            "{checkpoint:?}"
        );

        let target_fixture = RouterFixture {
            router: target_router,
            state: target_state.clone(),
            _cache_dir: TempDir::new().unwrap(),
            _log_dir: TempDir::new().unwrap(),
        };
        let (status, body) = target_fixture
            .post(
                "/_orion/databases/remote_standby_db/placement/standby",
                serde_json::json!({ "target_group_id": "rg_remote_standby_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["standby"]["source_group_id"],
            "rg_remote_standby_source"
        );
        assert_eq!(
            body["standby"]["target_group_id"],
            "rg_remote_standby_target"
        );
        let (status, metrics) = target_fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK, "{metrics:?}");
        assert_eq!(metrics["standby_checkpoint"]["attempts"], 1);
        assert_eq!(metrics["standby_checkpoint"]["successes"], 1);
        assert_eq!(metrics["standby_checkpoint"]["fallback_to_backup"], 0);
        assert!(
            metrics["standby_checkpoint"]["objects_copied"]
                .as_u64()
                .is_some_and(|count| count > 0),
            "{metrics:?}"
        );
        assert_eq!(body["standby"]["catalog_recorded"], true, "{body:?}");
        assert_eq!(body["standby"]["target_group_available"], true, "{body:?}");
        assert_eq!(body["standby"]["target_locally_openable"], true, "{body:?}");
        assert_eq!(body["standby"]["promotable"], true, "{body:?}");

        let (status, body) = source_fixture
            .pipeline(
                "/remote_standby_db/v2/pipeline",
                one_query_pipeline("update items set value = 'remote-delta' where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        source_state
            .close_database_sessions("remote_standby_db")
            .unwrap();
        let (status, body) = target_fixture
            .post(
                "/_orion/databases/remote_standby_db/placement/standby",
                serde_json::json!({ "target_group_id": "rg_remote_standby_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["standby"]["source_group_id"],
            "rg_remote_standby_source"
        );
        let (status, metrics) = target_fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK, "{metrics:?}");
        assert_eq!(
            metrics["standby_checkpoint"]["attempts"], 1,
            "second remote refresh should use page delta before checkpoint: {metrics:?}"
        );
        assert_eq!(metrics["standby_checkpoint"]["successes"], 1);
        assert_eq!(metrics["standby_checkpoint"]["fallback_to_backup"], 0);
        assert_eq!(metrics["standby_page_delta"]["attempts"], 1);
        assert_eq!(metrics["standby_page_delta"]["successes"], 1);
        assert_eq!(metrics["standby_page_delta"]["failures"], 0);
        assert_eq!(
            metrics["standby_page_delta"]["fallback_to_checkpoint"], 0,
            "{metrics:?}"
        );
        assert!(
            metrics["standby_page_delta"]["entries_applied"]
                .as_u64()
                .is_some_and(|count| count > 0),
            "{metrics:?}"
        );

        let survivor_registry = ReplicationGroupRegistry::empty();
        survivor_registry
            .register_for_test(
                DEFAULT_REPLICATION_GROUP_ID,
                fixture.state.replication_groups.default_runtime().unwrap(),
            )
            .unwrap();
        let (empty_target_runtime, _empty_target_log_dir, _empty_target_cache_dir) =
            isolated_test_runtime("remote-standby-empty-target").await;
        survivor_registry
            .register_for_test("rg_remote_standby_target", empty_target_runtime)
            .unwrap();
        let survivor_cache_dir = TempDir::new().unwrap();
        let survivor_state = LibsqlHttpState::new(
            fixture.state.replication_groups.default_runtime().unwrap(),
            &LibsqlHttpConfig {
                sqlite_cache_root: survivor_cache_dir.path().to_path_buf(),
                session_idle_timeout: Duration::from_secs(60),
                blob_max_chunk_bytes: 512 * 1024,
                idempotency: LibsqlHttpIdempotencyConfig::default(),
                auth: LibsqlHttpAuthConfig::default(),
                node_id: 3,
                peer_http_endpoints: BTreeMap::new(),
                placement_nodes: placement_nodes.clone(),
                metrics_registry: ClusterRaftMetricsRegistry::default(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: Some(survivor_registry),
            },
        );
        let survivor_standbys =
            list_database_placement_standbys(&survivor_state, "remote_standby_db").unwrap();
        assert_eq!(survivor_standbys.len(), 1);
        assert!(survivor_standbys[0].catalog_recorded);
        assert!(survivor_standbys[0].target_group_available);
        assert!(!survivor_standbys[0].target_locally_openable);
        assert!(!survivor_standbys[0].promotable);
        let survivor_metrics = placement_metrics(&survivor_state).unwrap();
        assert_eq!(survivor_metrics.standbys_total, 1);
        assert_eq!(survivor_metrics.standbys_promotable, 0);
        let survivor_error = promote_database_placement_standby(
            &survivor_state,
            "remote_standby_db",
            "rg_remote_standby_target",
            Some(60_000),
            false,
        )
        .unwrap_err();
        assert!(
            error_chain_message(&survivor_error)
                .contains("database remote_standby_db is not present on this target runtime"),
            "{survivor_error:?}"
        );

        let (status, body) = target_fixture
            .post(
                "/_orion/databases/remote_standby_db/placement/promote",
                serde_json::json!({ "target_group_id": "rg_remote_standby_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert!(
            target_state
                .replication_groups
                .contains("rg_remote_standby_target")
                .unwrap()
        );
        let db = target_state.database("remote_standby_db").unwrap();
        let conn = db.connect().unwrap();
        let copied_value: String = conn
            .query_row("select value from items where id = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(copied_value, "remote-delta");
        source_server.abort();
        let _ = source_server.await;
    }

    #[test]
    fn checkpoint_object_path_guard_rejects_paths_outside_database_prefix() {
        ensure_checkpoint_object_path_allowed(
            "node/state__sqlite/databases/tenant/state",
            "node/state__sqlite/databases/tenant/state/manifest/00000000000000000001",
        )
        .unwrap();
        assert!(
            ensure_checkpoint_object_path_allowed(
                "node/state__sqlite/databases/tenant/state",
                "node/state__sqlite/databases/other/state/manifest/00000000000000000001",
            )
            .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checkpoint_object_size_mismatch_fails_without_writing_target_object() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (target_runtime, _target_log_dir, _target_cache_dir) =
            isolated_test_runtime("checkpoint-size-mismatch-target").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_addr = listener.local_addr().unwrap();
        let source_server = tokio::spawn(async move {
            let app = Router::new().route(
                "/_orion/internal/databases/mismatch_db/placement/checkpoint/object",
                get(|| async { (StatusCode::OK, Body::from(Bytes::from_static(b"short"))) }),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let object_path =
            "node/state__sqlite/databases/mismatch_db/state/manifest/00000000000000000001";
        let object_location = ObjectPath::parse(object_path).unwrap();
        let checkpoint = StandbyCheckpointExport {
            source_group_id: "rg_source".to_string(),
            source_watermark: OrionSqliteRuntimeWatermark {
                applied_index: Some(7),
                applied_commit_ts: None,
            },
            artifact: SlateDbCheckpointArtifact {
                db_path: "node/state__sqlite/databases/mismatch_db/state".to_string(),
                checkpoint_id: "checkpoint-size-mismatch".to_string(),
                checkpoint_manifest_id: 1,
                object_prefix: "node/state__sqlite/databases/mismatch_db/state".to_string(),
            },
            objects: vec![StandbyCheckpointObjectRef {
                path: object_path.to_string(),
                size: 99,
            }],
        };

        let error = fetch_missing_checkpoint_objects_from_peer(
            &fixture.state,
            &HeaderMap::new(),
            &format!("http://{source_addr}"),
            "mismatch_db",
            "rg_source",
            &target_runtime,
            &checkpoint,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("checkpoint object node/state__sqlite/databases/mismatch_db/state/manifest/00000000000000000001 byte count mismatch: received 5, expected 99"),
            "{error:?}"
        );
        assert!(
            !target_object_matches(
                target_runtime.state_store().object_store().as_ref(),
                &object_location,
                99,
            )
            .await
            .unwrap()
        );

        source_server.abort();
        let _ = source_server.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_reconcile_automatically_promotes_fresh_standby() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for group_id in ["rg_auto_standby_source", "rg_auto_standby_target"] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 60_000
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "auto_standby_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "auto_standby_db",
            "rg_auto_standby_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }
        refresh_database_placement_standby(
            &fixture.state,
            "auto_standby_db",
            "rg_auto_standby_target",
        )
        .unwrap();
        fixture
            .state
            .replication_groups
            .unregister_for_test("rg_auto_standby_source")
            .unwrap();

        let summary = reconcile_placement(&fixture.state).unwrap();
        assert!(
            summary.actions.iter().any(|action| action
                == "automatic_standby_promotion:auto_standby_db:rg_auto_standby_target"),
            "{summary:?}"
        );
        assert!(
            summary.actions.iter().any(|action| action
                == "automatic_standby_failback_target:rg_auto_standby_target:rg_auto_standby_source"),
            "{summary:?}"
        );
        let placement = read_database_placement_record(&fixture.state, "auto_standby_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_auto_standby_target");

        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        assert_eq!(
            list_replication_group_standby_targets(&conn, "rg_auto_standby_target").unwrap(),
            vec!["rg_auto_standby_source"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_reconcile_warms_returned_source_after_automatic_standby_promotion() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for (group_id, standby_targets) in [
            ("rg_failback_source", vec!["rg_failback_target"]),
            ("rg_failback_target", Vec::new()),
        ] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 60_000,
                                "standby_targets": standby_targets
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "failback_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "failback_db",
            "rg_failback_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }
        refresh_database_placement_standby(&fixture.state, "failback_db", "rg_failback_target")
            .unwrap();
        fixture
            .state
            .replication_groups
            .unregister_for_test("rg_failback_source")
            .unwrap();

        let summary = reconcile_placement(&fixture.state).unwrap();
        assert!(
            summary.actions.iter().any(
                |action| action == "automatic_standby_promotion:failback_db:rg_failback_target"
            ),
            "{summary:?}"
        );
        assert!(
            summary.actions.iter().any(|action| action
                == "automatic_standby_failback_target:rg_failback_target:rg_failback_source"),
            "{summary:?}"
        );

        let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        fixture
            .state
            .replication_groups
            .register_for_test("rg_failback_source", loaded_runtime)
            .unwrap();
        let refresh = fixture
            .state
            .reconcile_standby_refreshes_once()
            .await
            .unwrap();
        assert_eq!(refresh.errors, 0, "{refresh:?}");
        assert!(
            refresh.actions.iter().any(|action| action
                == "automatic_standby_refresh:failback_db:rg_failback_target:rg_failback_source"),
            "{refresh:?}"
        );

        let standbys = list_database_placement_standbys(&fixture.state, "failback_db").unwrap();
        assert!(standbys.iter().any(|standby| {
            standby.source_group_id == "rg_failback_target"
                && standby.target_group_id == "rg_failback_source"
                && standby.promotable
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_standby_refresh_reconciler_keeps_automatic_standby_warm() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for (group_id, standby_targets) in [
            ("rg_refresh_source", vec!["rg_auto_refresh_target"]),
            ("rg_auto_refresh_target", Vec::new()),
        ] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 60_000,
                                "standby_targets": standby_targets
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "auto_refresh_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "auto_refresh_db",
            "rg_refresh_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }
        let placement = read_database_placement_record(&fixture.state, "auto_refresh_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_refresh_source");

        let (status, body) = fixture
            .post(
                "/_orion/placement/standby/reconcile",
                serde_json::json!({}),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["attempted"], 1);
        assert_eq!(body["refreshed"], 1);
        assert_eq!(body["errors"], 0);
        assert_eq!(
            body["actions"][0],
            "automatic_standby_refresh:auto_refresh_db:rg_refresh_source:rg_auto_refresh_target"
        );

        let standbys = list_database_placement_standbys(&fixture.state, "auto_refresh_db").unwrap();
        assert_eq!(standbys.len(), 1);
        assert_eq!(standbys[0].source_group_id, "rg_refresh_source");
        assert_eq!(standbys[0].target_group_id, "rg_auto_refresh_target");

        let second = fixture
            .state
            .reconcile_standby_refreshes_once()
            .await
            .unwrap();
        assert_eq!(second.attempted, 0, "{second:?}");
        assert_eq!(second.refreshed, 0, "{second:?}");
        assert_eq!(second.errors, 0, "{second:?}");

        let (status, metrics) = fixture.get("/_orion/metrics/placement", None).await;
        assert_eq!(status, StatusCode::OK, "{metrics:?}");
        assert_eq!(metrics["standbys_total"], 1);
        assert_eq!(metrics["standbys_promotable"], 1);
        assert_eq!(metrics["standbys_stale"], 0);
        assert_eq!(metrics["standbys_errors"], 0);

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline(
                    "select source_group_id, target_group_id from _orion.database_standbys where database_name = 'auto_refresh_db'",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "rg_refresh_source"
        );
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][1]["value"],
            "rg_auto_refresh_target"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_standby_refresh_reconciler_skips_groups_without_explicit_targets() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for group_id in ["rg_no_target_source", "rg_no_target_candidate"] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 60_000
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "no_implicit_refresh_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "no_implicit_refresh_db",
            "rg_no_target_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }

        let summary = fixture
            .state
            .reconcile_standby_refreshes_once()
            .await
            .unwrap();
        assert_eq!(summary.attempted, 0, "{summary:?}");
        assert_eq!(summary.refreshed, 0, "{summary:?}");
        assert_eq!(summary.errors, 0, "{summary:?}");
        assert!(summary.skipped >= 1, "{summary:?}");
        assert!(
            list_database_placement_standbys(&fixture.state, "no_implicit_refresh_db")
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_standby_refresh_reconciler_uses_explicit_standby_policy() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        for (group_id, standby_targets) in [
            ("rg_policy_source", vec!["rg_policy_z_target"]),
            ("rg_policy_a_target", Vec::new()),
            ("rg_policy_z_target", Vec::new()),
        ] {
            let (status, body) = fixture
                .post(
                    "/_orion/replication-groups",
                    serde_json::json!({
                        "group_id": group_id,
                        "placement": {
                            "mode": "single_region",
                            "failover": {
                                "automatic": true,
                                "promote_after_ms": 60_000,
                                "standby_targets": standby_targets
                            }
                        },
                        "members": [
                            {
                                "node_id": 1,
                                "role": "voter",
                                "priority": 0
                            }
                        ]
                    }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
            fixture
                .state
                .replication_groups
                .register_for_test(group_id, loaded_runtime)
                .unwrap();
        }

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "policy_refresh_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let source_move = create_database_move_operation(
            &fixture.state,
            "policy_refresh_db",
            "rg_policy_source",
            false,
        )
        .unwrap();
        for _ in 0..5 {
            let summary = reconcile_placement(&fixture.state).unwrap();
            if summary.actions.iter().any(|action| {
                action == &format!("placement_operation:{}:completed", source_move.operation_id)
            }) {
                break;
            }
        }

        let summary = fixture
            .state
            .reconcile_standby_refreshes_once()
            .await
            .unwrap();
        assert_eq!(summary.refreshed, 1, "{summary:?}");
        assert_eq!(
            summary.actions,
            vec!["automatic_standby_refresh:policy_refresh_db:rg_policy_source:rg_policy_z_target"]
        );

        let standbys =
            list_database_placement_standbys(&fixture.state, "policy_refresh_db").unwrap();
        assert_eq!(standbys.len(), 1);
        assert_eq!(standbys[0].target_group_id, "rg_policy_z_target");
    }

    #[test]
    fn placement_operation_treats_transient_disk_io_as_retryable() {
        let error = anyhow!("disk I/O error: Error code 1034: disk I/O error");
        assert!(placement_operation_error_is_retryable(&error));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_move_reconcile_resumes_from_each_durable_phase() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_resume_test",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let loaded_runtime = fixture.state.replication_groups.default_runtime().unwrap();
        fixture
            .state
            .replication_groups
            .register_for_test("rg_resume_test", loaded_runtime)
            .unwrap();

        for (phase, expected_phase, expected_action) in [
            ("planned", "fenced", ":fenced"),
            ("fenced", "cloning", ":cloning"),
            ("cloning", "catching_up", ":catching_up"),
            ("catching_up", "switching", ":switching"),
            ("switching", "completed", ":completed"),
        ] {
            let database = format!("move_resume_{phase}");
            let (status, body) = fixture
                .post(
                    "/_orion/databases",
                    serde_json::json!({ "name": database }),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body:?}");
            let operation =
                create_database_move_operation(&fixture.state, &database, "rg_resume_test", false)
                    .unwrap();
            prepare_move_operation_for_resume_test(&fixture.state, &operation, phase);

            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );

            let conn = open_system_catalog_connection(&fixture.state).unwrap();
            let resumed = read_placement_operation(&conn, &operation.operation_id)
                .unwrap()
                .unwrap();
            assert_eq!(resumed.phase, expected_phase);
            if expected_phase == "completed" {
                assert_eq!(resumed.status, "completed");
                let placement = read_database_placement_record(&fixture.state, &database)
                    .unwrap()
                    .unwrap();
                assert_eq!(placement.group.group_id, "rg_resume_test");
            } else {
                assert_eq!(resumed.status, "running");
                update_placement_operation_phase(
                    &conn,
                    &operation.operation_id,
                    "failed",
                    "failed",
                    Some("test cleanup"),
                )
                .unwrap();
            }
        }
    }

    fn prepare_move_operation_for_resume_test(
        state: &LibsqlHttpState,
        operation: &PlacementOperationRecord,
        phase: &str,
    ) {
        let conn = open_system_catalog_connection(state).unwrap();
        update_placement_operation_phase(&conn, &operation.operation_id, phase, "running", None)
            .unwrap();
        if matches!(phase, "catching_up" | "switching") {
            let source_runtime = state
                .replication_groups
                .runtime(&operation.source_group_id)
                .unwrap();
            let source = source_runtime.durability_watermark().unwrap();
            record_placement_source_fence_watermark(
                &conn,
                &operation.operation_id,
                source.applied_index,
                source.applied_commit_ts,
                current_time_millis(),
            )
            .unwrap();
            let target_runtime = state
                .replication_groups
                .runtime(&operation.target_group_id)
                .unwrap();
            let target = target_runtime.durability_watermark().unwrap();
            record_placement_target_clone_watermark(
                &conn,
                &operation.operation_id,
                target.applied_index,
                target.applied_commit_ts,
            )
            .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_move_clone_copies_sqlite_data_to_distinct_target_runtime() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "move_clone_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (status, body) = fixture
            .pipeline(
                "/move_clone_db/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table users(id integer primary key, name text)",
                        false,
                    ),
                    execute_request("insert into users(id, name) values (1, 'ada')", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(body["results"][1]["type"], "ok", "{body:?}");
        fixture
            .state
            .close_database_sessions("move_clone_db")
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_clone_target",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (target_runtime, _target_log_dir, _target_cache_dir) =
            isolated_test_runtime_with_store(
                "clone-target",
                fixture.state.replication_groups.default_runtime().unwrap(),
            )
            .await;
        fixture
            .state
            .replication_groups
            .register_for_test("rg_clone_target", target_runtime)
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases/move_clone_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_clone_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        let operation_id = body["operation_id"].as_str().unwrap().to_string();
        for expected_action in [
            ":fenced",
            ":cloning",
            ":catching_up",
            ":switching",
            ":completed",
        ] {
            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );
        }

        let placement = read_database_placement_record(&fixture.state, "move_clone_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_clone_target");

        let (status, body) = fixture
            .pipeline(
                "/move_clone_db/v2/pipeline",
                one_query_pipeline("select name from users where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "ada"
        );

        let metrics = placement_metrics(&fixture.state).unwrap();
        assert_eq!(metrics.placement_move_transfer.page_delta_attempts, 0);
        assert_eq!(metrics.placement_move_transfer.page_delta_successes, 0);
        assert_eq!(metrics.placement_move_transfer.page_delta_failures, 0);
        assert_eq!(metrics.placement_move_transfer.checkpoint_attempts, 1);
        assert_eq!(metrics.placement_move_transfer.checkpoint_successes, 1);
        assert_eq!(metrics.placement_move_transfer.checkpoint_failures, 0);
        assert_eq!(metrics.placement_move_transfer.backup_attempts, 0);
        assert!(metrics.placement_move_transfer.checkpoint_objects_seen > 0);
        assert_eq!(metrics.placement_transfer_voters.ready, 1);
        assert_eq!(metrics.placement_transfer_voters.failed, 0);
        assert!(metrics.placement_transfer_voters.checkpoint_objects_seen > 0);

        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        let operation = read_placement_operation(&conn, &operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(operation.transfer_kind.as_deref(), Some("checkpoint"));
        assert!(operation.transfer_epoch_id.is_some());
        assert!(operation.transfer_checkpoint_artifact.is_some());
        assert!(operation.transfer_source_applied_index.is_some());
        let ready_voters: i64 = conn
            .query_row(
                "select count(*) from placement_transfer_voter_status where operation_id = ? and status = 'ready'",
                [&operation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ready_voters, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn placement_move_uses_checkpoint_for_distinct_target_runtime() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "move_delta_db" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (status, body) = fixture
            .pipeline(
                "/move_delta_db/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table users(id integer primary key, name text)",
                        false,
                    ),
                    execute_request("insert into users(id, name) values (1, 'ada')", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        fixture
            .state
            .close_database_sessions("move_delta_db")
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/replication-groups",
                serde_json::json!({
                    "group_id": "rg_delta_target",
                    "placement": {
                        "mode": "single_region"
                    },
                    "members": [
                        {
                            "node_id": 1,
                            "role": "voter",
                            "priority": 0
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");
        let (target_runtime, _target_log_dir, _target_cache_dir) =
            isolated_test_runtime_with_store(
                "delta-target",
                fixture.state.replication_groups.default_runtime().unwrap(),
            )
            .await;
        fixture
            .state
            .replication_groups
            .register_for_test("rg_delta_target", target_runtime)
            .unwrap();

        let standby =
            refresh_database_placement_standby(&fixture.state, "move_delta_db", "rg_delta_target")
                .unwrap();
        assert_eq!(standby.standby.target_group_id, "rg_delta_target");

        let (status, body) = fixture
            .pipeline(
                "/move_delta_db/v2/pipeline",
                one_query_pipeline("update users set name = 'grace' where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        fixture
            .state
            .close_database_sessions("move_delta_db")
            .unwrap();

        let (status, body) = fixture
            .post(
                "/_orion/databases/move_delta_db/placement/move",
                serde_json::json!({ "target_group_id": "rg_delta_target" }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body:?}");
        for expected_action in [
            ":fenced",
            ":cloning",
            ":catching_up",
            ":switching",
            ":completed",
        ] {
            let summary = reconcile_placement(&fixture.state).unwrap();
            assert!(
                summary
                    .actions
                    .iter()
                    .any(|action| action.contains(expected_action)),
                "{summary:?}"
            );
        }

        let placement = read_database_placement_record(&fixture.state, "move_delta_db")
            .unwrap()
            .unwrap();
        assert_eq!(placement.group.group_id, "rg_delta_target");

        let (status, body) = fixture
            .pipeline(
                "/move_delta_db/v2/pipeline",
                one_query_pipeline("select name from users where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "grace"
        );

        let metrics = placement_metrics(&fixture.state).unwrap();
        assert_eq!(metrics.placement_move_transfer.page_delta_attempts, 0);
        assert_eq!(metrics.placement_move_transfer.page_delta_successes, 0);
        assert_eq!(metrics.placement_move_transfer.page_delta_failures, 0);
        assert_eq!(metrics.placement_move_transfer.checkpoint_attempts, 1);
        assert_eq!(metrics.placement_move_transfer.checkpoint_successes, 1);
        assert_eq!(metrics.placement_move_transfer.checkpoint_failures, 0);
        assert_eq!(metrics.placement_move_transfer.backup_attempts, 0);
        assert!(metrics.placement_move_transfer.checkpoint_objects_seen > 0);
    }

    async fn isolated_test_runtime(name: &str) -> (OrionSqliteRuntime, TempDir, TempDir) {
        isolated_test_runtime_with_store(name, OrionSqliteRuntimeStoreSource::FreshInMemory).await
    }

    enum OrionSqliteRuntimeStoreSource {
        FreshInMemory,
        Shared(Arc<dyn ObjectStore>),
    }

    impl From<OrionSqliteRuntime> for OrionSqliteRuntimeStoreSource {
        fn from(runtime: OrionSqliteRuntime) -> Self {
            Self::Shared(runtime.state_store().object_store())
        }
    }

    async fn isolated_test_runtime_with_store(
        name: &str,
        store_source: impl Into<OrionSqliteRuntimeStoreSource>,
    ) -> (OrionSqliteRuntime, TempDir, TempDir) {
        static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
        let runtime_id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        let cluster_name = format!("libsql-http-isolated-{name}-{runtime_id}");
        let log_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state = match store_source.into() {
            OrionSqliteRuntimeStoreSource::FreshInMemory => {
                SlateDbStateStore::open_in_memory(&cluster_name)
                    .await
                    .unwrap()
            }
            OrionSqliteRuntimeStoreSource::Shared(object_store) => {
                SlateDbStateStore::open(&cluster_name, object_store)
                    .await
                    .unwrap()
            }
        };
        let state_machine = OrionRaftStateMachine::new(state.clone());
        let raft_config = Arc::new(
            Config {
                cluster_name,
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            1,
            raft_config,
            TonicRaftNetwork::new(),
            log_store,
            state_machine,
        )
        .await
        .unwrap();
        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(3)))
            .current_leader(1, "isolated libsql runtime leader")
            .await
            .unwrap();
        let runtime = OrionSqliteRuntime::new(
            raft,
            state,
            OrionSqliteRuntimeConfig::new(cache_dir.path().to_path_buf()),
        );
        (runtime, log_dir, cache_dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn filesystem_backed_catalog_survives_database_create() {
        static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
        let runtime_id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        let cluster_name = format!("libsql-http-filesystem-catalog-{runtime_id}");
        let object_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();
        let log_dir = TempDir::new().unwrap();
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(object_dir.path()).unwrap());
        let state = SlateDbStateStore::open("node-1/state", object_store)
            .await
            .unwrap();
        let log_store = OrionRaftLogStore::open(log_dir.path()).unwrap();
        let state_machine = OrionRaftStateMachine::new(state.clone());
        let raft_config = Arc::new(
            Config {
                cluster_name,
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let raft = Raft::new(
            1,
            raft_config,
            TonicRaftNetwork::new(),
            log_store,
            state_machine,
        )
        .await
        .unwrap();
        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:0".to_string(),
            },
        );
        raft.initialize(members).await.unwrap();
        raft.wait(Some(Duration::from_secs(3)))
            .current_leader(1, "filesystem catalog test leader")
            .await
            .unwrap();

        let metrics_registry = ClusterRaftMetricsRegistry::default();
        metrics_registry.set_local_capabilities(node_software_capabilities());
        metrics_registry.record_observed(&raft);
        let config = LibsqlHttpConfig {
            sqlite_cache_root: cache_dir.path().to_path_buf(),
            session_idle_timeout: Duration::from_secs(60),
            blob_max_chunk_bytes: 512 * 1024,
            idempotency: LibsqlHttpIdempotencyConfig::default(),
            auth: LibsqlHttpAuthConfig::default(),
            node_id: 1,
            peer_http_endpoints: BTreeMap::new(),
            placement_nodes: BTreeMap::from([(
                1,
                PlacementNodeConfig {
                    node_id: 1,
                    cloud: "local".to_string(),
                    region: "local".to_string(),
                    zone: "local".to_string(),
                    raft_addr: "127.0.0.1:0".to_string(),
                    libsql_http_addr: None,
                },
            )]),
            metrics_registry,
            compaction_policy: SqlitePageCompactionPolicy::default(),
            replication_groups: None,
        };
        let runtime = OrionSqliteRuntime::new(
            raft,
            state,
            OrionSqliteRuntimeConfig::new(config.sqlite_cache_root.clone()),
        );
        ensure_database_catalog_schema_for_runtime(&runtime).unwrap();
        let state = LibsqlHttpState::new(runtime, &config);

        create_database_lifecycle(
            &state,
            CreateDatabaseRequest {
                name: "filesystem_catalog_db".to_string(),
                placement: None,
            },
        )
        .unwrap();

        let conn = open_system_catalog_connection(&state).unwrap();
        let quick_check: String = conn
            .query_row("pragma quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(quick_check, "ok");
        let record = read_database_catalog_record(&state, "filesystem_catalog_db")
            .unwrap()
            .unwrap();
        assert_eq!(record.state, "ready");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_manages_database_lifecycle_and_routes_only_ready_databases() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline_with_headers_raw(
                "/life_db/v2/pipeline",
                one_query_pipeline("select 1"),
                None,
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert!(
            body["results"][0]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("has not been created"),
            "{body:?}"
        );

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({
                    "name": "life_db"
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["name"], "life_db");
        assert_eq!(body["state"], "ready");
        assert_eq!(body["replication_group_id"], DEFAULT_REPLICATION_GROUP_ID);
        assert!(body["database_id"].as_str().unwrap().starts_with("db_"));
        assert!(
            body["object_prefix"]
                .as_str()
                .unwrap()
                .starts_with("replication-groups/rg_default/databases/db_")
        );
        assert_eq!(body["generation"], 1);

        let (status, body) = fixture.get("/_orion/databases/life_db", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], "ready");

        let (status, body) = fixture.get("/_orion/databases", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["databases"].as_array().unwrap().len(), 1);

        let (status, body) = fixture
            .pipeline(
                "/life_db/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table lifecycle_items (id integer primary key, value text)",
                        false,
                    ),
                    execute_request("insert into lifecycle_items values (1, 'ready')", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");

        let (status, body) = fixture.delete("/_orion/databases/life_db", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], "deleted");
        assert!(body["deleted_at_ms"].is_number());

        let (status, body) = fixture
            .pipeline_with_headers_raw(
                "/life_db/v2/pipeline",
                one_query_pipeline("select * from lifecycle_items"),
                None,
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert!(
            body["results"][0]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("current state is deleted")
        );

        let (status, body) = fixture
            .get("/_orion/databases?include_deleted=true", None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["databases"].as_array().unwrap().len(), 1);
        assert_eq!(body["databases"][0]["state"], "deleted");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_resolution_uses_database_replication_group_mapping() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let missing = match fixture.state.runtime_for_database("missing_runtime_db") {
            Ok(_) => panic!("missing database unexpectedly resolved to a runtime"),
            Err(error) => error,
        };
        assert!(missing.to_string().contains("has not been created"));

        create_database_lifecycle(
            &fixture.state,
            CreateDatabaseRequest {
                name: "runtime_mapped".to_string(),
                placement: None,
            },
        )
        .unwrap();
        let runtime = fixture
            .state
            .runtime_for_database("runtime_mapped")
            .unwrap();
        assert_eq!(
            runtime.metrics().current_leader,
            fixture
                .state
                .replication_groups
                .default_runtime()
                .unwrap()
                .metrics()
                .current_leader
        );

        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        conn.execute(
            "insert into replication_groups (
                group_id, state, placement_mode, object_prefix,
                failover_automatic, failover_promote_after_ms, created_at_ms, updated_at_ms
            ) values ('rg_missing_runtime', 'active', 'manual', 'replication-groups/rg_missing_runtime', 1, 1000, 1, 1)",
            [],
        )
        .unwrap();
        let record = read_database_catalog_record(&fixture.state, "runtime_mapped")
            .unwrap()
            .unwrap();
        conn.execute(
            "update database_replication_groups set group_id = 'rg_missing_runtime' where database_id = ?",
            [record.database_id],
        )
        .unwrap();
        fixture.state.evict_database("runtime_mapped").unwrap();

        let missing_runtime = match fixture.state.runtime_for_database("runtime_mapped") {
            Ok(_) => panic!("missing replication group unexpectedly resolved to a runtime"),
            Err(error) => error,
        };
        assert!(
            missing_runtime.to_string().contains(
                "resolving runtime for database runtime_mapped replication group rg_missing_runtime"
            ),
            "{missing_runtime:?}"
        );
        assert!(
            missing_runtime.chain().any(|error| error
                .to_string()
                .contains("replication group rg_missing_runtime is not loaded")),
            "{missing_runtime:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_replays_idempotent_database_lifecycle_results_and_rejects_conflicts() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let create_body = serde_json::json!({
            "name": "idem_lifecycle"
        });
        let (status, first_create) = fixture
            .post_with_headers(
                "/_orion/databases",
                create_body.clone(),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-create-key")],
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(first_create["state"], "ready");
        assert_eq!(first_create["generation"], 1);

        let (status, replayed_create) = fixture
            .post_with_headers(
                "/_orion/databases",
                create_body,
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-create-key")],
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(replayed_create, first_create);

        let (status, conflict) = fixture
            .post_with_headers(
                "/_orion/databases",
                serde_json::json!({
                    "name": "idem_lifecycle",
                    "placement": {
                        "mode": "regional_primary",
                        "write_home": {
                            "cloud": "aws",
                            "region": "us-east-1"
                        }
                    }
                }),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-create-key")],
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            conflict["error"]
                .as_str()
                .unwrap()
                .contains("idempotency key conflict")
        );

        let (status, conflict) = fixture
            .post_with_headers(
                "/_orion/databases",
                serde_json::json!({
                    "name": "idem_lifecycle_other"
                }),
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-create-key")],
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            conflict["error"]
                .as_str()
                .unwrap()
                .contains("idempotency key conflict")
        );

        let (status, conflict) = fixture
            .delete_with_headers(
                "/_orion/databases/idem_lifecycle",
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-create-key")],
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            conflict["error"]
                .as_str()
                .unwrap()
                .contains("idempotency key conflict")
        );

        let (status, first_delete) = fixture
            .delete_with_headers(
                "/_orion/databases/idem_lifecycle",
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-delete-key")],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first_delete["state"], "deleted");
        assert!(first_delete["deleted_at_ms"].is_number());

        let (status, replayed_delete) = fixture
            .delete_with_headers(
                "/_orion/databases/idem_lifecycle",
                None,
                &[(IDEMPOTENCY_KEY_HEADER, "lifecycle-delete-key")],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed_delete, first_delete);
    }

    #[test]
    fn database_catalog_open_does_not_activate_existing_v1_catalog() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            create table database_catalog (
                name text primary key,
                state text not null check (state in ('creating', 'ready', 'deleting', 'deleted', 'failed')),
                object_prefix text not null,
                generation integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                deleted_at_ms integer,
                error text
            );
            create index database_catalog_state_idx on database_catalog(state);
            insert into database_catalog (
                name, state, object_prefix, generation, created_at_ms, updated_at_ms, deleted_at_ms, error
            )
            values ('legacy_db', 'deleted', 'databases/legacy_db', 2, 10, 20, 30, null);
            "#,
        )
        .unwrap();

        ensure_database_catalog_schema(&conn).unwrap();

        assert_eq!(read_catalog_schema_version(&conn).unwrap(), None);
        assert!(!table_exists(&conn, LIFECYCLE_IDEMPOTENCY_TABLE).unwrap());
        assert!(!table_column_exists(&conn, "database_catalog", "purged_at_ms").unwrap());
        assert!(!table_column_exists(&conn, "database_catalog", "purge_error").unwrap());

        let record = read_database_catalog_record_from_conn(&conn, "legacy_db")
            .unwrap()
            .unwrap();
        assert_eq!(record.name, "legacy_db");
        assert_eq!(record.state, "deleted");
        assert_eq!(record.generation, 2);
        assert_eq!(record.deleted_at_ms, Some(30));
        assert_eq!(record.purged_at_ms, None);
        assert_eq!(record.purge_error, None);

        activate_database_catalog_schema_from_conn(
            &conn,
            DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION,
        )
        .unwrap();
        assert_eq!(
            read_catalog_schema_version(&conn).unwrap(),
            Some(DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION)
        );
        assert!(table_exists(&conn, LIFECYCLE_IDEMPOTENCY_TABLE).unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "purged_at_ms").unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "purge_error").unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "database_id").unwrap());
        assert!(table_exists(&conn, "replication_groups").unwrap());
        assert!(table_exists(&conn, "database_replication_groups").unwrap());
        let upgraded = read_database_catalog_record_from_conn(&conn, "legacy_db")
            .unwrap()
            .unwrap();
        assert_eq!(upgraded.replication_group_id, DEFAULT_REPLICATION_GROUP_ID);
    }

    #[test]
    fn database_catalog_bootstraps_empty_catalog_at_current_version() {
        let conn = Connection::open_in_memory().unwrap();

        ensure_database_catalog_schema(&conn).unwrap();

        assert_eq!(
            read_catalog_schema_version(&conn).unwrap(),
            Some(DATABASE_CATALOG_BOOTSTRAP_SCHEMA_VERSION)
        );
        assert!(table_exists(&conn, "database_catalog").unwrap());
        assert!(table_exists(&conn, LIFECYCLE_IDEMPOTENCY_TABLE).unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "purged_at_ms").unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "purge_error").unwrap());
        assert!(table_column_exists(&conn, "database_catalog", "database_id").unwrap());
        assert!(table_exists(&conn, "replication_groups").unwrap());
        assert!(table_exists(&conn, "replication_group_members").unwrap());
        assert!(table_exists(&conn, "database_replication_groups").unwrap());
    }

    #[test]
    fn lifecycle_idempotency_requires_activated_catalog_schema() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            create table database_catalog (
                name text primary key,
                state text not null check (state in ('creating', 'ready', 'deleting', 'deleted', 'failed')),
                object_prefix text not null,
                generation integer not null,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                deleted_at_ms integer,
                error text
            );
            "#,
        )
        .unwrap();
        ensure_database_catalog_schema(&conn).unwrap();

        let error = require_database_catalog_schema(&conn, 2).unwrap_err();
        assert!(error.to_string().contains("activate schema version 2"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_requires_admin_scope_for_catalog_schema_activation() {
        let fixture = RouterFixture::new(system_read_and_admin_auth_config()).await;

        let (status, body) = fixture
            .post(
                "/_orion/catalog/activate-schema",
                serde_json::json!({ "target_version": DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION }),
                Some("operator-read"),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body["error"],
            "bearer token is not authorized for system admin operations"
        );

        let (status, body) = fixture
            .post(
                "/_orion/catalog/activate-schema",
                serde_json::json!({ "target_version": DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION }),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["schema_version"],
            DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn background_standby_refresh_uses_configured_admin_token_for_peer_auth() {
        let fixture = RouterFixture::new(system_read_and_admin_auth_config()).await;

        let headers = fixture.state.internal_system_admin_headers();
        assert!(fixture.state.authorize_system_admin(&headers).is_ok());
        assert_eq!(bearer_token(&headers), Some("operator-admin"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn internal_checkpoint_endpoints_require_admin_scope() {
        let fixture = RouterFixture::new(system_read_and_admin_auth_config()).await;

        let checkpoint_path = "/_orion/internal/databases/auth_checkpoint/placement/checkpoint?source_group_id=rg_default";
        for token in [None, Some("operator-read")] {
            let (status, body) = fixture.get(checkpoint_path, token).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{body:?}");
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("bearer token")),
                "{body:?}"
            );
        }

        let object_path = "/_orion/internal/databases/auth_checkpoint/placement/checkpoint/object?source_group_id=rg_default&object_path=node/state__sqlite/databases/auth_checkpoint/state/manifest/1";
        for token in [None, Some("operator-read")] {
            let (status, _headers, body) = fixture.get_bytes(object_path, token).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("bearer token")),
                "{body:?}"
            );
        }

        let (status, body) = fixture.get(checkpoint_path, Some("operator-admin")).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{body:?}");
        assert_ne!(
            body["error"],
            "bearer token is not authorized for system admin operations"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn standby_action_forwarding_sends_request_to_target_leader_with_auth() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let forwarding_group_id = "rg_forwarding_target";
        let runtime = fixture.state.replication_groups.default_runtime().unwrap();
        let forwarding_cache_dir = TempDir::new().unwrap();
        let forwarding_placement_nodes = BTreeMap::from([
            (
                1,
                PlacementNodeConfig {
                    node_id: 1,
                    cloud: "local".to_string(),
                    region: "leader".to_string(),
                    zone: "leader-a".to_string(),
                    raft_addr: "127.0.0.1:0".to_string(),
                    libsql_http_addr: None,
                },
            ),
            (
                2,
                PlacementNodeConfig {
                    node_id: 2,
                    cloud: "local".to_string(),
                    region: "follower".to_string(),
                    zone: "follower-a".to_string(),
                    raft_addr: "127.0.0.1:0".to_string(),
                    libsql_http_addr: None,
                },
            ),
        ]);
        let catalog_state = LibsqlHttpState::new(
            runtime.clone(),
            &LibsqlHttpConfig {
                sqlite_cache_root: forwarding_cache_dir.path().to_path_buf(),
                session_idle_timeout: Duration::from_secs(60),
                blob_max_chunk_bytes: 512 * 1024,
                idempotency: LibsqlHttpIdempotencyConfig::default(),
                auth: LibsqlHttpAuthConfig::default(),
                node_id: 1,
                peer_http_endpoints: BTreeMap::new(),
                placement_nodes: forwarding_placement_nodes.clone(),
                metrics_registry: ClusterRaftMetricsRegistry::default(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: None,
            },
        );
        let (status, body) = RouterFixture {
            router: libsql_router(catalog_state.clone()),
            state: catalog_state,
            _cache_dir: TempDir::new().unwrap(),
            _log_dir: TempDir::new().unwrap(),
        }
        .post(
            "/_orion/replication-groups",
            serde_json::json!({
                "group_id": forwarding_group_id,
                "placement": { "mode": "manual" },
                "members": [
                    { "node_id": 1, "role": "voter", "priority": 0 },
                    { "node_id": 2, "role": "voter", "priority": 1 }
                ]
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body:?}");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/_orion/databases/forwarded_db/placement/standby",
                post(
                    |headers: HeaderMap, Json(body): Json<PlacementStandbyRequest>| async move {
                        assert_eq!(
                            headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok()),
                            Some("Bearer operator-admin")
                        );
                        assert_eq!(body.target_group_id, "rg_forwarding_target");
                        Json(serde_json::json!({
                            "forwarded": true,
                            "standby": {
                                "database_name": "forwarded_db",
                                "target_group_id": body.target_group_id
                            }
                        }))
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let forwarding_registry = ReplicationGroupRegistry::empty();
        forwarding_registry
            .register_for_test(DEFAULT_REPLICATION_GROUP_ID, runtime.clone())
            .unwrap();
        forwarding_registry
            .register_for_test(forwarding_group_id, runtime.clone())
            .unwrap();
        let forwarding_state = LibsqlHttpState::new(
            runtime,
            &LibsqlHttpConfig {
                sqlite_cache_root: forwarding_cache_dir.path().to_path_buf(),
                session_idle_timeout: Duration::from_secs(60),
                blob_max_chunk_bytes: 512 * 1024,
                idempotency: LibsqlHttpIdempotencyConfig::default(),
                auth: LibsqlHttpAuthConfig::default(),
                node_id: 2,
                peer_http_endpoints: BTreeMap::from([(1, format!("http://{addr}"))]),
                placement_nodes: forwarding_placement_nodes,
                metrics_registry: ClusterRaftMetricsRegistry::default(),
                compaction_policy: SqlitePageCompactionPolicy::default(),
                replication_groups: Some(forwarding_registry),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer operator-admin"),
        );

        let body = forward_standby_action_to_target_leader(
            &forwarding_state,
            &headers,
            "forwarded_db",
            forwarding_group_id,
            "standby",
            &PlacementStandbyRequest {
                target_group_id: forwarding_group_id.to_string(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(body["forwarded"], true);
        assert_eq!(body["standby"]["target_group_id"], forwarding_group_id);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn catalog_rollout_status_requires_fresh_capable_voters() {
        let now = current_time_millis();
        let status = catalog_rollout_status_from_entries(
            vec![
                catalog_rollout_test_entry(1, now, vec![1, 2], Some(node_software_capabilities())),
                catalog_rollout_test_entry(2, now, vec![1, 2], Some(node_software_capabilities())),
            ],
            3,
            now,
        );
        assert!(status.ready);
        assert!(status.blockers.is_empty());

        let stale = catalog_rollout_status_from_entries(
            vec![
                catalog_rollout_test_entry(1, now, vec![1, 2], Some(node_software_capabilities())),
                catalog_rollout_test_entry(
                    2,
                    now - RAFT_METRICS_STALE_AFTER_MS - 1,
                    vec![1, 2],
                    Some(node_software_capabilities()),
                ),
            ],
            3,
            now,
        );
        assert!(!stale.ready);
        assert!(stale.blockers[0].contains("stale"));

        let incapable = catalog_rollout_status_from_entries(
            vec![
                catalog_rollout_test_entry(1, now, vec![1, 2], Some(node_software_capabilities())),
                catalog_rollout_test_entry(
                    2,
                    now,
                    vec![1, 2],
                    Some(NodeSoftwareCapabilities {
                        catalog_min_read_schema_version: 1,
                        catalog_max_read_schema_version: 2,
                        catalog_max_write_schema_version: 2,
                    }),
                ),
            ],
            3,
            now,
        );
        assert!(!incapable.ready);
        assert!(incapable.blockers[0].contains("cannot read"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blocks_catalog_activation_until_rollout_ready() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        fixture
            .state
            .metrics_registry
            .record(catalog_rollout_test_metrics(2, vec![1, 2], None));

        let (status, body) = fixture
            .post(
                "/_orion/catalog/activate-schema",
                serde_json::json!({ "target_version": DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["rollout"]["blockers"][0]
                .as_str()
                .unwrap()
                .contains("has not advertised")
        );

        fixture
            .state
            .metrics_registry
            .record(catalog_rollout_test_metrics(
                2,
                vec![1, 2],
                Some(node_software_capabilities()),
            ));
        let (status, body) = fixture
            .post(
                "/_orion/catalog/activate-schema",
                serde_json::json!({ "target_version": DATABASE_CATALOG_MAX_WRITE_SCHEMA_VERSION }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rollout"]["ready"], true);
    }

    #[test]
    fn database_catalog_schema_rejects_newer_catalog_version() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            create table catalog_meta (
                key text primary key,
                value text not null,
                updated_at_ms integer not null
            );
            insert into catalog_meta (key, value, updated_at_ms)
            values ('schema_version', '999', 1);
            "#,
        )
        .unwrap();

        let error = ensure_database_catalog_schema(&conn).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("newer than this binary can read")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn database_lifecycle_reconciler_recovers_transient_states() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        ensure_configured_replication_group(
            &conn,
            &fixture.state,
            DEFAULT_REPLICATION_GROUP_ID,
            &CreateDatabasePlacementRequest::default(),
        )
        .unwrap();
        let reconcile_create_id = new_database_id("reconcile_create");
        let reconcile_create_prefix =
            database_object_prefix(DEFAULT_REPLICATION_GROUP_ID, &reconcile_create_id);
        upsert_database_creating(
            &conn,
            "reconcile_create",
            &reconcile_create_id,
            &reconcile_create_prefix,
            DEFAULT_REPLICATION_GROUP_ID,
        )
        .unwrap();
        create_database_lifecycle(
            &fixture.state,
            CreateDatabaseRequest {
                name: "reconcile_delete".to_string(),
                placement: None,
            },
        )
        .unwrap();
        mark_database_state(&conn, "reconcile_delete", "deleting", None, None).unwrap();

        let reconciled = fixture
            .state
            .reconcile_database_lifecycle_once()
            .await
            .unwrap();
        assert_eq!(reconciled, 2);

        let creating = read_database_catalog_record(&fixture.state, "reconcile_create")
            .unwrap()
            .unwrap();
        assert_eq!(creating.state, "ready");

        let deleting = read_database_catalog_record(&fixture.state, "reconcile_delete")
            .unwrap()
            .unwrap();
        assert_eq!(deleting.state, "deleted");
        assert!(deleting.deleted_at_ms.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn database_lifecycle_reconciler_purges_deleted_database_after_retention() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;
        create_database_lifecycle(
            &fixture.state,
            CreateDatabaseRequest {
                name: "purge_lifecycle".to_string(),
                placement: None,
            },
        )
        .unwrap();
        let (status, body) = fixture
            .pipeline(
                "/purge_lifecycle/v2/pipeline",
                sql_pipeline(vec![
                    execute_request("create table purge_items (id integer primary key)", false),
                    execute_request("insert into purge_items values (1)", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");

        drop_database_lifecycle(&fixture.state, "purge_lifecycle").unwrap();
        let conn = open_system_catalog_connection(&fixture.state).unwrap();
        conn.execute(
            "update database_catalog set deleted_at_ms = 0, purged_at_ms = null, purge_error = null where name = 'purge_lifecycle'",
            [],
        )
        .unwrap();

        let reconciled = fixture
            .state
            .reconcile_database_lifecycle_once()
            .await
            .unwrap();
        assert_eq!(reconciled, 1);

        let record = read_database_catalog_record(&fixture.state, "purge_lifecycle")
            .unwrap()
            .unwrap();
        assert_eq!(record.state, "deleted");
        assert_eq!(record.purge_error, None);
        assert!(record.purged_at_ms.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_requires_admin_scope_for_database_lifecycle_mutations() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig {
            tokens: vec![
                LibsqlHttpAuthTokenConfig {
                    token: "operator-read".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Read],
                },
                LibsqlHttpAuthTokenConfig {
                    token: "operator-admin".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Admin],
                },
            ],
        })
        .await;

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "auth_lifecycle" }),
                Some("operator-read"),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body["error"],
            "bearer token is not authorized for system admin operations"
        );

        let (status, body) = fixture
            .post(
                "/_orion/databases",
                serde_json::json!({ "name": "auth_lifecycle" }),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["state"], "ready");

        let (status, body) = fixture
            .delete("/_orion/databases/auth_lifecycle", Some("operator-read"))
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body["error"],
            "bearer token is not authorized for system admin operations"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_requires_system_scope_for_orion_namespace() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig {
            tokens: vec![
                LibsqlHttpAuthTokenConfig {
                    token: "tenant".to_string(),
                    database_prefixes: vec!["_orion".to_string()],
                    system_permissions: Vec::new(),
                },
                LibsqlHttpAuthTokenConfig {
                    token: "operator".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Read],
                },
            ],
        })
        .await;

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline("select * from raft_metrics"),
                Some("tenant"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(
            body["results"][0]["error"]["message"],
            "bearer token is not authorized for system namespace _orion"
        );

        let (status, body) = fixture
            .pipeline(
                "/_orion/v2/pipeline",
                one_query_pipeline("select * from raft_metrics"),
                Some("operator"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");

        let (status, body) = fixture.get("/_orion/metrics/raft", Some("tenant")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body["error"],
            "bearer token is not authorized for system namespace _orion"
        );

        let (status, _body) = fixture.get("/_orion/metrics/raft", Some("operator")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_requires_admin_scope_for_compaction_control() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig {
            tokens: vec![
                LibsqlHttpAuthTokenConfig {
                    token: "operator-read".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Read],
                },
                LibsqlHttpAuthTokenConfig {
                    token: "operator-admin".to_string(),
                    database_prefixes: Vec::new(),
                    system_permissions: vec![LibsqlHttpSystemPermission::Admin],
                },
            ],
        })
        .await;

        let (status, body) = fixture
            .post(
                "/_orion/compaction/pause",
                serde_json::json!({}),
                Some("operator-read"),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body["error"],
            "bearer token is not authorized for system admin operations"
        );

        let (status, body) = fixture
            .post(
                "/_orion/compaction/pause",
                serde_json::json!({}),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], true);
        assert_eq!(body["force_requested"], false);

        let (status, body) = fixture
            .post(
                "/_orion/compaction/resume",
                serde_json::json!({}),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["paused"], false);

        let (status, body) = fixture
            .post(
                "/_orion/compaction/force",
                serde_json::json!({}),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["force_requested"], true);

        let (status, body) = fixture
            .get("/_orion/compaction", Some("operator-read"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["control"]["force_requested"], true);
        assert_eq!(
            body["retention_floor"]["min_retained_version"],
            serde_json::Value::Null
        );
        assert!(body["leases"].is_array());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_exposes_admin_retention_floor_controls() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig {
            tokens: vec![LibsqlHttpAuthTokenConfig {
                token: "operator-admin".to_string(),
                database_prefixes: Vec::new(),
                system_permissions: vec![LibsqlHttpSystemPermission::Admin],
            }],
        })
        .await;

        let (status, body) = fixture
            .post(
                "/_orion/compaction/retention-floor",
                serde_json::json!({
                    "min_retained_version": 42,
                    "reason": "backup export"
                }),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["min_retained_version"], 42);
        assert_eq!(body["reason"], "backup export");
        assert!(body["updated_at_ms"].is_number());

        let (status, body) = fixture
            .get("/_orion/compaction", Some("operator-admin"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["retention_floor"]["min_retained_version"], 42);
        assert_eq!(body["retention_floor"]["reason"], "backup export");

        let (status, body) = fixture
            .post(
                "/_orion/compaction/retention-floor/clear",
                serde_json::json!({}),
                Some("operator-admin"),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["min_retained_version"], serde_json::Value::Null);
        assert_eq!(body["reason"], serde_json::Value::Null);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_exercises_sqlite_core_compatibility_corpus() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/compat_core/v2/pipeline",
                sql_pipeline(vec![
                    execute_request("pragma foreign_keys = on", false),
                    execute_request(
                        "create table parent (id integer primary key, name text not null unique, child_count integer not null default 0)",
                        false,
                    ),
                    execute_request(
                        "create table child (id integer primary key, parent_id integer not null references parent(id), label text not null, amount integer not null check (amount >= 0))",
                        false,
                    ),
                    execute_request("create index child_parent_amount_idx on child(parent_id, amount)", false),
                    execute_request(
                        "create view child_summary as select parent_id, count(*) as children, sum(amount) as total from child group by parent_id",
                        false,
                    ),
                    execute_request(
                        "create trigger child_ai after insert on child begin update parent set child_count = child_count + 1 where id = new.parent_id; end",
                        false,
                    ),
                    execute_request("insert into parent (id, name) values (1, 'acme')", false),
                    execute_request("begin", false),
                    execute_request("insert into child (id, parent_id, label, amount) values (10, 1, 'api', 5)", false),
                    execute_request("savepoint adjust_amount", false),
                    execute_request("update child set amount = 99 where id = 10", false),
                    execute_request("rollback to adjust_amount", false),
                    execute_request("release adjust_amount", false),
                    execute_request("insert into child (id, parent_id, label, amount) values (11, 1, 'worker', 7)", false),
                    execute_request("commit", false),
                    execute_request(
                        "with totals as (select parent_id, sum(amount) as total from child group by parent_id) select p.name, p.child_count, totals.total from parent p join totals on totals.parent_id = p.id",
                        true,
                    ),
                    execute_request("select children, total from child_summary where parent_id = 1", true),
                ]),
                None,
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        assert_eq!(
            body["results"][15]["response"]["result"]["rows"][0],
            serde_json::json!([
                { "type": "text", "value": "acme" },
                { "type": "integer", "value": "2" },
                { "type": "integer", "value": "12" }
            ])
        );
        assert_eq!(
            body["results"][16]["response"]["result"]["rows"][0],
            serde_json::json!([
                { "type": "integer", "value": "2" },
                { "type": "integer", "value": "12" }
            ])
        );

        let (status, body) = fixture
            .pipeline(
                "/compat_core/v2/pipeline",
                one_query_pipeline(
                    "insert into child (id, parent_id, label, amount) values (12, 404, 'orphan', 1)",
                ),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "SQLITE_CONSTRAINT");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_exercises_sqlite_pragma_and_metadata_policy() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/compat_pragmas/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table pragma_target (id integer primary key, value text not null)",
                        false,
                    ),
                    execute_request(
                        "create index pragma_target_value_idx on pragma_target(value)",
                        false,
                    ),
                    execute_request("pragma foreign_keys = on", false),
                    execute_request("pragma foreign_keys", true),
                    execute_request("pragma busy_timeout = 250", false),
                    execute_request("pragma busy_timeout", true),
                    execute_request("pragma query_only = on", false),
                    execute_request("select count(*) from pragma_target", true),
                    execute_request("pragma query_only = off", false),
                    execute_request("pragma user_version = 123", false),
                    execute_request("pragma user_version", true),
                    execute_request("pragma application_id = 123456", false),
                    execute_request("pragma application_id", true),
                    execute_request("pragma table_info('pragma_target')", true),
                    execute_request("pragma index_list('pragma_target')", true),
                    execute_request("pragma database_list", true),
                    execute_request("pragma integrity_check", true),
                    execute_request("pragma quick_check", true),
                    execute_request("pragma wal_checkpoint(passive)", true),
                ]),
                None,
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        assert_eq!(
            body["results"][3]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "integer", "value": "1" })
        );
        assert_eq!(
            body["results"][5]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "integer", "value": "250" })
        );
        assert_eq!(
            body["results"][10]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "integer", "value": "123" })
        );
        assert_eq!(
            body["results"][12]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "integer", "value": "123456" })
        );
        assert!(
            body["results"][13]["response"]["result"]["rows"]
                .as_array()
                .is_some_and(|rows| rows.len() >= 2)
        );
        assert!(
            body["results"][14]["response"]["result"]["rows"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        );
        assert_eq!(
            body["results"][16]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "ok" })
        );
        assert_eq!(
            body["results"][17]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "ok" })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_reads_writes_reopens_and_closes_handles() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_api/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request(
                        "insert into files (id, data) values (1, zeroblob(11)), (2, zeroblob(5))",
                        false,
                    ),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        assert_eq!(body["result"]["type"], "open");
        assert_eq!(body["result"]["size"], 11);
        assert_eq!(body["result"]["read_only"], false);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"hello world")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        assert_eq!(body["result"]["type"], "write");
        assert_eq!(body["result"]["bytes_written"], 11);
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 6,
                    "length": 5
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        assert_eq!(body["result"]["type"], "read");
        assert_eq!(body["result"]["bytes_read"], 5);
        assert_eq!(
            BASE64
                .decode(body["result"]["base64"].as_str().unwrap().as_bytes())
                .unwrap(),
            b"world"
        );
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/reopen",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "rowid": 2
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        assert_eq!(body["result"]["type"], "reopen");
        assert_eq!(body["result"]["rowid"], 2);
        assert_eq!(body["result"]["size"], 5);
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/close",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        assert_eq!(body["result"]["type"], "close");
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_api/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "HRANA_PROTO_ERROR");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("does not exist")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_binary_http_reads_and_writes_raw_chunks() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_bytes/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(8))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_bytes/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let write_path =
            format!("/blob_bytes/v2/blob/write-bytes?baton={baton}&blob_id={blob_id}&offset=2");
        let (status, headers, body) = fixture
            .post_bytes(&write_path, b"wxyz".to_vec(), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(headers[SESSION_TOKEN_HEADER], baton);
        assert_eq!(headers[BLOB_ID_HEADER], blob_id);
        assert_eq!(headers[BLOB_OFFSET_HEADER], "2");
        assert_eq!(headers[BLOB_BYTES_WRITTEN_HEADER], "4");
        assert_eq!(headers[BLOB_SIZE_HEADER], "8");

        let read_path = format!(
            "/blob_bytes/v2/blob/read-bytes?baton={baton}&blob_id={blob_id}&offset=1&length=5"
        );
        let (status, headers, body) = fixture.get_bytes(&read_path, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers[axum::http::header::CONTENT_TYPE],
            "application/octet-stream"
        );
        assert_eq!(headers[SESSION_TOKEN_HEADER], baton);
        assert_eq!(headers[BLOB_ID_HEADER], blob_id);
        assert_eq!(headers[BLOB_OFFSET_HEADER], "1");
        assert_eq!(headers[BLOB_BYTES_READ_HEADER], "5");
        assert_eq!(headers[BLOB_SIZE_HEADER], "8");
        assert_eq!(body, b"\0wxyz");

        let (status, body) = fixture
            .pipeline(
                "/blob_bytes/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [{
                        "type": "execute",
                        "stmt": {
                            "sql": "select hex(data) from files where id = 1",
                            "want_rows": true
                        }
                    }]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({
                "type": "text",
                "value": "00007778797A0000"
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_binary_http_streams_large_reads_and_writes_in_chunks() {
        let fixture =
            RouterFixture::new_with_blob_max_chunk_bytes(LibsqlHttpAuthConfig::default(), 4).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_stream/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(12))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_stream/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();
        let payload = b"abcdefghij".to_vec();

        let write_path = format!(
            "/blob_stream/v2/blob/write-stream?baton={baton}&blob_id={blob_id}&offset=1&length={}",
            payload.len()
        );
        let (status, headers, body) = fixture.post_bytes(&write_path, payload.clone(), None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(headers[SESSION_TOKEN_HEADER], baton);
        assert_eq!(headers[BLOB_BYTES_WRITTEN_HEADER], "10");
        assert_eq!(headers[BLOB_SIZE_HEADER], "12");

        let read_path = format!(
            "/blob_stream/v2/blob/read-stream?baton={baton}&blob_id={blob_id}&offset=1&length=10"
        );
        let (status, headers, body) = fixture.get_bytes(&read_path, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers[axum::http::header::CONTENT_TYPE],
            "application/octet-stream"
        );
        assert_eq!(headers[BLOB_BYTES_READ_HEADER], "10");
        assert_eq!(headers[BLOB_SIZE_HEADER], "12");
        assert_eq!(body, payload);

        let mismatch_path = format!(
            "/blob_stream/v2/blob/write-stream?baton={baton}&blob_id={blob_id}&offset=1&length=4"
        );
        let (status, headers, body) = fixture
            .post_bytes(&mismatch_path, b"abc".to_vec(), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers.get(BLOB_BYTES_WRITTEN_HEADER).is_none());
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("length mismatch")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hrana_ws_blob_binary_frames_read_and_write_raw_chunks() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_ws/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(6))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let mut connection = HranaWsConnection::new(
            fixture.state.clone(),
            "blob_ws".to_string(),
            HeaderMap::new(),
        );
        let messages = connection.handle_text(r#"{"type":"hello"}"#).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(ws_message_json(&messages[0])["type"], "hello_ok");

        let messages = connection
            .handle_text(
                r#"{"type":"request","request_id":1,"request":{"type":"open_stream","stream_id":7}}"#,
            )
            .await
            .unwrap();
        assert_eq!(
            ws_message_json(&messages[0])["response"]["type"],
            "open_stream"
        );

        let messages = connection
            .handle_text(
                r#"{"type":"request","request_id":2,"request":{"type":"blob_open","stream_id":7,"table":"files","column":"data","rowid":1,"read_only":false}}"#,
            )
            .await
            .unwrap();
        let body = ws_message_json(&messages[0]);
        assert_eq!(body["response"]["type"], "blob_open");
        let blob_id = body["response"]["result"]["blob_id"]
            .as_str()
            .unwrap()
            .to_string();

        let messages = connection
            .handle_text(&format!(
                r#"{{"type":"request","request_id":3,"request":{{"type":"blob_write_bytes","stream_id":7,"blob_id":"{blob_id}","offset":1}}}}"#
            ))
            .await
            .unwrap();
        assert!(messages.is_empty());

        let response = connection.handle_binary(b"pqrs".to_vec()).await.unwrap();
        let body: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(body["type"], "response_ok");
        assert_eq!(body["request_id"], 3);
        assert_eq!(body["response"]["type"], "blob_write_bytes");
        assert_eq!(body["response"]["result"]["bytes_written"], 4);

        let messages = connection
            .handle_text(&format!(
                r#"{{"type":"request","request_id":4,"request":{{"type":"blob_read_bytes","stream_id":7,"blob_id":"{blob_id}","offset":1,"length":4}}}}"#
            ))
            .await
            .unwrap();
        assert_eq!(messages.len(), 2);
        let body = ws_message_json(&messages[0]);
        assert_eq!(body["type"], "response_ok");
        assert_eq!(body["request_id"], 4);
        assert_eq!(body["response"]["type"], "blob_read_bytes");
        assert_eq!(body["response"]["result"]["bytes_read"], 4);
        match &messages[1] {
            Message::Binary(bytes) => assert_eq!(bytes.as_ref(), b"pqrs"),
            other => panic!("expected binary blob frame, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_rejects_read_only_and_out_of_range_writes() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_policy/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob)",
                        false,
                    ),
                    execute_request(
                        "insert into files (id, data) values (1, zeroblob(4))",
                        false,
                    ),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_policy/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["read_only"], true);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_policy/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"nope")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "SQLITE_READONLY");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("read-only")
        );

        let (status, body) = fixture
            .post(
                "/blob_policy/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_policy/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 3,
                    "base64": BASE64.encode(b"too long")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "SQLITE_ERROR");
        assert_eq!(body["error"]["message"], "Blob size is insufficient");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_enforces_configured_chunk_limit() {
        let fixture =
            RouterFixture::new_with_blob_max_chunk_bytes(LibsqlHttpAuthConfig::default(), 4).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_limit/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(8))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_limit/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_limit/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 5
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("max_chunk_bytes=4")
        );
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_limit/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"abcde")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("max_chunk_bytes=4")
        );

        let write_path =
            format!("/blob_limit/v2/blob/write-bytes?baton={baton}&blob_id={blob_id}&offset=0");
        let (status, headers, body) = fixture
            .post_bytes(&write_path, b"abcde".to_vec(), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers.get(BLOB_BYTES_WRITTEN_HEADER).is_none());
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("max_chunk_bytes=4")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_records_metrics_and_rejects_too_many_open_handles() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_metrics/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(8))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_metrics/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_metrics/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 1,
                    "base64": BASE64.encode(b"abcd")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_metrics/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 1,
                    "length": 4
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());

        let (status, body) = fixture.get("/_orion/metrics/blob", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["requests"]["open"], 1);
        assert_eq!(body["requests"]["write"], 1);
        assert_eq!(body["requests"]["read"], 1);
        assert_eq!(body["requests"]["failed"], 0);
        assert_eq!(body["bytes"]["written"], 4);
        assert_eq!(body["bytes"]["read"], 4);
        assert_eq!(body["current_open_handles"], 1);
        assert_eq!(body["max_open_handles_observed"], 1);
        assert!(body["latency"]["total_ns"].as_u64().unwrap() > 0);

        let mut session = memory_session();
        session
            .conn
            .execute_batch(
                "create table files (id integer primary key, data blob not null);
                 insert into files values (1, zeroblob(4));",
            )
            .unwrap();
        let first = blob_open_session_with_limit(
            &mut session,
            BlobOpenReqBody {
                baton: None,
                schema: None,
                table: "files".to_string(),
                column: "data".to_string(),
                rowid: 1,
                read_only: true,
            },
            1,
        )
        .unwrap();
        assert!(matches!(first, BlobResponseKind::Open { .. }));
        let error = blob_open_session_with_limit(
            &mut session,
            BlobOpenReqBody {
                baton: None,
                schema: None,
                table: "files".to_string(),
                column: "data".to_string(),
                rowid: 1,
                read_only: true,
            },
            1,
        )
        .unwrap_err();
        assert!(error.is::<OrionBlobTooManyOpenHandlesError>());
        assert!(
            error
                .to_string()
                .contains("too many open blob handles in session")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_commits_writes_inside_explicit_transaction() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_commit/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(6))", false),
                    execute_request("begin immediate", false),
                    serde_json::json!({ "type": "get_autocommit" }),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");
        assert_eq!(body["results"][2]["type"], "ok");
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_commit/v2/blob/open",
                serde_json::json!({
                    "baton": baton,
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_commit/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 1,
                    "base64": BASE64.encode(b"rust")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_commit/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select hex(data) from files where id = 1",
                                "want_rows": true
                            }
                        },
                        { "type": "execute", "stmt": { "sql": "commit" } },
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select hex(data) from files where id = 1",
                                "want_rows": true
                            }
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "007275737400" })
        );
        assert_eq!(
            body["results"][2]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "007275737400" })
        );

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_commit/v2/pipeline",
                one_query_pipeline("select hex(data) from files where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "007275737400" })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_rolls_back_writes_inside_explicit_transaction() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_rollback/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(6))", false),
                    execute_request("begin immediate", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_rollback/v2/blob/open",
                serde_json::json!({
                    "baton": baton,
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_rollback/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"cancel")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_rollback/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select hex(data) from files where id = 1",
                                "want_rows": true
                            }
                        },
                        { "type": "execute", "stmt": { "sql": "rollback" } },
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select hex(data) from files where id = 1",
                                "want_rows": true
                            }
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "63616E63656C" })
        );
        assert_eq!(
            body["results"][2]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "000000000000" })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_failed_write_does_not_poison_transaction() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_failed_write/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(4))", false),
                    execute_request("begin immediate", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_failed_write/v2/blob/open",
                serde_json::json!({
                    "baton": baton,
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_failed_write/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 3,
                    "base64": BASE64.encode(b"too long")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], "SQLITE_ERROR");
        assert_eq!(body["error"]["message"], "Blob size is insufficient");
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_failed_write/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"ok!!")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_failed_write/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [
                        { "type": "get_autocommit" },
                        { "type": "execute", "stmt": { "sql": "commit" } },
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select hex(data) from files where id = 1",
                                "want_rows": true
                            }
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        assert_eq!(body["results"][0]["response"]["is_autocommit"], false);
        assert_eq!(
            body["results"][2]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "6F6B2121" })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_session_close_rolls_back_open_transaction_and_handles() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_close/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(5))", false),
                    execute_request("begin immediate", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        for (index, result) in body["results"].as_array().unwrap().iter().enumerate() {
            assert_eq!(result["type"], "ok", "request {index} failed: {result:?}");
        }
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_close/v2/blob/open",
                serde_json::json!({
                    "baton": baton,
                    "table": "files",
                    "column": "data",
                    "rowid": 1,
                    "read_only": false
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_tx_close/v2/blob/write",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "base64": BASE64.encode(b"close")
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["error"].is_null());
        let baton = body["baton"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_close/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [{ "type": "close" }]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("baton").is_none());
        assert_eq!(body["results"][0]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_tx_close/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["baton"], baton);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert_eq!(body["error"]["message"], "unknown or expired baton");

        let (status, body) = fixture
            .pipeline(
                "/blob_tx_close/v2/pipeline",
                one_query_pipeline("select hex(data) from files where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["results"][0]["response"]["result"]["rows"][0][0],
            serde_json::json!({ "type": "text", "value": "0000000000" })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_pipeline_close_invalidates_blob_handles() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_close_session/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(4))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_close_session/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_close_session/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [{ "type": "close" }]
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("baton").is_none());
        assert_eq!(body["results"][0]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_close_session/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["baton"], baton);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert_eq!(body["error"]["message"], "unknown or expired baton");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_reaper_expires_blob_handles_with_session() {
        let fixture = RouterFixture::new_with_session_idle_timeout(
            LibsqlHttpAuthConfig::default(),
            Duration::from_millis(1),
        )
        .await;

        let (status, body) = fixture
            .pipeline(
                "/blob_expire/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(4))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_expire/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        tokio::time::sleep(Duration::from_millis(5)).await;
        fixture.state.clone().reap_idle_sessions_once();

        let (status, body) = fixture
            .post(
                "/blob_expire/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["baton"], baton);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert_eq!(body["error"]["message"], "unknown or expired baton");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_rejects_baton_from_another_database() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        for database in ["blob_db_a", "blob_db_b"] {
            let (status, body) = fixture
                .pipeline(
                    &format!("/{database}/v2/pipeline"),
                    sql_pipeline(vec![
                        execute_request(
                            "create table files (id integer primary key, data blob not null)",
                            false,
                        ),
                        execute_request("insert into files values (1, zeroblob(4))", false),
                    ]),
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["results"][0]["type"], "ok");
            assert_eq!(body["results"][1]["type"], "ok");
        }

        let (status, body) = fixture
            .post(
                "/blob_db_a/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .post(
                "/blob_db_b/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["baton"], baton);
        assert_eq!(body["error"]["code"], "SQLITE_IOERR");
        assert_eq!(
            body["error"]["message"],
            "baton belongs to database blob_db_a, not blob_db_b"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_blob_api_reports_row_mutation_after_handle_open() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/blob_mutation/v2/pipeline",
                sql_pipeline(vec![
                    execute_request(
                        "create table files (id integer primary key, data blob not null)",
                        false,
                    ),
                    execute_request("insert into files values (1, zeroblob(4))", false),
                ]),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");
        assert_eq!(body["results"][1]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_mutation/v2/blob/open",
                serde_json::json!({
                    "table": "files",
                    "column": "data",
                    "rowid": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let baton = body["baton"].as_str().unwrap().to_string();
        let blob_id = body["result"]["blob_id"].as_str().unwrap().to_string();

        let (status, body) = fixture
            .pipeline(
                "/blob_mutation/v2/pipeline",
                one_query_pipeline("delete from files where id = 1"),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "ok");

        let (status, body) = fixture
            .post(
                "/blob_mutation/v2/blob/read",
                serde_json::json!({
                    "baton": baton,
                    "blob_id": blob_id,
                    "offset": 0,
                    "length": 1
                }),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            ["SQLITE_ERROR", "SQLITE_ABORT"].contains(&body["error"]["code"].as_str().unwrap()),
            "{body:?}"
        );
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .to_ascii_lowercase()
                .contains("row")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_denies_unsafe_sqlite_surfaces() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        for sql in [
            "attach database ':memory:' as other",
            "pragma journal_mode = delete",
            "pragma synchronous = normal",
            "pragma locking_mode = exclusive",
            "pragma writable_schema = on",
            "pragma temp_store_directory = '/tmp'",
            "select load_extension('not-real')",
            "create virtual table docs using fts5(body)",
            "vacuum",
            "vacuum into '/tmp/orion-unsafe-copy.db'",
        ] {
            let (status, body) = fixture
                .pipeline("/compat_denied/v2/pipeline", one_query_pipeline(sql), None)
                .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["results"][0]["type"], "error", "{sql}: {body:?}");
            assert_eq!(
                body["results"][0]["error"]["code"], "SQLITE_AUTH",
                "{sql}: {body:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_returns_session_token_after_write_and_accepts_it_for_session_read() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (_, write_body) = fixture
            .pipeline(
                "/tenant_session/v2/pipeline",
                serde_json::json!({
                    "requests": [
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "create table items (id integer primary key, name text)"
                            }
                        },
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "insert into items values (1, 'alpha')"
                            }
                        }
                    ]
                }),
                None,
            )
            .await;
        let token = write_body["orion"]["session_token"]["token"]
            .as_str()
            .unwrap_or_else(|| {
                panic!("write response should include session token: {write_body:?}")
            })
            .to_string();

        let (status, read_body) = fixture
            .pipeline_with_headers(
                "/tenant_session/v2/pipeline",
                one_query_pipeline("select name from items where id = 1"),
                None,
                &[
                    (READ_POLICY_HEADER, "session"),
                    (SESSION_TOKEN_HEADER, &token),
                ],
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(read_body["results"][0]["type"], "ok", "{read_body:?}");
        assert_eq!(
            read_body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "alpha"
        );
        assert_eq!(read_body["orion"]["read_policy"], "session");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_rejects_invalid_session_token_as_protocol_error() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline_with_headers(
                "/tenant_bad_session/v2/pipeline",
                one_query_pipeline("select 1"),
                None,
                &[
                    (READ_POLICY_HEADER, "session"),
                    (SESSION_TOKEN_HEADER, "not-a-token"),
                ],
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "HRANA_PROTO_ERROR");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_accepts_explicit_read_freshness_policies() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        for headers in [
            vec![(READ_POLICY_HEADER, "strong")],
            vec![(READ_POLICY_HEADER, "local")],
            vec![
                (READ_POLICY_HEADER, "session"),
                (MIN_APPLIED_INDEX_HEADER, "0"),
            ],
            vec![
                (READ_POLICY_HEADER, "bounded_staleness"),
                (MAX_STALENESS_MS_HEADER, "1000"),
            ],
        ] {
            let (status, body) = fixture
                .pipeline_with_headers(
                    "/tenant_policy/v2/pipeline",
                    one_query_pipeline("select 42"),
                    None,
                    &headers,
                )
                .await;

            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["results"][0]["type"], "ok", "{body:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_reports_invalid_read_policy_as_protocol_error() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline_with_headers(
                "/tenant_policy_error/v2/pipeline",
                one_query_pipeline("select 42"),
                None,
                &[(READ_POLICY_HEADER, "eventual")],
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.get("baton").is_none());
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "HRANA_PROTO_ERROR");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_forwards_strong_reads_and_writes_to_configured_leader_http_endpoint() {
        let network = TonicRaftNetwork::new();
        let raft_config = Arc::new(
            Config {
                cluster_name: "libsql-http-forward-test".to_string(),
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        let leader_log_dir = TempDir::new().unwrap();
        let follower_log_dir = TempDir::new().unwrap();
        let leader_cache_dir = TempDir::new().unwrap();
        let follower_cache_dir = TempDir::new().unwrap();
        let leader_state = SlateDbStateStore::open_in_memory("libsql-http-forward-leader")
            .await
            .unwrap();
        let follower_state = SlateDbStateStore::open_in_memory("libsql-http-forward-follower")
            .await
            .unwrap();
        let raft1 = Raft::new(
            1,
            raft_config.clone(),
            network.clone(),
            OrionRaftLogStore::open(leader_log_dir.path()).unwrap(),
            OrionRaftStateMachine::new(leader_state.clone()),
        )
        .await
        .unwrap();
        let raft2 = Raft::new(
            2,
            raft_config,
            network,
            OrionRaftLogStore::open(follower_log_dir.path()).unwrap(),
            OrionRaftStateMachine::new(follower_state.clone()),
        )
        .await
        .unwrap();
        let (raft_addr1, _raft_server1) =
            bind_raft_transport("127.0.0.1:0".parse().unwrap(), raft1.clone())
                .await
                .unwrap();
        let (raft_addr2, _raft_server2) =
            bind_raft_transport("127.0.0.1:0".parse().unwrap(), raft2.clone())
                .await
                .unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            BasicNode {
                addr: raft_addr1.to_string(),
            },
        );
        members.insert(
            2,
            BasicNode {
                addr: raft_addr2.to_string(),
            },
        );
        raft1.initialize(members).await.unwrap();
        for raft in [&raft1, &raft2] {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(1, "libsql http forwarding leader")
                .await
                .unwrap();
        }

        let leader_http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_http_addr = leader_http_listener.local_addr().unwrap();
        let leader_http_config = LibsqlHttpConfig {
            sqlite_cache_root: leader_cache_dir.path().to_path_buf(),
            session_idle_timeout: Duration::from_secs(60),
            blob_max_chunk_bytes: 512 * 1024,
            idempotency: LibsqlHttpIdempotencyConfig::default(),
            auth: LibsqlHttpAuthConfig::default(),
            node_id: 1,
            peer_http_endpoints: BTreeMap::new(),
            placement_nodes: BTreeMap::from([
                (
                    1,
                    PlacementNodeConfig {
                        node_id: 1,
                        cloud: "aws".to_string(),
                        region: "us-east-1".to_string(),
                        zone: "use1-az1".to_string(),
                        raft_addr: raft_addr1.to_string(),
                        libsql_http_addr: Some(format!("http://{leader_http_addr}")),
                    },
                ),
                (
                    2,
                    PlacementNodeConfig {
                        node_id: 2,
                        cloud: "gcp".to_string(),
                        region: "us-central1".to_string(),
                        zone: "us-central1-a".to_string(),
                        raft_addr: raft_addr2.to_string(),
                        libsql_http_addr: None,
                    },
                ),
            ]),
            metrics_registry: ClusterRaftMetricsRegistry::default(),
            compaction_policy: SqlitePageCompactionPolicy::default(),
            replication_groups: None,
        };
        let leader_runtime = OrionSqliteRuntime::new(
            raft1.clone(),
            leader_state,
            OrionSqliteRuntimeConfig::new(leader_http_config.sqlite_cache_root.clone()),
        );
        let leader_state = LibsqlHttpState::new(leader_runtime, &leader_http_config);
        let leader_http_server = tokio::spawn(async move {
            axum::serve(leader_http_listener, libsql_router(leader_state))
                .await
                .unwrap();
        });
        let create_response: serde_json::Value = reqwest::Client::new()
            .post(format!("http://{leader_http_addr}/_orion/databases"))
            .json(&serde_json::json!({ "name": "tenant_forward" }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(create_response["state"], "ready", "{create_response:?}");

        let follower_http_config = LibsqlHttpConfig {
            sqlite_cache_root: follower_cache_dir.path().to_path_buf(),
            session_idle_timeout: Duration::from_secs(60),
            blob_max_chunk_bytes: 512 * 1024,
            idempotency: LibsqlHttpIdempotencyConfig::default(),
            auth: LibsqlHttpAuthConfig::default(),
            node_id: 2,
            peer_http_endpoints: BTreeMap::from([(1, format!("http://{leader_http_addr}"))]),
            placement_nodes: BTreeMap::from([
                (
                    1,
                    PlacementNodeConfig {
                        node_id: 1,
                        cloud: "aws".to_string(),
                        region: "us-east-1".to_string(),
                        zone: "use1-az1".to_string(),
                        raft_addr: raft_addr1.to_string(),
                        libsql_http_addr: Some(format!("http://{leader_http_addr}")),
                    },
                ),
                (
                    2,
                    PlacementNodeConfig {
                        node_id: 2,
                        cloud: "gcp".to_string(),
                        region: "us-central1".to_string(),
                        zone: "us-central1-a".to_string(),
                        raft_addr: raft_addr2.to_string(),
                        libsql_http_addr: None,
                    },
                ),
            ]),
            metrics_registry: ClusterRaftMetricsRegistry::default(),
            compaction_policy: SqlitePageCompactionPolicy::default(),
            replication_groups: None,
        };
        let follower_runtime = OrionSqliteRuntime::new(
            raft2.clone(),
            follower_state,
            OrionSqliteRuntimeConfig::new(follower_http_config.sqlite_cache_root.clone()),
        );
        assert_eq!(follower_runtime.metrics().current_leader, Some(1));
        let strong_read_request: PipelineReqBody =
            serde_json::from_value(one_query_pipeline("select 42")).unwrap();
        assert!(pipeline_should_enforce_read_policy(
            &strong_read_request,
            &OrionReadPolicy::Strong
        ));
        let follower_state = LibsqlHttpState::new(follower_runtime, &follower_http_config);
        let follower_router = libsql_router(follower_state);

        let response = follower_router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/tenant_forward/v2/pipeline")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .header(READ_POLICY_HEADER, "strong")
                    .body(Body::from(one_query_pipeline("select 42").to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["results"][0]["type"], "ok", "{json:?}");
        assert_eq!(json["orion"]["node_id"], 1);
        assert_eq!(json["orion"]["forwarded_from_node_id"], 2);
        let forwarded_baton = json["baton"].as_str().unwrap().to_string();

        let response = follower_router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/tenant_forward/v2/pipeline")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "baton": forwarded_baton,
                            "requests": [{ "type": "close" }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["results"][0]["type"], "ok", "{json:?}");
        assert_eq!(json["orion"]["node_id"], 1);
        assert_eq!(json["orion"]["forwarded_from_node_id"], 2);
        assert!(json.get("baton").is_none(), "{json:?}");

        for sql in [
            "create table if not exists forwarded_http_write (id integer primary key, value text not null)",
            "insert into forwarded_http_write values (1, 'leader-owned')",
        ] {
            let response = follower_router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/tenant_forward/v2/pipeline")
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            sql_pipeline(vec![execute_request(sql, false)]).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

            assert_eq!(status, StatusCode::OK);
            assert_eq!(json["results"][0]["type"], "ok", "{json:?}");
            assert_eq!(json["orion"]["node_id"], 1);
            assert_eq!(json["orion"]["forwarded_from_node_id"], 2);
        }

        let leader_response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{leader_http_addr}/tenant_forward/v2/pipeline"
            ))
            .json(&one_query_pipeline(
                "select value from forwarded_http_write where id = 1",
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            leader_response["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "leader-owned"
        );

        leader_http_server.abort();
        let _ = leader_http_server.await;
        raft1.shutdown().await.unwrap();
        raft2.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_auth_failure_and_success_are_reported_in_pipeline_body() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig {
            tokens: vec![LibsqlHttpAuthTokenConfig {
                token: "secret".to_string(),
                database_prefixes: vec!["tenant_".to_string()],
                system_permissions: Vec::new(),
            }],
        })
        .await;

        let (missing_status, missing_body) = fixture
            .pipeline(
                "/tenant_alpha/v2/pipeline",
                one_query_pipeline("select 1"),
                None,
            )
            .await;
        assert_eq!(missing_status, StatusCode::OK);
        assert!(missing_body.get("baton").is_none());
        assert_eq!(missing_body["results"][0]["type"], "error");
        assert_eq!(missing_body["results"][0]["error"]["code"], "SQLITE_AUTH");
        assert_eq!(
            missing_body["results"][0]["error"]["message"],
            "missing bearer token"
        );

        let (forbidden_status, forbidden_body) = fixture
            .pipeline(
                "/other_alpha/v2/pipeline",
                one_query_pipeline("select 1"),
                Some("secret"),
            )
            .await;
        assert_eq!(forbidden_status, StatusCode::OK);
        assert_eq!(forbidden_body["results"][0]["type"], "error");
        assert_eq!(forbidden_body["results"][0]["error"]["code"], "SQLITE_AUTH");
        assert_eq!(
            forbidden_body["results"][0]["error"]["message"],
            "bearer token is not authorized for database other_alpha"
        );

        let (success_status, success_body) = fixture
            .pipeline(
                "/tenant_alpha/v2/pipeline",
                one_query_pipeline("select 7"),
                Some("secret"),
            )
            .await;
        assert_eq!(success_status, StatusCode::OK);
        assert_eq!(success_body["results"][0]["type"], "ok");
        assert_eq!(
            success_body["results"][0]["response"]["result"]["rows"][0][0]["value"],
            "7"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_rejects_invalid_database_name_in_pipeline_body() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/tenant%20name/v2/pipeline",
                one_query_pipeline("select 1"),
                None,
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.get("baton").is_none());
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "SQLITE_AUTH");
        assert_eq!(
            body["results"][0]["error"]["message"],
            "database name may only contain ASCII letters, digits, dots, hyphens, and underscores"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_reports_invalid_baton_without_opening_new_session() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (status, body) = fixture
            .pipeline(
                "/tenant_baton/v2/pipeline",
                serde_json::json!({
                    "baton": "tenant_baton-404",
                    "requests": [
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select 1",
                                "want_rows": true
                            }
                        }
                    ]
                }),
                None,
            )
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["baton"], "tenant_baton-404");
        assert_eq!(body["results"][0]["type"], "error");
        assert_eq!(body["results"][0]["error"]["code"], "SQLITE_IOERR");
        assert_eq!(
            body["results"][0]["error"]["message"],
            "unknown or expired baton"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn router_close_after_pipeline_removes_baton() {
        let fixture = RouterFixture::new(LibsqlHttpAuthConfig::default()).await;

        let (_, opened) = fixture
            .pipeline(
                "/tenant_close/v2/pipeline",
                one_query_pipeline("select 11"),
                None,
            )
            .await;
        let baton = opened["baton"].as_str().unwrap();

        let (close_status, close_body) = fixture
            .pipeline(
                "/tenant_close/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [
                        { "type": "close" }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(close_status, StatusCode::OK);
        assert!(close_body.get("baton").is_none());
        assert_eq!(close_body["results"][0]["type"], "ok");
        assert_eq!(close_body["results"][0]["response"]["type"], "close");

        let (reuse_status, reuse_body) = fixture
            .pipeline(
                "/tenant_close/v2/pipeline",
                serde_json::json!({
                    "baton": baton,
                    "requests": [
                        {
                            "type": "execute",
                            "stmt": {
                                "sql": "select 12",
                                "want_rows": true
                            }
                        }
                    ]
                }),
                None,
            )
            .await;
        assert_eq!(reuse_status, StatusCode::OK);
        assert_eq!(reuse_body["baton"], baton);
        assert_eq!(reuse_body["results"][0]["type"], "error");
        assert_eq!(reuse_body["results"][0]["error"]["code"], "SQLITE_IOERR");
        assert_eq!(
            reuse_body["results"][0]["error"]["message"],
            "unknown or expired baton"
        );
    }

    #[test]
    fn describe_reports_statement_params_columns_and_readonly_status() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "create table services (tenant_id text, service_id text, weight integer)",
        )
        .unwrap();

        let select = describe_sql(
            &conn,
            "select tenant_id, weight from services where service_id = :service_id and weight > ?",
        )
        .unwrap();
        assert_eq!(select.params.len(), 2);
        assert_eq!(select.params[0].name.as_deref(), Some(":service_id"));
        assert_eq!(select.params[1].name, None);
        assert_eq!(select.cols[0].name, "tenant_id");
        assert_eq!(select.cols[0].decltype.as_deref(), Some("TEXT"));
        assert_eq!(select.cols[1].name, "weight");
        assert_eq!(select.cols[1].decltype.as_deref(), Some("INTEGER"));
        assert!(!select.is_explain);
        assert!(select.is_readonly);

        let insert = describe_sql(
            &conn,
            "insert into services (tenant_id, service_id, weight) values (?, ?, ?)",
        )
        .unwrap();
        assert_eq!(insert.params.len(), 3);
        assert!(insert.cols.is_empty());
        assert!(!insert.is_readonly);

        let explain = describe_sql(&conn, "explain select * from services").unwrap();
        assert!(explain.is_explain);
        assert!(explain.is_readonly);
    }

    #[test]
    fn execute_round_trips_declared_column_types() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("create table services (tenant_id text, weight integer)")
            .unwrap();
        conn.execute("insert into services values ('acme', 42)", [])
            .unwrap();
        let mut session = memory_session();
        session.conn = conn;

        let result = execute_stmt(
            &mut session,
            query_stmt("select tenant_id, weight from services"),
        )
        .unwrap();

        assert_eq!(result.cols[0].decltype.as_deref(), Some("TEXT"));
        assert_eq!(result.cols[1].decltype.as_deref(), Some("INTEGER"));
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn bare_named_args_resolve_to_statement_parameter_prefixes() {
        let mut session = memory_session();
        execute_stmt(
            &mut session,
            stmt("create table services (label text primary key, amount integer not null)"),
        )
        .unwrap();

        execute_stmt(
            &mut session,
            Stmt {
                sql: Some(
                    "insert into services (label, amount) values ($label, $amount)".to_string(),
                ),
                sql_id: None,
                args: Vec::new(),
                named_args: vec![
                    NamedArg {
                        name: "label".to_string(),
                        value: HranaValue::Text {
                            value: "named".to_string(),
                        },
                    },
                    NamedArg {
                        name: "amount".to_string(),
                        value: HranaValue::Integer {
                            value: "11".to_string(),
                        },
                    },
                ],
                want_rows: false,
            },
        )
        .unwrap();

        let result = execute_stmt(
            &mut session,
            query_stmt("select amount from services where label = 'named'"),
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![HranaValue::Integer {
                value: "11".to_string()
            }]]
        );
    }

    #[test]
    fn prefixed_named_args_are_preserved() {
        let mut session = memory_session();
        execute_stmt(
            &mut session,
            stmt("create table services (label text primary key, amount integer not null)"),
        )
        .unwrap();

        execute_stmt(
            &mut session,
            Stmt {
                sql: Some(
                    "insert into services (label, amount) values (@label, @amount)".to_string(),
                ),
                sql_id: None,
                args: Vec::new(),
                named_args: vec![
                    NamedArg {
                        name: "@label".to_string(),
                        value: HranaValue::Text {
                            value: "prefixed".to_string(),
                        },
                    },
                    NamedArg {
                        name: "@amount".to_string(),
                        value: HranaValue::Integer {
                            value: "19".to_string(),
                        },
                    },
                ],
                want_rows: false,
            },
        )
        .unwrap();

        let result = execute_stmt(
            &mut session,
            query_stmt("select amount from services where label = 'prefixed'"),
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![HranaValue::Integer {
                value: "19".to_string()
            }]]
        );
    }

    #[test]
    fn batch_conditions_preserve_ordered_results_and_errors() {
        let mut session = memory_session();

        let result = execute_batch(
            &mut session,
            Batch {
                steps: vec![
                    BatchStep {
                        condition: None,
                        stmt: stmt("create table services (id integer primary key)"),
                    },
                    BatchStep {
                        condition: None,
                        stmt: stmt("insert into services values (1)"),
                    },
                    BatchStep {
                        condition: None,
                        stmt: stmt("insert into services values (1)"),
                    },
                    BatchStep {
                        condition: Some(BatchCond::Error { step: 2 }),
                        stmt: query_stmt("select count(*) from services"),
                    },
                    BatchStep {
                        condition: Some(BatchCond::Ok { step: 2 }),
                        stmt: query_stmt("select 99"),
                    },
                ],
            },
        )
        .unwrap();

        assert_eq!(result.step_results.len(), 5);
        assert!(result.step_results[0].is_some());
        assert!(result.step_results[1].is_some());
        assert!(result.step_results[2].is_none());
        assert_eq!(
            result.step_errors[2]
                .as_ref()
                .and_then(|error| error.code.as_deref()),
            Some("SQLITE_CONSTRAINT")
        );
        assert_eq!(
            result.step_results[3].as_ref().unwrap().rows,
            vec![vec![HranaValue::Integer {
                value: "1".to_string()
            }]]
        );
        assert!(result.step_results[4].is_none());
        assert!(result.step_errors[4].is_none());
    }

    #[test]
    fn transaction_state_survives_across_session_statements() {
        let mut session = memory_session();

        execute_stmt(
            &mut session,
            stmt("create table services (id integer primary key)"),
        )
        .unwrap();
        assert!(session.conn.is_autocommit());

        execute_stmt(&mut session, stmt("begin immediate")).unwrap();
        assert!(!session.conn.is_autocommit());
        execute_stmt(&mut session, stmt("insert into services values (1)")).unwrap();
        execute_stmt(&mut session, stmt("rollback")).unwrap();
        assert!(session.conn.is_autocommit());

        let result =
            execute_stmt(&mut session, query_stmt("select count(*) from services")).unwrap();
        assert_eq!(
            result.rows,
            vec![vec![HranaValue::Integer {
                value: "0".to_string()
            }]]
        );
    }

    #[test]
    fn sqlite_error_mapping_preserves_actionable_result_codes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("create table services (id integer primary key)")
            .unwrap();
        conn.execute("insert into services values (1)", []).unwrap();

        let constraint = conn
            .execute("insert into services values (1)", [])
            .unwrap_err();
        assert_eq!(sqlite_error_code(&constraint), "SQLITE_CONSTRAINT");

        let syntax = conn.execute("select from", []).unwrap_err();
        assert_eq!(sqlite_error_code(&syntax), "SQLITE_ERROR");

        let mut stmt = conn.prepare("select * from services where id = ?").unwrap();
        let missing_param = match stmt.query([]) {
            Ok(_) => panic!("query without required parameter unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(sqlite_error_code(&missing_param), "SQLITE_RANGE");
    }

    #[test]
    fn malformed_hrana_values_are_protocol_errors_not_sqlite_io_errors() {
        let integer = hrana_to_sqlite_value(HranaValue::Integer {
            value: "not-an-int".to_string(),
        })
        .unwrap_err();
        assert_eq!(sqlite_error_code(integer.as_ref()), "HRANA_PROTO_ERROR");

        let blob = hrana_to_sqlite_value(HranaValue::Blob {
            base64: "%%%".to_string(),
        })
        .unwrap_err();
        assert_eq!(sqlite_error_code(blob.as_ref()), "HRANA_PROTO_ERROR");

        let session = memory_session();
        let missing_sql = resolve_sql(
            &session,
            StmtSql {
                sql: None,
                sql_id: None,
            },
        )
        .unwrap_err();
        assert_eq!(sqlite_error_code(missing_sql.as_ref()), "HRANA_PROTO_ERROR");
    }
}
