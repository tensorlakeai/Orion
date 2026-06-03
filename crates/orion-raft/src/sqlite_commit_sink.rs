use orion_sqlite::{CommitDecision, FileKind, RaftWalCommitSink, VfsFileOp, VfsSyncBatch};
use std::time::Instant;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::openraft_store::{
    LargeSqliteBatchChunk, LargeSqliteBatchManifest, LargeSqliteBatchRequest, OrionRaftRequest,
    OrionRaftRequestMeta, OrionTypeConfig,
};
use crate::sqlite_raft_client::{OrionSqliteRaftClient, OrionSqliteRaftError};
use crate::tonic_transport::OrionRaft;
use crate::types::{SqliteFileKind, SqliteVfsBatch, SqliteVfsOp, SqliteVfsWrite};

pub const DEFAULT_LARGE_BATCH_THRESHOLD_BYTES: usize = 512 * 1024;
pub const DEFAULT_LARGE_BATCH_CHUNK_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeBatchOptions {
    pub threshold_bytes: usize,
    pub chunk_bytes: usize,
}

impl Default for LargeBatchOptions {
    fn default() -> Self {
        Self {
            threshold_bytes: DEFAULT_LARGE_BATCH_THRESHOLD_BYTES,
            chunk_bytes: DEFAULT_LARGE_BATCH_CHUNK_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct OpenRaftSqliteCommitSink {
    raft_client: OrionSqliteRaftClient,
    large_batch_options: LargeBatchOptions,
}

impl OpenRaftSqliteCommitSink {
    pub fn new(raft: Option<OrionRaft>) -> Self {
        Self::with_large_batch_options(raft, LargeBatchOptions::default())
    }

    pub fn with_large_batch_options(
        raft: Option<OrionRaft>,
        large_batch_options: LargeBatchOptions,
    ) -> Self {
        Self {
            raft_client: OrionSqliteRaftClient::new(raft),
            large_batch_options: LargeBatchOptions {
                threshold_bytes: large_batch_options.threshold_bytes.max(1),
                chunk_bytes: large_batch_options.chunk_bytes.max(1),
            },
        }
    }

    pub async fn commit_batch(
        &self,
        batch: VfsSyncBatch,
    ) -> Result<CommitDecision, OrionSqliteRaftError> {
        let started = Instant::now();
        let file_kind = batch.file_kind;
        let op_count = batch.ops.len();
        let sqlite_batch = convert_batch(batch);
        let log_index = if sqlite_batch_payload_bytes(&sqlite_batch)
            > self.large_batch_options.threshold_bytes
        {
            self.commit_large_batch(sqlite_batch).await?
        } else {
            self.raft_client
                .propose(OrionRaftRequest::sqlite_batch(sqlite_batch))
                .await?
        };
        trace_latency(format_args!(
            "sqlite_commit_sink raft_propose file_kind={file_kind:?} ops={op_count} log_index={} elapsed_ms={:.3}",
            log_index.unwrap_or(0),
            started.elapsed().as_secs_f64() * 1000.0
        ));
        Ok(CommitDecision {
            raft_log_index: log_index.unwrap_or(0),
            materialized_by_commit: true,
        })
    }

    async fn commit_large_batch(
        &self,
        batch: SqliteVfsBatch,
    ) -> Result<Option<u64>, OrionSqliteRaftError> {
        let upload_id = Uuid::new_v4().to_string();
        let total_bytes = sqlite_batch_payload_bytes(&batch) as u64;
        let chunks = split_large_batch_ops(batch.ops, self.large_batch_options.chunk_bytes);
        let manifest = LargeSqliteBatchManifest {
            upload_id: upload_id.clone(),
            database: batch.database,
            file_path: batch.file_path,
            file_kind: batch.file_kind,
            total_chunks: chunks.len() as u32,
            total_bytes,
            created_at_ms: current_time_millis(),
        };
        let begin = OrionRaftRequest {
            meta: Some(OrionRaftRequestMeta::new(
                crate::HybridClock::global().next(),
            )),
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: Some(LargeSqliteBatchRequest::Begin(manifest)),
        };
        self.raft_client.propose(begin).await?;

        for (chunk_index, ops) in chunks.into_iter().enumerate() {
            let chunk = OrionRaftRequest {
                meta: Some(OrionRaftRequestMeta::new(
                    crate::HybridClock::global().next(),
                )),
                sqlite_batches: Vec::new(),
                sqlite_page_deltas: Vec::new(),
                large_sqlite_page_delta: None,
                large_sqlite_batch: Some(LargeSqliteBatchRequest::Chunk(LargeSqliteBatchChunk {
                    upload_id: upload_id.clone(),
                    chunk_index: chunk_index as u32,
                    ops,
                })),
            };
            if let Err(error) = self.raft_client.propose(chunk).await {
                self.abort_large_batch(&upload_id).await;
                return Err(error);
            }
        }

        let commit = OrionRaftRequest {
            meta: Some(OrionRaftRequestMeta::new(
                crate::HybridClock::global().next(),
            )),
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: Some(LargeSqliteBatchRequest::Commit {
                upload_id: upload_id.clone(),
            }),
        };
        match self.raft_client.propose(commit).await {
            Ok(index) => Ok(index),
            Err(error) => {
                self.abort_large_batch(&upload_id).await;
                Err(error)
            }
        }
    }

    async fn abort_large_batch(&self, upload_id: &str) {
        let abort = OrionRaftRequest {
            meta: Some(OrionRaftRequestMeta::new(
                crate::HybridClock::global().next(),
            )),
            sqlite_batches: Vec::new(),
            sqlite_page_deltas: Vec::new(),
            large_sqlite_page_delta: None,
            large_sqlite_batch: Some(LargeSqliteBatchRequest::Abort {
                upload_id: upload_id.to_string(),
            }),
        };
        let _ = self.raft_client.propose(abort).await;
    }
}

fn trace_latency(args: std::fmt::Arguments<'_>) {
    if std::env::var_os("ORION_TRACE_LATENCY").is_some() {
        eprintln!("orion latency {args}");
    }
}

impl RaftWalCommitSink for OpenRaftSqliteCommitSink {
    fn commit_sync_batch(&self, batch: VfsSyncBatch) -> anyhow::Result<CommitDecision> {
        let sink = self.clone();
        let handle = Handle::current();
        tokio::task::block_in_place(|| {
            handle
                .block_on(sink.commit_batch(batch))
                .map_err(|err| anyhow::anyhow!(err.to_string()))
        })
    }
}

fn convert_batch(batch: VfsSyncBatch) -> SqliteVfsBatch {
    SqliteVfsBatch {
        database: batch.database,
        file_path: batch.file_path,
        file_kind: convert_file_kind(batch.file_kind),
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                VfsFileOp::Write(write) => SqliteVfsOp::Write(SqliteVfsWrite {
                    offset: write.offset,
                    bytes: write.bytes,
                }),
                VfsFileOp::Truncate { size } => SqliteVfsOp::Truncate { size },
                VfsFileOp::Delete => SqliteVfsOp::Delete,
            })
            .collect(),
    }
}

fn sqlite_batch_payload_bytes(batch: &SqliteVfsBatch) -> usize {
    batch
        .ops
        .iter()
        .map(|op| match op {
            SqliteVfsOp::Write(write) => write.bytes.len(),
            SqliteVfsOp::Truncate { .. } | SqliteVfsOp::Delete => 0,
        })
        .sum()
}

fn split_large_batch_ops(
    ops: Vec<SqliteVfsOp>,
    target_chunk_bytes: usize,
) -> Vec<Vec<SqliteVfsOp>> {
    let target_chunk_bytes = target_chunk_bytes.max(1);
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0;

    for op in ops {
        match op {
            SqliteVfsOp::Write(write) if write.bytes.len() > target_chunk_bytes => {
                if !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                    current_bytes = 0;
                }
                let mut offset = write.offset;
                for bytes in write.bytes.chunks(target_chunk_bytes) {
                    chunks.push(vec![SqliteVfsOp::Write(SqliteVfsWrite {
                        offset,
                        bytes: bytes.to_vec(),
                    })]);
                    offset += bytes.len() as u64;
                }
            }
            other => {
                let op_bytes = match &other {
                    SqliteVfsOp::Write(write) => write.bytes.len(),
                    SqliteVfsOp::Truncate { .. } | SqliteVfsOp::Delete => 0,
                };
                if !current.is_empty() && current_bytes + op_bytes > target_chunk_bytes {
                    chunks.push(std::mem::take(&mut current));
                    current_bytes = 0;
                }
                current.push(other);
                current_bytes += op_bytes;
            }
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn convert_file_kind(kind: FileKind) -> SqliteFileKind {
    match kind {
        FileKind::MainDb => SqliteFileKind::MainDb,
        FileKind::Wal => SqliteFileKind::Wal,
        FileKind::Journal => SqliteFileKind::Journal,
        FileKind::Temp => SqliteFileKind::Temp,
        FileKind::Other => SqliteFileKind::Other,
    }
}

fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[allow(dead_code)]
fn _assert_openraft_type_config_is_send_sync()
where
    OrionTypeConfig: Send + Sync,
{
}
