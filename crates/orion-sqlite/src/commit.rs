use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsSyncBatch {
    pub database: String,
    pub file_path: String,
    pub file_kind: FileKind,
    pub ops: Vec<VfsFileOp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VfsFileOp {
    Write(VfsWrite),
    Truncate { size: u64 },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsWrite {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileKind {
    MainDb,
    Wal,
    Journal,
    Temp,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDecision {
    pub raft_log_index: u64,
    #[serde(default)]
    pub materialized_by_commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedVfsBatch {
    pub batch: VfsSyncBatch,
    pub decision: CommitDecision,
}

pub trait RaftWalCommitSink: Send + Sync + 'static {
    fn commit_sync_batch(&self, batch: VfsSyncBatch) -> anyhow::Result<CommitDecision>;
}

#[derive(Debug, Default, Clone)]
pub struct RecordingCommitSink {
    committed: Arc<Mutex<Vec<CommittedVfsBatch>>>,
}

impl RecordingCommitSink {
    pub fn committed_batches(&self) -> Vec<CommittedVfsBatch> {
        self.committed
            .lock()
            .expect("recording commit sink mutex poisoned")
            .clone()
    }
}

impl RaftWalCommitSink for RecordingCommitSink {
    fn commit_sync_batch(&self, batch: VfsSyncBatch) -> anyhow::Result<CommitDecision> {
        let mut committed = self
            .committed
            .lock()
            .expect("recording commit sink mutex poisoned");
        let decision = CommitDecision {
            raft_log_index: committed.len() as u64 + 1,
            materialized_by_commit: false,
        };
        committed.push(CommittedVfsBatch { batch, decision });
        Ok(decision)
    }
}
