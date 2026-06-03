use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use openraft_rt::WatchReceiver;

use crate::openraft_store::{OrionLogId, OrionNodeId};
use crate::tonic_transport::OrionRaft;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSoftwareCapabilities {
    pub catalog_min_read_schema_version: u32,
    pub catalog_max_read_schema_version: u32,
    pub catalog_max_write_schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMetricsSnapshot {
    pub node_id: OrionNodeId,
    pub state: String,
    pub running: bool,
    pub current_term: u64,
    pub current_leader: Option<OrionNodeId>,
    pub last_log_index: Option<u64>,
    pub committed_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub voter_ids: Vec<OrionNodeId>,
    pub learner_ids: Vec<OrionNodeId>,
    pub replication: Vec<RaftPeerMetricsSnapshot>,
    #[serde(default)]
    pub snapshot_transfer: SnapshotTransferMetrics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<NodeSoftwareCapabilities>,
}

impl RaftMetricsSnapshot {
    pub fn observe(raft: &OrionRaft) -> Self {
        let metrics = raft.metrics().borrow_watched().clone();
        let last_log_index = metrics.last_log_index;
        let membership = metrics.membership_config.membership();
        let replication = metrics
            .replication
            .unwrap_or_default()
            .into_iter()
            .map(|(node_id, matched): (OrionNodeId, Option<OrionLogId>)| {
                let matched_index = matched.map(|log_id| log_id.index);
                RaftPeerMetricsSnapshot {
                    node_id,
                    matched_index,
                    replication_lag: replication_lag(last_log_index, matched_index),
                }
            })
            .collect();

        Self {
            node_id: metrics.id,
            state: format!("{:?}", metrics.state),
            running: metrics.running_state.is_ok(),
            current_term: metrics.current_term,
            current_leader: metrics.current_leader,
            last_log_index,
            committed_index: metrics.committed.map(|log_id| log_id.index),
            applied_index: metrics.last_applied.map(|log_id| log_id.index),
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
            purged_index: metrics.purged.map(|log_id| log_id.index),
            voter_ids: membership.voter_ids().collect(),
            learner_ids: membership.learner_ids().collect(),
            replication,
            snapshot_transfer: SnapshotTransferMetrics::default(),
            capabilities: None,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.current_leader == Some(self.node_id)
    }

    pub fn has_known_leader(&self) -> bool {
        self.current_leader.is_some()
    }

    pub fn is_voter(&self) -> bool {
        self.voter_ids.contains(&self.node_id)
    }

    pub fn is_ready_for_linearizable_reads(&self) -> bool {
        self.running
            && self.has_known_leader()
            && self
                .committed_index
                .zip(self.applied_index)
                .is_none_or(|(committed, applied)| applied >= committed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftPeerMetricsSnapshot {
    pub node_id: OrionNodeId,
    pub matched_index: Option<u64>,
    pub replication_lag: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRaftMetricsEntry {
    pub observed_at_ms: u64,
    pub metrics: RaftMetricsSnapshot,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterRaftMetricsRegistry {
    inner: Arc<RwLock<BTreeMap<OrionNodeId, ClusterRaftMetricsEntry>>>,
    local_capabilities: Arc<RwLock<Option<NodeSoftwareCapabilities>>>,
    snapshot_transfer: Arc<RwLock<BTreeMap<OrionNodeId, SnapshotTransferMetrics>>>,
}

impl ClusterRaftMetricsRegistry {
    pub fn set_local_capabilities(&self, capabilities: NodeSoftwareCapabilities) {
        if let Ok(mut local_capabilities) = self.local_capabilities.write() {
            *local_capabilities = Some(capabilities);
        }
    }

    pub fn record(&self, metrics: RaftMetricsSnapshot) {
        let entry = ClusterRaftMetricsEntry {
            observed_at_ms: current_time_millis(),
            metrics,
        };
        if let Ok(mut inner) = self.inner.write() {
            inner.insert(entry.metrics.node_id, entry);
        }
    }

    pub fn record_observed(&self, raft: &OrionRaft) -> RaftMetricsSnapshot {
        let mut metrics = RaftMetricsSnapshot::observe(raft);
        metrics.snapshot_transfer = self.snapshot_transfer(metrics.node_id);
        metrics.capabilities = self
            .local_capabilities
            .read()
            .ok()
            .and_then(|capabilities| capabilities.clone());
        self.record(metrics.clone());
        metrics
    }

    pub fn record_snapshot_transfer(
        &self,
        node_id: OrionNodeId,
        mutate: impl FnOnce(&mut SnapshotTransferMetrics),
    ) {
        if let Ok(mut snapshot_transfer) = self.snapshot_transfer.write() {
            let metrics = snapshot_transfer.entry(node_id).or_default();
            mutate(metrics);
        }
    }

    pub fn snapshot_transfer(&self, node_id: OrionNodeId) -> SnapshotTransferMetrics {
        self.snapshot_transfer
            .read()
            .ok()
            .and_then(|snapshot_transfer| snapshot_transfer.get(&node_id).cloned())
            .unwrap_or_default()
    }

    pub fn get(&self, node_id: OrionNodeId) -> Option<ClusterRaftMetricsEntry> {
        self.inner
            .read()
            .ok()
            .and_then(|inner| inner.get(&node_id).cloned())
    }

    pub fn get_metrics(&self, node_id: OrionNodeId) -> Option<RaftMetricsSnapshot> {
        self.get(node_id).map(|entry| entry.metrics)
    }

    pub fn snapshot(&self) -> Vec<ClusterRaftMetricsEntry> {
        self.inner
            .read()
            .map(|inner| inner.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTransferMetrics {
    pub snapshots_sent: u64,
    pub snapshots_received: u64,
    pub snapshot_send_failures: u64,
    pub snapshot_receive_failures: u64,
    pub manifest_bytes_sent: u64,
    pub manifest_bytes_received: u64,
    pub checkpoint_objects_sent: u64,
    pub checkpoint_objects_received: u64,
    pub checkpoint_object_bytes_sent: u64,
    pub checkpoint_object_bytes_received: u64,
    pub multipart_upload_failures: u64,
    pub multipart_upload_aborts: u64,
}

fn replication_lag(last_log_index: Option<u64>, matched_index: Option<u64>) -> Option<u64> {
    let last = last_log_index?;
    let matched = matched_index?;
    Some(last.saturating_sub(matched))
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
