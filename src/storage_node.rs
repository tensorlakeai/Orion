use crate::node::{NodeConfig, TopologyConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodePlan {
    pub node_id: u64,
    pub topology: TopologyConfig,
    pub raft_addr: String,
    pub object_store_label: String,
    pub components: Vec<StorageNodeComponent>,
}

impl StorageNodePlan {
    pub fn from_config(config: &NodeConfig) -> Self {
        Self {
            node_id: config.node.id,
            topology: config.node.topology.clone(),
            raft_addr: config.advertised_raft_addr(),
            object_store_label: config.storage.object_store.label(),
            components: vec![
                StorageNodeComponent::WalQuorum,
                StorageNodeComponent::PageService,
                StorageNodeComponent::ObjectStoreSync,
                StorageNodeComponent::NvmeCache,
                StorageNodeComponent::Compactor,
                StorageNodeComponent::Repair,
            ],
        }
    }

    pub fn component_labels(&self) -> String {
        self.components
            .iter()
            .map(|component| component.label())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageNodeComponent {
    WalQuorum,
    PageService,
    ObjectStoreSync,
    NvmeCache,
    Compactor,
    Repair,
}

impl StorageNodeComponent {
    pub fn label(self) -> &'static str {
        match self {
            Self::WalQuorum => "wal_quorum",
            Self::PageService => "page_service",
            Self::ObjectStoreSync => "object_store_sync",
            Self::NvmeCache => "nvme_cache",
            Self::Compactor => "compactor",
            Self::Repair => "repair",
        }
    }
}
