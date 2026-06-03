use std::sync::Arc;

use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use slatedb::{Db, DbReadOps, DbWriteOps};
use tokio::sync::Mutex;

use crate::HybridTimestamp;
use crate::checkpoint_artifact::{
    SlateDbCheckpointArtifact, SlateDbCheckpointMaterializeMetrics,
    clone_slate_db_checkpoint_artifact_from_local_objects, create_slate_db_checkpoint_artifact,
    materialize_slate_db_checkpoint_incremental,
};
use crate::slatedb_sqlite_store::{
    SqliteDatabaseFileSnapshot, SqliteDatabasePageSyncDelta, SqliteDatabasePageSyncMetrics,
};

const APPLIED_INDEX_KEY: &[u8] = b"sys/ranges/default/applied_index";
const APPLIED_COMMIT_TS_KEY: &[u8] = b"sys/ranges/default/applied_commit_ts";
const SQLITE_DATABASE_PREFIX: &[u8] = b"sys/sqlite_databases/";
const SQLITE_DATABASE_READY_PREFIX: &[u8] = b"sys/sqlite_database_ready/";

#[derive(Clone)]
pub struct SlateDbStateStore {
    pub(crate) path: String,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) db: Arc<Db>,
    sqlite_databases: Arc<Mutex<std::collections::BTreeMap<String, SlateDbStateStore>>>,
}

impl SlateDbStateStore {
    pub async fn open_in_memory(path: &str) -> anyhow::Result<Self> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::open(path, object_store).await
    }

    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> anyhow::Result<Self> {
        Ok(Self {
            path: path.to_string(),
            db: Arc::new(Db::open(path, Arc::clone(&object_store)).await?),
            object_store,
            sqlite_databases: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub(crate) async fn swap_to_path(&mut self, path: String) -> anyhow::Result<()> {
        let next_db = Db::open(path.clone(), Arc::clone(&self.object_store)).await?;
        let previous_db = std::mem::replace(&mut self.db, Arc::new(next_db));
        self.path = path;
        self.sqlite_databases.lock().await.clear();
        previous_db.close().await?;
        Ok(())
    }

    pub async fn sqlite_database_state(&self, database: &str) -> anyhow::Result<Self> {
        validate_sqlite_database_name(database)?;
        let mut databases = self.sqlite_databases.lock().await;
        if let Some(existing) = databases.get(database).cloned() {
            return Ok(existing);
        }

        let child_path = self.sqlite_database_path(database);
        let child = Self::open(&child_path, Arc::clone(&self.object_store)).await?;
        DbWriteOps::put(
            self.db.as_ref(),
            &sqlite_database_marker_key(database),
            database.as_bytes(),
        )
        .await?;

        databases.insert(database.to_string(), child.clone());
        Ok(child)
    }

    pub async fn existing_sqlite_database_state(
        &self,
        database: &str,
    ) -> anyhow::Result<Option<Self>> {
        validate_sqlite_database_name(database)?;
        let mut databases = self.sqlite_databases.lock().await;
        if let Some(existing) = databases.get(database).cloned() {
            return Ok(Some(existing));
        }

        let marker_exists =
            DbReadOps::get(self.db.as_ref(), &sqlite_database_marker_key(database)).await?;
        if marker_exists.is_none() {
            return Ok(None);
        }

        let child_path = self.sqlite_database_path(database);
        let child = Self::open(&child_path, Arc::clone(&self.object_store)).await?;
        databases.insert(database.to_string(), child.clone());
        Ok(Some(child))
    }

    pub async fn clone_sqlite_database_from(
        &self,
        database: &str,
        source: &Self,
    ) -> anyhow::Result<Self> {
        validate_sqlite_database_name(database)?;
        let source_child = source.sqlite_database_state(database).await?;
        let target_path = self.sqlite_database_path(database);
        let checkpoint = source_child
            .db
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    name: Some(format!("placement-clone-{database}")),
                    ..CheckpointOptions::default()
                },
            )
            .await?;
        AdminBuilder::new(target_path.clone(), Arc::clone(&self.object_store))
            .build()
            .create_clone_builder(source_child.path.clone(), Some(checkpoint.id))
            .build()
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let child = Self::open(&target_path, Arc::clone(&self.object_store)).await?;
        DbWriteOps::put(
            self.db.as_ref(),
            &sqlite_database_marker_key(database),
            database.as_bytes(),
        )
        .await?;
        self.sqlite_databases
            .lock()
            .await
            .insert(database.to_string(), child.clone());
        Ok(child)
    }

    pub async fn sqlite_database_checkpoint_artifact(
        &self,
        database: &str,
        name: impl Into<String>,
    ) -> anyhow::Result<SlateDbCheckpointArtifact> {
        validate_sqlite_database_name(database)?;
        let child = self.sqlite_database_state(database).await?;
        create_slate_db_checkpoint_artifact(&child, name).await
    }

    pub async fn materialize_sqlite_database_checkpoint_incremental(
        &self,
        database: &str,
        artifact: &SlateDbCheckpointArtifact,
        source_object_store: Arc<dyn ObjectStore>,
    ) -> anyhow::Result<SlateDbCheckpointMaterializeMetrics> {
        validate_sqlite_database_name(database)?;
        let target_path = self.sqlite_database_path(database);
        let existing = self.sqlite_databases.lock().await.remove(database);
        if let Some(existing) = existing {
            existing.close().await?;
        }
        let metrics = materialize_slate_db_checkpoint_incremental(
            artifact,
            source_object_store,
            target_path.clone(),
            Arc::clone(&self.object_store),
        )
        .await?;
        let child = Self::open(&target_path, Arc::clone(&self.object_store)).await?;
        DbWriteOps::put(
            self.db.as_ref(),
            &sqlite_database_marker_key(database),
            database.as_bytes(),
        )
        .await?;
        self.sqlite_databases
            .lock()
            .await
            .insert(database.to_string(), child);
        Ok(metrics)
    }

    pub async fn clone_sqlite_database_checkpoint_from_local_objects(
        &self,
        database: &str,
        artifact: &SlateDbCheckpointArtifact,
    ) -> anyhow::Result<()> {
        validate_sqlite_database_name(database)?;
        let target_path = self.sqlite_database_path(database);
        let existing = self.sqlite_databases.lock().await.remove(database);
        if let Some(existing) = existing {
            existing.close().await?;
        }
        clone_slate_db_checkpoint_artifact_from_local_objects(
            artifact,
            target_path.clone(),
            Arc::clone(&self.object_store),
        )
        .await?;
        let child = Self::open(&target_path, Arc::clone(&self.object_store)).await?;
        DbWriteOps::put(
            self.db.as_ref(),
            &sqlite_database_marker_key(database),
            database.as_bytes(),
        )
        .await?;
        self.sqlite_databases
            .lock()
            .await
            .insert(database.to_string(), child);
        Ok(())
    }

    pub async fn export_sqlite_database_pages_since(
        &self,
        database: &str,
        min_exclusive_version: u64,
    ) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
        crate::slatedb_sqlite_store::export_sqlite_database_pages_since(
            self,
            database,
            min_exclusive_version,
        )
        .await
    }

    pub async fn export_sqlite_database_live_snapshot(
        &self,
        database: &str,
    ) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
        crate::slatedb_sqlite_store::export_sqlite_database_live_snapshot(self, database).await
    }

    pub async fn apply_sqlite_database_page_delta(
        &self,
        database: &str,
        delta: &SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::apply_sqlite_database_page_delta(self, database, delta).await
    }

    pub async fn materialize_sqlite_database_live_snapshot(
        &self,
        database: &str,
        source: &SlateDbStateStore,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::materialize_sqlite_database_live_snapshot(
            self, database, source,
        )
        .await
    }

    pub async fn materialize_sqlite_database_live_snapshot_delta(
        &self,
        database: &str,
        delta: &SqliteDatabasePageSyncDelta,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::materialize_sqlite_database_live_snapshot_delta(
            self, database, delta,
        )
        .await
    }

    pub async fn export_sqlite_database_file_snapshot(
        &self,
        database: &str,
        target_chunk_bytes: usize,
    ) -> anyhow::Result<SqliteDatabaseFileSnapshot> {
        crate::slatedb_sqlite_store::export_sqlite_database_file_snapshot(
            self,
            database,
            target_chunk_bytes,
        )
        .await
    }

    pub async fn materialize_sqlite_database_file_snapshot(
        &self,
        database: &str,
        snapshot: &SqliteDatabaseFileSnapshot,
    ) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
        crate::slatedb_sqlite_store::materialize_sqlite_database_file_snapshot(
            self, database, snapshot,
        )
        .await
    }

    pub async fn mark_sqlite_database_ready(&self, database: &str) -> anyhow::Result<()> {
        validate_sqlite_database_name(database)?;
        DbWriteOps::put(
            self.db.as_ref(),
            &sqlite_database_ready_key(database),
            database.as_bytes(),
        )
        .await?;
        Ok(())
    }

    pub async fn sqlite_database_ready(&self, database: &str) -> anyhow::Result<bool> {
        validate_sqlite_database_name(database)?;
        Ok(
            DbReadOps::get(self.db.as_ref(), &sqlite_database_ready_key(database))
                .await?
                .is_some(),
        )
    }

    pub async fn list_sqlite_databases(&self) -> anyhow::Result<Vec<String>> {
        let mut names = Vec::new();
        let mut iter = DbReadOps::scan_prefix(self.db.as_ref(), SQLITE_DATABASE_PREFIX).await?;
        while let Some(key_value) = iter.next().await? {
            let name = std::str::from_utf8(key_value.value.as_ref())
                .map_err(|_| anyhow::anyhow!("invalid sqlite database marker value"))?
                .to_string();
            validate_sqlite_database_name(&name)?;
            names.push(name);
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub fn sqlite_database_path(&self, database: &str) -> String {
        format!(
            "{}__sqlite/databases/{}/state",
            self.path,
            sanitize_path_segment(database)
        )
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub async fn applied_index(&self) -> anyhow::Result<u64> {
        let Some(bytes) = self.db.get(APPLIED_INDEX_KEY).await? else {
            return Ok(0);
        };
        let array: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid applied index watermark"))?;
        Ok(u64::from_be_bytes(array))
    }

    pub async fn applied_commit_timestamp(&self) -> anyhow::Result<Option<HybridTimestamp>> {
        self.db
            .get(APPLIED_COMMIT_TS_KEY)
            .await?
            .map(|bytes| crate::codec::from_bytes(&bytes).map_err(Into::into))
            .transpose()
    }
}

fn sqlite_database_marker_key(database: &str) -> Vec<u8> {
    let mut key = SQLITE_DATABASE_PREFIX.to_vec();
    key.extend_from_slice(sanitize_path_segment(database).as_bytes());
    key
}

fn sqlite_database_ready_key(database: &str) -> Vec<u8> {
    let mut key = SQLITE_DATABASE_READY_PREFIX.to_vec();
    key.extend_from_slice(sanitize_path_segment(database).as_bytes());
    key
}

fn validate_sqlite_database_name(database: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !database.is_empty(),
        "sqlite database name must not be empty"
    );
    anyhow::ensure!(
        database
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "sqlite database name contains unsupported characters"
    );
    Ok(())
}

pub(crate) fn sanitize_path_segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                byte as char
            } else {
                '_'
            }
        })
        .collect()
}
