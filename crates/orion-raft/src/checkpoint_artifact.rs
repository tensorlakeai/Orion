use std::sync::Arc;

use anyhow::Context;
use futures_util::TryStreamExt;
use futures_util::future::try_join_all;
use serde::{Deserialize, Serialize};
use slatedb::DbWriteOps;
use slatedb::VersionedManifest;
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::manifest::SsTableId;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};

use crate::state::SlateDbStateStore;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlateDbCheckpointArtifact {
    pub db_path: String,
    pub checkpoint_id: String,
    pub checkpoint_manifest_id: u64,
    pub object_prefix: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlateDbCheckpointMaterializeMetrics {
    pub objects_seen: usize,
    pub objects_copied: usize,
    pub objects_reused: usize,
    pub bytes_seen: u64,
    pub bytes_copied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlateDbCheckpointObjectRef {
    pub path: String,
    pub size: u64,
}

impl SlateDbCheckpointArtifact {
    pub fn object_prefix_path(&self) -> Path {
        Path::from(self.object_prefix.clone())
    }
}

pub async fn create_slate_db_checkpoint_artifact(
    state: &SlateDbStateStore,
    name: impl Into<String>,
) -> anyhow::Result<SlateDbCheckpointArtifact> {
    DbWriteOps::flush(state.db.as_ref())
        .await
        .context("flushing SlateDB before checkpoint artifact")?;
    let checkpoint = state
        .db
        .create_checkpoint(
            CheckpointScope::All,
            &CheckpointOptions {
                name: Some(name.into()),
                ..CheckpointOptions::default()
            },
        )
        .await?;
    Ok(SlateDbCheckpointArtifact {
        db_path: state.path.clone(),
        checkpoint_id: checkpoint.id.to_string(),
        checkpoint_manifest_id: checkpoint.manifest_id,
        object_prefix: state.path.clone(),
    })
}

pub async fn materialize_slate_db_checkpoint_incremental(
    artifact: &SlateDbCheckpointArtifact,
    source_object_store: Arc<dyn ObjectStore>,
    target_path: impl Into<String>,
    target_object_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<SlateDbCheckpointMaterializeMetrics> {
    let target_path = target_path.into();
    let metrics = copy_missing_objects_for_checkpoint(
        artifact,
        source_object_store,
        Arc::clone(&target_object_store),
    )
    .await?;
    clone_materialized_checkpoint(artifact, &target_path, target_object_store).await?;
    Ok(metrics)
}

pub async fn clone_slate_db_checkpoint_artifact_from_local_objects(
    artifact: &SlateDbCheckpointArtifact,
    target_path: impl Into<String>,
    target_object_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<()> {
    clone_materialized_checkpoint(artifact, &target_path.into(), target_object_store).await
}

pub async fn list_slate_db_checkpoint_objects(
    object_store: &Arc<dyn ObjectStore>,
    artifact: &SlateDbCheckpointArtifact,
) -> anyhow::Result<Vec<SlateDbCheckpointObjectRef>> {
    let manifest = read_checkpoint_manifest(object_store, artifact).await?;
    let mut paths = checkpoint_manifest_required_paths(artifact, &manifest);
    paths.sort();
    paths.dedup();

    let mut objects = try_join_all(paths.into_iter().map(|location| async move {
        let meta = object_store
            .head(&location)
            .await
            .with_context(|| format!("reading checkpoint object metadata {location}"))?;
        Ok::<_, anyhow::Error>(SlateDbCheckpointObjectRef {
            path: location.to_string(),
            size: meta.size,
        })
    }))
    .await?;
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    anyhow::ensure!(
        !objects.is_empty(),
        "checkpoint artifact {} has no referenced objects",
        artifact.checkpoint_id
    );
    Ok(objects)
}

pub fn ensure_checkpoint_object_path_allowed(
    artifact: &SlateDbCheckpointArtifact,
    object_path: &str,
) -> anyhow::Result<()> {
    ensure_checkpoint_object_path_has_prefix(&artifact.object_prefix, object_path)
}

pub fn ensure_checkpoint_object_path_has_prefix(
    prefix: &str,
    object_path: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        object_path == prefix || object_path.starts_with(&format!("{prefix}/")),
        "checkpoint object path {object_path} is outside expected prefix {prefix}"
    );
    Ok(())
}

async fn copy_missing_objects_for_checkpoint(
    artifact: &SlateDbCheckpointArtifact,
    source_object_store: Arc<dyn ObjectStore>,
    target_object_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<SlateDbCheckpointMaterializeMetrics> {
    let objects = list_slate_db_checkpoint_objects(&source_object_store, artifact).await?;
    let mut metrics = SlateDbCheckpointMaterializeMetrics::default();

    for object in objects {
        let location = Path::from(object.path);
        metrics.objects_seen += 1;
        metrics.bytes_seen = metrics.bytes_seen.saturating_add(object.size);

        if target_has_same_object(target_object_store.as_ref(), &location, object.size).await? {
            metrics.objects_reused += 1;
            continue;
        }

        let bytes = source_object_store
            .get(&location)
            .await
            .with_context(|| format!("reading checkpoint object {location}"))?
            .bytes()
            .await
            .with_context(|| format!("reading checkpoint object bytes {location}"))?;
        target_object_store
            .put(&location, PutPayload::from(bytes))
            .await
            .with_context(|| format!("writing checkpoint object {location}"))?;
        metrics.objects_copied += 1;
        metrics.bytes_copied = metrics.bytes_copied.saturating_add(object.size);
    }

    anyhow::ensure!(
        metrics.objects_seen > 0,
        "checkpoint artifact {} has no objects to materialize",
        artifact.checkpoint_id
    );
    Ok(metrics)
}

async fn read_checkpoint_manifest(
    object_store: &Arc<dyn ObjectStore>,
    artifact: &SlateDbCheckpointArtifact,
) -> anyhow::Result<VersionedManifest> {
    AdminBuilder::new(artifact.db_path.clone(), Arc::clone(object_store))
        .build()
        .read_manifest(Some(artifact.checkpoint_manifest_id))
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .with_context(|| {
            format!(
                "checkpoint manifest {} missing for {}",
                artifact.checkpoint_manifest_id, artifact.checkpoint_id
            )
        })
}

fn checkpoint_manifest_required_paths(
    artifact: &SlateDbCheckpointArtifact,
    manifest: &VersionedManifest,
) -> Vec<Path> {
    let mut paths = vec![manifest_object_path(
        &artifact.object_prefix,
        artifact.checkpoint_manifest_id,
    )];

    for view in manifest.l0() {
        paths.push(sstable_object_path(&artifact.object_prefix, &view.sst.id));
    }
    for run in manifest.compacted() {
        for view in &run.sst_views {
            paths.push(sstable_object_path(&artifact.object_prefix, &view.sst.id));
        }
    }
    for segment in manifest.segments() {
        for view in segment.l0() {
            paths.push(sstable_object_path(&artifact.object_prefix, &view.sst.id));
        }
        for run in segment.compacted() {
            for view in &run.sst_views {
                paths.push(sstable_object_path(&artifact.object_prefix, &view.sst.id));
            }
        }
    }

    for wal_id in manifest.replay_after_wal_id().saturating_add(1)..manifest.next_wal_sst_id() {
        paths.push(sstable_object_path(
            &artifact.object_prefix,
            &SsTableId::Wal(wal_id),
        ));
    }

    paths
}

fn manifest_object_path(root_path: &str, manifest_id: u64) -> Path {
    Path::from(format!(
        "{}/manifest/{:020}.manifest",
        root_path.trim_end_matches('/'),
        manifest_id
    ))
}

fn sstable_object_path(root_path: &str, table_id: &SsTableId) -> Path {
    match table_id {
        SsTableId::Wal(id) => Path::from(format!(
            "{}/wal/{:020}.sst",
            root_path.trim_end_matches('/'),
            id
        )),
        SsTableId::Compacted(ulid) => Path::from(format!(
            "{}/compacted/{}.sst",
            root_path.trim_end_matches('/'),
            ulid
        )),
    }
}

async fn target_has_same_object(
    target_object_store: &dyn ObjectStore,
    location: &Path,
    expected_size: u64,
) -> anyhow::Result<bool> {
    match target_object_store.head(location).await {
        Ok(meta) => Ok(meta.size == expected_size),
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn clone_materialized_checkpoint(
    artifact: &SlateDbCheckpointArtifact,
    target_path: &str,
    target_object_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        target_path != artifact.db_path,
        "checkpoint clone target path {target_path} matches source path {}",
        artifact.db_path
    );
    let checkpoint_id = uuid::Uuid::parse_str(&artifact.checkpoint_id)?;
    delete_objects_under_path(Arc::clone(&target_object_store), target_path)
        .await
        .with_context(|| format!("clearing existing checkpoint clone target {target_path}"))?;
    AdminBuilder::new(target_path.to_string(), target_object_store)
        .build()
        .create_clone_builder(artifact.db_path.clone(), Some(checkpoint_id))
        .build()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
        .with_context(|| {
            format!(
                "cloning SlateDB checkpoint {} from {} into {}",
                artifact.checkpoint_id, artifact.db_path, target_path
            )
        })?;
    Ok(())
}

async fn delete_objects_under_path(
    object_store: Arc<dyn ObjectStore>,
    target_path: &str,
) -> anyhow::Result<()> {
    let target_prefix = Path::from(target_path.to_string());
    let target_child_prefix = format!("{target_path}/");
    let mut stream = object_store.list(Some(&target_prefix));
    let mut locations = Vec::new();
    while let Some(meta) = stream.try_next().await? {
        let location = meta.location.to_string();
        if location == target_path || location.starts_with(&target_child_prefix) {
            locations.push(meta.location);
        }
    }
    for location in locations {
        object_store
            .delete(&location)
            .await
            .with_context(|| format!("deleting existing checkpoint clone object {location}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::DbReadOps;
    use slatedb::object_store::{local::LocalFileSystem, memory::InMemory};

    #[tokio::test]
    async fn materialize_checkpoint_copies_only_missing_objects_between_stores()
    -> anyhow::Result<()> {
        let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source =
            SlateDbStateStore::open("checkpoint-source", Arc::clone(&source_store)).await?;

        source.db.put(b"key-a", b"value-a").await?;
        source.db.flush().await?;
        let artifact = create_slate_db_checkpoint_artifact(&source, "first-checkpoint").await?;

        let first = materialize_slate_db_checkpoint_incremental(
            &artifact,
            Arc::clone(&source_store),
            "checkpoint-target",
            Arc::clone(&target_store),
        )
        .await?;
        assert!(first.objects_copied > 0);
        assert_eq!(first.objects_reused, 0);

        let target = slatedb::Db::open("checkpoint-target", Arc::clone(&target_store)).await?;
        assert_eq!(
            DbReadOps::get(&target, b"key-a").await?.as_deref(),
            Some(&b"value-a"[..])
        );
        target.close().await?;

        source.db.put(b"key-b", b"value-b").await?;
        source.db.flush().await?;
        let next_artifact =
            create_slate_db_checkpoint_artifact(&source, "second-checkpoint").await?;
        let second = materialize_slate_db_checkpoint_incremental(
            &next_artifact,
            Arc::clone(&source_store),
            "checkpoint-target-next",
            Arc::clone(&target_store),
        )
        .await?;
        assert!(second.objects_seen >= second.objects_reused);
        assert!(second.objects_reused > 0);
        assert!(second.objects_copied > 0);

        let target_next =
            slatedb::Db::open("checkpoint-target-next", Arc::clone(&target_store)).await?;
        assert_eq!(
            DbReadOps::get(&target_next, b"key-a").await?.as_deref(),
            Some(&b"value-a"[..])
        );
        assert_eq!(
            DbReadOps::get(&target_next, b"key-b").await?.as_deref(),
            Some(&b"value-b"[..])
        );
        target_next.close().await?;
        source.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_database_checkpoint_materialization_marks_child_ready() -> anyhow::Result<()> {
        let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source =
            SlateDbStateStore::open("sqlite-checkpoint-source", Arc::clone(&source_store)).await?;
        let source_child = source.sqlite_database_state("tenant-a").await?;
        source_child.db.put(b"sqlite/page", b"page-bytes").await?;
        source_child.db.flush().await?;

        let artifact = source
            .sqlite_database_checkpoint_artifact("tenant-a", "tenant-a-checkpoint")
            .await?;
        let target =
            SlateDbStateStore::open("sqlite-checkpoint-target", Arc::clone(&target_store)).await?;
        let metrics = target
            .materialize_sqlite_database_checkpoint_incremental(
                "tenant-a",
                &artifact,
                Arc::clone(&source_store),
            )
            .await?;
        assert!(metrics.objects_copied > 0);

        assert_eq!(target.list_sqlite_databases().await?, vec!["tenant-a"]);
        let target_child = target
            .existing_sqlite_database_state("tenant-a")
            .await?
            .unwrap();
        assert_eq!(
            DbReadOps::get(target_child.db.as_ref(), b"sqlite/page")
                .await?
                .as_deref(),
            Some(&b"page-bytes"[..])
        );

        source.close().await?;
        target.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn sqlite_database_checkpoint_clone_from_fetched_local_filesystem_objects()
    -> anyhow::Result<()> {
        let source_dir = tempfile::TempDir::new()?;
        let target_dir = tempfile::TempDir::new()?;
        let source_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(source_dir.path())?);
        let target_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(target_dir.path())?);
        let source =
            SlateDbStateStore::open("node-source/state", Arc::clone(&source_store)).await?;
        let source_child = source.sqlite_database_state("tenant-a").await?;
        source_child.db.put(b"sqlite/page", b"page-bytes").await?;
        source_child.db.flush().await?;

        let artifact = source
            .sqlite_database_checkpoint_artifact("tenant-a", "tenant-a-checkpoint")
            .await?;
        let objects = list_slate_db_checkpoint_objects(&source_store, &artifact).await?;
        for object in objects {
            let location = Path::from(object.path);
            let bytes = source_store.get(&location).await?.bytes().await?;
            target_store.put(&location, PutPayload::from(bytes)).await?;
        }

        let target =
            SlateDbStateStore::open("node-target/state", Arc::clone(&target_store)).await?;
        let stale_child = target.sqlite_database_state("tenant-a").await?;
        stale_child.db.put(b"sqlite/page", b"stale-page").await?;
        stale_child.db.flush().await?;
        target
            .clone_sqlite_database_checkpoint_from_local_objects("tenant-a", &artifact)
            .await?;

        assert_eq!(target.list_sqlite_databases().await?, vec!["tenant-a"]);
        let target_child = target
            .existing_sqlite_database_state("tenant-a")
            .await?
            .unwrap();
        assert_eq!(
            DbReadOps::get(target_child.db.as_ref(), b"sqlite/page")
                .await?
                .as_deref(),
            Some(&b"page-bytes"[..])
        );

        source.close().await?;
        target.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_artifact_flushes_pending_writes() -> anyhow::Result<()> {
        let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source =
            SlateDbStateStore::open("checkpoint-unflushed-source", Arc::clone(&source_store))
                .await?;

        source.db.put(b"unflushed-key", b"unflushed-value").await?;
        let artifact = create_slate_db_checkpoint_artifact(&source, "unflushed-checkpoint").await?;
        materialize_slate_db_checkpoint_incremental(
            &artifact,
            Arc::clone(&source_store),
            "checkpoint-unflushed-target",
            Arc::clone(&target_store),
        )
        .await?;

        let target =
            slatedb::Db::open("checkpoint-unflushed-target", Arc::clone(&target_store)).await?;
        assert_eq!(
            DbReadOps::get(&target, b"unflushed-key").await?.as_deref(),
            Some(&b"unflushed-value"[..])
        );
        target.close().await?;

        Ok(())
    }
}
