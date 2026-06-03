use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::ensure;
use futures_util::{Stream, StreamExt};
use orion_sqlite::{FileKind, SqliteFileStore, VfsFileOp, VfsSyncBatch, VfsWrite};
use serde::{Deserialize, Serialize};
use slatedb::config::WriteOptions as SlateWriteOptions;
use slatedb::{DbReadOps, DbWriteOps, WriteBatch};
use tokio::runtime::Handle;

use crate::state::SlateDbStateStore;

const SQLITE_PAGE_PREFIX: &[u8] = b"sqlite/pages/";
const SQLITE_PAGE_SIZE: usize = 16 * 1024;
const SQLITE_READ_PAGE_CACHE_CAPACITY: usize = 256;
const DEFAULT_DATABASE_PURGE_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_DATABASE_PURGE_MAX_KEYS_PER_PASS: usize = 10_000;
static LOCAL_MANIFEST_VERSION: AtomicU64 = AtomicU64::new(1_000_000_000_000);

#[derive(Debug, Clone)]
pub struct SqlitePageCompactionPolicy {
    pub obsolete_versions_per_file: usize,
    pub obsolete_version_ratio: f64,
    pub obsolete_bytes_per_file: u64,
    pub max_versions_per_pass: usize,
    pub max_bytes_per_pass: u64,
    pub retain_recent_versions: usize,
    pub min_retained_version: Option<u64>,
}

impl Default for SqlitePageCompactionPolicy {
    fn default() -> Self {
        Self {
            obsolete_versions_per_file: 512,
            obsolete_version_ratio: 2.0,
            obsolete_bytes_per_file: 32 * 1024 * 1024,
            max_versions_per_pass: 10_000,
            max_bytes_per_pass: 64 * 1024 * 1024,
            retain_recent_versions: 2,
            min_retained_version: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlitePageCompactionMetrics {
    pub files_scanned: usize,
    pub files_compacted: usize,
    pub versions_scanned: usize,
    pub obsolete_versions: usize,
    pub deleted_versions: usize,
    pub highest_deleted_version: Option<u64>,
    pub bytes_scanned: u64,
    pub obsolete_bytes: u64,
    pub deleted_bytes: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteStoragePressureMetrics {
    pub databases: usize,
    pub files: usize,
    pub current_pages: usize,
    pub obsolete_page_versions: usize,
    pub obsolete_versions: usize,
    pub bytes_scanned: u64,
    pub obsolete_bytes: u64,
    pub compaction_eligible_files: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabasePageSyncMetrics {
    pub keys_scanned: usize,
    pub keys_copied: usize,
    pub keys_deleted: usize,
    pub pages_copied: usize,
    pub metadata_copied: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabasePageSyncDelta {
    pub min_exclusive_version: u64,
    pub entries: Vec<SqliteDatabasePageSyncEntry>,
    pub metadata_deletes: Vec<Vec<u8>>,
    pub current_page_deletes: Vec<SqliteCurrentPageDeleteRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabasePageSyncEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabaseFileSnapshot {
    pub files: Vec<SqliteDatabaseFileSnapshotFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabaseFileSnapshotFile {
    pub path: String,
    pub kind: FileKind,
    pub size: u64,
    pub chunks: Vec<SqliteDatabaseFileSnapshotChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabaseFileSnapshotChunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SqliteCurrentPageDeleteRange {
    pub current_pages_prefix: Vec<u8>,
    pub first_page: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteDatabasePurgeMetrics {
    pub database: String,
    pub keys_scanned: usize,
    pub keys_deleted: usize,
    pub bytes_deleted: u64,
    pub retention_elapsed_ms: Option<u64>,
    pub skipped_for_retention: bool,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteDatabasePurgePolicy {
    pub retention_ms: u64,
    pub max_keys_per_pass: usize,
}

impl Default for SqliteDatabasePurgePolicy {
    fn default() -> Self {
        Self {
            retention_ms: DEFAULT_DATABASE_PURGE_RETENTION_MS,
            max_keys_per_pass: DEFAULT_DATABASE_PURGE_MAX_KEYS_PER_PASS,
        }
    }
}

#[derive(Clone)]
pub struct SlateDbSqliteFileStore {
    db: Arc<slatedb::Db>,
    database: String,
    read_page_cache: Arc<Mutex<ReadPageCache>>,
    #[cfg(test)]
    read_page_count: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct ReadPageCache {
    entries: BTreeMap<(String, u64, Option<u64>), Arc<Vec<u8>>>,
    order: VecDeque<(String, u64, Option<u64>)>,
}

impl ReadPageCache {
    fn get(&self, path: &str, page_index: u64, version: Option<u64>) -> Option<Arc<Vec<u8>>> {
        self.entries
            .get(&(path.to_string(), page_index, version))
            .cloned()
    }

    fn insert(
        &mut self,
        path: &str,
        page_index: u64,
        version: Option<u64>,
        bytes: Vec<u8>,
    ) -> Arc<Vec<u8>> {
        let key = (path.to_string(), page_index, version);
        if !self.entries.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        let bytes = Arc::new(bytes);
        self.entries.insert(key, Arc::clone(&bytes));
        while self.entries.len() > SQLITE_READ_PAGE_CACHE_CAPACITY {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
        bytes
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

#[derive(Debug, Clone, Default)]
struct DirtyPage {
    overlay: Vec<u8>,
    covered: Vec<std::ops::Range<usize>>,
    base_zeroed: bool,
}

enum DirtyPageMaterialization {
    Complete(Vec<u8>),
    Overlay {
        overlay: Vec<u8>,
        covered: Vec<std::ops::Range<usize>>,
    },
}

#[derive(Debug, Clone, Default)]
struct FileCompactionState {
    current_pages: usize,
}

impl DirtyPage {
    fn from_existing(bytes: Vec<u8>) -> Self {
        Self {
            overlay: bytes.clone(),
            covered: if bytes.is_empty() {
                Vec::new()
            } else {
                std::iter::once(0..bytes.len()).collect()
            },
            base_zeroed: true,
        }
    }

    fn mark_base_zeroed(&mut self) {
        self.base_zeroed = true;
    }

    fn apply_write(&mut self, offset: usize, bytes: &[u8]) {
        let end = offset + bytes.len();
        if self.overlay.len() < end {
            self.overlay.resize(end, 0);
        }
        self.overlay[offset..end].copy_from_slice(bytes);
        self.covered.push(offset..end);
        normalize_ranges(&mut self.covered);
    }

    fn truncate(&mut self, len: usize) {
        self.overlay.truncate(len);
        self.covered = self
            .covered
            .drain(..)
            .filter_map(|range| {
                let end = range.end.min(len);
                (range.start < end).then_some(range.start..end)
            })
            .collect();
        self.base_zeroed = true;
        normalize_ranges(&mut self.covered);
    }

    fn into_page(self) -> DirtyPageMaterialization {
        let fully_materialized =
            self.base_zeroed || covers_range(&self.covered, 0..SQLITE_PAGE_SIZE);
        if fully_materialized {
            DirtyPageMaterialization::Complete(self.overlay)
        } else {
            DirtyPageMaterialization::Overlay {
                overlay: self.overlay,
                covered: self.covered,
            }
        }
    }
}

fn normalize_ranges(ranges: &mut Vec<std::ops::Range<usize>>) {
    ranges.retain(|range| range.start < range.end);
    ranges.sort_by_key(|range| range.start);
    let mut normalized: Vec<std::ops::Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(last) = normalized.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
            continue;
        }
        normalized.push(range);
    }
    *ranges = normalized;
}

fn covers_range(ranges: &[std::ops::Range<usize>], target: std::ops::Range<usize>) -> bool {
    let mut cursor = target.start;
    for range in ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start > cursor {
            return false;
        }
        cursor = cursor.max(range.end);
        if cursor >= target.end {
            return true;
        }
    }
    cursor >= target.end
}

impl SlateDbSqliteFileStore {
    pub fn new(state: &SlateDbStateStore, database: impl Into<String>) -> Self {
        Self {
            db: state.db.clone(),
            database: database.into(),
            read_page_cache: Arc::new(Mutex::new(ReadPageCache::default())),
            #[cfg(test)]
            read_page_count: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) async fn read_file(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let size = self.read_size(path).await?;
        let mut bytes = vec![0; size];
        self.read_at_async(path, 0, &mut bytes).await?;
        Ok(bytes)
    }

    async fn exists_async(&self, path: &str) -> anyhow::Result<bool> {
        Ok(
            DbReadOps::get(self.db.as_ref(), &size_key(&self.database, path))
                .await?
                .is_some(),
        )
    }

    async fn read_size(&self, path: &str) -> anyhow::Result<usize> {
        let Some(bytes) = DbReadOps::get(self.db.as_ref(), &size_key(&self.database, path)).await?
        else {
            return Ok(0);
        };
        let bytes: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid SQLite file size metadata"))?;
        Ok(u64::from_be_bytes(bytes) as usize)
    }

    async fn read_latest_manifest_version(&self, path: &str) -> anyhow::Result<Option<u64>> {
        DbReadOps::get(self.db.as_ref(), &latest_manifest_key(&self.database, path))
            .await?
            .map(|bytes| u64_from_be_bytes(bytes.as_ref(), "latest manifest version"))
            .transpose()
    }

    async fn read_page(&self, path: &str, page_index: u64) -> anyhow::Result<Vec<u8>> {
        Ok(self
            .read_page_cached(path, page_index)
            .await?
            .as_ref()
            .clone())
    }

    async fn read_page_cached(&self, path: &str, page_index: u64) -> anyhow::Result<Arc<Vec<u8>>> {
        let version = self.read_latest_manifest_version(path).await?;
        self.read_page_cached_at_version(path, page_index, version)
            .await
    }

    async fn read_page_cached_at_version(
        &self,
        path: &str,
        page_index: u64,
        version: Option<u64>,
    ) -> anyhow::Result<Arc<Vec<u8>>> {
        if let Some(bytes) = self
            .read_page_cache
            .lock()
            .expect("SQLite read page cache mutex poisoned")
            .get(path, page_index, version)
        {
            return Ok(bytes);
        }

        #[cfg(test)]
        self.read_page_count.fetch_add(1, Ordering::Relaxed);

        let bytes = DbReadOps::get(
            self.db.as_ref(),
            &current_page_key(&self.database, path, page_index),
        )
        .await?
        .map(|bytes| bytes.to_vec())
        .unwrap_or_default();
        let bytes = self
            .read_page_cache
            .lock()
            .expect("SQLite read page cache mutex poisoned")
            .insert(path, page_index, version, bytes);
        Ok(bytes)
    }

    async fn read_at_async(
        &self,
        path: &str,
        offset: u64,
        data: &mut [u8],
    ) -> anyhow::Result<usize> {
        let size = self.read_size(path).await?;
        let offset = offset as usize;
        if offset >= size {
            return Ok(0);
        }
        let version = self.read_latest_manifest_version(path).await?;
        let len = data.len().min(size - offset);
        let mut copied = 0;
        while copied < len {
            let absolute = offset + copied;
            let page_index = (absolute / SQLITE_PAGE_SIZE) as u64;
            let page_offset = absolute % SQLITE_PAGE_SIZE;
            let page = self
                .read_page_cached_at_version(path, page_index, version)
                .await?;
            let available = page.len().saturating_sub(page_offset);
            let to_copy = (len - copied).min(SQLITE_PAGE_SIZE - page_offset);
            if available > 0 {
                let present = to_copy.min(available);
                data[copied..copied + present]
                    .copy_from_slice(&page[page_offset..page_offset + present]);
                if present < to_copy {
                    data[copied + present..copied + to_copy].fill(0);
                }
            } else {
                data[copied..copied + to_copy].fill(0);
            }
            copied += to_copy;
        }
        Ok(len)
    }

    async fn delete_async(&self, path: &str) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(size_key(&self.database, path));
        self.delete_current_pages_from(&mut batch, path, 0).await?;
        DbWriteOps::write_with_options(self.db.as_ref(), batch, &non_durable_write_options())
            .await?;
        self.clear_read_page_cache();
        Ok(())
    }

    async fn truncate_async(&self, path: &str, size: usize) -> anyhow::Result<()> {
        let old_size = self.read_size(path).await?;
        let mut batch = WriteBatch::new();
        batch.put(size_key(&self.database, path), (size as u64).to_be_bytes());

        if size < old_size {
            let keep_pages = size.div_ceil(SQLITE_PAGE_SIZE);
            if !size.is_multiple_of(SQLITE_PAGE_SIZE) {
                let last_index = keep_pages.saturating_sub(1) as u64;
                let mut page = self.read_page(path, last_index).await?;
                page.truncate(size % SQLITE_PAGE_SIZE);
                self.write_current_page(&mut batch, path, last_index, page);
            }
            self.delete_current_pages_from(&mut batch, path, keep_pages as u64)
                .await?;
        }

        DbWriteOps::write_with_options(self.db.as_ref(), batch, &non_durable_write_options())
            .await?;
        self.clear_read_page_cache();
        Ok(())
    }

    async fn apply_batch_async(&self, batch: &VfsSyncBatch) -> anyhow::Result<()> {
        self.apply_batch_async_at_version(batch, next_local_manifest_version())
            .await
    }

    pub(crate) async fn apply_batch_async_at_version(
        &self,
        batch: &VfsSyncBatch,
        version: u64,
    ) -> anyhow::Result<()> {
        self.apply_op_chunks_async_at_version(
            &batch.file_path,
            batch.file_kind,
            futures_util::stream::iter([Ok(batch.ops.clone())]),
            version,
        )
        .await
    }

    pub(crate) async fn apply_op_chunks_async_at_version<S>(
        &self,
        file_path: &str,
        _file_kind: orion_sqlite::FileKind,
        chunks: S,
        version: u64,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<Vec<VfsFileOp>>>,
    {
        let version = if version == 0 {
            next_local_manifest_version()
        } else {
            version
        };
        let mut file_exists = self.exists_async(file_path).await?;
        let mut file_size = self.read_size(file_path).await?;
        let mut slate_batch = WriteBatch::new();
        let mut dirty_pages: BTreeMap<u64, DirtyPage> = BTreeMap::new();
        let mut deleted_or_truncated_from_page = None;

        futures_util::pin_mut!(chunks);
        while let Some(ops) = chunks.next().await {
            let ops = ops?;
            for op in &ops {
                match op {
                    VfsFileOp::Delete => {
                        slate_batch.delete(size_key(&self.database, file_path));
                        self.delete_current_pages_from(&mut slate_batch, file_path, 0)
                            .await?;
                        self.write_manifest(
                            &mut slate_batch,
                            file_path,
                            version,
                            FileManifestKind::Delete,
                            0,
                        );
                        dirty_pages.clear();
                        file_exists = false;
                        file_size = 0;
                        deleted_or_truncated_from_page = Some(0);
                    }
                    VfsFileOp::Truncate { size } => {
                        let size = *size as usize;
                        file_exists = true;
                        if size < file_size {
                            let keep_pages = size.div_ceil(SQLITE_PAGE_SIZE);
                            deleted_or_truncated_from_page = Some(keep_pages as u64);
                            self.delete_current_pages_from(
                                &mut slate_batch,
                                file_path,
                                keep_pages as u64,
                            )
                            .await?;
                            dirty_pages.retain(|page_index, _| *page_index < keep_pages as u64);
                            if keep_pages > 0 && !size.is_multiple_of(SQLITE_PAGE_SIZE) {
                                let last_index = keep_pages.saturating_sub(1) as u64;
                                let mut page = match dirty_pages.remove(&last_index) {
                                    Some(page) => page,
                                    None => DirtyPage::from_existing(
                                        self.read_page(file_path, last_index).await?,
                                    ),
                                };
                                page.truncate(size % SQLITE_PAGE_SIZE);
                                dirty_pages.insert(last_index, page);
                            }
                        }
                        file_size = size;
                        self.write_manifest(
                            &mut slate_batch,
                            file_path,
                            version,
                            FileManifestKind::Truncate,
                            file_size as u64,
                        );
                    }
                    VfsFileOp::Write(write) => {
                        file_exists = true;
                        let mut remaining = write.bytes.as_slice();
                        let mut offset = write.offset as usize;
                        let file_size_before_write = file_size;
                        while !remaining.is_empty() {
                            let page_index = (offset / SQLITE_PAGE_SIZE) as u64;
                            let page_offset = offset % SQLITE_PAGE_SIZE;
                            let to_write = remaining.len().min(SQLITE_PAGE_SIZE - page_offset);
                            let page = dirty_pages.entry(page_index).or_default();
                            if deleted_or_truncated_from_page
                                .is_some_and(|first_deleted| page_index >= first_deleted)
                            {
                                page.mark_base_zeroed();
                            }
                            if page_index as usize * SQLITE_PAGE_SIZE >= file_size_before_write {
                                page.mark_base_zeroed();
                            }
                            page.apply_write(page_offset, &remaining[..to_write]);
                            offset += to_write;
                            remaining = &remaining[to_write..];
                        }
                        file_size = file_size.max(write.offset as usize + write.bytes.len());
                    }
                }
            }
        }

        for (page_index, dirty_page) in dirty_pages {
            let page = match dirty_page.into_page() {
                DirtyPageMaterialization::Complete(page) => page,
                DirtyPageMaterialization::Overlay {
                    mut overlay,
                    covered,
                } => {
                    let mut page = if deleted_or_truncated_from_page
                        .is_some_and(|first_deleted| page_index >= first_deleted)
                    {
                        Vec::new()
                    } else {
                        self.read_page(file_path, page_index).await?
                    };
                    if page.len() < overlay.len() {
                        page.resize(overlay.len(), 0);
                    }
                    for range in covered {
                        if overlay.len() < range.end {
                            overlay.resize(range.end, 0);
                        }
                        page[range.clone()].copy_from_slice(&overlay[range]);
                    }
                    page
                }
            };
            self.write_current_page(&mut slate_batch, file_path, page_index, page);
        }

        if file_exists {
            slate_batch.put(
                size_key(&self.database, file_path),
                (file_size as u64).to_be_bytes(),
            );
            self.write_manifest(
                &mut slate_batch,
                file_path,
                version,
                FileManifestKind::Write,
                file_size as u64,
            );
        }
        DbWriteOps::write_with_options(self.db.as_ref(), slate_batch, &non_durable_write_options())
            .await?;
        self.clear_read_page_cache();
        Ok(())
    }

    fn write_current_page(
        &self,
        batch: &mut WriteBatch,
        path: &str,
        page_index: u64,
        bytes: Vec<u8>,
    ) {
        batch.put(current_page_key(&self.database, path, page_index), bytes);
    }

    fn clear_read_page_cache(&self) {
        self.read_page_cache
            .lock()
            .expect("SQLite read page cache mutex poisoned")
            .clear();
    }

    fn write_manifest(
        &self,
        batch: &mut WriteBatch,
        path: &str,
        version: u64,
        kind: FileManifestKind,
        size: u64,
    ) {
        if version == 0 {
            return;
        }
        let manifest = FileManifest {
            version,
            kind,
            size,
        };
        if let Ok(bytes) = crate::codec::to_vec(&manifest) {
            batch.put(file_manifest_key(&self.database, path, version), bytes);
            batch.put(
                latest_manifest_key(&self.database, path),
                version.to_be_bytes(),
            );
        }
    }

    async fn delete_current_pages_from(
        &self,
        batch: &mut WriteBatch,
        path: &str,
        first_page: u64,
    ) -> anyhow::Result<()> {
        let prefix = current_pages_prefix(&self.database, path);
        let mut iter = DbReadOps::scan_prefix(self.db.as_ref(), &prefix).await?;
        while let Some(key_value) = iter.next().await? {
            let Some(index) = page_index_from_key(&prefix, key_value.key.as_ref()) else {
                continue;
            };
            if index >= first_page {
                batch.delete(key_value.key.as_ref());
            }
        }
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        DbWriteOps::flush(self.db.as_ref()).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reset_read_page_count(&self) {
        self.read_page_count.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn read_page_count(&self) -> u64 {
        self.read_page_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    async fn compact_obsolete_page_versions(&self, path: &str) -> anyhow::Result<usize> {
        let _ = path;
        Ok(0)
    }
}

impl SqliteFileStore for SlateDbSqliteFileStore {
    fn exists(&self, path: &str) -> anyhow::Result<bool> {
        block_on_store(self.exists_async(path))
    }

    fn delete(&self, path: &str) -> anyhow::Result<()> {
        block_on_store(self.delete_async(path))
    }

    fn file_size(&self, path: &str) -> anyhow::Result<usize> {
        block_on_store(self.read_size(path))
    }

    fn truncate(&self, path: &str, size: usize) -> anyhow::Result<()> {
        block_on_store(self.truncate_async(path, size))
    }

    fn read_at(&self, path: &str, offset: u64, data: &mut [u8]) -> anyhow::Result<usize> {
        block_on_store(self.read_at_async(path, offset, data))
    }

    fn apply_batch(&self, batch: &VfsSyncBatch) -> anyhow::Result<()> {
        block_on_store(self.apply_batch_async(batch))
    }

    fn sync(&self, _path: &str) -> anyhow::Result<()> {
        block_on_store(self.flush())
    }
}

pub async fn apply_sqlite_batch_to_slate_db(
    state: &SlateDbStateStore,
    batch: &VfsSyncBatch,
) -> anyhow::Result<()> {
    apply_sqlite_batch_to_slate_db_at_version(state, batch, next_local_manifest_version()).await
}

pub async fn apply_sqlite_batch_to_slate_db_at_version(
    state: &SlateDbStateStore,
    batch: &VfsSyncBatch,
    version: u64,
) -> anyhow::Result<()> {
    let database_state = state.sqlite_database_state(&batch.database).await?;
    SlateDbSqliteFileStore::new(&database_state, batch.database.clone())
        .apply_batch_async_at_version(batch, version)
        .await
}

pub(crate) async fn apply_sqlite_op_chunks_to_slate_db_at_version<S>(
    state: &SlateDbStateStore,
    database: &str,
    file_path: &str,
    file_kind: orion_sqlite::FileKind,
    chunks: S,
    version: u64,
) -> anyhow::Result<()>
where
    S: Stream<Item = std::io::Result<Vec<VfsFileOp>>>,
{
    let database_state = state.sqlite_database_state(database).await?;
    SlateDbSqliteFileStore::new(&database_state, database)
        .apply_op_chunks_async_at_version(file_path, file_kind, chunks, version)
        .await
}

pub async fn sync_sqlite_database_pages_since(
    target: &SlateDbStateStore,
    database: &str,
    source: &SlateDbStateStore,
    min_exclusive_version: u64,
) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
    let delta = export_sqlite_database_pages_since(source, database, min_exclusive_version).await?;
    apply_sqlite_database_page_delta(target, database, &delta).await
}

pub async fn materialize_sqlite_database_live_snapshot(
    target: &SlateDbStateStore,
    database: &str,
    source: &SlateDbStateStore,
) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
    let delta = export_sqlite_database_live_snapshot(source, database).await?;
    materialize_sqlite_database_live_snapshot_delta(target, database, &delta).await
}

pub async fn materialize_sqlite_database_live_snapshot_delta(
    target: &SlateDbStateStore,
    database: &str,
    delta: &SqliteDatabasePageSyncDelta,
) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
    purge_sqlite_database_fully(target, database).await?;
    apply_sqlite_database_page_delta(target, database, &delta).await
}

pub async fn export_sqlite_database_file_snapshot(
    source: &SlateDbStateStore,
    database: &str,
    target_chunk_bytes: usize,
) -> anyhow::Result<SqliteDatabaseFileSnapshot> {
    let source_state = source
        .existing_sqlite_database_state(database)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source SQLite database {database} does not exist"))?;
    let target_chunk_bytes = target_chunk_bytes.max(SQLITE_PAGE_SIZE);
    let mut file_prefixes = BTreeSet::<Vec<u8>>::new();
    for path in sqlite_snapshot_candidate_paths(database) {
        let file_prefix = page_file_prefix(database, &path);
        if DbReadOps::get(
            source_state.db.as_ref(),
            &size_key_from_file_prefix(&file_prefix),
        )
        .await?
        .is_some()
        {
            file_prefixes.insert(file_prefix);
        }
    }
    if file_prefixes.is_empty() {
        let mut source_iter =
            DbReadOps::scan_prefix(source_state.db.as_ref(), SQLITE_PAGE_PREFIX).await?;
        while let Some(key_value) = source_iter.next().await? {
            if let Some((file_prefix, _)) =
                parse_latest_manifest_key(key_value.key.as_ref(), key_value.value.as_ref())?
            {
                file_prefixes.insert(file_prefix);
            }
        }
    }

    let mut files = Vec::new();
    for file_prefix in file_prefixes {
        let Some(path) = file_path_from_file_prefix(&file_prefix) else {
            continue;
        };
        let Some(size) = DbReadOps::get(
            source_state.db.as_ref(),
            &size_key_from_file_prefix(&file_prefix),
        )
        .await?
        else {
            continue;
        };
        let size = u64_from_be_bytes(size.as_ref(), "file size")?;
        let chunk_count = (size as usize).div_ceil(target_chunk_bytes);
        let mut chunks = (0..chunk_count)
            .map(|chunk_index| {
                let offset = (chunk_index * target_chunk_bytes) as u64;
                let len = (size - offset).min(target_chunk_bytes as u64) as usize;
                SqliteDatabaseFileSnapshotChunk {
                    offset,
                    bytes: vec![0; len],
                }
            })
            .collect::<Vec<_>>();
        let current_prefix = current_pages_prefix_from_file_prefix(&file_prefix);
        let mut current_iter =
            DbReadOps::scan_prefix(source_state.db.as_ref(), &current_prefix).await?;
        while let Some(key_value) = current_iter.next().await? {
            let Some((current_file_prefix, page_index)) =
                parse_current_page_key(key_value.key.as_ref())
            else {
                continue;
            };
            if current_file_prefix != file_prefix {
                continue;
            }
            let page_offset = page_index.saturating_mul(SQLITE_PAGE_SIZE as u64);
            if page_offset >= size {
                continue;
            }
            let mut remaining = key_value.value.as_ref();
            let mut absolute_offset = page_offset as usize;
            let end_offset = size as usize;
            while !remaining.is_empty() && absolute_offset < end_offset {
                let chunk_index = absolute_offset / target_chunk_bytes;
                let chunk_offset = absolute_offset % target_chunk_bytes;
                let chunk = &mut chunks[chunk_index];
                let writable = (chunk.bytes.len() - chunk_offset)
                    .min(remaining.len())
                    .min(end_offset - absolute_offset);
                chunk.bytes[chunk_offset..chunk_offset + writable]
                    .copy_from_slice(&remaining[..writable]);
                remaining = &remaining[writable..];
                absolute_offset += writable;
            }
        }
        files.push(SqliteDatabaseFileSnapshotFile {
            kind: sqlite_file_kind_from_path(&path),
            path,
            size,
            chunks,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(SqliteDatabaseFileSnapshot { files })
}

pub async fn materialize_sqlite_database_file_snapshot(
    target: &SlateDbStateStore,
    database: &str,
    snapshot: &SqliteDatabaseFileSnapshot,
) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
    purge_sqlite_database_fully(target, database).await?;
    let target_state = target.sqlite_database_state(database).await?;
    let target_store = SlateDbSqliteFileStore::new(&target_state, database);
    let mut metrics = SqliteDatabasePageSyncMetrics::default();
    for file in &snapshot.files {
        let mut ops = Vec::with_capacity(file.chunks.len().saturating_add(1));
        ops.push(VfsFileOp::Truncate { size: file.size });
        for chunk in &file.chunks {
            ops.push(VfsFileOp::Write(VfsWrite {
                offset: chunk.offset,
                bytes: chunk.bytes.clone(),
            }));
            metrics.keys_copied += 1;
            metrics.pages_copied += chunk.bytes.len().div_ceil(SQLITE_PAGE_SIZE);
        }
        target_store
            .apply_batch_async(&VfsSyncBatch {
                database: database.to_string(),
                file_path: file.path.clone(),
                file_kind: file.kind,
                ops,
            })
            .await?;
        metrics.metadata_copied += 1;
    }
    Ok(metrics)
}

pub async fn export_sqlite_database_live_snapshot(
    source: &SlateDbStateStore,
    database: &str,
) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
    let source_state = source
        .existing_sqlite_database_state(database)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source SQLite database {database} does not exist"))?;
    let mut entries = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut file_prefixes = BTreeSet::<Vec<u8>>::new();

    let mut source_iter =
        DbReadOps::scan_prefix(source_state.db.as_ref(), SQLITE_PAGE_PREFIX).await?;
    while let Some(key_value) = source_iter.next().await? {
        let key = key_value.key.as_ref();
        if let Some((file_prefix, _page_index)) = parse_current_page_key(key) {
            file_prefixes.insert(file_prefix.clone());
            entries.insert(key.to_vec(), key_value.value.as_ref().to_vec());
            continue;
        }

        if let Some((file_prefix, _)) = parse_latest_manifest_key(key, key_value.value.as_ref())? {
            file_prefixes.insert(file_prefix.clone());
            entries.insert(key.to_vec(), key_value.value.as_ref().to_vec());
            continue;
        }
    }

    for file_prefix in file_prefixes {
        let size_key = size_key_from_file_prefix(&file_prefix);
        if let Some(size) = DbReadOps::get(source_state.db.as_ref(), &size_key).await? {
            entries.insert(size_key, size.as_ref().to_vec());
        }
    }

    Ok(SqliteDatabasePageSyncDelta {
        min_exclusive_version: 0,
        entries: entries
            .into_iter()
            .map(|(key, value)| SqliteDatabasePageSyncEntry { key, value })
            .collect(),
        metadata_deletes: Vec::new(),
        current_page_deletes: Vec::new(),
    })
}

pub async fn export_sqlite_database_pages_since(
    source: &SlateDbStateStore,
    database: &str,
    min_exclusive_version: u64,
) -> anyhow::Result<SqliteDatabasePageSyncDelta> {
    let source_state = source
        .existing_sqlite_database_state(database)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source SQLite database {database} does not exist"))?;
    let mut entries = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut changed_file_prefixes = BTreeSet::<Vec<u8>>::new();
    let mut metadata_deletes = BTreeSet::<Vec<u8>>::new();
    let mut current_page_deletes = BTreeSet::<SqliteCurrentPageDeleteRange>::new();

    let mut source_iter =
        DbReadOps::scan_prefix(source_state.db.as_ref(), SQLITE_PAGE_PREFIX).await?;
    while let Some(key_value) = source_iter.next().await? {
        let key = key_value.key.as_ref();
        if let Some((file_prefix, _)) = parse_current_page_key(key) {
            changed_file_prefixes.insert(file_prefix);
            continue;
        }

        if let Some((file_prefix, version)) = parse_file_manifest_key(key) {
            if version > min_exclusive_version {
                changed_file_prefixes.insert(file_prefix.clone());
                entries.insert(key.to_vec(), key_value.value.as_ref().to_vec());
                let manifest: FileManifest = crate::codec::from_bytes(key_value.value.as_ref())?;
                match manifest.kind {
                    FileManifestKind::Write => {}
                    FileManifestKind::Truncate => {
                        current_page_deletes.insert(SqliteCurrentPageDeleteRange {
                            current_pages_prefix: current_pages_prefix_from_file_prefix(
                                &file_prefix,
                            ),
                            first_page: manifest.size.div_ceil(SQLITE_PAGE_SIZE as u64),
                        });
                    }
                    FileManifestKind::Delete => {
                        current_page_deletes.insert(SqliteCurrentPageDeleteRange {
                            current_pages_prefix: current_pages_prefix_from_file_prefix(
                                &file_prefix,
                            ),
                            first_page: 0,
                        });
                        metadata_deletes.insert(size_key_from_file_prefix(&file_prefix));
                    }
                }
            }
            continue;
        }

        if let Some((file_prefix, version)) =
            parse_latest_manifest_key(key, key_value.value.as_ref())?
        {
            if version > min_exclusive_version {
                changed_file_prefixes.insert(file_prefix);
                entries.insert(key.to_vec(), key_value.value.as_ref().to_vec());
            }
        }
    }

    for file_prefix in changed_file_prefixes {
        let size_key = size_key_from_file_prefix(&file_prefix);
        if let Some(size) = DbReadOps::get(source_state.db.as_ref(), &size_key).await? {
            let size = size.as_ref().to_vec();
            current_page_deletes.insert(SqliteCurrentPageDeleteRange {
                current_pages_prefix: current_pages_prefix_from_file_prefix(&file_prefix),
                first_page: u64_from_be_bytes(&size, "file size")?
                    .div_ceil(SQLITE_PAGE_SIZE as u64),
            });
            entries.insert(size_key, size);
            let current_prefix = current_pages_prefix_from_file_prefix(&file_prefix);
            let mut current_iter =
                DbReadOps::scan_prefix(source_state.db.as_ref(), &current_prefix).await?;
            while let Some(key_value) = current_iter.next().await? {
                entries.insert(
                    key_value.key.as_ref().to_vec(),
                    key_value.value.as_ref().to_vec(),
                );
            }
        } else {
            current_page_deletes.insert(SqliteCurrentPageDeleteRange {
                current_pages_prefix: current_pages_prefix_from_file_prefix(&file_prefix),
                first_page: 0,
            });
            metadata_deletes.insert(size_key);
        }
    }

    Ok(SqliteDatabasePageSyncDelta {
        min_exclusive_version,
        entries: entries
            .into_iter()
            .map(|(key, value)| SqliteDatabasePageSyncEntry { key, value })
            .collect(),
        metadata_deletes: metadata_deletes.into_iter().collect(),
        current_page_deletes: current_page_deletes.into_iter().collect(),
    })
}

pub async fn apply_sqlite_database_page_delta(
    target: &SlateDbStateStore,
    database: &str,
    delta: &SqliteDatabasePageSyncDelta,
) -> anyhow::Result<SqliteDatabasePageSyncMetrics> {
    let target_state = target.sqlite_database_state(database).await?;
    let mut metrics = SqliteDatabasePageSyncMetrics {
        keys_scanned: delta.entries.len(),
        ..SqliteDatabasePageSyncMetrics::default()
    };
    let mut batch = WriteBatch::new();

    for delete in &delta.metadata_deletes {
        batch.delete(delete);
        metrics.keys_deleted += 1;
    }

    for delete in &delta.current_page_deletes {
        let mut target_iter =
            DbReadOps::scan_prefix(target_state.db.as_ref(), &delete.current_pages_prefix).await?;
        while let Some(key_value) = target_iter.next().await? {
            let Some(index) =
                page_index_from_key(&delete.current_pages_prefix, key_value.key.as_ref())
            else {
                continue;
            };
            if index >= delete.first_page {
                batch.delete(key_value.key.as_ref());
                metrics.keys_deleted += 1;
            }
        }
    }

    for entry in &delta.entries {
        if parse_current_page_key(&entry.key).is_some() {
            metrics.pages_copied += 1;
        } else {
            metrics.metadata_copied += 1;
        }
        batch.put(&entry.key, &entry.value);
        metrics.keys_copied += 1;
    }

    if metrics.keys_copied > 0 || metrics.keys_deleted > 0 {
        DbWriteOps::write_with_options(
            target_state.db.as_ref(),
            batch,
            &non_durable_write_options(),
        )
        .await?;
    }
    Ok(metrics)
}

pub async fn compact_sqlite_page_versions(
    state: &SlateDbStateStore,
    policy: &SqlitePageCompactionPolicy,
) -> anyhow::Result<SqlitePageCompactionMetrics> {
    compact_sqlite_page_versions_excluding(state, policy, &[]).await
}

pub async fn compact_sqlite_page_versions_excluding(
    state: &SlateDbStateStore,
    policy: &SqlitePageCompactionPolicy,
    excluded_databases: &[&str],
) -> anyhow::Result<SqlitePageCompactionMetrics> {
    let databases = state.list_sqlite_databases().await?;
    if !databases.is_empty() {
        let mut total = SqlitePageCompactionMetrics::default();
        for database in databases {
            if excluded_databases.contains(&database.as_str()) {
                continue;
            }
            let database_state = state.sqlite_database_state(&database).await?;
            let metrics = compact_sqlite_page_versions_in_state(&database_state, policy).await?;
            total.files_scanned += metrics.files_scanned;
            total.files_compacted += metrics.files_compacted;
            total.versions_scanned += metrics.versions_scanned;
            total.obsolete_versions += metrics.obsolete_versions;
            total.deleted_versions += metrics.deleted_versions;
            total.highest_deleted_version = total
                .highest_deleted_version
                .max(metrics.highest_deleted_version);
            total.bytes_scanned += metrics.bytes_scanned;
            total.obsolete_bytes += metrics.obsolete_bytes;
            total.deleted_bytes += metrics.deleted_bytes;
            total.duration_ms += metrics.duration_ms;
        }
        return Ok(total);
    }
    compact_sqlite_page_versions_in_state(state, policy).await
}

async fn compact_sqlite_page_versions_in_state(
    state: &SlateDbStateStore,
    policy: &SqlitePageCompactionPolicy,
) -> anyhow::Result<SqlitePageCompactionMetrics> {
    let started = Instant::now();
    let _ = (state, policy);
    let mut metrics = SqlitePageCompactionMetrics::default();
    metrics.duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    Ok(metrics)
}

pub async fn sqlite_storage_pressure(
    state: &SlateDbStateStore,
    policy: &SqlitePageCompactionPolicy,
) -> anyhow::Result<SqliteStoragePressureMetrics> {
    let databases = state.list_sqlite_databases().await?;
    if !databases.is_empty() {
        let mut total = SqliteStoragePressureMetrics {
            databases: databases.len(),
            ..SqliteStoragePressureMetrics::default()
        };
        for database in databases {
            let database_state = state.sqlite_database_state(&database).await?;
            let metrics = sqlite_storage_pressure_in_state(&database_state, policy, 0).await?;
            total.files += metrics.files;
            total.current_pages += metrics.current_pages;
            total.obsolete_page_versions += metrics.obsolete_page_versions;
            total.obsolete_versions += metrics.obsolete_versions;
            total.bytes_scanned += metrics.bytes_scanned;
            total.obsolete_bytes += metrics.obsolete_bytes;
            total.compaction_eligible_files += metrics.compaction_eligible_files;
        }
        return Ok(total);
    }
    sqlite_storage_pressure_in_state(state, policy, 1).await
}

async fn sqlite_storage_pressure_in_state(
    state: &SlateDbStateStore,
    policy: &SqlitePageCompactionPolicy,
    database_count_when_non_empty: usize,
) -> anyhow::Result<SqliteStoragePressureMetrics> {
    let mut files = BTreeMap::<Vec<u8>, FileCompactionState>::new();
    let mut metrics = SqliteStoragePressureMetrics::default();

    let mut iter = DbReadOps::scan_prefix(state.db.as_ref(), SQLITE_PAGE_PREFIX).await?;
    while let Some(key_value) = iter.next().await? {
        let key = key_value.key.as_ref();
        if let Some((file_prefix, _page_index)) = parse_current_page_key(key) {
            metrics.bytes_scanned = metrics
                .bytes_scanned
                .saturating_add(key_value.value.len() as u64);
            files.entry(file_prefix).or_default().current_pages += 1;
        }
    }

    metrics.databases = if files.is_empty() {
        0
    } else {
        database_count_when_non_empty
    };
    metrics.files = files.len();
    let _ = policy;
    for file in files.values() {
        metrics.current_pages += file.current_pages;
    }
    Ok(metrics)
}

async fn purge_sqlite_database_fully(
    state: &SlateDbStateStore,
    database: &str,
) -> anyhow::Result<SqliteDatabasePurgeMetrics> {
    let mut total = SqliteDatabasePurgeMetrics {
        database: database.to_string(),
        ..SqliteDatabasePurgeMetrics::default()
    };
    loop {
        let pass = purge_sqlite_database(state, database, 10_000).await?;
        total.keys_scanned = total.keys_scanned.saturating_add(pass.keys_scanned);
        total.keys_deleted = total.keys_deleted.saturating_add(pass.keys_deleted);
        total.bytes_deleted = total.bytes_deleted.saturating_add(pass.bytes_deleted);
        if pass.complete || pass.keys_deleted == 0 {
            total.complete = pass.complete;
            return Ok(total);
        }
    }
}

pub async fn purge_sqlite_database(
    state: &SlateDbStateStore,
    database: &str,
    max_keys: usize,
) -> anyhow::Result<SqliteDatabasePurgeMetrics> {
    let databases = state.list_sqlite_databases().await?;
    if !databases.is_empty() {
        if !databases.iter().any(|existing| existing == database) {
            return Ok(SqliteDatabasePurgeMetrics {
                database: database.to_string(),
                complete: true,
                ..SqliteDatabasePurgeMetrics::default()
            });
        }
        let database_state = state.sqlite_database_state(database).await?;
        return purge_sqlite_database_in_state(&database_state, database, max_keys).await;
    }
    purge_sqlite_database_in_state(state, database, max_keys).await
}

async fn purge_sqlite_database_in_state(
    state: &SlateDbStateStore,
    database: &str,
    max_keys: usize,
) -> anyhow::Result<SqliteDatabasePurgeMetrics> {
    let prefix = database_pages_prefix(database);
    let mut metrics = SqliteDatabasePurgeMetrics {
        database: database.to_string(),
        ..SqliteDatabasePurgeMetrics::default()
    };
    if max_keys == 0 {
        return Ok(metrics);
    }

    let mut batch = WriteBatch::new();
    let mut iter = DbReadOps::scan_prefix(state.db.as_ref(), &prefix).await?;
    while let Some(key_value) = iter.next().await? {
        metrics.keys_scanned += 1;
        if metrics.keys_deleted >= max_keys {
            break;
        }
        metrics.bytes_deleted = metrics
            .bytes_deleted
            .saturating_add(key_value.value.len() as u64);
        batch.delete(key_value.key.as_ref());
        metrics.keys_deleted += 1;
    }
    metrics.complete = metrics.keys_scanned == metrics.keys_deleted;

    if metrics.keys_deleted > 0 {
        DbWriteOps::write_with_options(state.db.as_ref(), batch, &non_durable_write_options())
            .await?;
    }
    Ok(metrics)
}

pub async fn purge_tombstoned_sqlite_database(
    state: &SlateDbStateStore,
    database: &str,
    deleted_at_ms: u64,
    now_ms: u64,
    policy: &SqliteDatabasePurgePolicy,
) -> anyhow::Result<SqliteDatabasePurgeMetrics> {
    ensure!(!database.is_empty(), "database name must not be empty");
    ensure!(
        policy.max_keys_per_pass > 0,
        "SQLite database purge max_keys_per_pass must be greater than zero"
    );

    let retention_elapsed_ms = now_ms.saturating_sub(deleted_at_ms);
    if retention_elapsed_ms < policy.retention_ms {
        return Ok(SqliteDatabasePurgeMetrics {
            database: database.to_string(),
            retention_elapsed_ms: Some(retention_elapsed_ms),
            skipped_for_retention: true,
            ..SqliteDatabasePurgeMetrics::default()
        });
    }

    let mut metrics = purge_sqlite_database(state, database, policy.max_keys_per_pass).await?;
    metrics.retention_elapsed_ms = Some(retention_elapsed_ms);
    Ok(metrics)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum FileManifestKind {
    Write,
    Truncate,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileManifest {
    version: u64,
    kind: FileManifestKind,
    size: u64,
}

fn size_key(database: &str, path: &str) -> Vec<u8> {
    let mut key = page_file_prefix(database, path);
    key.extend_from_slice(b"size");
    key
}

fn size_key_from_file_prefix(file_prefix: &[u8]) -> Vec<u8> {
    let mut key = file_prefix.to_vec();
    key.extend_from_slice(b"size");
    key
}

fn current_pages_prefix_from_file_prefix(file_prefix: &[u8]) -> Vec<u8> {
    let mut key = file_prefix.to_vec();
    key.extend_from_slice(b"current/");
    key
}

fn parse_current_page_key(key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let marker = b"current/";
    let marker_index = find_subslice(key, marker)?;
    let file_prefix = key[..marker_index].to_vec();
    let page_index = std::str::from_utf8(&key[marker_index + marker.len()..])
        .ok()?
        .parse()
        .ok()?;
    Some((file_prefix, page_index))
}

fn parse_file_manifest_key(key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let marker = b"manifests/";
    let marker_index = find_subslice(key, marker)?;
    let file_prefix = key[..marker_index].to_vec();
    let version = std::str::from_utf8(&key[marker_index + marker.len()..])
        .ok()?
        .parse()
        .ok()?;
    Some((file_prefix, version))
}

fn parse_latest_manifest_key(key: &[u8], value: &[u8]) -> anyhow::Result<Option<(Vec<u8>, u64)>> {
    let marker = b"latest_manifest";
    if !key.ends_with(marker) {
        return Ok(None);
    }
    let file_prefix_len = key.len().saturating_sub(marker.len());
    Ok(Some((
        key[..file_prefix_len].to_vec(),
        u64_from_be_bytes(value, "latest manifest version")?,
    )))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn u64_from_be_bytes(bytes: &[u8], label: &str) -> anyhow::Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid SQLite {label} metadata"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn current_page_key(database: &str, path: &str, page_index: u64) -> Vec<u8> {
    let mut key = current_pages_prefix(database, path);
    key.extend_from_slice(format!("{page_index:020}").as_bytes());
    key
}

fn current_pages_prefix(database: &str, path: &str) -> Vec<u8> {
    let mut key = page_file_prefix(database, path);
    key.extend_from_slice(b"current/");
    key
}

fn file_manifest_key(database: &str, path: &str, version: u64) -> Vec<u8> {
    let mut key = page_file_prefix(database, path);
    key.extend_from_slice(b"manifests/");
    key.extend_from_slice(format!("{version:020}").as_bytes());
    key
}

fn latest_manifest_key(database: &str, path: &str) -> Vec<u8> {
    let mut key = page_file_prefix(database, path);
    key.extend_from_slice(b"latest_manifest");
    key
}

fn page_file_prefix(database: &str, path: &str) -> Vec<u8> {
    prefixed_file_key(SQLITE_PAGE_PREFIX, database, path)
}

fn file_path_from_file_prefix(file_prefix: &[u8]) -> Option<String> {
    let path = file_prefix.strip_prefix(SQLITE_PAGE_PREFIX)?;
    let path = path.strip_suffix(b"/").unwrap_or(path);
    std::str::from_utf8(path).ok().map(|path| path.to_string())
}

fn sqlite_file_kind_from_path(path: &str) -> FileKind {
    if path.ends_with("-wal") {
        FileKind::Wal
    } else if path.ends_with("-journal") {
        FileKind::Journal
    } else if path.ends_with(".db") || path == "main.db" {
        FileKind::MainDb
    } else {
        FileKind::Other
    }
}

fn sqlite_snapshot_candidate_paths(database: &str) -> Vec<String> {
    let mut paths = vec![
        "main.db".to_string(),
        "main.db-wal".to_string(),
        "main.db-journal".to_string(),
    ];
    if database != "main" {
        paths.push(database.to_string());
        paths.push(format!("{database}-wal"));
        paths.push(format!("{database}-journal"));
        paths.push(format!("{database}.db"));
        paths.push(format!("{database}.db-wal"));
        paths.push(format!("{database}.db-journal"));
    }
    paths.sort();
    paths.dedup();
    paths
}

fn database_pages_prefix(database: &str) -> Vec<u8> {
    let _ = database;
    SQLITE_PAGE_PREFIX.to_vec()
}

fn prefixed_file_key(prefix: &[u8], database: &str, path: &str) -> Vec<u8> {
    let mut key = prefix.to_vec();
    let _ = database;
    for component in Path::new(path).components() {
        if let std::path::Component::Normal(part) = component {
            key.extend_from_slice(part.to_string_lossy().as_bytes());
            key.push(b'/');
        }
    }
    key
}

fn page_index_from_key(prefix: &[u8], key: &[u8]) -> Option<u64> {
    key.strip_prefix(prefix)
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .and_then(|suffix| suffix.parse::<u64>().ok())
}

pub(crate) fn block_on_store<T>(
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(future),
    }
}

fn non_durable_write_options() -> SlateWriteOptions {
    SlateWriteOptions {
        await_durable: false,
        ..SlateWriteOptions::default()
    }
}

fn next_local_manifest_version() -> u64 {
    LOCAL_MANIFEST_VERSION.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_sqlite::{FileKind, VfsFileOp, VfsWrite};
    use slatedb::object_store::{ObjectStore, memory::InMemory};
    use std::sync::Arc;

    #[tokio::test]
    async fn applies_cross_page_writes_as_current_pages() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-cross-write", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");
        let payload = vec![7; 32];

        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db".to_string(),
                file_kind: FileKind::MainDb,
                ops: vec![VfsFileOp::Write(VfsWrite {
                    offset: SQLITE_PAGE_SIZE as u64 - 8,
                    bytes: payload.clone(),
                })],
            })
            .await
            .unwrap();

        assert_eq!(
            store.read_size("main.db").await.unwrap(),
            SQLITE_PAGE_SIZE + 24
        );
        assert_eq!(
            store.read_page("main.db", 0).await.unwrap().len(),
            SQLITE_PAGE_SIZE
        );
        assert_eq!(store.read_page("main.db", 1).await.unwrap().len(), 24);

        let mut read = vec![0; 32];
        let n = store
            .read_at_async("main.db", SQLITE_PAGE_SIZE as u64 - 8, &mut read)
            .await
            .unwrap();
        assert_eq!(n, 32);
        assert_eq!(read, payload);
    }

    #[tokio::test]
    async fn truncation_removes_pages_beyond_new_size() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-truncate", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");
        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db".to_string(),
                file_kind: FileKind::MainDb,
                ops: vec![VfsFileOp::Write(VfsWrite {
                    offset: 0,
                    bytes: vec![1; SQLITE_PAGE_SIZE * 3],
                })],
            })
            .await
            .unwrap();

        store
            .truncate_async("main.db", SQLITE_PAGE_SIZE + 3)
            .await
            .unwrap();

        assert_eq!(
            store.read_size("main.db").await.unwrap(),
            SQLITE_PAGE_SIZE + 3
        );
        assert_eq!(
            store.read_page("main.db", 0).await.unwrap().len(),
            SQLITE_PAGE_SIZE
        );
        assert_eq!(store.read_page("main.db", 1).await.unwrap().len(), 3);
        assert!(store.read_page("main.db", 2).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn same_batch_writes_to_one_page_preserve_prior_writes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-same-batch", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db".to_string(),
                file_kind: FileKind::MainDb,
                ops: vec![
                    VfsFileOp::Write(VfsWrite {
                        offset: 128,
                        bytes: b"CREATE TABLE se".to_vec(),
                    }),
                    VfsFileOp::Write(VfsWrite {
                        offset: 128 + b"CREATE TABLE se".len() as u64,
                        bytes: b"rvices (id integer)".to_vec(),
                    }),
                ],
            })
            .await
            .unwrap();

        let mut read = vec![0; "CREATE TABLE services (id integer)".len()];
        let n = store
            .read_at_async("main.db", 128, &mut read)
            .await
            .unwrap();
        assert_eq!(n, read.len());
        assert_eq!(read, b"CREATE TABLE services (id integer)");
    }

    #[tokio::test]
    async fn delete_batch_removes_size_and_pages() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-delete", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");
        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db-wal".to_string(),
                file_kind: FileKind::Wal,
                ops: vec![VfsFileOp::Write(VfsWrite {
                    offset: 0,
                    bytes: vec![9; SQLITE_PAGE_SIZE + 12],
                })],
            })
            .await
            .unwrap();

        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db-wal".to_string(),
                file_kind: FileKind::Wal,
                ops: vec![VfsFileOp::Delete],
            })
            .await
            .unwrap();

        assert!(!store.exists_async("main.db-wal").await.unwrap());
        assert_eq!(store.read_size("main.db-wal").await.unwrap(), 0);
        assert!(store.read_page("main.db-wal", 0).await.unwrap().is_empty());
        assert!(store.read_page("main.db-wal", 1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn purge_sqlite_database_deletes_only_target_database_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-database-purge", object_store)
            .await
            .unwrap();
        let target_state = state.sqlite_database_state("tenant-a").await.unwrap();
        let neighbor_state = state.sqlite_database_state("tenant-b").await.unwrap();
        let target = SlateDbSqliteFileStore::new(&target_state, "tenant-a");
        let neighbor = SlateDbSqliteFileStore::new(&neighbor_state, "tenant-b");

        for (store, database, byte) in [(&target, "tenant-a", 7), (&neighbor, "tenant-b", 9)] {
            store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: database.to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![byte; SQLITE_PAGE_SIZE + 16],
                        })],
                    },
                    10,
                )
                .await
                .unwrap();
        }

        let first = purge_sqlite_database(&state, "tenant-a", 2).await.unwrap();
        assert_eq!(first.database, "tenant-a");
        assert_eq!(first.keys_deleted, 2);
        assert_eq!(first.retention_elapsed_ms, None);
        assert!(!first.skipped_for_retention);
        assert!(!first.complete);

        let second = purge_sqlite_database(&state, "tenant-a", 100)
            .await
            .unwrap();
        assert!(second.keys_deleted > 0);
        assert!(second.complete);

        assert!(!target.exists_async("main.db").await.unwrap());
        assert!(target.read_page("main.db", 0).await.unwrap().is_empty());
        assert!(neighbor.exists_async("main.db").await.unwrap());
        assert_eq!(neighbor.read_page("main.db", 0).await.unwrap()[0], 9);

        let third = purge_sqlite_database(&state, "tenant-a", 100)
            .await
            .unwrap();
        assert_eq!(third.keys_deleted, 0);
        assert!(third.complete);
    }

    #[tokio::test]
    async fn tombstoned_sqlite_database_purge_waits_for_retention_window() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-database-purge-retention", object_store)
            .await
            .unwrap();
        let target = SlateDbSqliteFileStore::new(&state, "tenant-a");
        target
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![7; SQLITE_PAGE_SIZE],
                    })],
                },
                10,
            )
            .await
            .unwrap();

        let policy = SqliteDatabasePurgePolicy {
            retention_ms: 1_000,
            max_keys_per_pass: 100,
        };
        let skipped = purge_tombstoned_sqlite_database(&state, "tenant-a", 10_000, 10_999, &policy)
            .await
            .unwrap();
        assert_eq!(skipped.retention_elapsed_ms, Some(999));
        assert!(skipped.skipped_for_retention);
        assert_eq!(skipped.keys_deleted, 0);
        assert!(target.exists_async("main.db").await.unwrap());

        let purged = purge_tombstoned_sqlite_database(&state, "tenant-a", 10_000, 11_000, &policy)
            .await
            .unwrap();
        assert_eq!(purged.retention_elapsed_ms, Some(1_000));
        assert!(!purged.skipped_for_retention);
        assert!(purged.keys_deleted > 0);
        assert!(purged.complete);
        assert!(!target.exists_async("main.db").await.unwrap());
    }

    #[tokio::test]
    async fn ordered_batch_ops_can_delete_and_recreate_file() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("paged-ordered-ops", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");
        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db".to_string(),
                file_kind: FileKind::MainDb,
                ops: vec![VfsFileOp::Write(VfsWrite {
                    offset: 0,
                    bytes: vec![1; SQLITE_PAGE_SIZE + 16],
                })],
            })
            .await
            .unwrap();

        store
            .apply_batch_async(&VfsSyncBatch {
                database: "tenant-a".to_string(),
                file_path: "main.db".to_string(),
                file_kind: FileKind::MainDb,
                ops: vec![
                    VfsFileOp::Delete,
                    VfsFileOp::Truncate { size: 64 },
                    VfsFileOp::Write(VfsWrite {
                        offset: 8,
                        bytes: b"new-image".to_vec(),
                    }),
                ],
            })
            .await
            .unwrap();

        let mut read = vec![0; 64];
        let n = store.read_at_async("main.db", 0, &mut read).await.unwrap();
        assert_eq!(n, 64);
        assert_eq!(&read[8..17], b"new-image");
        assert!(read[..8].iter().all(|byte| *byte == 0));
        assert!(read[17..].iter().all(|byte| *byte == 0));
        assert!(store.read_page("main.db", 1).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn full_page_writes_replace_current_page() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-full-write", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");
        let page_v10 = vec![10; SQLITE_PAGE_SIZE];
        let page_v11 = vec![11; SQLITE_PAGE_SIZE];

        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: page_v10.clone(),
                    })],
                },
                10,
            )
            .await
            .unwrap();
        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: page_v11.clone(),
                    })],
                },
                11,
            )
            .await
            .unwrap();

        assert_eq!(
            DbReadOps::get(
                state.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            page_v11
        );
        assert_eq!(store.read_page("main.db", 0).await.unwrap(), page_v11);
        assert_ne!(store.read_page("main.db", 0).await.unwrap(), page_v10);
    }

    #[tokio::test]
    async fn full_page_overwrite_does_not_read_previous_page() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-full-overwrite-no-read", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE],
                    })],
                },
                30,
            )
            .await
            .unwrap();

        store.reset_read_page_count();
        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![2; SQLITE_PAGE_SIZE],
                    })],
                },
                31,
            )
            .await
            .unwrap();

        assert_eq!(store.read_page_count(), 0);
        assert_eq!(
            store.read_page("main.db", 0).await.unwrap(),
            vec![2; SQLITE_PAGE_SIZE]
        );
    }

    #[tokio::test]
    async fn partial_writes_to_same_existing_page_read_previous_page_once() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-partial-read-once", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE],
                    })],
                },
                40,
            )
            .await
            .unwrap();

        store.reset_read_page_count();
        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![
                        VfsFileOp::Write(VfsWrite {
                            offset: 8,
                            bytes: vec![2; 4],
                        }),
                        VfsFileOp::Write(VfsWrite {
                            offset: 16,
                            bytes: vec![3; 4],
                        }),
                    ],
                },
                41,
            )
            .await
            .unwrap();

        assert_eq!(store.read_page_count(), 1);
        let page = store.read_page("main.db", 0).await.unwrap();
        assert_eq!(&page[..8], &[1; 8]);
        assert_eq!(&page[8..12], &[2; 4]);
        assert_eq!(&page[12..16], &[1; 4]);
        assert_eq!(&page[16..20], &[3; 4]);
    }

    #[tokio::test]
    async fn full_page_covered_by_multiple_writes_does_not_read_previous_page() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-covered-no-read", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE],
                    })],
                },
                50,
            )
            .await
            .unwrap();

        store.reset_read_page_count();
        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![
                        VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![2; SQLITE_PAGE_SIZE / 2],
                        }),
                        VfsFileOp::Write(VfsWrite {
                            offset: (SQLITE_PAGE_SIZE / 2) as u64,
                            bytes: vec![3; SQLITE_PAGE_SIZE / 2],
                        }),
                    ],
                },
                51,
            )
            .await
            .unwrap();

        assert_eq!(store.read_page_count(), 0);
        let page = store.read_page("main.db", 0).await.unwrap();
        assert_eq!(&page[..SQLITE_PAGE_SIZE / 2], vec![2; SQLITE_PAGE_SIZE / 2]);
        assert_eq!(&page[SQLITE_PAGE_SIZE / 2..], vec![3; SQLITE_PAGE_SIZE / 2]);
    }

    #[tokio::test]
    async fn partial_write_merges_current_page() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-partial-write", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE],
                    })],
                },
                20,
            )
            .await
            .unwrap();
        store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 8,
                        bytes: vec![2; 4],
                    })],
                },
                21,
            )
            .await
            .unwrap();

        let page = store.read_page("main.db", 0).await.unwrap();
        assert_eq!(&page[..8], &[1; 8]);
        assert_eq!(&page[8..12], &[2; 4]);
        assert_eq!(&page[12..16], &[1; 4]);
    }

    #[tokio::test]
    async fn page_compaction_is_noop_after_current_page_overwrites() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-version-compact", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        for version in 1..=3 {
            store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "tenant-a".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![version as u8; SQLITE_PAGE_SIZE],
                        })],
                    },
                    version,
                )
                .await
                .unwrap();
        }

        assert_eq!(
            store
                .compact_obsolete_page_versions("main.db")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            DbReadOps::get(
                state.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            vec![3; SQLITE_PAGE_SIZE]
        );
        assert_eq!(
            store.read_page("main.db", 0).await.unwrap(),
            vec![3; SQLITE_PAGE_SIZE]
        );
    }

    #[tokio::test]
    async fn policy_compaction_reports_no_obsolete_versions_for_current_pages() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-policy-compact", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        for version in 1..=5 {
            store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "tenant-a".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![version as u8; SQLITE_PAGE_SIZE],
                        })],
                    },
                    version,
                )
                .await
                .unwrap();
        }

        let metrics = compact_sqlite_page_versions(
            &state,
            &SqlitePageCompactionPolicy {
                obsolete_versions_per_file: 99,
                obsolete_version_ratio: 2.0,
                obsolete_bytes_per_file: u64::MAX,
                max_versions_per_pass: 10,
                max_bytes_per_pass: u64::MAX,
                retain_recent_versions: 1,
                min_retained_version: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(metrics.files_scanned, 0);
        assert_eq!(metrics.files_compacted, 0);
        assert_eq!(metrics.obsolete_versions, 0);
        assert_eq!(metrics.deleted_versions, 0);
        assert_eq!(
            DbReadOps::get(
                state.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            vec![5; SQLITE_PAGE_SIZE]
        );
    }

    #[tokio::test]
    async fn policy_compaction_can_exclude_system_catalog_database() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-policy-exclude-catalog", object_store)
            .await
            .unwrap();
        let catalog_state = state.sqlite_database_state("orion_catalog").await.unwrap();
        let tenant_state = state.sqlite_database_state("tenant-a").await.unwrap();
        let catalog_store = SlateDbSqliteFileStore::new(&catalog_state, "orion_catalog");
        let tenant_store = SlateDbSqliteFileStore::new(&tenant_state, "tenant-a");

        for version in 1..=4 {
            let op = VfsFileOp::Write(VfsWrite {
                offset: 0,
                bytes: vec![version as u8; SQLITE_PAGE_SIZE],
            });
            catalog_store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "orion_catalog".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![op.clone()],
                    },
                    version,
                )
                .await
                .unwrap();
            tenant_store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "tenant-a".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![op],
                    },
                    version,
                )
                .await
                .unwrap();
        }

        let metrics = compact_sqlite_page_versions_excluding(
            &state,
            &SqlitePageCompactionPolicy {
                obsolete_versions_per_file: 1,
                obsolete_version_ratio: 1.0,
                obsolete_bytes_per_file: 1,
                max_versions_per_pass: 10,
                max_bytes_per_pass: u64::MAX,
                retain_recent_versions: 0,
                min_retained_version: None,
            },
            &["orion_catalog"],
        )
        .await
        .unwrap();

        assert_eq!(metrics.files_scanned, 0);
        assert_eq!(metrics.files_compacted, 0);
        assert_eq!(metrics.deleted_versions, 0);
        assert_eq!(
            DbReadOps::get(
                catalog_state.db.as_ref(),
                &current_page_key("orion_catalog", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            vec![4; SQLITE_PAGE_SIZE]
        );
        assert_eq!(
            DbReadOps::get(
                tenant_state.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            vec![4; SQLITE_PAGE_SIZE]
        );
    }

    #[tokio::test]
    async fn database_page_sync_copies_current_pages_and_removes_truncated_pages() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = SlateDbStateStore::open("page-sync-source", Arc::clone(&object_store))
            .await
            .unwrap();
        let target = SlateDbStateStore::open("page-sync-target", object_store)
            .await
            .unwrap();
        let source_child = source.sqlite_database_state("tenant-a").await.unwrap();
        let target_child = target.sqlite_database_state("tenant-a").await.unwrap();
        let source_store = SlateDbSqliteFileStore::new(&source_child, "tenant-a");
        let target_store = SlateDbSqliteFileStore::new(&target_child, "tenant-a");

        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE * 2],
                    })],
                },
                1,
            )
            .await
            .unwrap();
        let first = sync_sqlite_database_pages_since(&target, "tenant-a", &source, 0)
            .await
            .unwrap();
        assert_eq!(first.pages_copied, 2);
        assert_eq!(
            target_store.read_page("main.db", 1).await.unwrap(),
            vec![1; SQLITE_PAGE_SIZE]
        );

        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![
                        VfsFileOp::Truncate {
                            size: SQLITE_PAGE_SIZE as u64,
                        },
                        VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![2; SQLITE_PAGE_SIZE],
                        }),
                    ],
                },
                2,
            )
            .await
            .unwrap();
        let delta = export_sqlite_database_pages_since(&source, "tenant-a", 1)
            .await
            .unwrap();
        assert!(
            !delta
                .entries
                .iter()
                .any(|entry| entry.key == current_page_key("tenant-a", "main.db", 1)),
            "remote delta must not include pages beyond the truncated file size"
        );
        let delta: SqliteDatabasePageSyncDelta =
            serde_json::from_slice(&serde_json::to_vec(&delta).unwrap()).unwrap();
        let second = apply_sqlite_database_page_delta(&target, "tenant-a", &delta)
            .await
            .unwrap();
        assert_eq!(second.pages_copied, 1);
        assert!(second.metadata_copied > 0);
        assert!(second.keys_deleted > 0);
        assert_eq!(
            target_store.read_page("main.db", 0).await.unwrap(),
            vec![2; SQLITE_PAGE_SIZE]
        );
        assert!(
            target_store
                .read_page("main.db", 1)
                .await
                .unwrap()
                .is_empty(),
            "target visible metadata for truncated pages must be removed"
        );
        assert!(
            DbReadOps::get(
                target_child.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 1)
            )
            .await
            .unwrap()
            .is_none(),
            "truncated current pages must be removed"
        );
    }

    #[tokio::test]
    async fn live_snapshot_exports_only_current_pages() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = SlateDbStateStore::open("live-snapshot-source", Arc::clone(&object_store))
            .await
            .unwrap();
        let target = SlateDbStateStore::open("live-snapshot-target", object_store)
            .await
            .unwrap();
        let source_child = source.sqlite_database_state("tenant-a").await.unwrap();
        let source_store = SlateDbSqliteFileStore::new(&source_child, "tenant-a");

        for version in 1..=4 {
            source_store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "tenant-a".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![
                            VfsFileOp::Write(VfsWrite {
                                offset: 0,
                                bytes: vec![version as u8; SQLITE_PAGE_SIZE],
                            }),
                            VfsFileOp::Write(VfsWrite {
                                offset: SQLITE_PAGE_SIZE as u64,
                                bytes: vec![(version + 10) as u8; SQLITE_PAGE_SIZE],
                            }),
                        ],
                    },
                    version,
                )
                .await
                .unwrap();
        }

        let delta = export_sqlite_database_live_snapshot(&source, "tenant-a")
            .await
            .unwrap();
        let current_pages = delta
            .entries
            .iter()
            .filter(|entry| parse_current_page_key(&entry.key).is_some())
            .count();
        assert_eq!(current_pages, 2, "{delta:?}");
        assert!(delta.entries.iter().any(|entry| {
            parse_current_page_key(&entry.key) == Some((page_file_prefix("tenant-a", "main.db"), 0))
                && entry.value == vec![4; SQLITE_PAGE_SIZE]
        }));
        assert!(delta.entries.iter().any(|entry| {
            parse_current_page_key(&entry.key) == Some((page_file_prefix("tenant-a", "main.db"), 1))
                && entry.value == vec![14; SQLITE_PAGE_SIZE]
        }));

        materialize_sqlite_database_live_snapshot(&target, "tenant-a", &source)
            .await
            .unwrap();
        let target_child = target.sqlite_database_state("tenant-a").await.unwrap();
        let target_store = SlateDbSqliteFileStore::new(&target_child, "tenant-a");
        assert_eq!(
            target_store.read_page("main.db", 0).await.unwrap(),
            vec![4; SQLITE_PAGE_SIZE]
        );
        assert_eq!(
            target_store.read_page("main.db", 1).await.unwrap(),
            vec![14; SQLITE_PAGE_SIZE]
        );

        let mut iter = DbReadOps::scan_prefix(target_child.db.as_ref(), SQLITE_PAGE_PREFIX)
            .await
            .unwrap();
        let mut target_current_pages = 0;
        while let Some(key_value) = iter.next().await.unwrap() {
            if parse_current_page_key(key_value.key.as_ref()).is_some() {
                target_current_pages += 1;
            }
        }
        assert_eq!(target_current_pages, 2);
    }

    #[tokio::test]
    async fn file_snapshot_exports_current_file_chunks() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = SlateDbStateStore::open("file-snapshot-source", Arc::clone(&object_store))
            .await
            .unwrap();
        let target = SlateDbStateStore::open("file-snapshot-target", object_store)
            .await
            .unwrap();
        let source_child = source.sqlite_database_state("tenant-a").await.unwrap();
        let source_store = SlateDbSqliteFileStore::new(&source_child, "tenant-a");
        let bytes = (0..SQLITE_PAGE_SIZE * 4 + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();

        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: vec![1; SQLITE_PAGE_SIZE * 4 + 17],
                    })],
                },
                1,
            )
            .await
            .unwrap();
        source_store
            .apply_batch_async_at_version(
                &VfsSyncBatch {
                    database: "tenant-a".to_string(),
                    file_path: "main.db".to_string(),
                    file_kind: FileKind::MainDb,
                    ops: vec![VfsFileOp::Write(VfsWrite {
                        offset: 0,
                        bytes: bytes.clone(),
                    })],
                },
                2,
            )
            .await
            .unwrap();

        let snapshot =
            export_sqlite_database_file_snapshot(&source, "tenant-a", SQLITE_PAGE_SIZE * 2)
                .await
                .unwrap();
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].path, "main.db");
        assert_eq!(snapshot.files[0].chunks.len(), 3);
        let snapshot_bytes: usize = snapshot.files[0]
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.len())
            .sum();
        assert_eq!(snapshot_bytes, bytes.len());

        materialize_sqlite_database_file_snapshot(&target, "tenant-a", &snapshot)
            .await
            .unwrap();
        let target_child = target.sqlite_database_state("tenant-a").await.unwrap();
        let target_store = SlateDbSqliteFileStore::new(&target_child, "tenant-a");
        assert_eq!(target_store.read_file("main.db").await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn policy_compaction_honors_delete_budget_and_snapshot_floor() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = SlateDbStateStore::open("page-policy-budget", object_store)
            .await
            .unwrap();
        let store = SlateDbSqliteFileStore::new(&state, "tenant-a");

        for version in 1..=6 {
            store
                .apply_batch_async_at_version(
                    &VfsSyncBatch {
                        database: "tenant-a".to_string(),
                        file_path: "main.db".to_string(),
                        file_kind: FileKind::MainDb,
                        ops: vec![VfsFileOp::Write(VfsWrite {
                            offset: 0,
                            bytes: vec![version as u8; SQLITE_PAGE_SIZE],
                        })],
                    },
                    version,
                )
                .await
                .unwrap();
        }

        let metrics = compact_sqlite_page_versions(
            &state,
            &SqlitePageCompactionPolicy {
                obsolete_versions_per_file: 1,
                obsolete_version_ratio: 1.0,
                obsolete_bytes_per_file: 1,
                max_versions_per_pass: 2,
                max_bytes_per_pass: u64::MAX,
                retain_recent_versions: 0,
                min_retained_version: Some(4),
            },
        )
        .await
        .unwrap();

        assert_eq!(metrics.deleted_versions, 0);
        assert_eq!(metrics.obsolete_versions, 0);
        assert_eq!(
            DbReadOps::get(
                state.db.as_ref(),
                &current_page_key("tenant-a", "main.db", 0)
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec(),
            vec![6; SQLITE_PAGE_SIZE]
        );
    }
}
