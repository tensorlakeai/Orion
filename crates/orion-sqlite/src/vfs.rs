use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::{Context, ensure};
use sqlite_plugin::flags::{
    AccessFlags, CreateMode, LockLevel, OpenKind, OpenMode, OpenOpts, ShmLockMode,
};
use sqlite_plugin::vars;
use sqlite_plugin::vfs::{RegisterOpts, Vfs, VfsHandle, VfsResult, register_static};

use crate::commit::{FileKind, RaftWalCommitSink, VfsFileOp, VfsSyncBatch, VfsWrite};
use crate::store::{LocalFileStore, SqliteFileStore};

#[derive(Debug, Clone)]
pub struct OrionVfsConfig {
    pub database: String,
    pub cache_root: PathBuf,
}

impl OrionVfsConfig {
    pub fn new(database: impl Into<String>, cache_root: impl Into<PathBuf>) -> Self {
        Self {
            database: database.into(),
            cache_root: cache_root.into(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.database.is_empty(), "database name must not be empty");
        ensure!(
            !self.cache_root.as_os_str().is_empty(),
            "cache_root must not be empty"
        );
        Ok(())
    }
}

pub struct OrionVfs<S, F = LocalFileStore> {
    config: OrionVfsConfig,
    commit_sink: Arc<S>,
    file_store: Arc<F>,
    next_temp_id: AtomicU64,
}

impl<S> OrionVfs<S, LocalFileStore>
where
    S: RaftWalCommitSink,
{
    pub fn new(config: OrionVfsConfig, commit_sink: Arc<S>) -> anyhow::Result<Self> {
        config.validate()?;
        let file_store = LocalFileStore::new(&config.cache_root)
            .with_context(|| {
                format!(
                    "creating local SQLite file store {}",
                    config.cache_root.display()
                )
            })?
            .into_shared();
        Self::with_file_store(config, commit_sink, file_store)
    }
}

impl<S, F> OrionVfs<S, F>
where
    S: RaftWalCommitSink,
    F: SqliteFileStore,
{
    pub fn with_file_store(
        config: OrionVfsConfig,
        commit_sink: Arc<S>,
        file_store: Arc<F>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            commit_sink,
            file_store,
            next_temp_id: AtomicU64::new(1),
        })
    }

    fn resolve_path(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<(String, PathBuf)> {
        let logical_path = path
            .map(str::to_string)
            .unwrap_or_else(|| self.next_temp_path(opts.kind()));
        let relative = Path::new(&logical_path)
            .components()
            .filter_map(|component| match component {
                std::path::Component::Normal(part) => Some(part.to_owned()),
                _ => None,
            })
            .collect::<PathBuf>();
        if relative.as_os_str().is_empty() {
            return Err(vars::SQLITE_CANTOPEN);
        }
        Ok((logical_path, self.config.cache_root.join(relative)))
    }

    fn next_temp_path(&self, kind: OpenKind) -> String {
        let id = self.next_temp_id.fetch_add(1, Ordering::Relaxed);
        format!("__orion_tmp/{kind:?}-{id}")
    }
}

pub fn register_orion_vfs<S, F>(
    name: &str,
    vfs: OrionVfs<S, F>,
    make_default: bool,
) -> anyhow::Result<()>
where
    S: RaftWalCommitSink,
    F: SqliteFileStore,
{
    let name = CString::new(name).context("VFS name must not contain nul bytes")?;
    register_static(name, vfs, RegisterOpts { make_default })
        .map(|_| ())
        .map_err(|code| anyhow::anyhow!("registering SQLite VFS failed with code {code}"))
}

pub fn clear_orion_vfs_shared_state(cache_root: &Path, database: &str) -> anyhow::Result<usize> {
    let mut states = DATABASE_FILE_STATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow::anyhow!("orion VFS shared state mutex poisoned"))?;
    let prefix = format!("{}:{database}/", cache_root.display());
    let before = states.len();
    states.retain(|key, _| !key.starts_with(&prefix));
    Ok(before.saturating_sub(states.len()))
}

pub struct OrionVfsHandle {
    id: u64,
    logical_path: String,
    kind: FileKind,
    readonly: bool,
    delete_on_close: bool,
    lock_level: LockLevel,
    file_lock: SharedFileLock,
    pending_ops: Arc<Mutex<Vec<VfsFileOp>>>,
    shm: SharedMemoryRegion,
    shm_lock: SharedShmLock,
}

type SharedMemoryRegion = Arc<Mutex<ShmRegionState>>;
type SharedFileLock = Arc<Mutex<FileLockState>>;
type SharedShmLock = Arc<Mutex<ShmLockState>>;

type SharedDatabaseFileState = Arc<DatabaseFileState>;

static NEXT_HANDLE_ID: AtomicU64 = AtomicU64::new(1);
static DATABASE_FILE_STATES: OnceLock<Mutex<HashMap<String, SharedDatabaseFileState>>> =
    OnceLock::new();

#[derive(Debug, Default)]
struct DatabaseFileState {
    file_lock: SharedFileLock,
    shm_lock: SharedShmLock,
    shm: SharedMemoryRegion,
    locally_created: AtomicBool,
}

#[derive(Debug, Default)]
struct ShmRegionState {
    regions: Vec<Box<[u8]>>,
    mapped_holders: HashSet<u64>,
}

#[derive(Debug, Default)]
struct FileLockState {
    shared_holders: HashSet<u64>,
    reserved_holder: Option<u64>,
    pending_holder: Option<u64>,
    exclusive_holder: Option<u64>,
}

#[derive(Debug, Default)]
struct ShmLockState {
    bytes: HashMap<u32, ShmByteLock>,
}

#[derive(Debug, Default)]
struct ShmByteLock {
    shared_holders: HashSet<u64>,
    exclusive_holder: Option<u64>,
}

fn next_handle_id() -> u64 {
    NEXT_HANDLE_ID.fetch_add(1, Ordering::Relaxed)
}

fn create_mode(mode: &OpenMode) -> CreateMode {
    match mode {
        OpenMode::ReadOnly => CreateMode::None,
        OpenMode::ReadWrite { create } => match create {
            CreateMode::None => CreateMode::None,
            CreateMode::Create => CreateMode::Create,
            CreateMode::MustCreate => CreateMode::MustCreate,
        },
    }
}

fn shared_database_file_state(
    cache_root: &Path,
    database: &str,
    logical_path: &str,
) -> VfsResult<SharedDatabaseFileState> {
    let mut states = DATABASE_FILE_STATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| vars::SQLITE_IOERR)?;
    let key = format!("{}:{database}/{logical_path}", cache_root.display());
    Ok(states.entry(key).or_default().clone())
}

impl VfsHandle for OrionVfsHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }

    fn in_memory(&self) -> bool {
        false
    }
}

impl<S, F> Vfs for OrionVfs<S, F>
where
    S: RaftWalCommitSink,
    F: SqliteFileStore,
{
    type Handle = OrionVfsHandle;

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        let (logical_path, _local_path) = self.resolve_path(path, opts)?;

        let mode = opts.mode();
        let readonly = mode.is_readonly();
        let create_mode = create_mode(&mode);
        let kind = file_kind_for_open(opts.kind(), &logical_path);
        let shared_state = shared_database_file_state(
            &self.config.cache_root,
            &self.config.database,
            &logical_path,
        )?;
        let exists = shared_state.locally_created.load(Ordering::Acquire)
            || self
                .file_store
                .exists(&logical_path)
                .map_err(|_| vars::SQLITE_CANTOPEN)?;
        if !readonly && create_mode == CreateMode::MustCreate && exists {
            return Err(vars::SQLITE_CANTOPEN);
        }
        if !exists && (readonly || create_mode == CreateMode::None) {
            return Err(vars::SQLITE_CANTOPEN);
        }
        if !readonly && !exists && !should_replicate(kind) {
            self.file_store
                .truncate(&logical_path, 0)
                .map_err(|_| vars::SQLITE_CANTOPEN)?;
        }
        if !readonly && !exists {
            shared_state.locally_created.store(true, Ordering::Release);
        }
        Ok(OrionVfsHandle {
            id: next_handle_id(),
            logical_path,
            kind,
            readonly,
            delete_on_close: opts.delete_on_close(),
            lock_level: LockLevel::Unlocked,
            file_lock: Arc::clone(&shared_state.file_lock),
            pending_ops: Arc::new(Mutex::new(Vec::new())),
            shm: Arc::clone(&shared_state.shm),
            shm_lock: Arc::clone(&shared_state.shm_lock),
        })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        let (logical_path, _) = self.resolve_path(Some(path), OpenOpts::new(0))?;
        let kind = file_kind_from_path(&logical_path);
        if should_replicate(kind) {
            self.commit_and_apply_batch(
                VfsSyncBatch {
                    database: self.config.database.clone(),
                    file_path: logical_path,
                    file_kind: kind,
                    ops: vec![VfsFileOp::Delete],
                },
                vars::SQLITE_IOERR_DELETE,
            )?;
            Ok(())
        } else {
            self.file_store
                .delete(&logical_path)
                .map_err(|_| vars::SQLITE_IOERR_DELETE)?;
            if let Ok(shared_state) = shared_database_file_state(
                &self.config.cache_root,
                &self.config.database,
                &logical_path,
            ) {
                shared_state.locally_created.store(false, Ordering::Release);
            }
            Ok(())
        }
    }

    fn access(&self, path: &str, _flags: AccessFlags) -> VfsResult<bool> {
        let (logical_path, _) = self.resolve_path(Some(path), OpenOpts::new(0))?;
        let shared_state = shared_database_file_state(
            &self.config.cache_root,
            &self.config.database,
            &logical_path,
        )?;
        if shared_state.locally_created.load(Ordering::Acquire) {
            return Ok(true);
        }
        self.file_store
            .exists(&logical_path)
            .map_err(|_| vars::SQLITE_IOERR_ACCESS)
    }

    fn file_size(&self, handle: &mut Self::Handle) -> VfsResult<usize> {
        let base = self
            .file_store
            .file_size(&handle.logical_path)
            .map_err(|_| vars::SQLITE_IOERR_FSTAT)?;
        Ok(overlay_file_size(handle, base)?)
    }

    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        if handle.readonly {
            return Err(vars::SQLITE_READONLY);
        }
        if should_replicate(handle.kind) {
            handle
                .pending_ops
                .lock()
                .map_err(|_| vars::SQLITE_IOERR_TRUNCATE)?
                .push(VfsFileOp::Truncate { size: size as u64 });
            Ok(())
        } else {
            self.file_store
                .truncate(&handle.logical_path, size)
                .map_err(|_| vars::SQLITE_IOERR_TRUNCATE)?;
            Ok(())
        }
    }

    fn write(&self, handle: &mut Self::Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        if handle.readonly {
            return Err(vars::SQLITE_READONLY);
        }
        let write = VfsFileOp::Write(VfsWrite {
            offset: offset as u64,
            bytes: data.to_vec(),
        });

        if should_replicate(handle.kind) {
            handle
                .pending_ops
                .lock()
                .map_err(|_| vars::SQLITE_IOERR)?
                .push(write);
        } else {
            self.file_store
                .apply_batch(&VfsSyncBatch {
                    database: self.config.database.clone(),
                    file_path: handle.logical_path.clone(),
                    file_kind: handle.kind,
                    ops: vec![write],
                })
                .map_err(|_| vars::SQLITE_IOERR_WRITE)?;
        }
        Ok(data.len())
    }

    fn read(&self, handle: &mut Self::Handle, offset: usize, data: &mut [u8]) -> VfsResult<usize> {
        let bytes_read = self
            .file_store
            .read_at(&handle.logical_path, offset as u64, data)
            .map_err(|_| vars::SQLITE_IOERR_READ)?;
        if bytes_read < data.len() {
            data[bytes_read..].fill(0);
        }
        overlay_pending_ops(handle, offset as u64, data)?;
        let visible_size = overlay_file_size(
            handle,
            self.file_store.file_size(&handle.logical_path).unwrap_or(0),
        )?;
        if offset + data.len() <= visible_size {
            Ok(data.len())
        } else {
            Ok(bytes_read)
        }
    }

    fn lock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        lock_file(handle, level)?;
        handle.lock_level = level;
        Ok(())
    }

    fn unlock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        unlock_file(handle, level)?;
        handle.lock_level = level;
        Ok(())
    }

    fn check_reserved_lock(&self, handle: &mut Self::Handle) -> VfsResult<bool> {
        let lock = handle
            .file_lock
            .lock()
            .map_err(|_| vars::SQLITE_IOERR_LOCK)?;
        Ok(lock.reserved_holder.is_some()
            || lock.pending_holder.is_some()
            || lock.exclusive_holder.is_some())
    }

    fn sync(&self, handle: &mut Self::Handle) -> VfsResult<()> {
        self.flush_pending_handle(handle, vars::SQLITE_IOERR_FSYNC)?;
        self.file_store
            .sync(&handle.logical_path)
            .map_err(|_| vars::SQLITE_IOERR_FSYNC)
    }

    fn close(&self, handle: Self::Handle) -> VfsResult<()> {
        release_file_locks(&handle)?;
        release_shm_locks(&handle)?;
        release_shm_mapping(&handle, false)?;
        if handle.delete_on_close {
            if should_replicate(handle.kind) {
                self.commit_and_apply_batch(
                    VfsSyncBatch {
                        database: self.config.database.clone(),
                        file_path: handle.logical_path,
                        file_kind: handle.kind,
                        ops: vec![VfsFileOp::Delete],
                    },
                    vars::SQLITE_IOERR_DELETE,
                )?;
            } else {
                self.file_store
                    .delete(&handle.logical_path)
                    .map_err(|_| vars::SQLITE_IOERR_DELETE)?;
            }
        } else if should_replicate(handle.kind) {
            clear_unmaterialized_local_create(
                &self.config.cache_root,
                &self.config.database,
                &handle.logical_path,
                &*self.file_store,
            )?;
        }
        Ok(())
    }

    fn shm_map(
        &self,
        handle: &mut Self::Handle,
        region_idx: usize,
        region_size: usize,
        extend: bool,
    ) -> VfsResult<Option<NonNull<u8>>> {
        if region_size == 0 {
            return Err(vars::SQLITE_IOERR_SHMMAP);
        }
        let mut shm = handle.shm.lock().map_err(|_| vars::SQLITE_IOERR_SHMMAP)?;
        if shm.regions.len() <= region_idx {
            if !extend {
                return Ok(None);
            }
            while shm.regions.len() <= region_idx {
                shm.regions.push(vec![0; region_size].into_boxed_slice());
            }
        }
        if shm.regions[region_idx].len() != region_size {
            return Err(vars::SQLITE_IOERR_SHMMAP);
        }
        shm.mapped_holders.insert(handle.id);
        Ok(NonNull::new(shm.regions[region_idx].as_mut_ptr()))
    }

    fn shm_lock(
        &self,
        handle: &mut Self::Handle,
        offset: u32,
        count: u32,
        mode: ShmLockMode,
    ) -> VfsResult<()> {
        shm_lock_range(handle, offset, count, mode)?;
        Ok(())
    }

    fn shm_barrier(&self, _handle: &mut Self::Handle) {}

    fn shm_unmap(&self, handle: &mut Self::Handle, delete: bool) -> VfsResult<()> {
        release_shm_locks(handle)?;
        release_shm_mapping(handle, delete)?;
        Ok(())
    }
}

impl<S, F> OrionVfs<S, F>
where
    S: RaftWalCommitSink,
    F: SqliteFileStore,
{
    fn flush_pending_handle(&self, handle: &OrionVfsHandle, error_code: i32) -> VfsResult<()> {
        let ops = drain_pending_ops(handle)?;
        if ops.is_empty() {
            return Ok(());
        }

        let batch = VfsSyncBatch {
            database: self.config.database.clone(),
            file_path: handle.logical_path.clone(),
            file_kind: handle.kind,
            ops,
        };

        if should_replicate(handle.kind) {
            self.commit_and_apply_batch(batch, error_code)
        } else {
            self.file_store.apply_batch(&batch).map_err(|_| error_code)
        }
    }

    fn commit_and_apply_batch(&self, batch: VfsSyncBatch, error_code: i32) -> VfsResult<()> {
        let started = Instant::now();
        let file_kind = batch.file_kind;
        let op_count = batch.ops.len();
        let decision = self
            .commit_sink
            .commit_sync_batch(batch.clone())
            .map_err(|_| error_code)?;
        let commit_elapsed = started.elapsed();
        if !decision.materialized_by_commit {
            self.file_store
                .apply_batch(&batch)
                .map_err(|_| error_code)?;
        }
        let shared_state = shared_database_file_state(
            &self.config.cache_root,
            &self.config.database,
            &batch.file_path,
        )?;
        shared_state.locally_created.store(
            !matches!(batch.ops.last(), Some(VfsFileOp::Delete)),
            Ordering::Release,
        );
        trace_latency(format_args!(
            "vfs_commit file_kind={file_kind:?} ops={op_count} materialized_by_commit={} commit_ms={:.3} total_ms={:.3}",
            decision.materialized_by_commit,
            commit_elapsed.as_secs_f64() * 1000.0,
            started.elapsed().as_secs_f64() * 1000.0
        ));
        Ok(())
    }
}

fn lock_file(handle: &OrionVfsHandle, level: LockLevel) -> VfsResult<()> {
    let mut lock = handle
        .file_lock
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_LOCK)?;
    match level {
        LockLevel::Unlocked => {}
        LockLevel::Shared => {
            if lock
                .exclusive_holder
                .is_some_and(|holder| holder != handle.id)
                || lock
                    .pending_holder
                    .is_some_and(|holder| holder != handle.id)
            {
                return Err(vars::SQLITE_BUSY);
            }
            lock.shared_holders.insert(handle.id);
        }
        LockLevel::Reserved => {
            if lock
                .exclusive_holder
                .is_some_and(|holder| holder != handle.id)
                || lock
                    .reserved_holder
                    .is_some_and(|holder| holder != handle.id)
            {
                return Err(vars::SQLITE_BUSY);
            }
            lock.shared_holders.insert(handle.id);
            lock.reserved_holder = Some(handle.id);
        }
        LockLevel::Pending => {
            if lock
                .exclusive_holder
                .is_some_and(|holder| holder != handle.id)
                || lock
                    .pending_holder
                    .is_some_and(|holder| holder != handle.id)
            {
                return Err(vars::SQLITE_BUSY);
            }
            lock.shared_holders.insert(handle.id);
            lock.pending_holder = Some(handle.id);
        }
        LockLevel::Exclusive => {
            if lock
                .exclusive_holder
                .is_some_and(|holder| holder != handle.id)
                || lock
                    .reserved_holder
                    .is_some_and(|holder| holder != handle.id)
                || lock
                    .pending_holder
                    .is_some_and(|holder| holder != handle.id)
                || lock
                    .shared_holders
                    .iter()
                    .any(|holder| *holder != handle.id)
            {
                return Err(vars::SQLITE_BUSY);
            }
            lock.shared_holders.insert(handle.id);
            lock.reserved_holder = Some(handle.id);
            lock.pending_holder = Some(handle.id);
            lock.exclusive_holder = Some(handle.id);
        }
    }
    Ok(())
}

fn unlock_file(handle: &OrionVfsHandle, level: LockLevel) -> VfsResult<()> {
    let mut lock = handle
        .file_lock
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_UNLOCK)?;
    match level {
        LockLevel::Unlocked => {
            release_file_locks_inner(&mut lock, handle.id);
        }
        LockLevel::Shared => {
            release_exclusive_pending_reserved(&mut lock, handle.id);
            lock.shared_holders.insert(handle.id);
        }
        LockLevel::Reserved => {
            release_exclusive_pending(&mut lock, handle.id);
            lock.shared_holders.insert(handle.id);
            lock.reserved_holder = Some(handle.id);
        }
        LockLevel::Pending => {
            if lock.exclusive_holder == Some(handle.id) {
                lock.exclusive_holder = None;
            }
            lock.shared_holders.insert(handle.id);
            lock.reserved_holder.get_or_insert(handle.id);
            lock.pending_holder = Some(handle.id);
        }
        LockLevel::Exclusive => {}
    }
    Ok(())
}

fn release_file_locks(handle: &OrionVfsHandle) -> VfsResult<()> {
    let mut lock = handle
        .file_lock
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_UNLOCK)?;
    release_file_locks_inner(&mut lock, handle.id);
    Ok(())
}

fn release_file_locks_inner(lock: &mut FileLockState, holder: u64) {
    lock.shared_holders.remove(&holder);
    if lock.reserved_holder == Some(holder) {
        lock.reserved_holder = None;
    }
    if lock.pending_holder == Some(holder) {
        lock.pending_holder = None;
    }
    if lock.exclusive_holder == Some(holder) {
        lock.exclusive_holder = None;
    }
}

fn release_exclusive_pending_reserved(lock: &mut FileLockState, holder: u64) {
    if lock.exclusive_holder == Some(holder) {
        lock.exclusive_holder = None;
    }
    if lock.pending_holder == Some(holder) {
        lock.pending_holder = None;
    }
    if lock.reserved_holder == Some(holder) {
        lock.reserved_holder = None;
    }
}

fn release_exclusive_pending(lock: &mut FileLockState, holder: u64) {
    if lock.exclusive_holder == Some(holder) {
        lock.exclusive_holder = None;
    }
    if lock.pending_holder == Some(holder) {
        lock.pending_holder = None;
    }
}

fn shm_lock_range(
    handle: &OrionVfsHandle,
    offset: u32,
    count: u32,
    mode: ShmLockMode,
) -> VfsResult<()> {
    if count == 0 {
        return Ok(());
    }
    let end = offset
        .checked_add(count)
        .ok_or(vars::SQLITE_IOERR_SHMLOCK)?;
    let mut lock = handle
        .shm_lock
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_SHMLOCK)?;

    match mode {
        ShmLockMode::LockShared => {
            for byte in offset..end {
                if lock
                    .bytes
                    .get(&byte)
                    .and_then(|state| state.exclusive_holder)
                    .is_some_and(|holder| holder != handle.id)
                {
                    return Err(vars::SQLITE_BUSY);
                }
            }
            for byte in offset..end {
                lock.bytes
                    .entry(byte)
                    .or_default()
                    .shared_holders
                    .insert(handle.id);
            }
        }
        ShmLockMode::LockExclusive => {
            for byte in offset..end {
                if let Some(state) = lock.bytes.get(&byte) {
                    if state
                        .exclusive_holder
                        .is_some_and(|holder| holder != handle.id)
                        || state
                            .shared_holders
                            .iter()
                            .any(|holder| *holder != handle.id)
                    {
                        return Err(vars::SQLITE_BUSY);
                    }
                }
            }
            for byte in offset..end {
                let state = lock.bytes.entry(byte).or_default();
                state.shared_holders.remove(&handle.id);
                state.exclusive_holder = Some(handle.id);
            }
        }
        ShmLockMode::UnlockShared => {
            for byte in offset..end {
                if let Some(state) = lock.bytes.get_mut(&byte) {
                    state.shared_holders.remove(&handle.id);
                }
            }
            lock.bytes.retain(|_, state| !state.is_empty());
        }
        ShmLockMode::UnlockExclusive => {
            for byte in offset..end {
                if let Some(state) = lock.bytes.get_mut(&byte) {
                    if state.exclusive_holder == Some(handle.id) {
                        state.exclusive_holder = None;
                    }
                }
            }
            lock.bytes.retain(|_, state| !state.is_empty());
        }
    }
    Ok(())
}

fn release_shm_locks(handle: &OrionVfsHandle) -> VfsResult<()> {
    let mut lock = handle
        .shm_lock
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_SHMLOCK)?;
    for state in lock.bytes.values_mut() {
        state.shared_holders.remove(&handle.id);
        if state.exclusive_holder == Some(handle.id) {
            state.exclusive_holder = None;
        }
    }
    lock.bytes.retain(|_, state| !state.is_empty());
    Ok(())
}

fn release_shm_mapping(handle: &OrionVfsHandle, delete: bool) -> VfsResult<()> {
    let mut shm = handle.shm.lock().map_err(|_| vars::SQLITE_IOERR_SHMOPEN)?;
    shm.mapped_holders.remove(&handle.id);
    if delete && shm.mapped_holders.is_empty() {
        shm.regions.clear();
    }
    Ok(())
}

impl ShmByteLock {
    fn is_empty(&self) -> bool {
        self.shared_holders.is_empty() && self.exclusive_holder.is_none()
    }
}

fn drain_pending_ops(handle: &OrionVfsHandle) -> VfsResult<Vec<VfsFileOp>> {
    let mut pending = handle.pending_ops.lock().map_err(|_| vars::SQLITE_IOERR)?;
    Ok(std::mem::take(&mut *pending))
}

fn clear_unmaterialized_local_create<F>(
    cache_root: &Path,
    database: &str,
    logical_path: &str,
    file_store: &F,
) -> VfsResult<()>
where
    F: SqliteFileStore + ?Sized,
{
    let shared_state = shared_database_file_state(cache_root, database, logical_path)?;
    if !shared_state.locally_created.load(Ordering::Acquire) {
        return Ok(());
    }
    let exists = file_store
        .exists(logical_path)
        .map_err(|_| vars::SQLITE_IOERR_CLOSE)?;
    if !exists {
        shared_state.locally_created.store(false, Ordering::Release);
    }
    Ok(())
}

fn overlay_file_size(handle: &OrionVfsHandle, base_size: usize) -> VfsResult<usize> {
    let mut size = base_size;
    for op in handle
        .pending_ops
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_FSTAT)?
        .iter()
    {
        match op {
            VfsFileOp::Delete => size = 0,
            VfsFileOp::Truncate { size: new_size } => size = *new_size as usize,
            VfsFileOp::Write(write) => {
                size = size.max(write.offset as usize + write.bytes.len());
            }
        }
    }
    Ok(size)
}

fn overlay_pending_ops(
    handle: &OrionVfsHandle,
    read_offset: u64,
    data: &mut [u8],
) -> VfsResult<()> {
    let read_end = read_offset + data.len() as u64;
    for op in handle
        .pending_ops
        .lock()
        .map_err(|_| vars::SQLITE_IOERR_READ)?
        .iter()
    {
        match op {
            VfsFileOp::Delete => data.fill(0),
            VfsFileOp::Truncate { size } => {
                if *size < read_end {
                    let fill_start = size.saturating_sub(read_offset) as usize;
                    let fill_start = fill_start.min(data.len());
                    data[fill_start..].fill(0);
                }
            }
            VfsFileOp::Write(write) => {
                let write_start = write.offset;
                let write_end = write.offset + write.bytes.len() as u64;
                if write_end <= read_offset || write_start >= read_end {
                    continue;
                }
                let copy_start = write_start.max(read_offset);
                let copy_end = write_end.min(read_end);
                let data_start = (copy_start - read_offset) as usize;
                let write_start = (copy_start - write.offset) as usize;
                let len = (copy_end - copy_start) as usize;
                data[data_start..data_start + len]
                    .copy_from_slice(&write.bytes[write_start..write_start + len]);
            }
        }
    }
    Ok(())
}

fn file_kind(kind: OpenKind) -> FileKind {
    match kind {
        OpenKind::MainDb => FileKind::MainDb,
        OpenKind::Wal => FileKind::Wal,
        OpenKind::MainJournal | OpenKind::SuperJournal => FileKind::Journal,
        OpenKind::TempDb | OpenKind::TempJournal | OpenKind::TransientDb | OpenKind::SubJournal => {
            FileKind::Temp
        }
        OpenKind::Unknown => FileKind::Other,
    }
}

fn file_kind_for_open(kind: OpenKind, path: &str) -> FileKind {
    match file_kind(kind) {
        FileKind::Other => file_kind_from_path(path),
        known => known,
    }
}

fn file_kind_from_path(path: &str) -> FileKind {
    if path.ends_with("-wal") {
        FileKind::Wal
    } else if path.ends_with("-journal") {
        FileKind::Journal
    } else if path.ends_with(".db") || !path.contains('.') {
        FileKind::MainDb
    } else {
        FileKind::Other
    }
}

fn should_replicate(kind: FileKind) -> bool {
    matches!(kind, FileKind::MainDb | FileKind::Wal | FileKind::Journal)
}

fn trace_latency(args: std::fmt::Arguments<'_>) {
    if std::env::var_os("ORION_TRACE_LATENCY").is_some() {
        eprintln!("orion latency {args}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitDecision, RecordingCommitSink};
    use rusqlite::{Connection, OpenFlags};
    use sqlite_plugin::flags::OpenOpts;
    use sqlite_plugin::vars;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn sync_commits_persistent_writes_to_sink_before_acknowledgement() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                Some("tenant-a.db-wal"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
                ),
            )
            .unwrap();

        vfs.write(&mut handle, 32, b"wal-frame").unwrap();
        assert!(sink.committed_batches().is_empty());

        vfs.sync(&mut handle).unwrap();
        let committed = sink.committed_batches();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].batch.database, "tenant-a");
        assert_eq!(committed[0].batch.file_kind, FileKind::Wal);
        assert_eq!(
            committed[0].batch.ops[0],
            VfsFileOp::Write(VfsWrite {
                offset: 32,
                bytes: b"wal-frame".to_vec()
            })
        );
    }

    #[test]
    fn unknown_open_kind_infers_main_database_from_db_path() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                Some("tenant-a.db"),
                OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE),
            )
            .unwrap();

        vfs.write(&mut handle, 0, b"SQLite format 3\0").unwrap();
        vfs.sync(&mut handle).unwrap();

        let committed = sink.committed_batches();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].batch.file_kind, FileKind::MainDb);
    }

    #[test]
    fn temp_files_are_not_replicated() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                None,
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE
                        | vars::SQLITE_OPEN_CREATE
                        | vars::SQLITE_OPEN_TEMP_DB,
                ),
            )
            .unwrap();

        vfs.write(&mut handle, 0, b"scratch").unwrap();
        vfs.sync(&mut handle).unwrap();

        assert!(sink.committed_batches().is_empty());
    }

    #[test]
    fn delete_commits_replicated_files_before_removal() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                Some("tenant-a.db-wal"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
                ),
            )
            .unwrap();

        vfs.write(&mut handle, 0, b"wal-frame").unwrap();
        vfs.sync(&mut handle).unwrap();
        assert!(vfs.access("tenant-a.db-wal", AccessFlags::Exists).unwrap());

        vfs.delete("tenant-a.db-wal").unwrap();

        let committed = sink.committed_batches();
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[1].batch.file_kind, FileKind::Wal);
        assert_eq!(committed[1].batch.ops, vec![VfsFileOp::Delete]);
        assert!(!vfs.access("tenant-a.db-wal", AccessFlags::Exists).unwrap());
    }

    #[test]
    fn delete_on_close_commits_replicated_files_before_removal() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let handle = vfs
            .open(
                Some("tenant-a.db-journal"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE
                        | vars::SQLITE_OPEN_CREATE
                        | vars::SQLITE_OPEN_MAIN_JOURNAL
                        | vars::SQLITE_OPEN_DELETEONCLOSE,
                ),
            )
            .unwrap();

        vfs.close(handle).unwrap();

        let committed = sink.committed_batches();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].batch.file_kind, FileKind::Journal);
        assert_eq!(committed[0].batch.ops, vec![VfsFileOp::Delete]);
    }

    #[test]
    fn close_does_not_commit_unsynced_replicated_writes() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                Some("tenant-a.db"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE
                        | vars::SQLITE_OPEN_CREATE
                        | vars::SQLITE_OPEN_MAIN_DB,
                ),
            )
            .unwrap();

        vfs.write(&mut handle, 0, b"sqlite-page").unwrap();
        vfs.close(handle).unwrap();

        assert!(sink.committed_batches().is_empty());
        let err = match vfs.open(
            Some("tenant-a.db"),
            OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_MAIN_DB),
        ) {
            Ok(_) => panic!("unsynced replicated create unexpectedly remained openable"),
            Err(err) => err,
        };
        assert_eq!(err, vars::SQLITE_CANTOPEN);
        assert!(!vfs.access("tenant-a.db", AccessFlags::Exists).unwrap());
    }

    #[test]
    fn replicated_writes_become_visible_to_other_handles_after_xsync() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_MAIN_DB,
        );
        let mut writer = vfs.open(Some("tenant-a.db"), opts).unwrap();
        let mut reader = vfs.open(Some("tenant-a.db"), opts).unwrap();

        vfs.write(&mut writer, 0, b"visible-now").unwrap();
        assert!(sink.committed_batches().is_empty());

        let mut data = [0; 11];
        assert_eq!(vfs.read(&mut writer, 0, &mut data).unwrap(), 11);
        assert_eq!(&data, b"visible-now");

        data.fill(0);
        assert_eq!(vfs.read(&mut reader, 0, &mut data).unwrap(), 0);
        assert_eq!(&data, &[0; 11]);

        vfs.sync(&mut writer).unwrap();
        assert_eq!(sink.committed_batches().len(), 1);

        assert_eq!(vfs.read(&mut reader, 0, &mut data).unwrap(), 11);
        assert_eq!(&data, b"visible-now");

        vfs.close(writer).unwrap();
        vfs.close(reader).unwrap();
    }

    #[test]
    fn open_without_create_does_not_create_missing_file() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();

        let err = match vfs.open(
            Some("missing.db"),
            OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_MAIN_DB),
        ) {
            Ok(_) => panic!("open without create unexpectedly created a missing file"),
            Err(err) => err,
        };
        assert_eq!(err, vars::SQLITE_CANTOPEN);
        assert!(!vfs.access("missing.db", AccessFlags::Exists).unwrap());
    }

    #[test]
    fn create_open_does_not_materialize_replicated_file_before_sync() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let mut handle = vfs
            .open(
                Some("tenant-a.db"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE
                        | vars::SQLITE_OPEN_CREATE
                        | vars::SQLITE_OPEN_MAIN_DB,
                ),
            )
            .unwrap();

        assert!(vfs.access("tenant-a.db", AccessFlags::Exists).unwrap());
        assert_eq!(vfs.file_size(&mut handle).unwrap(), 0);
        vfs.close(handle).unwrap();
        assert!(sink.committed_batches().is_empty());
        assert!(!vfs.access("tenant-a.db", AccessFlags::Exists).unwrap());
    }

    #[test]
    fn close_clears_unsynced_replicated_wal_create_marker() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let handle = vfs
            .open(
                Some("tenant-a.db-wal"),
                OpenOpts::new(
                    vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
                ),
            )
            .unwrap();

        assert!(vfs.access("tenant-a.db-wal", AccessFlags::Exists).unwrap());
        vfs.close(handle).unwrap();

        assert!(sink.committed_batches().is_empty());
        assert!(!vfs.access("tenant-a.db-wal", AccessFlags::Exists).unwrap());
        let err = match vfs.open(
            Some("tenant-a.db-wal"),
            OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_WAL),
        ) {
            Ok(_) => panic!("unsynced replicated WAL create unexpectedly remained openable"),
            Err(err) => err,
        };
        assert_eq!(err, vars::SQLITE_CANTOPEN);
    }

    #[test]
    fn readonly_open_missing_file_fails() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();

        let err = match vfs.open(
            Some("missing.db"),
            OpenOpts::new(vars::SQLITE_OPEN_READONLY | vars::SQLITE_OPEN_MAIN_DB),
        ) {
            Ok(_) => panic!("readonly open unexpectedly created a missing file"),
            Err(err) => err,
        };
        assert_eq!(err, vars::SQLITE_CANTOPEN);
    }

    #[test]
    fn main_lock_shared_allows_multiple_readers() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_MAIN_DB,
        );
        let mut first = vfs.open(Some("tenant-a.db"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db"), opts).unwrap();

        vfs.lock(&mut first, LockLevel::Shared).unwrap();
        vfs.lock(&mut second, LockLevel::Shared).unwrap();
        assert!(!vfs.check_reserved_lock(&mut second).unwrap());

        vfs.close(first).unwrap();
        vfs.close(second).unwrap();
    }

    #[test]
    fn main_locks_are_shared_across_vfs_registrations_for_same_database_file() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let cache_root = tmp.path().join("cache");
        let vfs_a = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", cache_root.clone()),
            Arc::clone(&sink),
        )
        .unwrap();
        let vfs_b = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", cache_root),
            Arc::clone(&sink),
        )
        .unwrap();
        let create_opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_MAIN_DB,
        );
        let reopen_opts = OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_MAIN_DB);
        let mut first = vfs_a.open(Some("tenant-a.db"), create_opts).unwrap();
        let mut second = vfs_b.open(Some("tenant-a.db"), reopen_opts).unwrap();

        vfs_a.lock(&mut first, LockLevel::Shared).unwrap();
        vfs_a.lock(&mut first, LockLevel::Reserved).unwrap();

        assert!(vfs_b.check_reserved_lock(&mut second).unwrap());
        assert_eq!(
            vfs_b.lock(&mut second, LockLevel::Reserved).unwrap_err(),
            vars::SQLITE_BUSY
        );

        vfs_a.close(first).unwrap();
        vfs_b.lock(&mut second, LockLevel::Reserved).unwrap();
        vfs_b.close(second).unwrap();
    }

    #[test]
    fn main_lock_reserved_is_single_owner_and_visible_to_other_handles() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_MAIN_DB,
        );
        let mut first = vfs.open(Some("tenant-a.db"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db"), opts).unwrap();

        vfs.lock(&mut first, LockLevel::Shared).unwrap();
        vfs.lock(&mut second, LockLevel::Shared).unwrap();
        vfs.lock(&mut first, LockLevel::Reserved).unwrap();

        assert!(vfs.check_reserved_lock(&mut second).unwrap());
        assert_eq!(
            vfs.lock(&mut second, LockLevel::Reserved).unwrap_err(),
            vars::SQLITE_BUSY
        );

        vfs.close(first).unwrap();
        vfs.lock(&mut second, LockLevel::Reserved).unwrap();
        vfs.close(second).unwrap();
    }

    #[test]
    fn main_lock_exclusive_requires_other_handles_unlocked() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_MAIN_DB,
        );
        let mut first = vfs.open(Some("tenant-a.db"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db"), opts).unwrap();

        vfs.lock(&mut first, LockLevel::Shared).unwrap();
        vfs.lock(&mut second, LockLevel::Shared).unwrap();
        assert_eq!(
            vfs.lock(&mut first, LockLevel::Exclusive).unwrap_err(),
            vars::SQLITE_BUSY
        );

        vfs.unlock(&mut second, LockLevel::Unlocked).unwrap();
        vfs.lock(&mut first, LockLevel::Exclusive).unwrap();

        vfs.close(first).unwrap();
        vfs.close(second).unwrap();
    }

    #[test]
    fn shm_lock_shared_allows_multiple_holders_but_exclusive_conflicts() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
        );
        let mut first = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();

        vfs.shm_lock(&mut first, 3, 1, ShmLockMode::LockShared)
            .unwrap();
        vfs.shm_lock(&mut second, 3, 1, ShmLockMode::LockShared)
            .unwrap();
        assert_eq!(
            vfs.shm_lock(&mut first, 3, 1, ShmLockMode::LockExclusive)
                .unwrap_err(),
            vars::SQLITE_BUSY
        );

        vfs.shm_lock(&mut second, 3, 1, ShmLockMode::UnlockShared)
            .unwrap();
        vfs.shm_lock(&mut first, 3, 1, ShmLockMode::LockExclusive)
            .unwrap();

        vfs.close(first).unwrap();
        vfs.close(second).unwrap();
    }

    #[test]
    fn shm_lock_range_acquire_is_atomic_on_conflict() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
        );
        let mut first = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();

        vfs.shm_lock(&mut first, 4, 1, ShmLockMode::LockExclusive)
            .unwrap();
        assert_eq!(
            vfs.shm_lock(&mut second, 3, 2, ShmLockMode::LockShared)
                .unwrap_err(),
            vars::SQLITE_BUSY
        );
        vfs.shm_lock(&mut first, 4, 1, ShmLockMode::UnlockExclusive)
            .unwrap();
        vfs.shm_lock(&mut first, 3, 1, ShmLockMode::LockExclusive)
            .unwrap();

        vfs.close(first).unwrap();
        vfs.close(second).unwrap();
    }

    #[test]
    fn shm_map_region_pointer_survives_later_region_extension() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
        );
        let mut handle = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();

        let region0 = vfs
            .shm_map(&mut handle, 0, 32 * 1024, true)
            .unwrap()
            .unwrap();
        unsafe {
            region0.as_ptr().write(0x2a);
        }
        let _region1 = vfs
            .shm_map(&mut handle, 1, 32 * 1024, true)
            .unwrap()
            .unwrap();

        assert_eq!(unsafe { *region0.as_ptr() }, 0x2a);
        vfs.close(handle).unwrap();
    }

    #[test]
    fn shm_unmap_delete_waits_for_last_mapping() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        let opts = OpenOpts::new(
            vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_CREATE | vars::SQLITE_OPEN_WAL,
        );
        let mut first = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();
        let mut second = vfs.open(Some("tenant-a.db-wal"), opts).unwrap();

        let first_region = vfs
            .shm_map(&mut first, 0, 32 * 1024, true)
            .unwrap()
            .unwrap();
        let second_region = vfs
            .shm_map(&mut second, 0, 32 * 1024, false)
            .unwrap()
            .unwrap();
        unsafe {
            first_region.as_ptr().write(0x7b);
        }

        vfs.shm_unmap(&mut first, true).unwrap();
        assert_eq!(unsafe { *second_region.as_ptr() }, 0x7b);

        vfs.shm_unmap(&mut second, true).unwrap();
        let mut third = vfs
            .open(
                Some("tenant-a.db-wal"),
                OpenOpts::new(vars::SQLITE_OPEN_READWRITE | vars::SQLITE_OPEN_WAL),
            )
            .unwrap();
        let remapped = vfs
            .shm_map(&mut third, 0, 32 * 1024, true)
            .unwrap()
            .unwrap();
        assert_eq!(unsafe { *remapped.as_ptr() }, 0);

        vfs.close(first).unwrap();
        vfs.close(second).unwrap();
        vfs.close(third).unwrap();
    }

    #[test]
    fn sqlite_transaction_through_registered_vfs_commits_sync_batches() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs_name = format!("orion_test_{}", std::process::id());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        register_orion_vfs(&vfs_name, vfs, false).unwrap();

        let url = format!("file:tenant-a.db?vfs={vfs_name}");
        let conn = Connection::open_with_flags(
            url,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();

        let journal_mode: String = conn
            .query_row("pragma journal_mode = wal", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");

        conn.execute_batch(
            r#"
            create table services (tenant_id text, service_id text, weight integer);
            begin immediate;
            insert into services values ('acme', 'api', 20);
            commit;
            "#,
        )
        .unwrap();

        let weight: i64 = conn
            .query_row(
                "select weight from services where tenant_id = 'acme' and service_id = 'api'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(weight, 20);

        let committed = sink.committed_batches();
        assert!(
            committed
                .iter()
                .any(|batch| batch.batch.file_kind == FileKind::Wal),
            "expected SQLite WAL sync to pass through the Orion commit sink, got {committed:?}"
        );

        drop(conn);
        let reopened = Connection::open_with_flags(
            format!("file:tenant-a.db?vfs={vfs_name}"),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        let reopened_weight: i64 = reopened
            .query_row(
                "select weight from services where tenant_id = 'acme' and service_id = 'api'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reopened_weight, 20);
    }

    #[test]
    fn sqlite_wal_reader_observes_committed_snapshot_during_another_writer_transaction() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs_name = format!("orion_reader_writer_test_{}", std::process::id());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        register_orion_vfs(&vfs_name, vfs, false).unwrap();

        let url = format!("file:tenant-a.db?vfs={vfs_name}");
        let writer = Connection::open_with_flags(
            &url,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        let reader = Connection::open_with_flags(
            &url,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();

        let journal_mode: String = writer
            .query_row("pragma journal_mode = wal", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        writer
            .execute_batch(
                r#"
                create table services (id integer primary key);
                insert into services values (1);
                begin immediate;
                insert into services values (2);
                "#,
            )
            .unwrap();

        let visible_count: i64 = reader
            .query_row("select count(*) from services", [], |row| row.get(0))
            .unwrap();
        assert_eq!(visible_count, 1);

        writer.execute_batch("commit").unwrap();
        let committed_count: i64 = reader
            .query_row("select count(*) from services", [], |row| row.get(0))
            .unwrap();
        assert_eq!(committed_count, 2);
    }

    #[test]
    fn sqlite_wal_writer_contention_returns_busy_until_writer_commits() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs_name = format!("orion_writer_contention_test_{}", std::process::id());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        register_orion_vfs(&vfs_name, vfs, false).unwrap();

        let url = format!("file:tenant-a.db?vfs={vfs_name}");
        let first = Connection::open_with_flags(
            &url,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        let second = Connection::open_with_flags(
            &url,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();

        let journal_mode: String = first
            .query_row("pragma journal_mode = wal", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        first
            .execute_batch("create table services (id integer primary key);")
            .unwrap();
        first.pragma_update(None, "busy_timeout", 25).unwrap();
        second.pragma_update(None, "busy_timeout", 25).unwrap();

        first
            .execute_batch(
                r#"
                begin immediate;
                insert into services values (1);
                "#,
            )
            .unwrap();

        let err = second
            .execute("insert into services values (2)", [])
            .unwrap_err();
        assert!(
            err.to_string().contains("locked") || err.to_string().contains("busy"),
            "expected writer contention to surface a busy/locked error, got {err}"
        );

        first.execute_batch("commit").unwrap();
        second
            .execute("insert into services values (2)", [])
            .unwrap();
        let count: i64 = second
            .query_row("select count(*) from services", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn sqlite_operational_pragmas_work_through_registered_wal_vfs() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(RecordingCommitSink::default());
        let vfs_name = format!("orion_operational_pragma_test_{}", std::process::id());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        register_orion_vfs(&vfs_name, vfs, false).unwrap();

        let conn = Connection::open_with_flags(
            format!("file:tenant-a.db?vfs={vfs_name}"),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();

        let journal_mode: String = conn
            .query_row("pragma journal_mode = wal", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        conn.execute_batch(
            r#"
            create table services (id integer primary key, name text not null);
            insert into services values (1, 'api'), (2, 'worker');
            "#,
        )
        .unwrap();

        let integrity: String = conn
            .query_row("pragma integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");

        let quick_check: String = conn
            .query_row("pragma quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(quick_check, "ok");

        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
            .query_row("pragma wal_checkpoint(passive)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(busy, 0);
        assert!(log_frames >= 0);
        assert!(checkpointed_frames >= 0);

        let count: i64 = conn
            .query_row("select count(*) from services", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn sqlite_transaction_fails_when_commit_sink_rejects_sync_batch() {
        let tmp = TempDir::new().unwrap();
        let sink = Arc::new(FailingCommitSink);
        let vfs_name = format!("orion_failing_test_{}", std::process::id());
        let vfs = OrionVfs::new(
            OrionVfsConfig::new("tenant-a", tmp.path().join("cache")),
            Arc::clone(&sink),
        )
        .unwrap();
        register_orion_vfs(&vfs_name, vfs, false).unwrap();

        let url = format!("file:tenant-a.db?vfs={vfs_name}");
        let conn = Connection::open_with_flags(
            url,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();

        let err = conn
            .execute_batch("create table services (tenant_id text);")
            .unwrap_err();
        assert!(
            err.to_string().contains("disk I/O error"),
            "expected SQLite to surface a sync failure when Raft commit fails, got {err}"
        );
    }

    struct FailingCommitSink;

    impl RaftWalCommitSink for FailingCommitSink {
        fn commit_sync_batch(&self, _batch: VfsSyncBatch) -> anyhow::Result<CommitDecision> {
            anyhow::bail!("injected Raft commit failure")
        }
    }
}
