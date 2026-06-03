use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::commit::{VfsFileOp, VfsSyncBatch, VfsWrite};

pub trait SqliteFileStore: Send + Sync + 'static {
    fn exists(&self, path: &str) -> anyhow::Result<bool>;
    fn delete(&self, path: &str) -> anyhow::Result<()>;
    fn file_size(&self, path: &str) -> anyhow::Result<usize>;
    fn truncate(&self, path: &str, size: usize) -> anyhow::Result<()>;
    fn read_at(&self, path: &str, offset: u64, data: &mut [u8]) -> anyhow::Result<usize>;
    fn apply_batch(&self, batch: &VfsSyncBatch) -> anyhow::Result<()>;
    fn sync(&self, path: &str) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub struct LocalFileStore {
    root: PathBuf,
}

impl LocalFileStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn into_shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    fn local_path(&self, path: &str) -> anyhow::Result<PathBuf> {
        let relative = Path::new(path)
            .components()
            .filter_map(|component| match component {
                std::path::Component::Normal(part) => Some(part.to_owned()),
                _ => None,
            })
            .collect::<PathBuf>();
        if relative.as_os_str().is_empty() {
            anyhow::bail!("SQLite file path must not be empty");
        }
        Ok(self.root.join(relative))
    }
}

impl SqliteFileStore for LocalFileStore {
    fn exists(&self, path: &str) -> anyhow::Result<bool> {
        Ok(self.local_path(path)?.exists())
    }

    fn delete(&self, path: &str) -> anyhow::Result<()> {
        match std::fs::remove_file(self.local_path(path)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn file_size(&self, path: &str) -> anyhow::Result<usize> {
        match std::fs::metadata(self.local_path(path)?) {
            Ok(metadata) => Ok(metadata.len() as usize),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    fn truncate(&self, path: &str, size: usize) -> anyhow::Result<()> {
        let local_path = self.local_path(path)?;
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(local_path)?
            .set_len(size as u64)?;
        Ok(())
    }

    fn read_at(&self, path: &str, offset: u64, data: &mut [u8]) -> anyhow::Result<usize> {
        let local_path = self.local_path(path)?;
        let mut file = match OpenOptions::new().read(true).open(local_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        Ok(file.read(data)?)
    }

    fn apply_batch(&self, batch: &VfsSyncBatch) -> anyhow::Result<()> {
        for op in &batch.ops {
            match op {
                VfsFileOp::Delete => {
                    self.delete(&batch.file_path)?;
                }
                VfsFileOp::Truncate { size } => {
                    self.truncate(&batch.file_path, *size as usize)?;
                }
                VfsFileOp::Write(write) => {
                    let local_path = self.local_path(&batch.file_path)?;
                    if let Some(parent) = local_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let mut file = OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .truncate(false)
                        .open(local_path)?;
                    apply_writes(&mut file, std::slice::from_ref(write))?;
                    file.sync_all()?;
                }
            }
        }
        Ok(())
    }

    fn sync(&self, path: &str) -> anyhow::Result<()> {
        let local_path = self.local_path(path)?;
        if !local_path.exists() {
            return Ok(());
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(local_path)?
            .sync_all()?;
        Ok(())
    }
}

fn apply_writes(file: &mut std::fs::File, writes: &[VfsWrite]) -> anyhow::Result<()> {
    for write in writes {
        file.seek(SeekFrom::Start(write.offset))?;
        file.write_all(&write.bytes)?;
    }
    Ok(())
}
