use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteVfsBatch {
    pub database: String,
    pub file_path: String,
    pub file_kind: SqliteFileKind,
    pub ops: Vec<SqliteVfsOp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqliteVfsOp {
    Write(SqliteVfsWrite),
    Truncate { size: u64 },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteVfsWrite {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqliteFileKind {
    MainDb,
    Wal,
    Journal,
    Temp,
    Other,
}
