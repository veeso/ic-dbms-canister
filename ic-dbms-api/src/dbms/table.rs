//! This module contains types related to database tables.

mod column_def;
mod record;
mod schema;

use thiserror::Error;

pub use self::column_def::{ColumnDef, ForeignKeyDef};
pub use self::record::{
    InsertRecord, TableColumns, TableName, TableRecord, UpdateRecord, ValuesSource,
};
pub use self::schema::{TableFingerprint, TableSchema};

/// Table related errors
#[derive(Debug, Error)]
pub enum TableError {
    #[error("Table not found")]
    TableNotFound,
    #[error("Schema mismatch")]
    SchemaMismatch,
}
