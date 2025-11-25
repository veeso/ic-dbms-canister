//! This module contains types related to database tables.

mod column_def;
mod record;
mod schema;

pub use self::column_def::{ColumnDef, ForeignKeyDef};
pub use self::record::{InsertRecord, TableRecord, UpdateRecord};
pub use self::schema::{TableFingerprint, TableSchema};
