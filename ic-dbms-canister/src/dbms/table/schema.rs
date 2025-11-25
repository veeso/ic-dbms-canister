use std::hash::{Hash as _, Hasher as _};

use crate::dbms::table::column_def::{ColumnDef, ForeignKeyDef};
use crate::dbms::table::{InsertRecord, TableRecord, UpdateRecord};

/// A type representing a unique fingerprint for a table schema.
pub type TableFingerprint = u64;

/// Table schema representation.
///
/// It is used to define the structure of a database table.
pub trait TableSchema
where
    Self: 'static,
{
    /// The [`TableRecord`] type associated with this table schema;
    /// which is the data returned by a query.
    type Record: TableRecord<Schema = Self>;
    /// The [`InsertRecord`] type associated with this table schema.
    type Insert: InsertRecord<Schema = Self>;
    /// The [`UpdateRecord`] type associated with this table schema.
    type Update: UpdateRecord<Schema = Self>;

    /// Returns the name of the table.
    fn table_name() -> &'static str;

    /// Returns the column definitions of the table.
    fn columns() -> &'static [ColumnDef];

    /// Returns the name of the primary key column.
    fn primary_key() -> &'static str;

    /// Returns the foreign key definitions of the table.
    fn foreign_keys() -> &'static [ForeignKeyDef];

    /// Returns the fingerprint of the table schema.
    fn fingerprint() -> TableFingerprint {
        let mut hasher = std::hash::DefaultHasher::new();
        std::any::TypeId::of::<Self>().hash(&mut hasher);
        hasher.finish()
    }
}
