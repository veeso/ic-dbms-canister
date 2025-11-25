use crate::dbms::table::{ColumnDef, TableSchema};
use crate::dbms::value::Value;
use crate::prelude::Filter;

/// This trait represents a record returned by a [`crate::dbms::query::Query`] for a table.
pub trait TableRecord {
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self>;

    /// Constructs [`TableRecord`] from a list of column values.
    fn from_values(values: &[(ColumnDef, Value)]) -> Self;

    /// Converts the record into a list of column [`Value`]s.
    fn to_values(&self) -> Vec<Value>;
}

/// This trait represents a record for inserting into a table.
pub trait InsertRecord {
    /// The [`TableRecord`] type associated with this table schema.
    type Record: TableRecord;
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self::Record>;

    /// Converts the record into a list of column [`Value`]s for insertion.
    fn into_values(self) -> Vec<Value>;
}

/// This trait represents a record for updating a table.
pub trait UpdateRecord {
    /// The [`TableRecord`] type associated with this table schema.
    type Record: TableRecord;
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self::Record>;

    /// Get the list of column [`Value`]s to be updated.
    fn update_values(&self) -> Vec<(ColumnDef, Value)>;

    /// Get the [`Filter`] condition for the update operation.
    fn where_clause(&self) -> Option<Filter>;
}
