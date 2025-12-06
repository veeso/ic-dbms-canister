use crate::dbms::table::{ColumnDef, TableSchema};
use crate::dbms::value::Value;
use crate::prelude::{Filter, IcDbmsResult};

pub type TableName = &'static str;
pub type TableColumns = Vec<(ValuesSource, Vec<(ColumnDef, Value)>)>;

/// Indicates the source of the column values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValuesSource {
    /// Column values belong to the current table.
    This,
    /// Column values belong to a foreign table.
    Foreign {
        table: TableName,
        column: &'static str,
    },
}

/// This trait represents a record returned by a [`crate::dbms::query::Query`] for a table.
pub trait TableRecord {
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self>;

    /// Constructs [`TableRecord`] from a list of column values grouped by table.
    fn from_values(values: TableColumns) -> Self;

    /// Converts the record into a list of column [`Value`]s.
    fn to_values(&self) -> Vec<(ColumnDef, Value)>;
}

/// This trait represents a record for inserting into a table.
pub trait InsertRecord: Sized + Clone {
    /// The [`TableRecord`] type associated with this table schema.
    type Record: TableRecord;
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self::Record>;

    /// Creates an insert record from a list of column [`Value`]s.
    fn from_values(values: &[(ColumnDef, Value)]) -> IcDbmsResult<Self>;

    /// Converts the record into a list of column [`Value`]s for insertion.
    fn into_values(self) -> Vec<(ColumnDef, Value)>;

    /// Converts the insert record into the corresponding table record.
    fn into_record(self) -> Self::Schema;
}

/// This trait represents a record for updating a table.
pub trait UpdateRecord: Sized {
    /// The [`TableRecord`] type associated with this table schema.
    type Record: TableRecord;
    /// The table schema associated with this record.
    type Schema: TableSchema<Record = Self::Record>;

    /// Creates an update record from a list of column [`Value`]s and an optional [`Filter`] for the where clause.
    fn from_values(values: &[(ColumnDef, Value)], where_clause: Option<Filter>) -> Self;

    /// Get the list of column [`Value`]s to be updated.
    fn update_values(&self) -> Vec<(ColumnDef, Value)>;

    /// Get the [`Filter`] condition for the update operation.
    fn where_clause(&self) -> Option<Filter>;
}
