use super::Query;
use crate::dbms::table::{TableRecord, TableSchema};
use crate::memory::MemoryResult;

/// TableOps is a trait which must be implemented by a table to export CRUD operations on it.
///
/// The implementation must also provide a type for the record returned from select.
pub trait TableOps {
    /// The type of the [`TableRecord`] returned from select operations.
    type Record: TableRecord;
    /// The [`TableSchema`] associated with this table.
    type Schema: TableSchema;

    /// Select records from the table based on the provided query.
    fn select(query: Query<Self::Schema>) -> MemoryResult<Vec<Self::Record>>;

    /// Insert a new record into the table.
    fn insert(&self) -> MemoryResult<()>;

    /// Update existing records in the table based on the provided query.
    fn update(query: Query<Self::Schema>) -> MemoryResult<()>;

    /// Delete records from the table based on the provided query.
    fn delete(query: Query<Self::Schema>) -> MemoryResult<()>;
}
