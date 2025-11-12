use super::Query;
use crate::memory::MemoryResult;

/// TableOps is a trait which must be implemented by a table to export CRUD operations on it.
///
/// The implementation must also provide a type for the record returned from select.
pub trait TableOps {
    /// The type of the record returned from select operations.
    type Record;

    /// Select records from the table based on the provided query.
    fn select(query: Query) -> MemoryResult<Vec<Self::Record>>;

    /// Insert a new record into the table.
    fn insert(&self) -> MemoryResult<()>;

    /// Update existing records in the table based on the provided query.
    fn update(query: Query) -> MemoryResult<()>;

    /// Delete records from the table based on the provided query.
    fn delete(query: Query) -> MemoryResult<()>;
}
