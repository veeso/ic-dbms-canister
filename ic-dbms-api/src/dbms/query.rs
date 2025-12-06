//! This module exposes all the types related to queries that can be performed on the DBMS.

mod builder;
mod delete;
mod filter;

use std::marker::PhantomData;

use thiserror::Error;

pub use self::builder::QueryBuilder;
pub use self::delete::DeleteBehavior;
pub use self::filter::Filter;
use crate::dbms::table::TableSchema;
use crate::dbms::value::Value;
use crate::memory::MemoryError;

/// The result type for query operations.
pub type QueryResult<T> = Result<T, QueryError>;

/// An enum representing possible errors that can occur during query operations.
#[derive(Debug, Error)]
pub enum QueryError {
    /// The specified primary key value already exists in the table.
    #[error("Primary key conflict: record with the same primary key already exists")]
    PrimaryKeyConflict,

    /// A foreign key references a non-existent record in another table.
    #[error("Broken foreign key reference to table '{table}' with key '{key:?}'")]
    BrokenForeignKeyReference { table: &'static str, key: Value },

    /// Tried to delete or update a record that is referenced by another table's foreign key.
    #[error("Foreign key constraint violation on table '{referencing_table}' for field '{field}'")]
    ForeignKeyConstraintViolation {
        referencing_table: &'static str,
        field: &'static str,
    },

    /// Tried to reference a column that does not exist in the table schema.
    #[error("Unknown column: {0}")]
    UnknownColumn(String),

    /// Tried to insert a record missing non-nullable fields.
    #[error("Missing non-nullable field: {0}")]
    MissingNonNullableField(&'static str),

    /// Tried to cast or compare values of incompatible types (e.g. Integer vs Text).
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        column: &'static str,
        expected: &'static str,
        found: &'static str,
    },

    /// The specified transaction was not found or has expired.
    #[error("transaction not found")]
    TransactionNotFound,

    /// Query contains syntactically or semantically invalid conditions.
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Generic constraint violation (e.g., UNIQUE, CHECK, etc.)
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    /// The memory allocator or memory manager failed to allocate or access stable memory.
    #[error("Memory error: {0}")]
    MemoryError(MemoryError),

    /// The table or schema was not found.
    #[error("Table not found: {0}")]
    TableNotFound(&'static str),

    /// The record identified by the given key or filter does not exist.
    #[error("Record not found")]
    RecordNotFound,

    /// Any low-level IO or serialization/deserialization issue.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Generic catch-all error (for internal, unexpected conditions).
    #[error("Internal error: {0}")]
    Internal(String),
}

/// An enum representing the fields to select in a query.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Select {
    #[default]
    All,
    Columns(Vec<&'static str>),
}

/// An enum representing the direction of ordering in a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

/// A struct representing a query in the DBMS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query<T>
where
    T: TableSchema,
{
    /// Fields to select in the query.
    columns: Select,
    /// Relations to eagerly load with the main records.
    pub eager_relations: Vec<&'static str>,
    /// [`Filter`] to apply to the query.
    pub filter: Option<Filter>,
    /// Order by clauses for sorting the results.
    pub order_by: Vec<(&'static str, OrderDirection)>,
    /// Limit on the number of records to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
    /// Marker for the table schema type.
    _marker: PhantomData<T>,
}

impl<T> Default for Query<T>
where
    T: TableSchema,
{
    fn default() -> Self {
        Self {
            columns: Select::All,
            eager_relations: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            _marker: PhantomData,
        }
    }
}

impl<T> Query<T>
where
    T: TableSchema,
{
    /// Creates a new [`QueryBuilder`] for building a query.
    pub fn builder() -> QueryBuilder<T> {
        QueryBuilder::default()
    }

    /// Returns whether all columns are selected in the query.
    pub fn all_selected(&self) -> bool {
        matches!(self.columns, Select::All)
    }

    /// Returns the list of columns to be selected in the query.
    pub fn columns(&self) -> Vec<&'static str> {
        match &self.columns {
            Select::All => T::columns().iter().map(|col| col.name).collect(),
            Select::Columns(cols) => cols.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::tests::User;

    #[test]
    fn test_should_build_default_query() {
        let query: Query<User> = Query::default();
        assert!(matches!(query.columns, Select::All));
        assert!(query.eager_relations.is_empty());
        assert!(query.filter.is_none());
        assert!(query.order_by.is_empty());
        assert!(query.limit.is_none());
        assert!(query.offset.is_none());
    }

    #[test]
    fn test_should_get_columns() {
        let query = Query::<User>::default();
        let columns = query.columns();
        assert_eq!(columns, vec!["id", "name",]);

        let query = Query::<User> {
            columns: Select::Columns(vec!["id"]),
            ..Default::default()
        };

        let columns = query.columns();
        assert_eq!(columns, vec!["id"]);
    }

    #[test]
    fn test_should_check_all_selected() {
        let query = Query::<User>::default();
        assert!(query.all_selected());
    }
}
