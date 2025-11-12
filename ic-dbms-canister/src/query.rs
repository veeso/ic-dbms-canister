//! This module exposes all the types related to queries that can be performed on the DBMS.

mod builder;
mod table_ops;

use thiserror::Error;

pub use self::builder::QueryBuilder;
pub use self::table_ops::TableOps;
use crate::memory::MemoryError;

/// The result type for query operations.
pub type QueryResult<T> = Result<T, QueryError>;

/// An enum representing possible errors that can occur during query operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// Attempted to create or modify a table with more than one primary key.
    #[error("Duplicate primary key defined in table schema")]
    DuplicatePrimaryKey,

    /// The specified primary key value already exists in the table.
    #[error("Primary key conflict: record with the same primary key already exists")]
    PrimaryKeyConflict,

    /// A foreign key references a non-existent record in another table.
    #[error("Broken foreign key reference to table '{table}' with key '{key}'")]
    BrokenForeignKeyReference { table: &'static str, key: String },

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
        expected: &'static str,
        found: &'static str,
    },

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

/// A struct representing a query in the DBMS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query;
