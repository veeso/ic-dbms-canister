//! This module contains the implementation of transactions within the DBMS engine.

mod overlay;
mod session;

pub use self::overlay::DatabaseOverlay;
pub use self::session::{TRANSACTION_SESSION, TransactionId, TransactionSession};
use crate::dbms::table::{UntypedInsertRecord, UntypedUpdateRecord};
use crate::prelude::Filter;

/// A transaction represents a sequence of operations performed as a single logical unit of work.
#[derive(Debug, Default, Clone)]
pub struct Transaction {
    operations: Vec<Operation>,
    pub overlay: DatabaseOverlay,
}

/// An operation within a [`Transaction`].
#[derive(Debug, Clone)]
pub enum Operation {
    /// An insert operation. The first element is the table name, and the second is the record to be inserted.
    Insert(&'static str, UntypedInsertRecord),
    /// An update operation. The first element is the table name, and the second is the record to be updated.
    Update(&'static str, UntypedUpdateRecord),
    /// A delete operation. The first element is the table name, and the second is an optional filter to specify which records to delete.
    Delete(&'static str, Option<Filter>),
}

/// An enum representing possible errors that can occur during transaction operations.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("No active transaction")]
    NoActiveTransaction,
}
