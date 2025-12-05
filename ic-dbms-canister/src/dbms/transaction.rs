//! This module contains the implementation of transactions within the DBMS engine.

mod overlay;
mod session;

pub use self::overlay::DatabaseOverlay;
pub use self::session::{TRANSACTION_SESSION, TransactionId, TransactionSession};
use crate::IcDbmsResult;
use crate::dbms::table::ColumnDef;
use crate::dbms::value::Value;
use crate::prelude::TableSchema;

/// A transaction represents a sequence of operations performed as a single logical unit of work.
#[derive(Debug, Default)]
pub struct Transaction {
    /// Stack of operations performed in this transaction.
    ops: Vec<TransactionOp>,
    /// Overlay to track uncommitted changes.
    overlay: DatabaseOverlay,
}

impl Transaction {
    /// Insert a new `insert` operation into the transaction.
    pub fn insert<T>(&mut self, values: Vec<(ColumnDef, Value)>) -> IcDbmsResult<()>
    where
        T: TableSchema,
    {
        self.overlay.insert::<T>(values.clone())?;
        self.ops.push(TransactionOp::Insert {
            table: T::table_name(),
            values,
        });
        Ok(())
    }

    /// Iterate over the operations performed in this transaction.
    pub fn operations(&self) -> &Vec<TransactionOp> {
        &self.ops
    }

    /// Get a reference to the [`DatabaseOverlay`] associated with this transaction.
    pub fn overlay(&self) -> &DatabaseOverlay {
        &self.overlay
    }

    /// Get a mutable reference to the [`DatabaseOverlay`] associated with this transaction.
    pub fn overlay_mut(&mut self) -> &mut DatabaseOverlay {
        &mut self.overlay
    }
}

/// An enum representing possible errors that can occur during transaction operations.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("No active transaction")]
    NoActiveTransaction,
}

/// An enum representing the different types of operations that can be performed within a transaction.
#[derive(Debug)]
pub enum TransactionOp {
    Insert {
        table: &'static str,
        values: Vec<(ColumnDef, Value)>,
    },
}
