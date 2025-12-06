//! This module contains the implementation of transactions within the DBMS engine.

mod overlay;
mod session;

use ic_dbms_api::prelude::{ColumnDef, DeleteBehavior, IcDbmsResult, Value};

pub use self::overlay::DatabaseOverlay;
pub use self::session::{TRANSACTION_SESSION, TransactionSession};
use crate::prelude::{Filter, TableSchema, UpdateRecord as _};

/// A transaction represents a sequence of operations performed as a single logical unit of work.
#[derive(Debug, Default)]
pub struct Transaction {
    /// Stack of operations performed in this transaction.
    pub(super) operations: Vec<TransactionOp>,
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
        self.operations.push(TransactionOp::Insert {
            table: T::table_name(),
            values,
        });
        Ok(())
    }

    /// Insert a new `update` operation into the transaction.
    pub fn update<T>(
        &mut self,
        patch: T::Update,
        filter: Option<Filter>,
        primary_keys: Vec<Value>,
    ) -> IcDbmsResult<()>
    where
        T: TableSchema,
    {
        let patch_values = patch.update_values();
        let overlay_patch: Vec<_> = patch_values
            .iter()
            .map(|(col, val)| (col.name, val.clone()))
            .collect();

        for pk in primary_keys {
            self.overlay.update::<T>(pk, overlay_patch.clone());
        }

        self.operations.push(TransactionOp::Update {
            table: T::table_name(),
            patch: patch_values,
            filter,
        });
        Ok(())
    }

    /// Insert a new `delete` operation into the transaction.
    pub fn delete<T>(
        &mut self,
        behaviour: DeleteBehavior,
        filter: Option<Filter>,
        primary_keys: Vec<Value>,
    ) -> IcDbmsResult<()>
    where
        T: TableSchema,
    {
        for pk in primary_keys {
            self.overlay.delete::<T>(pk);
        }

        self.operations.push(TransactionOp::Delete {
            table: T::table_name(),
            behaviour,
            filter,
        });
        Ok(())
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

/// An enum representing the different types of operations that can be performed within a transaction.
#[derive(Debug)]
pub enum TransactionOp {
    Insert {
        table: &'static str,
        values: Vec<(ColumnDef, Value)>,
    },
    Delete {
        table: &'static str,
        behaviour: DeleteBehavior,
        filter: Option<Filter>,
    },
    Update {
        table: &'static str,
        patch: Vec<(ColumnDef, Value)>,
        filter: Option<Filter>,
    },
}
