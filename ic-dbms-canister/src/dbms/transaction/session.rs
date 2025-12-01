use std::cell::RefCell;
use std::collections::HashMap;

use candid::{Nat, Principal};

use super::Transaction;
use crate::prelude::QueryError;
use crate::{IcDbmsError, IcDbmsResult};

/// Type alias for Transaction ID
pub type TransactionId = Nat;

thread_local! {
    pub static TRANSACTION_SESSION: RefCell<TransactionSession> = RefCell::new(TransactionSession::default());
}

/// The [`Transaction`] session storage
#[derive(Default, Debug)]
pub struct TransactionSession {
    /// Map between transaction IDs and Transactions
    transactions: HashMap<TransactionId, Transaction>,
    /// Map between transaction IDs and their owner ([`Principal`]).
    owners: HashMap<TransactionId, Principal>,
    /// Next transaction ID
    next_transaction_id: TransactionId,
}

impl TransactionSession {
    /// Begins a new transaction for the given owner ([`Principal`]) and returns its [`TransactionId`].
    pub fn begin_transaction(&mut self, owner: Principal) -> TransactionId {
        let transaction_id = self.next_transaction_id.clone();
        self.next_transaction_id += Nat::from(1u64);

        self.transactions
            .insert(transaction_id.clone(), Transaction::default());
        self.owners.insert(transaction_id.clone(), owner);

        transaction_id
    }

    /// Checks if a transaction with the given [`TransactionId`] exists and is owned by the given [`Principal`].
    pub fn has_transaction(&self, transaction_id: &TransactionId, caller: Principal) -> bool {
        self.owners
            .get(transaction_id)
            .is_some_and(|owner| *owner == caller)
    }

    /// Pushes an [`super::Operation`] to the transaction associated with the given [`TransactionId`].
    pub fn push_operation(
        &mut self,
        transaction_id: &TransactionId,
        operation: super::Operation,
    ) -> IcDbmsResult<()> {
        let transaction = self.get_transaction_mut(transaction_id)?;

        transaction.operations.push(operation);
        Ok(())
    }

    /// Retrieves the [`Transaction`] associated with the given [`TransactionId`].
    pub fn get_transaction(&self, transaction_id: &TransactionId) -> IcDbmsResult<&Transaction> {
        let transaction = self
            .transactions
            .get(transaction_id)
            .ok_or(IcDbmsError::Query(QueryError::TransactionNotFound))?;

        Ok(transaction)
    }

    /// Closes the transaction associated with the given [`TransactionId`].
    pub fn close_transaction(&mut self, transaction_id: &TransactionId) {
        self.transactions.remove(transaction_id);
        self.owners.remove(transaction_id);
    }

    /// Retrieves a mutable reference to the [`Transaction`] associated with the given [`TransactionId`].
    pub fn get_transaction_mut(
        &mut self,
        transaction_id: &TransactionId,
    ) -> IcDbmsResult<&mut Transaction> {
        self.transactions
            .get_mut(transaction_id)
            .ok_or(IcDbmsError::Query(QueryError::TransactionNotFound))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dbms::transaction::Operation;

    #[test]
    fn test_should_begin_transaction() {
        let mut session = TransactionSession::default();
        let transaction_id = session.begin_transaction(alice());

        assert!(session.has_transaction(&transaction_id, alice()));
        assert!(!session.has_transaction(&transaction_id, bob()));

        let transaction = session.get_transaction_mut(&transaction_id);
        assert!(transaction.is_ok());
    }

    #[test]
    fn test_should_close_transaction() {
        let mut session = TransactionSession::default();
        let transaction_id = session.begin_transaction(alice());

        assert!(session.has_transaction(&transaction_id, alice()));

        session.close_transaction(&transaction_id);

        assert!(!session.has_transaction(&transaction_id, alice()));
        let transaction = session.get_transaction_mut(&transaction_id);
        assert!(transaction.is_err());
        assert!(!session.owners.contains_key(&transaction_id));
        assert!(!session.transactions.contains_key(&transaction_id));
    }

    #[test]
    fn test_should_push_operation() {
        let mut session = TransactionSession::default();
        let transaction_id = session.begin_transaction(alice());

        let operation = Operation::Insert(
            "test_table",
            crate::dbms::table::UntypedInsertRecord { fields: vec![] },
        );
        let result = session.push_operation(&transaction_id, operation.clone());
        assert!(result.is_ok());

        let transaction = session.get_transaction_mut(&transaction_id).unwrap();
        assert_eq!(transaction.operations.len(), 1);
        assert!(matches!(transaction.operations[0], Operation::Insert(_, _)));
    }

    #[test]
    fn test_should_get_transaction() {
        let mut session = TransactionSession::default();
        let transaction_id = session.begin_transaction(alice());

        let operation = Operation::Insert(
            "test_table",
            crate::dbms::table::UntypedInsertRecord { fields: vec![] },
        );
        session
            .push_operation(&transaction_id, operation.clone())
            .unwrap();

        let tx = session
            .get_transaction(&transaction_id)
            .expect("failed to get tx");
        let operations = &tx.operations;
        assert_eq!(operations.len(), 1);
        assert!(matches!(operations[0], Operation::Insert(_, _)));
    }

    fn alice() -> Principal {
        Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap()
    }

    fn bob() -> Principal {
        Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap()
    }
}
