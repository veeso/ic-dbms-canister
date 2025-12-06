/// Type alias for Transaction ID
pub type TransactionId = candid::Nat;

/// An enum representing possible errors that can occur during transaction operations.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("No active transaction")]
    NoActiveTransaction,
}
