//! Memory module provides stable memory management for the IC DBMS Canister.

mod delegate;
mod encode;

use thiserror::Error;

pub use self::delegate::MemoryDelegate;
pub use self::encode::{DataSize, Encode};

/// The result type for memory operations.
pub type MemoryResult<T> = Result<T, MemoryError>;

/// An enum representing possible memory-related errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MemoryError {}

/// The memory manager is the main struct responsible for handling the stable memory operations.
///
/// It takes advantage of [`MemoryDelegate`]s to know how to allocate and write memory for different kind of data.
pub struct MemoryManager;
