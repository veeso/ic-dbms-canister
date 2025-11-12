/// A trait for delegating memory management operations.
///
/// It is used by the [`super::MemoryManager`] to handle memory allocation and writing for different data types.
///
/// Each database table should have its own implementation of this trait.
pub trait MemoryDelegate {}
