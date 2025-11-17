use std::borrow::Cow;

use crate::memory::{MSize, MemoryResult};

/// This trait defines the encoding and decoding behaviour for data types used in the DBMS canister.
pub trait Encode {
    const SIZE: DataSize;

    /// Encodes the data type into a vector of bytes.
    fn encode(&'_ self) -> Cow<'_, [u8]>;

    /// Decodes the data type from a slice of bytes.
    fn decode(data: Cow<[u8]>) -> MemoryResult<Self>
    where
        Self: Sized;

    /// Returns the size in bytes of the encoded data type.
    fn size(&self) -> MSize;
}

/// Represents the size of data types used in the DBMS canister.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSize {
    /// A fixed size in bytes.
    Fixed(MSize),
    /// A variable size.
    Variable,
}

impl DataSize {
    /// Returns the size in bytes if the data size is fixed.
    pub fn get_fixed_size(&self) -> Option<MSize> {
        match self {
            DataSize::Fixed(size) => Some(*size),
            DataSize::Variable => None,
        }
    }
}
