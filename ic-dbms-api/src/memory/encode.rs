use std::borrow::Cow;

use crate::memory::{MSize, MemoryResult};

/// This trait defines the encoding and decoding behaviour for data types used in the DBMS canister.
pub trait Encode: Clone {
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
    Dynamic,
}

impl DataSize {
    /// Returns the size in bytes if the data size is fixed.
    pub fn get_fixed_size(&self) -> Option<MSize> {
        match self {
            DataSize::Fixed(size) => Some(*size),
            DataSize::Dynamic => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_get_data_size_fixed() {
        let size = DataSize::Fixed(10);
        assert_eq!(size.get_fixed_size(), Some(10));

        let variable_size = DataSize::Dynamic;
        assert_eq!(variable_size.get_fixed_size(), None);
    }
}
