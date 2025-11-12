use std::borrow::Cow;

/// This trait defines the encoding and decoding behaviour for data types used in the DBMS canister.
pub trait Encode {
    const SIZE: DataSize;

    /// Encodes the data type into a vector of bytes.
    fn encode(&'_ self) -> Cow<'_, [u8]>;

    /// Decodes the data type from a slice of bytes.
    fn decode(data: Cow<[u8]>) -> Self
    where
        Self: Sized;
}

/// Represents the size of data types used in the DBMS canister.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSize {
    Fixed(usize),
    Variable,
}
