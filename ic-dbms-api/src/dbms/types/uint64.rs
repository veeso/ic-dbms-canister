use std::fmt;

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::dbms::types::DataType;
use crate::memory::{DataSize, Encode};

/// Unsigned integer 64-bit data type for the DBMS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Uint64(pub u64);

impl CandidType for Uint64 {
    fn _ty() -> candid::types::Type {
        candid::types::Type(std::rc::Rc::new(candid::types::TypeInner::Nat64))
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_nat64(self.0)
    }
}

impl fmt::Display for Uint64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Encode for Uint64 {
    const SIZE: DataSize = DataSize::Fixed(8);

    fn size(&self) -> crate::memory::MSize {
        Self::SIZE.get_fixed_size().expect("should be fixed")
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Owned(self.0.to_le_bytes().to_vec())
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> crate::memory::MemoryResult<Self>
    where
        Self: Sized,
    {
        if data.len() < 8 {
            return Err(crate::memory::MemoryError::DecodeError(
                crate::memory::DecodeError::TooShort,
            ));
        }

        let mut array = [0u8; 8];
        array.copy_from_slice(&data[0..8]);
        Ok(Self(u64::from_le_bytes(array)))
    }
}

impl DataType for Uint64 {}

impl From<u64> for Uint64 {
    fn from(value: u64) -> Self {
        Uint64(value)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_uint64_encode_decode() {
        let value = Uint64(123456);
        let encoded = value.encode();
        let decoded = Uint64::decode(encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_should_candid_encode_decode() {
        let src = Uint64(123456);
        let buf = candid::encode_one(src).expect("Candid encoding failed");
        let decoded: Uint64 = candid::decode_one(&buf).expect("Candid decoding failed");
        assert_eq!(src, decoded);
    }
}
