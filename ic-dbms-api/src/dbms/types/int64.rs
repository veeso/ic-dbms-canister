use std::fmt;

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::dbms::types::DataType;
use crate::memory::{DataSize, Encode};

/// Integer 64-bit data type for the DBMS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int64(pub i64);

impl fmt::Display for Int64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CandidType for Int64 {
    fn _ty() -> candid::types::Type {
        candid::types::Type(std::rc::Rc::new(candid::types::TypeInner::Int64))
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_int64(self.0)
    }
}

impl Encode for Int64 {
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
        Ok(Self(i64::from_le_bytes(array)))
    }
}

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Int64(value)
    }
}

impl DataType for Int64 {}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_int32_encode_decode() {
        let value = Int64(1234568888);
        let encoded = value.encode();
        let decoded = Int64::decode(encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_should_candid_encode_decode() {
        let src = Int64(1234568888);
        let buf = candid::encode_one(src).expect("Candid encoding failed");
        let decoded: Int64 = candid::decode_one(&buf).expect("Candid decoding failed");
        assert_eq!(src, decoded);
    }
}
