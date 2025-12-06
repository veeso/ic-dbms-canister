use std::fmt;

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::dbms::types::DataType;
use crate::memory::{DataSize, Encode};

/// Integer 32-bit data type for the DBMS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int32(pub i32);

impl fmt::Display for Int32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CandidType for Int32 {
    fn _ty() -> candid::types::Type {
        candid::types::Type(std::rc::Rc::new(candid::types::TypeInner::Int32))
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_int32(self.0)
    }
}

impl Encode for Int32 {
    const SIZE: DataSize = DataSize::Fixed(4);

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
        if data.len() < 4 {
            return Err(crate::memory::MemoryError::DecodeError(
                crate::memory::DecodeError::TooShort,
            ));
        }

        let mut array = [0u8; 4];
        array.copy_from_slice(&data[0..4]);
        Ok(Self(i32::from_le_bytes(array)))
    }
}

impl DataType for Int32 {}

impl From<i32> for Int32 {
    fn from(value: i32) -> Self {
        Int32(value)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_int32_encode_decode() {
        let value = Int32(123456);
        let encoded = value.encode();
        let decoded = Int32::decode(encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_should_candid_encode_decode() {
        let src = Int32(123456);
        let buf = candid::encode_one(src).expect("Candid encoding failed");
        let decoded: Int32 = candid::decode_one(&buf).expect("Candid decoding failed");
        assert_eq!(src, decoded);
    }
}
