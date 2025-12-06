use std::fmt;

use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::dbms::types::DataType;
use crate::memory::{DataSize, Encode};

/// Unsigned integer 32-bit data type for the DBMS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Uint32(pub u32);

impl CandidType for Uint32 {
    fn _ty() -> candid::types::Type {
        candid::types::Type(std::rc::Rc::new(candid::types::TypeInner::Nat32))
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_nat32(self.0)
    }
}

impl fmt::Display for Uint32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Encode for Uint32 {
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
        Ok(Self(u32::from_le_bytes(array)))
    }
}

impl DataType for Uint32 {}

impl From<u32> for Uint32 {
    fn from(value: u32) -> Self {
        Uint32(value)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_uint32_encode_decode() {
        let value = Uint32(123456);
        let encoded = value.encode();
        let decoded = Uint32::decode(encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_should_candid_encode_decode() {
        let src = Uint32(123456);
        let buf = candid::encode_one(src).expect("Candid encoding failed");
        let decoded: Uint32 = candid::decode_one(&buf).expect("Candid decoding failed");
        assert_eq!(src, decoded);
    }
}
