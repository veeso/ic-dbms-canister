use super::types;

/// A generic wrapper enum to hold any DBMS value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Blob(types::Blob),
    Boolean(types::Boolean),
    Date(types::Date),
    DateTime(types::DateTime),
    Decimal(types::Decimal),
    Int32(types::Int32),
    Int64(types::Int64),
    Null,
    Principal(types::Principal),
    Text(types::Text),
    Uint32(types::Uint32),
    Uint64(types::Uint64),
    Uuid(types::Uuid),
}

// macro rules for implementing From trait for Value enum variants
macro_rules! impl_conv_for_value {
    ($variant:ident, $ty:ty, $name:ident) => {
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self {
                Value::$variant(value)
            }
        }

        impl Value {
            /// Attempts to extract a reference to the inner value if it matches the variant.
            pub fn $name(&self) -> Option<&$ty> {
                if let Value::$variant(v) = self {
                    Some(v)
                } else {
                    None
                }
            }
        }
    };
}

impl_conv_for_value!(Blob, types::Blob, as_blob);
impl_conv_for_value!(Boolean, types::Boolean, as_boolean);
impl_conv_for_value!(Date, types::Date, as_date);
impl_conv_for_value!(DateTime, types::DateTime, as_datetime);
impl_conv_for_value!(Decimal, types::Decimal, as_decimal);
impl_conv_for_value!(Int32, types::Int32, as_int32);
impl_conv_for_value!(Int64, types::Int64, as_int64);
impl_conv_for_value!(Principal, types::Principal, as_principal);
impl_conv_for_value!(Text, types::Text, as_text);
impl_conv_for_value!(Uint32, types::Uint32, as_uint32);
impl_conv_for_value!(Uint64, types::Uint64, as_uint64);
impl_conv_for_value!(Uuid, types::Uuid, as_uuid);

impl Value {
    /// Checks if the value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the type name of the value as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Blob(_) => "Blob",
            Value::Boolean(_) => "Boolean",
            Value::Date(_) => "Date",
            Value::DateTime(_) => "DateTime",
            Value::Decimal(_) => "Decimal",
            Value::Int32(_) => "Int32",
            Value::Int64(_) => "Int64",
            Value::Null => "Null",
            Value::Principal(_) => "Principal",
            Value::Text(_) => "Text",
            Value::Uint32(_) => "Uint32",
            Value::Uint64(_) => "Uint64",
            Value::Uuid(_) => "Uuid",
        }
    }
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_null() {
        let int_value: Value = types::Int32(42).into();
        assert!(!int_value.is_null());

        let null_value = Value::Null;
        assert!(null_value.is_null());
    }

    #[test]
    fn test_value_conversion_blob() {
        let blob = types::Blob(vec![1, 2, 3]);
        let value: Value = blob.clone().into();
        assert_eq!(value.as_blob(), Some(&blob));
    }

    #[test]
    fn test_value_conversion_boolean() {
        let boolean = types::Boolean(true);
        let value: Value = boolean.into();
        assert_eq!(value.as_boolean(), Some(&boolean));
    }

    #[test]
    fn test_value_conversion_date() {
        let date = types::Date {
            year: 2023,
            month: 3,
            day: 15,
        }; // Example date
        let value: Value = date.into();
        assert_eq!(value.as_date(), Some(&date));
    }

    #[test]
    fn test_value_conversion_datetime() {
        let datetime = types::DateTime {
            year: 2023,
            month: 3,
            day: 15,
            hour: 12,
            minute: 30,
            second: 45,
            microsecond: 123456,
            timezone_offset_minutes: 0,
        }; // Example datetime
        let value: Value = datetime.into();
        assert_eq!(value.as_datetime(), Some(&datetime));
    }

    #[test]
    fn test_value_conversion_decimal() {
        let decimal = types::Decimal(rust_decimal::Decimal::new(12345, 2)); // 123.45
        let value: Value = decimal.into();
        assert_eq!(value.as_decimal(), Some(&decimal));
    }

    #[test]
    fn test_value_conversion_int32() {
        let int32 = types::Int32(1234567890);
        let value: Value = int32.into();
        assert_eq!(value.as_int32(), Some(&int32));
    }

    #[test]
    fn test_value_conversion_int64() {
        let int64 = types::Int64(1234567890);
        let value: Value = int64.into();
        assert_eq!(value.as_int64(), Some(&int64));
    }

    #[test]
    fn test_value_conversion_principal() {
        let principal = types::Principal(candid::Principal::from_text("aaaaa-aa").unwrap());
        let value: Value = principal.clone().into();
        assert_eq!(value.as_principal(), Some(&principal));
    }

    #[test]
    fn test_value_conversion_text() {
        let text = types::Text("Hello, World!".to_string());
        let value: Value = text.clone().into();
        assert_eq!(value.as_text(), Some(&text));
    }

    #[test]
    fn test_value_conversion_uint32() {
        let uint32 = types::Uint32(123456);
        let value: Value = uint32.into();
        assert_eq!(value.as_uint32(), Some(&uint32));
    }

    #[test]
    fn test_value_conversion_uint64() {
        let uint64 = types::Uint64(12345678901234);
        let value: Value = uint64.into();
        assert_eq!(value.as_uint64(), Some(&uint64));
    }

    #[test]
    fn test_value_conversion_uuid() {
        let uuid = types::Uuid(
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("failed to parse uuid"),
        );
        let value: Value = uuid.clone().into();
        assert_eq!(value.as_uuid(), Some(&uuid));
    }

    #[test]
    fn test_value_type_name() {
        let int_value: Value = types::Int32(42).into();
        assert_eq!(int_value.type_name(), "Int32");

        let text_value: Value = types::Text("Hello".to_string()).into();
        assert_eq!(text_value.type_name(), "Text");

        let null_value = Value::Null;
        assert_eq!(null_value.type_name(), "Null");
    }
}
