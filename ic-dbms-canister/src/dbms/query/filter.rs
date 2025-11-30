use crate::dbms::query::QueryResult;
use crate::dbms::table::ColumnDef;
use crate::dbms::types::Text;
use crate::dbms::value::Value;
use crate::prelude::QueryError;

/// [`super::Query`] filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    Eq(&'static str, Value),
    Ne(&'static str, Value),
    Gt(&'static str, Value),
    Lt(&'static str, Value),
    Ge(&'static str, Value),
    In(&'static str, Vec<Value>),
    Le(&'static str, Value),
    Like(&'static str, String),
    NotNull(&'static str),
    IsNull(&'static str),
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    /// Creates an equality filter.
    pub fn eq(field: &'static str, value: Value) -> Self {
        Filter::Eq(field, value)
    }

    /// Creates a not-equal filter.
    pub fn ne(field: &'static str, value: Value) -> Self {
        Filter::Ne(field, value)
    }

    /// Creates a greater-than filter.
    pub fn gt(field: &'static str, value: Value) -> Self {
        Filter::Gt(field, value)
    }

    /// Creates a less-than filter.
    pub fn lt(field: &'static str, value: Value) -> Self {
        Filter::Lt(field, value)
    }

    /// Creates a greater-than-or-equal filter.
    pub fn ge(field: &'static str, value: Value) -> Self {
        Filter::Ge(field, value)
    }

    /// Creates a less-than-or-equal filter.
    pub fn le(field: &'static str, value: Value) -> Self {
        Filter::Le(field, value)
    }

    /// Creates an IN filter.
    pub fn in_list(field: &'static str, values: Vec<Value>) -> Self {
        Filter::In(field, values)
    }

    /// Creates a LIKE filter.
    pub fn like(field: &'static str, pattern: &str) -> Self {
        Filter::Like(field, pattern.to_string())
    }

    /// Creates a NOT NULL filter.
    pub fn not_null(field: &'static str) -> Self {
        Filter::NotNull(field)
    }

    /// Creates an IS NULL filter.
    pub fn is_null(field: &'static str) -> Self {
        Filter::IsNull(field)
    }

    /// Chain two filters with AND.
    pub(crate) fn and(self, other: Filter) -> Self {
        Filter::And(Box::new(self), Box::new(other))
    }

    /// Chain two filters with OR.
    pub(crate) fn or(self, other: Filter) -> Self {
        Filter::Or(Box::new(self), Box::new(other))
    }

    /// Negate a filter with NOT.
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Filter::Not(Box::new(self))
    }

    /// Checks if the given values match the filter.
    pub fn matches(&self, values: &[(ColumnDef, Value)]) -> QueryResult<bool> {
        let res = match self {
            Filter::Eq(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val == value),
            Filter::Ne(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val != value),
            Filter::Gt(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val > value),
            Filter::Lt(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val < value),
            Filter::Ge(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val >= value),
            Filter::Le(field, value) => values
                .iter()
                .any(|(col, val)| col.name == *field && val <= value),
            Filter::In(field, list) => values
                .iter()
                .any(|(col, val)| col.name == *field && list.iter().any(|v| v == val)),
            Filter::Like(field, pattern) => {
                for (col, val) in values {
                    if col.name == *field {
                        if let Value::Text(Text(text)) = val {
                            let res =
                                like::Like::<true>::like(text.as_str(), pattern).map_err(|e| {
                                    QueryError::InvalidQuery(format!(
                                        "Invalid LIKE pattern {pattern}: {e}"
                                    ))
                                })?;

                            return Ok(res);
                        }
                        return Err(QueryError::InvalidQuery(
                            "LIKE operator can only be applied to Text values".to_string(),
                        ));
                    }
                }
                false
            }
            Filter::NotNull(field) => values
                .iter()
                .any(|(col, val)| col.name == *field && !val.is_null()),
            Filter::IsNull(field) => values
                .iter()
                .any(|(col, val)| col.name == *field && val.is_null()),
            Filter::And(left, right) => left.matches(values)? && right.matches(values)?,
            Filter::Or(left, right) => left.matches(values)? || right.matches(values)?,
            Filter::Not(inner) => !inner.matches(values)?,
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dbms::types::{DataTypeKind, Int32};

    #[test]
    fn test_should_build_filter() {
        let eq = Filter::eq("age", Value::Int32(30.into()));
        assert!(matches!(eq, Filter::Eq("age", Value::Int32(Int32(30)))));

        let ne = Filter::ne("name", Value::Int32(30.into()));
        assert!(matches!(ne, Filter::Ne("name", Value::Int32(Int32(30)))));

        let gt = Filter::gt("score", Value::Int32(100.into()));
        assert!(matches!(gt, Filter::Gt("score", Value::Int32(Int32(100)))));

        let ge = Filter::ge("level", Value::Int32(5.into()));
        assert!(matches!(ge, Filter::Ge("level", Value::Int32(Int32(5)))));

        let lt = Filter::lt("rank", Value::Int32(10.into()));
        assert!(matches!(lt, Filter::Lt("rank", Value::Int32(Int32(10)))));

        let le = Filter::le("height", Value::Int32(180.into()));
        assert!(matches!(le, Filter::Le("height", Value::Int32(Int32(180)))));

        let not_null = Filter::not_null("address");
        assert!(matches!(not_null, Filter::NotNull("address")));

        let is_null = Filter::is_null("phone");
        assert!(matches!(is_null, Filter::IsNull("phone")));

        let like = Filter::like("name", "John%");
        assert!(matches!(like, Filter::Like("name", _)));

        // chained filters
        let combined = eq.and(gt).or(is_null.not());
        if let Filter::Or(left, right) = combined {
            assert!(matches!(*left, Filter::And(_, _)));
            assert!(matches!(*right, Filter::Not(_)));
        } else {
            panic!("Expected combined filter to be an Or filter");
        }
    }

    #[test]
    fn test_should_check_eq() {
        let filter = Filter::eq("id", Value::Int32(30.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(30.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(35.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_ne() {
        let filter = Filter::ne("id", Value::Int32(30.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(25.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(30.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_gt() {
        let filter = Filter::gt("id", Value::Int32(20.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(25.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(10.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_lt() {
        let filter = Filter::lt("id", Value::Int32(30.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(25.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(40.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_ge() {
        let filter = Filter::ge("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(25.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let filter = Filter::ge("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(30.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let filter = Filter::ge("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(20.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_le() {
        let filter = Filter::le("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(25.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let filter = Filter::le("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(20.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let filter = Filter::le("id", Value::Int32(25.into()));
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(35.into()),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_is_null() {
        let filter = Filter::is_null("name");
        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Null,
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("Alice".to_string())),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_not_null() {
        let filter = Filter::not_null("name");
        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("Alice".to_string())),
        )];
        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Null,
        )];
        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_like() {
        let filter = Filter::like("name", "%ohn%");
        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("Johnathan".to_string())),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("Alice".to_string())),
        )];

        let result = filter.matches(&values).expect("LIKE match failed");
        assert!(!result);
    }

    #[test]
    fn test_should_raise_error_or_like_on_non_text() {
        let filter = Filter::like("age", "%30%");
        let values = vec![(
            ColumnDef {
                name: "age",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            Value::Int32(30.into()),
        )];

        let result = filter.matches(&values);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_escape_like() {
        let filter = Filter::like("name", "100%% match");
        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("100% match".to_string())),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);
    }

    #[test]
    fn test_should_check_and_or_not() {
        let filter = Filter::eq("id", Value::Int32(30.into()))
            .and(Filter::gt("age", Value::Int32(18.into())))
            .or(Filter::is_null("name").not());

        let values = vec![
            (
                ColumnDef {
                    name: "id",
                    data_type: DataTypeKind::Int32,
                    nullable: false,
                    primary_key: true,
                    foreign_key: None,
                },
                Value::Int32(30.into()),
            ),
            (
                ColumnDef {
                    name: "age",
                    data_type: DataTypeKind::Int32,
                    nullable: false,
                    primary_key: false,
                    foreign_key: None,
                },
                Value::Int32(20.into()),
            ),
            (
                ColumnDef {
                    name: "name",
                    data_type: DataTypeKind::Text,
                    nullable: true,
                    primary_key: false,
                    foreign_key: None,
                },
                Value::Text(Text("Alice".to_string())),
            ),
        ];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        // check false
        let values = vec![
            (
                ColumnDef {
                    name: "id",
                    data_type: DataTypeKind::Int32,
                    nullable: false,
                    primary_key: true,
                    foreign_key: None,
                },
                Value::Int32(25.into()),
            ),
            (
                ColumnDef {
                    name: "age",
                    data_type: DataTypeKind::Int32,
                    nullable: false,
                    primary_key: false,
                    foreign_key: None,
                },
                Value::Int32(16.into()),
            ),
            (
                ColumnDef {
                    name: "name",
                    data_type: DataTypeKind::Text,
                    nullable: true,
                    primary_key: false,
                    foreign_key: None,
                },
                Value::Null,
            ),
        ];
        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_not() {
        let filter = Filter::not_null("name").not();

        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Null,
        )];

        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
            Value::Text(Text("Bob".to_string())),
        )];

        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_should_check_in_list() {
        let filter = Filter::in_list(
            "id",
            vec![
                Value::Int32(10.into()),
                Value::Int32(20.into()),
                Value::Int32(30.into()),
            ],
        );
        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(20.into()),
        )];
        let result = filter.matches(&values).unwrap();
        assert!(result);

        let values = vec![(
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Int32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            Value::Int32(40.into()),
        )];
        let result = filter.matches(&values).unwrap();
        assert!(!result);
    }
}
