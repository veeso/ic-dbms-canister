use crate::dbms::value::Value;

/// [`super::Query`] filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    Eq(&'static str, Value),
    Ne(&'static str, Value),
    Gt(&'static str, Value),
    Lt(&'static str, Value),
    Ge(&'static str, Value),
    Le(&'static str, Value),
    Like(&'static str, Value),
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

    /// Creates a LIKE filter.
    pub fn like(field: &'static str, value: &str) -> Self {
        Filter::Like(field, Value::Text(value.to_string().into()))
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
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dbms::types::Int32;

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
        assert!(matches!(like, Filter::Like("name", Value::Text(_))));

        // chained filters
        let combined = eq.and(gt).or(is_null.not());
        if let Filter::Or(left, right) = combined {
            assert!(matches!(*left, Filter::And(_, _)));
            assert!(matches!(*right, Filter::Not(_)));
        } else {
            panic!("Expected combined filter to be an Or filter");
        }
    }
}
