use crate::query::Query;

/// A builder for constructing database queries.
#[derive(Default, Debug, Clone)]
pub struct QueryBuilder {}

impl QueryBuilder {
    /// Builds and returns a [`Query`] object based on the current state of the builder.
    pub fn build(self) -> Query {
        todo!()
    }
}
