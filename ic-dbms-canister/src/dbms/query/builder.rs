use std::marker::PhantomData;

use crate::dbms::query::Query;
use crate::dbms::table::TableSchema;

/// A builder for constructing database [`Query`]es.
#[derive(Default, Debug, Clone)]
pub struct QueryBuilder<T>
where
    T: TableSchema,
{
    _marker: PhantomData<T>,
}

impl<T> QueryBuilder<T>
where
    T: TableSchema,
{
    /// Builds and returns a [`Query`] object based on the current state of the builder.
    pub fn build(self) -> Query<T> {
        todo!()
    }
}
