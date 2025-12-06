use crate::dbms::table::TableColumns;
use crate::dbms::value::Value;
use crate::prelude::{Database, IcDbmsResult};

/// This trait defines the behavior of a foreign fetcher, which is responsible for
/// fetching data from foreign sources or databases.
///
/// It takes a table name and returns the values associated with that table.
pub trait ForeignFetcher: Default {
    /// Fetches the data for the specified table and primary key values.
    ///
    /// # Arguments
    ///
    /// * `database` - The database from which to fetch the data.
    /// * `table` - The name of the table to fetch data from.
    /// * `pk_values` - The primary key to look for.
    ///
    /// # Returns
    ///
    /// A result containing the fetched table columns or an error.
    fn fetch(
        &self,
        database: &impl Database,
        table: &'static str,
        local_column: &'static str,
        pk_value: Value,
    ) -> IcDbmsResult<TableColumns>;
}

/// A no-op foreign fetcher that does not perform any fetching.
#[derive(Default)]
pub struct NoForeignFetcher;

impl ForeignFetcher for NoForeignFetcher {
    fn fetch(
        &self,
        _database: &impl Database,
        _table: &'static str,
        _local_column: &'static str,
        _pk_value: Value,
    ) -> IcDbmsResult<TableColumns> {
        unimplemented!("NoForeignFetcher should have a table without foreign keys");
    }
}
