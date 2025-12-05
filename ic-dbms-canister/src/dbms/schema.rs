use crate::IcDbmsResult;
use crate::dbms::Database;
use crate::dbms::table::ColumnDef;
use crate::dbms::value::Value;

/// This trait provides the schema operation for the current database.
///
/// It must provide the functionalities to validate the operations and perform them using the [`Database`] instance.
///
/// This is required because all of the [`Database`] operations rely on `T`, a [`crate::prelude::TableSchema`], but we can't store them inside
/// of transactions without knowing the concrete type at compile time.
pub trait DatabaseSchema {
    /// Performs an insert operation for the given table name and record values.
    ///
    /// Use [`Database::insert`] internally to perform the operation.
    fn insert(
        &self,
        dbms: &Database,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> IcDbmsResult<()>;

    /// Validates an insert operation for the given table name and record values.
    ///
    /// Use a [`crate::prelude::InsertIntegrityValidator`] to perform the validation.
    fn validate_insert(
        &self,
        dbms: &Database,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> IcDbmsResult<()>;
}
