use crate::dbms::Database;
use crate::dbms::table::{ColumnDef, ForeignKeyDef};
use crate::dbms::value::Value;
use crate::prelude::{Filter, ForeignFetcher, Query, QueryError, TableSchema};
use crate::{IcDbmsError, IcDbmsResult};

/// Integrity validator for insert operations.
pub struct InsertIntegrityValidator<'a, T>
where
    T: TableSchema,
{
    database: &'a Database,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> InsertIntegrityValidator<'a, T>
where
    T: TableSchema,
{
    /// Creates a new insert integrity validator.
    pub fn new(dbms: &'a Database) -> Self {
        Self {
            database: dbms,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> InsertIntegrityValidator<'_, T>
where
    T: TableSchema,
{
    /// Verify whether the given insert record is valid.
    ///
    /// An insert is valid when:
    /// - No primary key conflicts with existing records.
    /// - All foreign keys reference existing records.
    /// - All non-nullable columns are provided.
    pub fn validate(&self, record_values: &[(ColumnDef, Value)]) -> IcDbmsResult<()> {
        self.check_primary_key_conflict(record_values)?;
        self.check_foreign_keys(record_values)?;
        self.check_non_nullable_fields(record_values)?;

        Ok(())
    }

    /// Checks for primary key conflicts.
    fn check_primary_key_conflict(&self, record_values: &[(ColumnDef, Value)]) -> IcDbmsResult<()> {
        let pk_name = T::primary_key();
        let pk = record_values
            .iter()
            .find(|(col_def, _)| col_def.name == pk_name)
            .map(|(_, value)| value.clone())
            .ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                pk_name,
            )))?;

        // select
        let query: Query<T> = Query::builder()
            .field(pk_name)
            .and_where(Filter::Eq(pk_name, pk))
            .build();

        let res = self.database.select(query)?;
        if res.is_empty() {
            Ok(())
        } else {
            Err(IcDbmsError::Query(QueryError::PrimaryKeyConflict))
        }
    }

    /// Checks whether all the foreign keys reference existing records.
    fn check_foreign_keys(&self, record_values: &[(ColumnDef, Value)]) -> IcDbmsResult<()> {
        record_values
            .iter()
            .filter_map(|(col, value)| col.foreign_key.as_ref().map(|fk| (fk, value)))
            .try_for_each(|(col, value)| self.check_foreign_key_existence(col, value))
    }

    /// Checks whether a foreign key references an existing record.
    fn check_foreign_key_existence(
        &self,
        foreign_key: &ForeignKeyDef,
        value: &Value,
    ) -> IcDbmsResult<()> {
        let res = T::foreign_fetcher().fetch(
            self.database,
            foreign_key.foreign_table,
            foreign_key.local_column,
            value.clone(),
        )?;
        if res.is_empty() {
            Err(IcDbmsError::Query(
                QueryError::ForeignKeyConstraintViolation {
                    field: foreign_key.local_column,
                    referencing_table: foreign_key.foreign_table,
                },
            ))
        } else {
            Ok(())
        }
    }

    /// Check whether all non-nullable fields are provided.
    fn check_non_nullable_fields(&self, record_values: &[(ColumnDef, Value)]) -> IcDbmsResult<()> {
        for column in T::columns().iter().filter(|col| !col.nullable) {
            if !record_values
                .iter()
                .any(|(col_def, _)| col_def.name == column.name)
            {
                return Err(IcDbmsError::Query(QueryError::MissingNonNullableField(
                    column.name,
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dbms::types::DateTime;
    use crate::tests::{Message, Post, TestDatabaseSchema, User, load_fixtures};

    #[test]
    fn test_should_not_pass_check_for_pk_conflict() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        let values = User::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Alice".to_string().into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<User>::new(&dbms);
        let result = validator.validate(&values);
        assert!(matches!(
            result,
            Err(IcDbmsError::Query(QueryError::PrimaryKeyConflict))
        ));
    }
    #[test]
    fn test_should_pass_check_for_pk_conflict() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        // no conflict case
        let values = User::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1000.into()),
                Value::Text("Alice".to_string().into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<User>::new(&dbms);
        let result = validator.validate(&values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_not_pass_check_for_fk_conflict() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        let values = Post::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Title".to_string().into()),
                Value::Text("Content".to_string().into()),
                Value::Uint32(9999.into()), // non-existing user_id
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<Post>::new(&dbms);
        let result = validator.check_foreign_keys(&values);
        println!("{:?}", result);
        assert!(matches!(
            result,
            Err(IcDbmsError::Query(QueryError::BrokenForeignKeyReference {
                table: "users",
                ..
            }))
        ));
    }

    #[test]
    fn test_should_pass_check_for_fk_conflict() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        let values = Post::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Title".to_string().into()),
                Value::Text("Content".to_string().into()),
                Value::Uint32(1.into()), // existing user_id
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<Post>::new(&dbms);
        let result = validator.check_foreign_keys(&values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_not_pass_non_nullable_field_check() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        let values = Post::columns()
            .iter()
            .cloned()
            .filter(|col| col.name != "title") // omit non-nullable field
            .zip(vec![
                Value::Uint32(1.into()),
                // Missing title
                Value::Text("Content".to_string().into()),
                Value::Uint32(1.into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<Post>::new(&dbms);
        let result = validator.check_non_nullable_fields(&values);
        assert!(matches!(
            result,
            Err(IcDbmsError::Query(QueryError::MissingNonNullableField(
                field_name
            ))) if field_name == "title"
        ));
    }

    #[test]
    fn test_should_pass_non_nullable_field_check() {
        load_fixtures();
        let dbms = Database::oneshot(TestDatabaseSchema);

        let values = Message::columns()
            .iter()
            .filter(|col| !col.nullable)
            .cloned()
            .zip(vec![
                Value::Uint32(100.into()),
                Value::Text("Hello".to_string().into()),
                Value::Uint32(1.into()),
                Value::Uint32(2.into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<Message>::new(&dbms);
        let result = validator.check_non_nullable_fields(&values);
        assert!(result.is_ok());

        // should pass with nullable set

        let values = Message::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(100.into()),
                Value::Text("Hello".to_string().into()),
                Value::Uint32(1.into()),
                Value::Uint32(2.into()),
                Value::DateTime(DateTime {
                    year: 2024,
                    month: 6,
                    day: 1,
                    hour: 12,
                    minute: 0,
                    second: 0,
                    microsecond: 0,
                    timezone_offset_minutes: 0,
                }),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let validator = InsertIntegrityValidator::<Message>::new(&dbms);
        let result = validator.check_non_nullable_fields(&values);
        assert!(result.is_ok());
    }
}
