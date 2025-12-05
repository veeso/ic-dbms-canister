//! Test types, fixtures and mocks.

mod message;
mod post;
mod user;

#[allow(unused_imports)]
pub use self::message::{
    MESSAGES_FIXTURES, Message, MessageInsertRequest, MessageRecord, MessageUpdateRequest,
};
#[allow(unused_imports)]
pub use self::post::{POSTS_FIXTURES, Post, PostInsertRequest, PostRecord, PostUpdateRequest};
#[allow(unused_imports)]
pub use self::user::{USERS_FIXTURES, User, UserInsertRequest, UserRecord, UserUpdateRequest};
use crate::dbms::Database;
use crate::dbms::table::{ColumnDef, ValuesSource};
use crate::dbms::value::Value;
use crate::prelude::{
    DatabaseSchema, InsertIntegrityValidator, InsertRecord as _, QueryError, TableSchema as _,
};

/// Loads fixtures into the database for testing purposes.
///
/// # Panics
///
/// Panics if any operation fails.
pub fn load_fixtures() {
    user::load_fixtures();
    post::load_fixtures();
    message::load_fixtures();
}

/// Helper function which takes a list of `(ValuesSource, Value)` tuples, take only those with
/// [`ValuesSource::Foreign`] matching the provided table and column names, and returns a vector of
/// the corresponding `Value`s. with the [`ValuesSource`] set to [`ValuesSource::This`].
fn self_reference_values(
    values: &[(ValuesSource, Vec<(ColumnDef, Value)>)],
    table: &'static str,
    local_column: &'static str,
) -> Vec<(ValuesSource, Vec<(ColumnDef, Value)>)> {
    values
        .iter()
        .filter(|(source, _)| matches!(source, ValuesSource::Foreign { table: t, column } if *t == table && *column == local_column))
        .map(|(_, value)| (ValuesSource::This, value.clone())
    )
    .collect()
}

pub struct TestDatabaseSchema;

impl DatabaseSchema for TestDatabaseSchema {
    fn insert(
        &self,
        dbms: &Database,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> crate::IcDbmsResult<()> {
        if table_name == User::table_name() {
            let insert_request = UserInsertRequest::from_values(record_values)?;
            dbms.insert::<User>(insert_request)
        } else if table_name == Post::table_name() {
            let insert_request = PostInsertRequest::from_values(record_values)?;
            dbms.insert::<Post>(insert_request)
        } else if table_name == Message::table_name() {
            let insert_request = MessageInsertRequest::from_values(record_values)?;
            dbms.insert::<Message>(insert_request)
        } else {
            Err(crate::IcDbmsError::Query(QueryError::TableNotFound(
                table_name,
            )))
        }
    }

    fn validate_insert(
        &self,
        dbms: &Database,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> crate::IcDbmsResult<()> {
        if table_name == User::table_name() {
            InsertIntegrityValidator::<User>::new(dbms).validate(record_values)
        } else if table_name == Post::table_name() {
            InsertIntegrityValidator::<Post>::new(dbms).validate(record_values)
        } else if table_name == Message::table_name() {
            InsertIntegrityValidator::<Message>::new(dbms).validate(record_values)
        } else {
            Err(crate::IcDbmsError::Query(QueryError::TableNotFound(
                table_name,
            )))
        }
    }
}
