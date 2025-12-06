//! Test types, fixtures and mocks.

mod message;
mod post;
mod user;

use ic_dbms_api::prelude::{ColumnDef, Database as _, Value, ValuesSource};

#[allow(unused_imports)]
pub use self::message::{
    MESSAGES_FIXTURES, Message, MessageInsertRequest, MessageRecord, MessageUpdateRequest,
};
#[allow(unused_imports)]
pub use self::post::{POSTS_FIXTURES, Post, PostInsertRequest, PostRecord, PostUpdateRequest};
#[allow(unused_imports)]
pub use self::user::{USERS_FIXTURES, User, UserInsertRequest, UserRecord, UserUpdateRequest};
use crate::dbms::IcDbmsDatabase;
use crate::prelude::{
    DatabaseSchema, InsertIntegrityValidator, InsertRecord as _, QueryError, TableSchema as _,
    UpdateRecord as _,
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
    #[allow(clippy::if_same_then_else)]
    fn referenced_tables(
        &self,
        table: &'static str,
    ) -> &'static [(&'static str, &'static [&'static str])] {
        if table == User::table_name() {
            &[
                ("posts", &["user_id"]),
                ("messages", &["sender_id", "recipient_id"]),
            ]
        } else if table == Post::table_name() {
            &[]
        } else if table == Message::table_name() {
            &[]
        } else {
            &[]
        }
    }

    fn insert(
        &self,
        dbms: &IcDbmsDatabase,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> ic_dbms_api::prelude::IcDbmsResult<()> {
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
            Err(ic_dbms_api::prelude::IcDbmsError::Query(
                QueryError::TableNotFound(table_name),
            ))
        }
    }

    fn delete(
        &self,
        dbms: &IcDbmsDatabase,
        table_name: &'static str,
        delete_behavior: ic_dbms_api::prelude::DeleteBehavior,
        filter: Option<ic_dbms_api::prelude::Filter>,
    ) -> ic_dbms_api::prelude::IcDbmsResult<u64> {
        if table_name == User::table_name() {
            dbms.delete::<User>(delete_behavior, filter)
        } else if table_name == Post::table_name() {
            dbms.delete::<Post>(delete_behavior, filter)
        } else if table_name == Message::table_name() {
            dbms.delete::<Message>(delete_behavior, filter)
        } else {
            Err(ic_dbms_api::prelude::IcDbmsError::Query(
                QueryError::TableNotFound(table_name),
            ))
        }
    }

    fn update(
        &self,
        dbms: &IcDbmsDatabase,
        table_name: &'static str,
        patch_values: &[(ColumnDef, Value)],
        filter: Option<crate::prelude::Filter>,
    ) -> ic_dbms_api::prelude::IcDbmsResult<u64> {
        if table_name == User::table_name() {
            let update_request = UserUpdateRequest::from_values(patch_values, filter);
            dbms.update::<User>(update_request)
        } else if table_name == Post::table_name() {
            let update_request = PostUpdateRequest::from_values(patch_values, filter);
            dbms.update::<Post>(update_request)
        } else if table_name == Message::table_name() {
            let update_request = MessageUpdateRequest::from_values(patch_values, filter);
            dbms.update::<Message>(update_request)
        } else {
            Err(ic_dbms_api::prelude::IcDbmsError::Query(
                QueryError::TableNotFound(table_name),
            ))
        }
    }

    fn validate_insert(
        &self,
        dbms: &IcDbmsDatabase,
        table_name: &'static str,
        record_values: &[(ColumnDef, Value)],
    ) -> ic_dbms_api::prelude::IcDbmsResult<()> {
        if table_name == User::table_name() {
            InsertIntegrityValidator::<User>::new(dbms).validate(record_values)
        } else if table_name == Post::table_name() {
            InsertIntegrityValidator::<Post>::new(dbms).validate(record_values)
        } else if table_name == Message::table_name() {
            InsertIntegrityValidator::<Message>::new(dbms).validate(record_values)
        } else {
            Err(ic_dbms_api::prelude::IcDbmsError::Query(
                QueryError::TableNotFound(table_name),
            ))
        }
    }
}
