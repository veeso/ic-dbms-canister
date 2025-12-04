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
use crate::dbms::table::{ColumnDef, ValuesSource};
use crate::dbms::value::Value;

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
