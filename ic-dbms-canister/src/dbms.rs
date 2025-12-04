//! This module exposes all the types related to the DBMS engine.

pub mod foreign_fetcher;
mod integrity;
pub mod query;
pub mod table;
pub mod transaction;
pub mod types;
pub mod value;

use self::foreign_fetcher::ForeignFetcher;
use crate::dbms::integrity::InsertIntegrityValidator;
use crate::dbms::table::{ColumnDef, TableColumns, TableRecord, ValuesSource};
use crate::dbms::transaction::{DatabaseOverlay, Transaction, TransactionId};
use crate::dbms::value::Value;
use crate::memory::{SCHEMA_REGISTRY, TableRegistry};
use crate::prelude::{
    Filter, InsertRecord, Query, QueryError, TRANSACTION_SESSION, TableError, TableSchema,
    TransactionError,
};
use crate::{IcDbmsError, IcDbmsResult};

/// Default capacity limit for SELECT queries.
const DEFAULT_SELECT_LIMIT: usize = 128;

/// The main DBMS struct.
///
/// This struct serves as the entry point for interacting with the DBMS engine.
///
/// It provides methods for executing queries.
///
/// - [`Database::select`] - Execute a SELECT query.
/// - [`Database::insert`] - Execute an INSERT query.
/// - [`Database::update`] - Execute an UPDATE query.
/// - [`Database::delete`] - Execute a DELETE query.
/// - [`Database::commit`] - Commit the current transaction.
/// - [`Database::rollback`] - Rollback the current transaction.
///
/// The `transaction` field indicates whether the instance is operating within a transaction context.
/// The [`Database`] can be instantiated for one-shot, with [`Database::oneshot`] operations (no transaction),
/// or within a transaction context with [`Database::from_transaction`].
///
/// If a transaction is active, all operations will be part of that transaction until it is committed or rolled back.
pub struct Database {
    /// Id of the loaded transaction, if any.
    transaction: Option<TransactionId>,
}

impl From<TransactionId> for Database {
    fn from(transaction_id: TransactionId) -> Self {
        Self {
            transaction: Some(transaction_id),
        }
    }
}

impl Database {
    /// Load an instance of the [`Database`] for one-shot operations (no transaction).
    pub fn oneshot() -> Self {
        Self { transaction: None }
    }

    /// Load an instance of the [`Database`] within a transaction context.
    pub fn from_transaction(transaction_id: TransactionId) -> Self {
        Self {
            transaction: Some(transaction_id),
        }
    }

    /// Executes a SELECT query and returns the results.
    ///
    /// # Arguments
    ///
    /// - `query` - The SELECT [`Query`] to be executed.
    ///
    /// # Returns
    ///
    /// The returned results are a vector of [`table::TableRecord`] matching the query.
    pub fn select<T>(&self, query: Query<T>) -> IcDbmsResult<Vec<T::Record>>
    where
        T: TableSchema,
    {
        // load table registry
        let table_registry = self.load_table_registry::<T>()?;
        // read table
        let table_reader = table_registry.read();
        // get database overlay
        let mut table_overlay = if self.transaction.is_some() {
            self.overlay()?
        } else {
            DatabaseOverlay::default()
        };
        // overlay table reader
        let mut table_reader = table_overlay.reader(table_reader);

        // prepare results vector
        let mut results = Vec::with_capacity(query.limit.unwrap_or(DEFAULT_SELECT_LIMIT));
        // iter and select
        let mut count = 0;

        while let Some(values) = table_reader.try_next()? {
            // check whether it matches the filter
            if let Some(filter) = &query.filter {
                if !self.record_matches_filter(&values, filter)? {
                    continue;
                }
            }
            // filter matched, check limit and offset
            count += 1;
            // check whether is before offset
            if query.offset.is_some_and(|offset| count <= offset) {
                continue;
            }
            // get queried fields
            let values = self.select_queried_fields::<T>(values, &query)?;
            // convert to record
            let record = T::Record::from_values(values);
            // push to results
            results.push(record);
            // check whether reached limit
            if query.limit.is_some_and(|limit| results.len() >= limit) {
                break;
            }
        }

        Ok(results)
    }

    /// Executes an INSERT query.
    ///
    /// # Arguments
    ///
    /// - `record` - The INSERT record to be executed.
    pub fn insert<T>(&self, record: T::Insert) -> IcDbmsResult<()>
    where
        T: TableSchema,
        T::Insert: InsertRecord<Schema = T>,
    {
        // check whether the insert is valid
        let record_values = record.clone().into_values();
        InsertIntegrityValidator::<T>::new(self).validate(&record_values)?;

        if self.transaction.is_some() {
            // TODO: handle insert tx; should we use `dyn TableSchema` here?
            todo!();
        } else {
            // insert directly into the database
            let mut table_registry = self.load_table_registry::<T>()?;
            table_registry.insert(record.into_record())?;
        }

        Ok(())
    }

    /// Executes an UPDATE query.
    ///
    /// # Arguments
    ///
    /// - `record` - The UPDATE record to be executed.
    ///
    /// # Returns
    ///
    /// The number of rows updated.
    pub fn update<T>(&self, record: T::Update) -> IcDbmsResult<u64>
    where
        T: TableSchema,
        T::Update: table::UpdateRecord<Schema = T>,
    {
        todo!()
    }

    /// Executes a DELETE query.
    ///
    /// # Arguments
    ///
    /// - `filter` - An optional [`prelude::Filter`] to specify which records to delete.
    ///
    /// # Returns
    ///
    /// The number of rows deleted.
    pub fn delete<T>(&self, filter: Option<Filter>) -> IcDbmsResult<u64>
    where
        T: TableSchema,
    {
        // TODO: check whether we are in a transaction context
        // TODO: cascade for foreign keys
        todo!()
    }

    /// Commits the current transaction.
    pub fn commit(&self) -> IcDbmsResult<()> {
        let Some(txid) = self.transaction.as_ref() else {
            return Err(IcDbmsError::Transaction(
                TransactionError::NoActiveTransaction,
            ));
        };
        todo!();
        TRANSACTION_SESSION.with_borrow_mut(|ts| ts.close_transaction(txid));

        Ok(())
    }

    /// Rolls back the current transaction.
    pub fn rollback(&self) -> IcDbmsResult<()> {
        let Some(txid) = self.transaction.as_ref() else {
            return Err(IcDbmsError::Transaction(
                TransactionError::NoActiveTransaction,
            ));
        };

        todo!();

        TRANSACTION_SESSION.with_borrow_mut(|ts| ts.close_transaction(txid));
        Ok(())
    }

    /// Executes a closure with a mutable reference to the current [`Transaction`].
    fn with_transaction_mut<F, R>(&self, f: F) -> IcDbmsResult<R>
    where
        F: FnOnce(&mut Transaction) -> IcDbmsResult<R>,
    {
        let txid = self.transaction.as_ref().ok_or(IcDbmsError::Transaction(
            TransactionError::NoActiveTransaction,
        ))?;

        TRANSACTION_SESSION.with_borrow_mut(|ts| {
            let tx = ts.get_transaction_mut(txid)?;
            f(tx)
        })
    }

    /// Retrieves the current [`Transaction`].
    fn transaction(&self) -> IcDbmsResult<Transaction> {
        let txid = self.transaction.as_ref().ok_or(IcDbmsError::Transaction(
            TransactionError::NoActiveTransaction,
        ))?;

        TRANSACTION_SESSION.with_borrow(|ts| ts.get_transaction(txid).cloned())
    }

    /// Retrieves the current [`DatabaseOverlay`].
    fn overlay(&self) -> IcDbmsResult<DatabaseOverlay> {
        Ok(self.transaction()?.overlay().clone())
    }

    /// Returns whether the read given record matches the provided filter.
    fn record_matches_filter(
        &self,
        record_values: &[(ColumnDef, Value)],
        filter: &Filter,
    ) -> IcDbmsResult<bool> {
        filter.matches(record_values).map_err(IcDbmsError::from)
    }

    /// Select only the queried fields from the given record values.
    ///
    /// It also loads eager relations if any.
    fn select_queried_fields<T>(
        &self,
        mut record_values: Vec<(ColumnDef, Value)>,
        query: &Query<T>,
    ) -> IcDbmsResult<TableColumns>
    where
        T: TableSchema,
    {
        let mut queried_fields = vec![];

        // handle eager relations
        // FIXME: currently we fetch the FK for each record, which is shit.
        // In the future, we should batch fetch foreign keys for all records in the result set.
        for relation in &query.eager_relations {
            let mut fetched = false;
            // iter all foreign key with that table
            for (fk, fk_value) in record_values
                .iter()
                .filter(|(col_def, _)| {
                    col_def
                        .foreign_key
                        .is_some_and(|fk| fk.foreign_table == *relation)
                })
                .map(|(col, value)| {
                    (
                        col.foreign_key.as_ref().expect("cannot be empty"),
                        value.clone(),
                    )
                })
            {
                // get foreign values
                queried_fields.extend(T::foreign_fetcher().fetch(
                    self,
                    relation,
                    fk.local_column,
                    fk_value,
                )?);
                fetched = true;
            }

            if !fetched {
                return Err(IcDbmsError::Query(QueryError::InvalidQuery(format!(
                    "Cannot load relation '{}' for table '{}': no foreign key found",
                    relation,
                    T::table_name()
                ))));
            }
        }

        // short-circuit if all selected
        if query.all_selected() {
            queried_fields.extend(vec![(ValuesSource::This, record_values)]);
            return Ok(queried_fields);
        }
        record_values.retain(|(col_def, _)| query.columns().contains(&col_def.name));
        queried_fields.extend(vec![(ValuesSource::This, record_values)]);
        Ok(queried_fields)
    }

    /// Load the table registry for the given table schema.
    fn load_table_registry<T>(&self) -> IcDbmsResult<TableRegistry<T>>
    where
        T: TableSchema,
    {
        // get pages of the table registry from schema registry
        let registry_pages = SCHEMA_REGISTRY
            .with_borrow(|schema| schema.table_registry_page::<T>())
            .ok_or(IcDbmsError::Table(TableError::TableNotFound))?;

        TableRegistry::load(registry_pages).map_err(IcDbmsError::from)
    }
}

#[cfg(test)]
mod tests {

    use candid::{Nat, Principal};

    use super::*;
    use crate::dbms::types::{Text, Uint32};
    use crate::tests::{
        Message, POSTS_FIXTURES, Post, USERS_FIXTURES, User, UserInsertRequest, load_fixtures,
    };

    #[test]
    fn test_should_init_dbms() {
        let dbms = Database::oneshot();
        assert!(dbms.transaction.is_none());

        let tx_dbms = Database::from_transaction(Nat::from(1u64));
        assert!(tx_dbms.transaction.is_some());
    }

    #[test]
    fn test_should_select_all_users() {
        load_fixtures();
        let dbms = Database::oneshot();
        let query = Query::<User>::builder().all().build();
        let users = dbms.select(query).expect("failed to select users");

        assert_eq!(users.len(), USERS_FIXTURES.len());
        // check if all users all loaded
        for (i, user) in users.iter().enumerate() {
            assert_eq!(user.id.expect("should have id").0 as usize, i);
            assert_eq!(
                user.name.as_ref().expect("should have name").0,
                USERS_FIXTURES[i]
            );
        }
    }

    #[test]
    fn test_should_select_user_in_overlay() {
        load_fixtures();
        // create a transaction
        let transaction_id =
            TRANSACTION_SESSION.with_borrow_mut(|ts| ts.begin_transaction(Principal::anonymous()));
        // insert
        TRANSACTION_SESSION.with_borrow_mut(|ts| {
            let tx = ts
                .get_transaction_mut(&transaction_id)
                .expect("should have tx");
            tx.overlay_mut()
                .insert::<User>(vec![
                    (
                        ColumnDef {
                            name: "id",
                            data_type: types::DataTypeKind::Uint32,
                            nullable: false,
                            primary_key: true,
                            foreign_key: None,
                        },
                        Value::Uint32(999.into()),
                    ),
                    (
                        ColumnDef {
                            name: "name",
                            data_type: types::DataTypeKind::Text,
                            nullable: false,
                            primary_key: false,
                            foreign_key: None,
                        },
                        Value::Text("OverlayUser".to_string().into()),
                    ),
                ])
                .expect("failed to insert");
        });

        // select by pk
        let dbms = Database::from_transaction(transaction_id);
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(999.into())))
            .build();
        let users = dbms.select(query).expect("failed to select users");

        assert_eq!(users.len(), 1);
        let user = &users[0];
        assert_eq!(user.id.expect("should have id").0, 999);
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            "OverlayUser"
        );
    }

    #[test]
    fn test_should_select_users_with_offset_and_limit() {
        load_fixtures();
        let dbms = Database::oneshot();
        let query = Query::<User>::builder().offset(2).limit(3).build();
        let users = dbms.select(query).expect("failed to select users");

        assert_eq!(users.len(), 3);
        // check if correct users are loaded
        for (i, user) in users.iter().enumerate() {
            let expected_index = i + 2;
            assert_eq!(user.id.expect("should have id").0 as usize, expected_index);
            assert_eq!(
                user.name.as_ref().expect("should have name").0,
                USERS_FIXTURES[expected_index]
            );
        }
    }

    #[test]
    fn test_should_select_users_with_offset_and_filter() {
        load_fixtures();
        let dbms = Database::oneshot();
        let query = Query::<User>::builder()
            .offset(1)
            .and_where(Filter::gt("id", Value::Uint32(4.into())))
            .build();
        let users = dbms.select(query).expect("failed to select users");

        assert_eq!(users.len(), 4);
        // check if correct users are loaded
        for (i, user) in users.iter().enumerate() {
            let expected_index = i + 6;
            assert_eq!(user.id.expect("should have id").0 as usize, expected_index);
            assert_eq!(
                user.name.as_ref().expect("should have name").0,
                USERS_FIXTURES[expected_index]
            );
        }
    }

    #[test]
    fn test_should_select_post_with_relation() {
        load_fixtures();
        let dbms = Database::oneshot();
        let query = Query::<Post>::builder()
            .all()
            .with(User::table_name())
            .build();
        let posts = dbms.select(query).expect("failed to select posts");
        assert_eq!(posts.len(), POSTS_FIXTURES.len());

        for (id, post) in posts.into_iter().enumerate() {
            let (expected_title, expected_content, expected_user_id) = &POSTS_FIXTURES[id];
            assert_eq!(post.id.expect("should have id").0 as usize, id);
            assert_eq!(
                post.title.as_ref().expect("should have title").0,
                *expected_title
            );
            assert_eq!(
                post.content.as_ref().expect("should have content").0,
                *expected_content
            );
            let user_query = Query::<User>::builder()
                .and_where(Filter::eq("id", Value::Uint32((*expected_user_id).into())))
                .build();
            let author = dbms
                .select(user_query)
                .expect("failed to load user")
                .pop()
                .expect("should have user");
            assert_eq!(post.user.expect("should have loaded user"), author);
        }
    }

    #[test]
    fn test_should_fail_loading_unexisting_column_on_select() {
        let dbms = Database::oneshot();
        let query = Query::<User>::builder().field("unexisting_column").build();
        let result = dbms.select(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_select_queried_fields() {
        let dbms = Database::oneshot();

        let record_values = User::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Alice".to_string().into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let query: Query<User> = Query::builder().field("name").build();
        let selected_fields = dbms
            .select_queried_fields::<User>(record_values, &query)
            .expect("failed to select queried fields");
        let user_fields = selected_fields
            .into_iter()
            .find(|(table_name, _)| *table_name == ValuesSource::This)
            .map(|(_, cols)| cols)
            .unwrap_or_default();

        assert_eq!(user_fields.len(), 1);
        assert_eq!(user_fields[0].0.name, "name");
        assert_eq!(user_fields[0].1, Value::Text("Alice".to_string().into()));
    }

    #[test]
    fn test_should_select_queried_fields_with_relations() {
        load_fixtures();
        let dbms = Database::oneshot();

        let record_values = Post::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Title".to_string().into()),
                Value::Text("Content".to_string().into()),
                Value::Uint32(2.into()), // author_id
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let query: Query<Post> = Query::builder()
            .field("title")
            .with(User::table_name())
            .build();
        let selected_fields = dbms
            .select_queried_fields::<Post>(record_values, &query)
            .expect("failed to select queried fields");

        // check post fields
        let post_fields = selected_fields
            .iter()
            .find(|(table_name, _)| *table_name == ValuesSource::This)
            .map(|(_, cols)| cols)
            .cloned()
            .unwrap_or_default();
        assert_eq!(post_fields.len(), 1);
        assert_eq!(post_fields[0].0.name, "title");
        assert_eq!(post_fields[0].1, Value::Text("Title".to_string().into()));

        // check user fields
        let user_fields = selected_fields
            .iter()
            .find(|(table_name, _)| {
                *table_name
                    == ValuesSource::Foreign {
                        table: User::table_name(),
                        column: "user_id",
                    }
            })
            .map(|(_, cols)| cols)
            .cloned()
            .unwrap_or_default();

        let expected_user = USERS_FIXTURES[2]; // author_id = 2

        assert_eq!(user_fields.len(), 2);
        assert_eq!(user_fields[0].0.name, "id");
        assert_eq!(user_fields[0].1, Value::Uint32(2.into()));
        assert_eq!(user_fields[1].0.name, "name");
        assert_eq!(
            user_fields[1].1,
            Value::Text(expected_user.to_string().into())
        );
    }

    #[test]
    fn test_should_select_with_two_fk_on_the_same_table() {
        load_fixtures();

        let query: Query<Message> = Query::builder()
            .all()
            .and_where(Filter::Eq("id", Value::Uint32(0.into())))
            .with("users")
            .build();

        let dbms = Database::oneshot();
        let messages = dbms.select(query).expect("failed to select messages");
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_eq!(message.id.expect("should have id").0, 0);
        assert_eq!(
            message
                .sender
                .as_ref()
                .expect("should have sender")
                .name
                .as_ref()
                .unwrap()
                .0,
            "Alice"
        );
        assert_eq!(
            message
                .recipient
                .as_ref()
                .expect("should have recipient")
                .name
                .as_ref()
                .unwrap()
                .0,
            "Bob"
        );
    }

    #[test]
    fn test_should_fail_loading_unexisting_relation() {
        let dbms = Database::oneshot();

        let record_values = Post::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Title".to_string().into()),
                Value::Text("Content".to_string().into()),
                Value::Uint32(2.into()), // author_id
            ])
            .collect::<Vec<(ColumnDef, Value)>>();

        let query: Query<Post> = Query::builder()
            .field("title")
            .with("unexisting_relation")
            .build();
        let result = dbms.select_queried_fields::<Post>(record_values, &query);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_get_whether_record_matches_filter() {
        let dbms = Database::oneshot();

        let record_values = User::columns()
            .iter()
            .cloned()
            .zip(vec![
                Value::Uint32(1.into()),
                Value::Text("Alice".to_string().into()),
            ])
            .collect::<Vec<(ColumnDef, Value)>>();
        let filter = Filter::eq("name", Value::Text("Alice".to_string().into()));

        let matches = dbms
            .record_matches_filter(&record_values, &filter)
            .expect("failed to match");
        assert!(matches);

        let non_matching_filter = Filter::eq("name", Value::Text("Bob".to_string().into()));
        let non_matches = dbms
            .record_matches_filter(&record_values, &non_matching_filter)
            .expect("failed to match");
        assert!(!non_matches);
    }

    #[test]
    fn test_should_load_table_registry() {
        init_user_table();

        let dbms = Database::oneshot();
        let table_registry = dbms.load_table_registry::<User>();
        assert!(table_registry.is_ok());
    }

    #[test]
    fn test_should_insert_record_without_transaction() {
        load_fixtures();

        let dbms = Database::oneshot();
        let new_user = UserInsertRequest {
            id: Uint32(100u32),
            name: Text("NewUser".to_string()),
        };

        let result = dbms.insert::<User>(new_user);
        assert!(result.is_ok());

        // find user
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(100u32.into())))
            .build();
        let users = dbms.select(query).expect("failed to select users");
        assert_eq!(users.len(), 1);
        let user = &users[0];
        assert_eq!(user.id.expect("should have id").0, 100);
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            "NewUser".to_string()
        );
    }

    #[test]
    fn test_should_validate_user_insert_conflict() {
        load_fixtures();

        let dbms = Database::oneshot();
        let new_user = UserInsertRequest {
            id: Uint32(1u32),
            name: Text("NewUser".to_string()),
        };

        let result = dbms.insert::<User>(new_user);
        assert!(result.is_err());
    }

    fn init_user_table() {
        SCHEMA_REGISTRY
            .with_borrow_mut(|sr| sr.register_table::<User>())
            .expect("failed to register `User` table");
    }
}
