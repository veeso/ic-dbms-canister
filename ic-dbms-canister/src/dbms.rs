//! This module exposes all the types related to the DBMS engine.

pub mod integrity;
pub mod schema;
pub mod transaction;

use ic_dbms_api::prelude::{
    ColumnDef, Database, DeleteBehavior, Filter, ForeignFetcher, IcDbmsError, IcDbmsResult,
    InsertRecord, OrderDirection, Query, QueryError, TableColumns, TableError, TableRecord,
    TableSchema, TransactionError, TransactionId, UpdateRecord, Value, ValuesSource,
};

use crate::dbms::transaction::{DatabaseOverlay, Transaction, TransactionOp};
use crate::memory::{SCHEMA_REGISTRY, TableRegistry};
use crate::prelude::{DatabaseSchema, TRANSACTION_SESSION};
use crate::utils::trap;

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
pub struct IcDbmsDatabase {
    /// Database schema to perform generic operations, without knowing the concrete table schema at compile time.
    schema: Box<dyn DatabaseSchema>,
    /// Id of the loaded transaction, if any.
    transaction: Option<TransactionId>,
}

impl IcDbmsDatabase {
    /// Load an instance of the [`Database`] for one-shot operations (no transaction).
    pub fn oneshot(schema: impl DatabaseSchema + 'static) -> Self {
        Self {
            schema: Box::new(schema),
            transaction: None,
        }
    }

    /// Load an instance of the [`Database`] within a transaction context.
    pub fn from_transaction(
        schema: impl DatabaseSchema + 'static,
        transaction_id: TransactionId,
    ) -> Self {
        Self {
            schema: Box::new(schema),
            transaction: Some(transaction_id),
        }
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

    /// Executes a closure with a reference to the current [`Transaction`].
    fn with_transaction<F, R>(&self, f: F) -> IcDbmsResult<R>
    where
        F: FnOnce(&Transaction) -> IcDbmsResult<R>,
    {
        let txid = self.transaction.as_ref().ok_or(IcDbmsError::Transaction(
            TransactionError::NoActiveTransaction,
        ))?;

        TRANSACTION_SESSION.with_borrow_mut(|ts| {
            let tx = ts.get_transaction_mut(txid)?;
            f(tx)
        })
    }

    /// Executes a closure atomically within the database context.
    ///
    /// If the closure returns an error, the changes are rolled back by trapping the canister.
    fn atomic<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&IcDbmsDatabase) -> IcDbmsResult<R>,
    {
        match f(self) {
            Ok(res) => res,
            Err(err) => trap(err.to_string()),
        }
    }

    /// Deletes foreign key related records recursively if the delete behavior is [`DeleteBehavior::Cascade`].
    fn delete_foreign_keys_cascade<T>(
        &self,
        record_values: &[(ColumnDef, Value)],
    ) -> IcDbmsResult<u64>
    where
        T: TableSchema,
    {
        let mut count = 0;
        // verify referenced tables for foreign key constraints
        for (table, columns) in self.schema.referenced_tables(T::table_name()) {
            for column in columns.iter() {
                // prepare filter
                let pk = record_values
                    .iter()
                    .find(|(col_def, _)| col_def.primary_key)
                    .ok_or(IcDbmsError::Query(QueryError::UnknownColumn(
                        column.to_string(),
                    )))?
                    .1
                    .clone();
                // make filter to find records in the referenced table
                let filter = Filter::eq(column, pk);
                let res = self
                    .schema
                    .delete(self, table, DeleteBehavior::Cascade, Some(filter))?;
                count += res;
            }
        }
        Ok(count)
    }

    /// Retrieves the current [`DatabaseOverlay`].
    fn overlay(&self) -> IcDbmsResult<DatabaseOverlay> {
        self.with_transaction(|tx| Ok(tx.overlay().clone()))
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

    /// Retrieves existing primary keys for records matching the given filter.
    fn existing_primary_keys_for_filter<T>(
        &self,
        filter: Option<Filter>,
    ) -> IcDbmsResult<Vec<Value>>
    where
        T: TableSchema,
    {
        let pk = T::primary_key();
        let fields = self.select(Query::<T>::builder().filter(filter).build())?;
        let pks = fields
            .into_iter()
            .map(|record| {
                record
                    .to_values()
                    .into_iter()
                    .find(|(col_def, _value)| col_def.name == pk)
                    .expect("primary key not found") // this can't fail.
                    .1
            })
            .collect::<Vec<Value>>();

        Ok(pks)
    }

    /// Load the table registry for the given table schema.
    fn load_table_registry<T>(&self) -> IcDbmsResult<TableRegistry>
    where
        T: TableSchema,
    {
        // get pages of the table registry from schema registry
        let registry_pages = SCHEMA_REGISTRY
            .with_borrow(|schema| schema.table_registry_page::<T>())
            .ok_or(IcDbmsError::Table(TableError::TableNotFound))?;

        TableRegistry::load(registry_pages).map_err(IcDbmsError::from)
    }

    /// Sorts the query results based on the specified column and order direction.
    ///
    /// We only sort values which have [`ValuesSource::This`].
    #[allow(clippy::type_complexity)]
    fn sort_query_results(
        &self,
        results: &mut [Vec<(ValuesSource, Vec<(ColumnDef, Value)>)>],
        column: &'static str,
        direction: OrderDirection,
    ) {
        results.sort_by(|a, b| {
            let a_value = a
                .iter()
                .find(|(source, _)| *source == ValuesSource::This)
                .and_then(|(_, cols)| {
                    cols.iter()
                        .find(|(col_def, _)| col_def.name == column)
                        .map(|(_, value)| value)
                });
            let b_value = b
                .iter()
                .find(|(source, _)| *source == ValuesSource::This)
                .and_then(|(_, cols)| {
                    cols.iter()
                        .find(|(col_def, _)| col_def.name == column)
                        .map(|(_, value)| value)
                });

            match (a_value, b_value) {
                (Some(a_val), Some(b_val)) => match direction {
                    OrderDirection::Ascending => a_val.cmp(b_val),
                    OrderDirection::Descending => b_val.cmp(a_val),
                },
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
    }
}

impl Database for IcDbmsDatabase {
    /// Executes a SELECT query and returns the results.
    ///
    /// # Arguments
    ///
    /// - `query` - The SELECT [`Query`] to be executed.
    ///
    /// # Returns
    ///
    /// The returned results are a vector of [`table::TableRecord`] matching the query.
    fn select<T>(&self, query: Query<T>) -> IcDbmsResult<Vec<T::Record>>
    where
        T: TableSchema,
    {
        // load table registry
        let table_registry = self.load_table_registry::<T>()?;
        // read table
        let table_reader = table_registry.read::<T>();
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
            // push to results
            results.push(values);
            // check whether reached limit
            if query.limit.is_some_and(|limit| results.len() >= limit) {
                break;
            }
        }

        // sort results if needed and map to records
        for (column, direction) in query.order_by {
            self.sort_query_results(&mut results, column, direction);
        }

        Ok(results.into_iter().map(T::Record::from_values).collect())
    }

    /// Executes an INSERT query.
    ///
    /// # Arguments
    ///
    /// - `record` - The INSERT record to be executed.
    fn insert<T>(&self, record: T::Insert) -> IcDbmsResult<()>
    where
        T: TableSchema,
        T::Insert: InsertRecord<Schema = T>,
    {
        // check whether the insert is valid
        let record_values = record.clone().into_values();
        self.schema
            .validate_insert(self, T::table_name(), &record_values)?;

        if self.transaction.is_some() {
            // insert a new `insert` into the transaction
            self.with_transaction_mut(|tx| tx.insert::<T>(record_values))?;
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
    /// - `patch` - The UPDATE patch to be applied.
    /// - `filter` - An optional [`Filter`] to specify which records to update.
    ///
    /// # Returns
    ///
    /// The number of rows updated.
    fn update<T>(&self, patch: T::Update) -> IcDbmsResult<u64>
    where
        T: TableSchema,
        T::Update: UpdateRecord<Schema = T>,
    {
        // get all records matching the filter
        let query = Query::<T>::builder().filter(patch.where_clause()).build();
        let records = self.select::<T>(query)?;
        let count = records.len() as u64;

        if self.transaction.is_some() {
            let filter = patch.where_clause().clone();
            let pks = self.existing_primary_keys_for_filter::<T>(filter.clone())?;
            // insert a new `update` into the transaction
            self.with_transaction_mut(|tx| tx.update::<T>(patch, filter, pks))?;

            return Ok(count);
        }

        let patch = patch.update_values();
        // convert updates to values
        // for each record apply update; delete and insert
        let res = self.atomic(|db| {
            for record in records {
                let mut record_values = record.to_values();
                // apply patch
                for (col_def, value) in &patch {
                    if let Some((_, record_value)) = record_values
                        .iter_mut()
                        .find(|(record_col_def, _)| record_col_def.name == col_def.name)
                    {
                        *record_value = value.clone();
                    }
                }
                // create insert record
                let insert_record = T::Insert::from_values(&record_values)?;
                // delete old record
                let pk = record_values
                    .iter()
                    .find(|(col_def, _)| col_def.primary_key)
                    .expect("primary key not found") // this can't fail.
                    .1
                    .clone();
                db.delete::<T>(
                    DeleteBehavior::Break, // we just want to delete the old record
                    Some(Filter::eq(T::primary_key(), pk)),
                )?;
                // insert new record
                db.insert::<T>(insert_record)?;
            }
            Ok(count)
        });

        Ok(res)
    }

    /// Executes a DELETE query.
    ///
    /// # Arguments
    ///
    /// - `behaviour` - The [`DeleteBehavior`] to apply for foreign key constraints.
    /// - `filter` - An optional [`Filter`] to specify which records to delete.
    ///
    /// # Returns
    ///
    /// The number of rows deleted.
    fn delete<T>(&self, behaviour: DeleteBehavior, filter: Option<Filter>) -> IcDbmsResult<u64>
    where
        T: TableSchema,
    {
        if self.transaction.is_some() {
            let pks = self.existing_primary_keys_for_filter::<T>(filter.clone())?;
            let count = pks.len() as u64;

            self.with_transaction_mut(|tx| tx.delete::<T>(behaviour, filter, pks))?;

            return Ok(count);
        }

        // delete must be atomic
        let res = self.atomic(|db| {
            // delete directly from the database
            // select all records matching the filter
            // read table
            let mut table_registry = db.load_table_registry::<T>()?;
            let mut records = vec![];
            // iter all records
            // FIXME: this may be huge, we should do better
            {
                let mut table_reader = table_registry.read::<T>();
                while let Some(values) = table_reader.try_next()? {
                    let record_values = values.record.clone().to_values();
                    if let Some(filter) = &filter {
                        if !db.record_matches_filter(&record_values, filter)? {
                            continue;
                        }
                    }
                    records.push((values, record_values));
                }
            }
            // deleted records
            let mut count = records.len() as u64;
            for (record, record_values) in records {
                // match delete behaviour
                match behaviour {
                    DeleteBehavior::Cascade => {
                        // delete recursively foreign keys if cascade
                        count += self.delete_foreign_keys_cascade::<T>(&record_values)?;
                    }
                    DeleteBehavior::Restrict => {
                        if self.delete_foreign_keys_cascade::<T>(&record_values)? > 0 {
                            // it's okay; we panic here because we are in an atomic closure
                            return Err(IcDbmsError::Query(
                                QueryError::ForeignKeyConstraintViolation {
                                    referencing_table: T::table_name(),
                                    field: T::primary_key(),
                                },
                            ));
                        }
                    }
                    DeleteBehavior::Break => {
                        // do nothing
                    }
                }
                // eventually delete the record
                table_registry.delete(record.record, record.page, record.offset)?;
            }

            Ok(count)
        });

        Ok(res)
    }

    /// Commits the current transaction.
    ///
    /// The transaction is consumed.
    ///
    /// Any error during commit will trap the canister to ensure consistency.
    fn commit(&mut self) -> IcDbmsResult<()> {
        // take transaction out of self and get the transaction out of the storage
        // this also invalidates the overlay, so we won't have conflicts during validation
        let Some(txid) = self.transaction.take() else {
            return Err(IcDbmsError::Transaction(
                TransactionError::NoActiveTransaction,
            ));
        };
        let transaction = TRANSACTION_SESSION.with_borrow_mut(|ts| ts.take_transaction(&txid))?;

        // iterate over operations and apply them;
        // for each operation, first validate, then apply
        // using `self.atomic` when applying to ensure consistency
        for op in transaction.operations {
            match op {
                TransactionOp::Insert { table, values } => {
                    // validate
                    self.schema.validate_insert(self, table, &values)?;
                    // insert
                    self.atomic(|db| db.schema.insert(db, table, &values));
                }
                TransactionOp::Delete {
                    table,
                    behaviour,
                    filter,
                } => {
                    self.atomic(|db| db.schema.delete(db, table, behaviour, filter));
                }
                TransactionOp::Update {
                    table,
                    patch,
                    filter,
                } => {
                    self.atomic(|db| db.schema.update(db, table, &patch, filter));
                }
            }
        }

        Ok(())
    }

    /// Rolls back the current transaction.
    ///
    /// The transaction is consumed.
    fn rollback(&mut self) -> IcDbmsResult<()> {
        let Some(txid) = self.transaction.take() else {
            return Err(IcDbmsError::Transaction(
                TransactionError::NoActiveTransaction,
            ));
        };

        TRANSACTION_SESSION.with_borrow_mut(|ts| ts.close_transaction(&txid));
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use candid::{Nat, Principal};
    use ic_dbms_api::prelude::{Text, Uint32};

    use super::*;
    use crate::tests::{
        Message, POSTS_FIXTURES, Post, TestDatabaseSchema, USERS_FIXTURES, User, UserInsertRequest,
        UserUpdateRequest, load_fixtures,
    };

    #[test]
    fn test_should_init_dbms() {
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        assert!(dbms.transaction.is_none());

        let tx_dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, Nat::from(1u64));
        assert!(tx_dbms.transaction.is_some());
    }

    #[test]
    fn test_should_select_all_users() {
        load_fixtures();
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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
                            data_type: ic_dbms_api::prelude::DataTypeKind::Uint32,
                            nullable: false,
                            primary_key: true,
                            foreign_key: None,
                        },
                        Value::Uint32(999.into()),
                    ),
                    (
                        ColumnDef {
                            name: "name",
                            data_type: ic_dbms_api::prelude::DataTypeKind::Text,
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
        let dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, transaction_id);
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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder().field("unexisting_column").build();
        let result = dbms.select(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_select_queried_fields() {
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);

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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);

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

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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
    fn test_should_select_users_sorted_by_name_descending() {
        load_fixtures();
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder().all().order_by_desc("name").build();
        let users = dbms.select(query).expect("failed to select users");

        let mut sorted_usernames = USERS_FIXTURES.to_vec();
        sorted_usernames.sort_by(|a, b| b.cmp(a)); // descending

        assert_eq!(users.len(), USERS_FIXTURES.len());
        // check if all users all loaded in sorted order
        for (i, user) in users.iter().enumerate() {
            assert_eq!(
                user.name.as_ref().expect("should have name").0,
                sorted_usernames[i]
            );
        }
    }

    #[test]
    fn test_should_fail_loading_unexisting_relation() {
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);

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
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);

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

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let table_registry = dbms.load_table_registry::<User>();
        assert!(table_registry.is_ok());
    }

    #[test]
    fn test_should_insert_record_without_transaction() {
        load_fixtures();

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
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

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let new_user = UserInsertRequest {
            id: Uint32(1u32),
            name: Text("NewUser".to_string()),
        };

        let result = dbms.insert::<User>(new_user);
        assert!(result.is_err());
    }

    #[test]
    fn test_should_insert_within_a_transaction() {
        load_fixtures();

        // create a transaction
        let transaction_id =
            TRANSACTION_SESSION.with_borrow_mut(|ts| ts.begin_transaction(Principal::anonymous()));
        let mut dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, transaction_id.clone());

        let new_user = UserInsertRequest {
            id: Uint32(200u32),
            name: Text("TxUser".to_string()),
        };

        let result = dbms.insert::<User>(new_user);
        assert!(result.is_ok());

        // user should not be visible outside the transaction
        let oneshot_dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(200u32.into())))
            .build();
        let users = oneshot_dbms
            .select(query.clone())
            .expect("failed to select users");
        assert_eq!(users.len(), 0);

        // commit transaction
        let commit_result = dbms.commit();
        assert!(commit_result.is_ok());

        // now user should be visible
        let users_after_commit = oneshot_dbms.select(query).expect("failed to select users");
        assert_eq!(users_after_commit.len(), 1);

        let user = &users_after_commit[0];
        assert_eq!(user.id.expect("should have id").0, 200);
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            "TxUser".to_string()
        );

        // transaction should have been removed
        TRANSACTION_SESSION.with_borrow(|ts| {
            let tx_res = ts.get_transaction(&transaction_id);
            assert!(tx_res.is_err());
        });
    }

    #[test]
    fn test_should_rollback_transaction() {
        load_fixtures();

        // create a transaction
        let transaction_id =
            TRANSACTION_SESSION.with_borrow_mut(|ts| ts.begin_transaction(Principal::anonymous()));
        let mut dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, transaction_id.clone());
        let new_user = UserInsertRequest {
            id: Uint32(300u32),
            name: Text("RollbackUser".to_string()),
        };
        let result = dbms.insert::<User>(new_user);
        assert!(result.is_ok());

        // rollback transaction
        let rollback_result = dbms.rollback();
        assert!(rollback_result.is_ok());

        // user should not be visible
        let oneshot_dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(300u32.into())))
            .build();
        let users = oneshot_dbms.select(query).expect("failed to select users");
        assert_eq!(users.len(), 0);

        // transaction should have been removed
        TRANSACTION_SESSION.with_borrow(|ts| {
            let tx_res = ts.get_transaction(&transaction_id);
            assert!(tx_res.is_err());
        });
    }

    #[test]
    fn test_should_delete_one_shot() {
        load_fixtures();

        // insert user with id 100
        let new_user = UserInsertRequest {
            id: Uint32(100u32),
            name: Text("DeleteUser".to_string()),
        };
        assert!(
            IcDbmsDatabase::oneshot(TestDatabaseSchema)
                .insert::<User>(new_user)
                .is_ok()
        );

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(100u32.into())))
            .build();
        let delete_count = dbms
            .delete::<User>(
                DeleteBehavior::Restrict,
                Some(Filter::eq("id", Value::Uint32(100u32.into()))),
            )
            .expect("failed to delete user");
        assert_eq!(delete_count, 1);

        // verify user is deleted
        let users = dbms.select(query).expect("failed to select users");
        assert_eq!(users.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Foreign key constraint violation")]
    fn test_should_not_delete_with_fk_restrict() {
        load_fixtures();

        // user 1 has post and messages for sure.
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        dbms.delete::<User>(
            DeleteBehavior::Restrict,
            Some(Filter::eq("id", Value::Uint32(1u32.into()))),
        )
        .expect("failed to delete user");
    }

    #[test]
    fn test_should_delete_with_fk_cascade() {
        load_fixtures();

        // user 1 has posts and messages for sure.
        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let delete_count = dbms
            .delete::<User>(
                DeleteBehavior::Cascade,
                Some(Filter::eq("id", Value::Uint32(1u32.into()))),
            )
            .expect("failed to delete user");
        assert!(delete_count > 0);

        // verify user is deleted
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(1u32.into())))
            .build();
        let users = dbms.select(query).expect("failed to select users");
        assert_eq!(users.len(), 0);

        // check posts are deleted (post ID 2)
        let post_query = Query::<Post>::builder()
            .and_where(Filter::eq("user_id", Value::Uint32(1u32.into())))
            .build();
        let posts = dbms.select(post_query).expect("failed to select posts");
        assert_eq!(posts.len(), 0);

        // check messages are deleted (message ID 1)
        let message_query = Query::<Message>::builder()
            .and_where(Filter::eq("sender_id", Value::Uint32(1u32.into())))
            .or_where(Filter::eq("recipient_id", Value::Uint32(1u32.into())))
            .build();
        let messages = dbms
            .select(message_query)
            .expect("failed to select messages");
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_should_delete_within_transaction() {
        load_fixtures();

        // create a transaction
        let transaction_id =
            TRANSACTION_SESSION.with_borrow_mut(|ts| ts.begin_transaction(Principal::anonymous()));
        let mut dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, transaction_id.clone());

        let delete_count = dbms
            .delete::<User>(
                DeleteBehavior::Cascade,
                Some(Filter::eq("id", Value::Uint32(2u32.into()))),
            )
            .expect("failed to delete user");
        assert!(delete_count > 0);

        // user should not be visible outside the transaction
        let oneshot_dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder()
            .and_where(Filter::eq("id", Value::Uint32(2u32.into())))
            .build();
        let users = oneshot_dbms
            .select(query.clone())
            .expect("failed to select users");
        assert_eq!(users.len(), 1);

        // commit transaction
        let commit_result = dbms.commit();
        assert!(commit_result.is_ok());

        // now user should be deleted
        let users_after_commit = oneshot_dbms.select(query).expect("failed to select users");
        assert_eq!(users_after_commit.len(), 0);

        // check posts are deleted
        let post_query = Query::<Post>::builder()
            .and_where(Filter::eq("user_id", Value::Uint32(2u32.into())))
            .build();
        let posts = oneshot_dbms
            .select(post_query)
            .expect("failed to select posts");
        assert_eq!(posts.len(), 0);

        // check messages are deleted
        let message_query = Query::<Message>::builder()
            .and_where(Filter::eq("sender_id", Value::Uint32(2u32.into())))
            .or_where(Filter::eq("recipient_id", Value::Uint32(2u32.into())))
            .build();
        let messages = oneshot_dbms
            .select(message_query)
            .expect("failed to select messages");
        assert_eq!(messages.len(), 0);

        // transaction should have been removed
        TRANSACTION_SESSION.with_borrow(|ts| {
            let tx_res = ts.get_transaction(&transaction_id);
            assert!(tx_res.is_err());
        });
    }

    #[test]
    fn test_should_update_one_shot() {
        load_fixtures();

        let dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let filter = Filter::eq("id", Value::Uint32(3u32.into()));

        let patch = UserUpdateRequest {
            id: None,
            name: Some(Text("UpdatedName".to_string())),
            where_clause: Some(filter.clone()),
        };

        let update_count = dbms.update::<User>(patch).expect("failed to update user");
        assert_eq!(update_count, 1);

        // verify user is updated
        let query = Query::<User>::builder().and_where(filter).build();
        let users = dbms.select(query).expect("failed to select users");
        assert_eq!(users.len(), 1);
        let user = &users[0];
        assert_eq!(user.id.expect("should have id").0, 3);
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            "UpdatedName".to_string()
        );
    }

    #[test]
    fn test_should_update_within_transaction() {
        load_fixtures();

        // create a transaction
        let transaction_id =
            TRANSACTION_SESSION.with_borrow_mut(|ts| ts.begin_transaction(Principal::anonymous()));
        let mut dbms = IcDbmsDatabase::from_transaction(TestDatabaseSchema, transaction_id.clone());

        let filter = Filter::eq("id", Value::Uint32(4u32.into()));
        let patch = UserUpdateRequest {
            id: None,
            name: Some(Text("TxUpdatedName".to_string())),
            where_clause: Some(filter.clone()),
        };

        let update_count = dbms.update::<User>(patch).expect("failed to update user");
        assert_eq!(update_count, 1);

        // user should not be visible outside the transaction
        let oneshot_dbms = IcDbmsDatabase::oneshot(TestDatabaseSchema);
        let query = Query::<User>::builder().and_where(filter.clone()).build();
        let users = oneshot_dbms
            .select(query.clone())
            .expect("failed to select users");
        let user = &users[0];
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            USERS_FIXTURES[4]
        );

        // commit transaction
        let commit_result = dbms.commit();
        assert!(commit_result.is_ok());

        // now user should be updated
        let users_after_commit = oneshot_dbms.select(query).expect("failed to select users");
        assert_eq!(users_after_commit.len(), 1);
        let user = &users_after_commit[0];
        assert_eq!(
            user.name.as_ref().expect("should have name").0,
            "TxUpdatedName".to_string()
        );

        // transaction should have been removed
        TRANSACTION_SESSION.with_borrow(|ts| {
            let tx_res = ts.get_transaction(&transaction_id);
            assert!(tx_res.is_err());
        });
    }

    fn init_user_table() {
        SCHEMA_REGISTRY
            .with_borrow_mut(|sr| sr.register_table::<User>())
            .expect("failed to register `User` table");
    }
}
