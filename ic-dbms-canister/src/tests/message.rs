use ic_dbms_macros::Encode;

use crate::IcDbmsError;
use crate::dbms::table::{ColumnDef, ForeignKeyDef, TableColumns, ValuesSource};
use crate::dbms::types::{DataTypeKind, DateTime, Nullable, Text, Uint32};
use crate::dbms::value::Value;
use crate::memory::{SCHEMA_REGISTRY, TableRegistry};
use crate::prelude::{
    Filter, ForeignFetcher, InsertRecord, Query, QueryError, TableRecord, TableSchema, UpdateRecord,
};
use crate::tests::{User, UserRecord, self_reference_values};

/// A simple message struct for testing purposes.
#[derive(Debug, Encode, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: Uint32,
    pub text: Text,
    pub sender_id: Uint32,
    pub recipient_id: Uint32,
    pub read_at: Nullable<DateTime>,
}

/// A record returned by queries for the `messages` table.
pub struct MessageRecord {
    pub id: Option<Uint32>,
    pub text: Option<Text>,
    pub sender: Option<UserRecord>,
    pub recipient: Option<UserRecord>,
    pub read_at: Option<Nullable<DateTime>>,
}

/// An insert request for the `messages` table.
#[derive(Clone)]
pub struct MessageInsertRequest {
    pub id: Uint32,
    pub text: Text,
    pub sender_id: Uint32,
    pub recipient_id: Uint32,
    pub read_at: Nullable<DateTime>,
}

/// An update request for the `posts` table.
pub struct MessageUpdateRequest {
    pub id: Option<Uint32>,
    pub text: Option<Text>,
    pub sender_id: Option<Uint32>,
    pub recipient_id: Option<Uint32>,
    pub read_at: Option<Nullable<DateTime>>,
    pub where_clause: Option<Filter>,
}

#[derive(Default)]
pub struct MessageForeignFetcher;

impl ForeignFetcher for MessageForeignFetcher {
    fn fetch(
        &self,
        database: &crate::prelude::Database,
        table: &'static str,
        local_column: &'static str,
        pk_value: Value,
    ) -> crate::IcDbmsResult<TableColumns> {
        if table != User::table_name() {
            return Err(IcDbmsError::Query(QueryError::InvalidQuery(format!(
                "ForeignFetcher: unknown table '{table}' for {table_name} foreign fetcher",
                table_name = Message::table_name()
            ))));
        }

        // query all records from the foreign table
        let mut users = database.select(
            Query::<User>::builder()
                .all()
                .limit(1)
                .and_where(Filter::Eq(User::primary_key(), pk_value.clone()))
                .build(),
        )?;
        let user = match users.pop() {
            Some(user) => user,
            None => {
                return Err(IcDbmsError::Query(QueryError::BrokenForeignKeyReference {
                    table: User::table_name(),
                    key: pk_value,
                }));
            }
        };

        let values = User::columns()
            .iter()
            .zip(user.to_values())
            .map(|(col_def, value)| (*col_def, value))
            .collect();
        println!(
            "MessageForeignFetcher: fetched user values: {:?}; table: {table}; column: {local_column}",
            values
        );
        Ok(vec![(
            ValuesSource::Foreign {
                table,
                column: local_column,
            },
            values,
        )])
    }
}

impl TableSchema for Message {
    type Insert = MessageInsertRequest;
    type Record = MessageRecord;
    type Update = MessageUpdateRequest;
    type ForeignFetcher = MessageForeignFetcher;

    fn columns() -> &'static [ColumnDef] {
        &[
            ColumnDef {
                name: "id",
                data_type: DataTypeKind::Uint32,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            ColumnDef {
                name: "text",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            ColumnDef {
                name: "sender_id",
                data_type: DataTypeKind::Uint32,
                nullable: false,
                primary_key: false,
                foreign_key: Some(ForeignKeyDef {
                    local_column: "sender_id",
                    foreign_table: "users",
                    foreign_column: "id",
                }),
            },
            ColumnDef {
                name: "recipient_id",
                data_type: DataTypeKind::Uint32,
                nullable: false,
                primary_key: false,
                foreign_key: Some(ForeignKeyDef {
                    local_column: "recipient_id",
                    foreign_table: "users",
                    foreign_column: "id",
                }),
            },
            ColumnDef {
                name: "read_at",
                data_type: DataTypeKind::DateTime,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
        ]
    }

    fn table_name() -> &'static str {
        "messages"
    }

    fn primary_key() -> &'static str {
        "id"
    }

    fn to_values(self) -> Vec<(ColumnDef, Value)> {
        vec![
            (Self::columns()[0], Value::Uint32(self.id)),
            (Self::columns()[1], Value::Text(self.text)),
            (Self::columns()[2], Value::Uint32(self.sender_id)),
            (Self::columns()[3], Value::Uint32(self.recipient_id)),
            (
                Self::columns()[4],
                match self.read_at {
                    Nullable::Value(dt) => Value::DateTime(dt),
                    Nullable::Null => Value::Null,
                },
            ),
        ]
    }
}

impl TableRecord for MessageRecord {
    type Schema = Message;

    fn from_values(values: TableColumns) -> Self {
        let mut id: Option<Uint32> = None;
        let mut text: Option<Text> = None;
        let mut read_at: Option<Nullable<DateTime>> = None;

        let messages_values = values
            .iter()
            .find(|(table_name, _)| *table_name == ValuesSource::This)
            .map(|(_, cols)| cols);

        for (column, value) in messages_values.unwrap_or(&vec![]) {
            match column.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "text" => {
                    if let Value::Text(v) = value {
                        text = Some(v.clone());
                    }
                }
                "read_at" => {
                    if let Value::DateTime(v) = value {
                        // Assuming Nullable<DateTime> can be constructed from DateTime
                        read_at = Some(Nullable::Value(*v));
                    } else if let Value::Null = value {
                        read_at = Some(Nullable::Null);
                    }
                }
                _ => { /* Ignore unknown columns */ }
            }
        }

        let has_sender = values.iter().any(|(table_name, _)| {
            *table_name
                == ValuesSource::Foreign {
                    table: User::table_name(),
                    column: "sender_id",
                }
        });
        println!("MessageRecord: has_sender = {}", has_sender);
        let sender = if has_sender {
            println!(
                "DIo canstructing UserRecord from foreign values: {:?}",
                values
            );
            Some(UserRecord::from_values(self_reference_values(
                &values,
                User::table_name(),
                "sender_id",
            )))
        } else {
            None
        };
        let has_recipient = values.iter().any(|(table_name, _)| {
            *table_name
                == ValuesSource::Foreign {
                    table: User::table_name(),
                    column: "recipient_id",
                }
        });
        let recipient = if has_recipient {
            Some(UserRecord::from_values(self_reference_values(
                &values,
                User::table_name(),
                "recipient_id",
            )))
        } else {
            None
        };

        Self {
            id,
            text,
            sender,
            recipient,
            read_at,
        }
    }

    fn to_values(&self) -> Vec<Value> {
        vec![
            match self.id {
                Some(v) => Value::Uint32(v),
                None => Value::Null,
            },
            match &self.text {
                Some(v) => Value::Text(v.clone()),
                None => Value::Null,
            },
            match &self.read_at {
                Some(Nullable::Value(v)) => Value::DateTime(*v),
                Some(Nullable::Null) | None => Value::Null,
            },
        ]
    }
}

impl InsertRecord for MessageInsertRequest {
    type Record = MessageRecord;
    type Schema = Message;

    fn into_values(self) -> Vec<(ColumnDef, Value)> {
        vec![
            (Self::Schema::columns()[0], Value::Uint32(self.id)),
            (Self::Schema::columns()[1], Value::Text(self.text)),
            (Self::Schema::columns()[2], Value::Uint32(self.sender_id)),
            (Self::Schema::columns()[3], self.recipient_id.into()),
            (Self::Schema::columns()[4], self.read_at.into()),
        ]
    }

    fn into_record(self) -> Self::Schema {
        Message {
            id: self.id,
            text: self.text,
            sender_id: self.sender_id,
            recipient_id: self.recipient_id,
            read_at: self.read_at,
        }
    }
}

impl UpdateRecord for MessageUpdateRequest {
    type Record = MessageRecord;
    type Schema = Message;

    fn update_values(&self) -> Vec<(ColumnDef, Value)> {
        let mut updates = Vec::new();

        if let Some(id) = self.id {
            updates.push((Self::Schema::columns()[0], Value::Uint32(id)));
        }
        if let Some(text) = &self.text {
            updates.push((Self::Schema::columns()[1], Value::Text(text.clone())));
        }
        if let Some(sender_id) = self.sender_id {
            updates.push((Self::Schema::columns()[2], Value::Uint32(sender_id)));
        }
        if let Some(recipient_id) = self.recipient_id {
            updates.push((Self::Schema::columns()[3], Value::Uint32(recipient_id)));
        }
        if let Some(read_at) = &self.read_at {
            updates.push((Self::Schema::columns()[4], (*read_at).into()));
        }

        updates
    }

    fn where_clause(&self) -> Option<Filter> {
        self.where_clause.clone()
    }
}

pub const MESSAGES_FIXTURES: &[(&str, u32, u32)] = &[
    ("Hello, World!", 0, 1),
    ("How are you?", 1, 0),
    ("Goodbye!", 1, 3),
];

pub fn load_fixtures() {
    // register tables
    let messages_pages = SCHEMA_REGISTRY
        .with_borrow_mut(|sr| sr.register_table::<Message>())
        .expect("failed to register `Message` table");

    let mut messages_table: TableRegistry<Message> =
        TableRegistry::load(messages_pages).expect("failed to load `Message` table registry");

    // insert users
    for (id, (text, sender_id, recipient_id)) in MESSAGES_FIXTURES.iter().enumerate() {
        let post = Message {
            id: Uint32(id as u32),
            text: Text(text.to_string()),
            sender_id: Uint32(*sender_id),
            recipient_id: Uint32(*recipient_id),
            read_at: Nullable::Null,
        };
        messages_table.insert(post).expect("failed to insert post");
    }
}
