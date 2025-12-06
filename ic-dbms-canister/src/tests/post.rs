//! Post mock type; 1 user has many posts.

use ic_dbms_api::prelude::{
    ColumnDef, DataTypeKind, Database, Filter, ForeignFetcher, ForeignKeyDef, IcDbmsError,
    InsertRecord, Query, QueryError, TableColumns, TableRecord, TableSchema, Text, Uint32,
    UpdateRecord, Value, ValuesSource,
};
use ic_dbms_macros::Encode;

use crate::memory::{SCHEMA_REGISTRY, TableRegistry};
use crate::tests::{User, UserRecord, self_reference_values};

/// A simple post struct for testing purposes.
///
/// One [`super::User`] has many [`Post`]s.
#[derive(Debug, Encode, Clone, PartialEq, Eq)]
pub struct Post {
    pub id: Uint32,
    pub title: Text,
    pub content: Text,
    pub user_id: Uint32,
}

/// A record returned by queries for the `posts` table.
pub struct PostRecord {
    pub id: Option<Uint32>,
    pub title: Option<Text>,
    pub content: Option<Text>,
    pub user: Option<UserRecord>,
}

/// An insert request for the `posts` table.
#[derive(Clone)]
pub struct PostInsertRequest {
    pub id: Uint32,
    pub title: Text,
    pub content: Text,
    pub user_id: Uint32,
}

/// An update request for the `posts` table.
pub struct PostUpdateRequest {
    pub id: Option<Uint32>,
    pub title: Option<Text>,
    pub content: Option<Text>,
    pub user_id: Option<Uint32>,
    pub where_clause: Option<Filter>,
}

#[derive(Default)]
pub struct PostForeignFetcher;

impl ForeignFetcher for PostForeignFetcher {
    fn fetch(
        &self,
        database: &impl Database,
        table: &'static str,
        local_column: &'static str,
        pk_value: Value,
    ) -> ic_dbms_api::prelude::IcDbmsResult<TableColumns> {
        if table != User::table_name() {
            return Err(IcDbmsError::Query(QueryError::InvalidQuery(format!(
                "ForeignFetcher: unknown table '{table}' for {table_name} foreign fetcher",
                table_name = Post::table_name()
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

        let values = user.to_values();
        Ok(vec![(
            ValuesSource::Foreign {
                table,
                column: local_column,
            },
            values,
        )])
    }
}

impl TableSchema for Post {
    type Insert = PostInsertRequest;
    type Record = PostRecord;
    type Update = PostUpdateRequest;
    type ForeignFetcher = PostForeignFetcher;

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
                name: "title",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            ColumnDef {
                name: "content",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
            ColumnDef {
                name: "user_id",
                data_type: DataTypeKind::Uint32,
                nullable: false,
                primary_key: false,
                foreign_key: Some(ForeignKeyDef {
                    local_column: "user_id",
                    foreign_table: "users",
                    foreign_column: "id",
                }),
            },
        ]
    }

    fn table_name() -> &'static str {
        "posts"
    }

    fn primary_key() -> &'static str {
        "id"
    }

    fn to_values(self) -> Vec<(ColumnDef, Value)> {
        vec![
            (Self::columns()[0], Value::Uint32(self.id)),
            (Self::columns()[1], Value::Text(self.title)),
            (Self::columns()[2], Value::Text(self.content)),
            (Self::columns()[3], Value::Uint32(self.user_id)),
        ]
    }
}

impl TableRecord for PostRecord {
    type Schema = Post;

    fn from_values(values: TableColumns) -> Self {
        let mut id: Option<Uint32> = None;
        let mut title: Option<Text> = None;
        let mut content: Option<Text> = None;

        let post_values = values
            .iter()
            .find(|(table_name, _)| *table_name == ValuesSource::This)
            .map(|(_, cols)| cols);

        for (column, value) in post_values.unwrap_or(&vec![]) {
            match column.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "title" => {
                    if let Value::Text(v) = value {
                        title = Some(v.clone());
                    }
                }
                "content" => {
                    if let Value::Text(v) = value {
                        content = Some(v.clone());
                    }
                }
                _ => { /* Ignore unknown columns */ }
            }
        }

        let has_user = values.iter().any(|(source, _)| {
            *source
                == ValuesSource::Foreign {
                    table: User::table_name(),
                    column: "user_id",
                }
        });
        let user = if has_user {
            Some(UserRecord::from_values(self_reference_values(
                &values,
                User::table_name(),
                "user_id",
            )))
        } else {
            None
        };

        Self {
            id,
            title,
            content,
            user,
        }
    }

    fn to_values(&self) -> Vec<(ColumnDef, Value)> {
        Self::Schema::columns()
            .iter()
            .zip(vec![
                match self.id {
                    Some(v) => Value::Uint32(v),
                    None => Value::Null,
                },
                match &self.title {
                    Some(v) => Value::Text(v.clone()),
                    None => Value::Null,
                },
                match &self.content {
                    Some(v) => Value::Text(v.clone()),
                    None => Value::Null,
                },
            ])
            .map(|(col_def, value)| (*col_def, value))
            .collect()
    }
}

impl InsertRecord for PostInsertRequest {
    type Record = PostRecord;
    type Schema = Post;

    fn from_values(values: &[(ColumnDef, Value)]) -> ic_dbms_api::prelude::IcDbmsResult<Self> {
        let mut id: Option<Uint32> = None;
        let mut title: Option<Text> = None;
        let mut content: Option<Text> = None;
        let mut user_id: Option<Uint32> = None;

        for (column, value) in values {
            match column.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "title" => {
                    if let Value::Text(v) = value {
                        title = Some(v.clone());
                    }
                }
                "content" => {
                    if let Value::Text(v) = value {
                        content = Some(v.clone());
                    }
                }
                "user_id" => {
                    if let Value::Uint32(v) = value {
                        user_id = Some(*v);
                    }
                }
                _ => { /* Ignore unknown columns */ }
            }
        }

        Ok(Self {
            id: id.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "id",
            )))?,
            title: title.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "title",
            )))?,
            content: content.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "content",
            )))?,
            user_id: user_id.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "user_id",
            )))?,
        })
    }

    fn into_values(self) -> Vec<(ColumnDef, Value)> {
        vec![
            (Self::Schema::columns()[0], Value::Uint32(self.id)),
            (Self::Schema::columns()[1], Value::Text(self.title)),
            (Self::Schema::columns()[2], Value::Text(self.content)),
            (Self::Schema::columns()[3], Value::Uint32(self.user_id)),
        ]
    }

    fn into_record(self) -> Self::Schema {
        Post {
            id: self.id,
            title: self.title,
            content: self.content,
            user_id: self.user_id,
        }
    }
}

impl UpdateRecord for PostUpdateRequest {
    type Record = PostRecord;
    type Schema = Post;

    fn from_values(values: &[(ColumnDef, Value)], where_clause: Option<Filter>) -> Self {
        let mut id: Option<Uint32> = None;
        let mut title: Option<Text> = None;
        let mut content: Option<Text> = None;
        let mut user_id: Option<Uint32> = None;

        for (column, value) in values {
            match column.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "title" => {
                    if let Value::Text(v) = value {
                        title = Some(v.clone());
                    }
                }
                "content" => {
                    if let Value::Text(v) = value {
                        content = Some(v.clone());
                    }
                }
                "user_id" => {
                    if let Value::Uint32(v) = value {
                        user_id = Some(*v);
                    }
                }
                _ => { /* Ignore unknown columns */ }
            }
        }

        Self {
            id,
            title,
            content,
            user_id,
            where_clause,
        }
    }

    fn update_values(&self) -> Vec<(ColumnDef, Value)> {
        let mut updates = Vec::new();

        if let Some(id) = self.id {
            updates.push((Self::Schema::columns()[0], Value::Uint32(id)));
        }
        if let Some(title) = &self.title {
            updates.push((Self::Schema::columns()[1], Value::Text(title.clone())));
        }
        if let Some(content) = &self.content {
            updates.push((Self::Schema::columns()[2], Value::Text(content.clone())));
        }
        if let Some(user_id) = self.user_id {
            updates.push((Self::Schema::columns()[3], Value::Uint32(user_id)));
        }

        updates
    }

    fn where_clause(&self) -> Option<Filter> {
        self.where_clause.clone()
    }
}

pub const POSTS_FIXTURES: &[(&str, &str, u32)] = &[
    ("First Post", "This is the content of the first post.", 0),
    ("Second Post", "This is the content of the second post.", 0),
    ("Third Post", "This is the content of the third post.", 1),
    ("Fourth Post", "This is the content of the fourth post.", 1),
    ("Fifth Post", "This is the content of the fifth post.", 2),
    ("Sixth Post", "This is the content of the sixth post.", 2),
    (
        "Seventh Post",
        "This is the content of the seventh post.",
        3,
    ),
    ("Eighth Post", "This is the content of the eighth post.", 3),
    ("Ninth Post", "This is the content of the ninth post.", 4),
    ("Tenth Post", "This is the content of the tenth post.", 4),
    (
        "Eleventh Post",
        "This is the content of the eleventh post.",
        5,
    ),
    (
        "Twelfth Post",
        "This is the content of the twelfth post.",
        5,
    ),
    (
        "Thirteenth Post",
        "This is the content of the thirteenth post.",
        6,
    ),
    (
        "Fourteenth Post",
        "This is the content of the fourteenth post.",
        6,
    ),
    (
        "Fifteenth Post",
        "This is the content of the fifteenth post.",
        7,
    ),
    (
        "Sixteenth Post",
        "This is the content of the sixteenth post.",
        7,
    ),
    (
        "Seventeenth Post",
        "This is the content of the seventeenth post.",
        8,
    ),
    (
        "Eighteenth Post",
        "This is the content of the eighteenth post.",
        8,
    ),
    (
        "Nineteenth Post",
        "This is the content of the nineteenth post.",
        9,
    ),
    (
        "Twentieth Post",
        "This is the content of the twentieth post.",
        9,
    ),
];

pub fn load_fixtures() {
    // register tables
    let posts_pages = SCHEMA_REGISTRY
        .with_borrow_mut(|sr| sr.register_table::<Post>())
        .expect("failed to register `Post` table");

    let mut posts_table: TableRegistry =
        TableRegistry::load(posts_pages).expect("failed to load `Post` table registry");

    // insert users
    for (id, (title, content, user_id)) in POSTS_FIXTURES.iter().enumerate() {
        let post = Post {
            id: Uint32(id as u32),
            title: Text(title.to_string()),
            content: Text(content.to_string()),
            user_id: Uint32(*user_id),
        };
        posts_table.insert(post).expect("failed to insert post");
    }
}
