use ic_dbms_macros::Encode;

use crate::dbms::table::{ColumnDef, TableColumns, TableRecord, TableSchema, ValuesSource};
use crate::dbms::types::{DataTypeKind, Text, Uint32};
use crate::dbms::value::Value;
use crate::memory::Encode;
use crate::prelude::{
    Filter, IcDbmsError, InsertRecord, NoForeignFetcher, QueryError, UpdateRecord,
};

/// A simple user struct for testing purposes.
#[derive(Debug, Encode, Clone, PartialEq, Eq)]
pub struct User {
    pub id: Uint32,
    pub name: Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    pub id: Option<Uint32>,
    pub name: Option<Text>,
}

#[derive(Clone)]
pub struct UserInsertRequest {
    pub id: Uint32,
    pub name: Text,
}

pub struct UserUpdateRequest {
    pub id: Option<Uint32>,
    pub name: Option<Text>,
    pub where_clause: Option<Filter>,
}

impl InsertRecord for UserInsertRequest {
    type Record = UserRecord;
    type Schema = User;

    fn from_values(values: &[(ColumnDef, Value)]) -> crate::prelude::IcDbmsResult<Self> {
        let mut id = None;
        let mut name = None;

        for (col_def, value) in values {
            match col_def.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "name" => {
                    if let Value::Text(v) = value {
                        name = Some(v.clone());
                    }
                }
                _ => {}
            }
        }

        Ok(UserInsertRequest {
            id: id.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "id",
            )))?,
            name: name.ok_or(IcDbmsError::Query(QueryError::MissingNonNullableField(
                "name",
            )))?,
        })
    }

    fn into_values(self) -> Vec<(ColumnDef, crate::dbms::value::Value)> {
        vec![
            (Self::Schema::columns()[0], Value::Uint32(self.id)),
            (Self::Schema::columns()[1], Value::Text(self.name)),
        ]
    }

    fn into_record(self) -> Self::Schema {
        User {
            id: self.id,
            name: self.name,
        }
    }
}

impl UpdateRecord for UserUpdateRequest {
    type Record = UserRecord;
    type Schema = User;

    fn from_values(values: &[(ColumnDef, Value)], where_clause: Option<Filter>) -> Self {
        let mut id = None;
        let mut name = None;

        for (col_def, value) in values {
            match col_def.name {
                "id" => {
                    if let Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "name" => {
                    if let Value::Text(v) = value {
                        name = Some(v.clone());
                    }
                }
                _ => {}
            }
        }

        UserUpdateRequest {
            id,
            name,
            where_clause,
        }
    }

    fn update_values(&self) -> Vec<(ColumnDef, crate::dbms::value::Value)> {
        let mut values = vec![];
        if let Some(id) = self.id {
            values.push((
                ColumnDef {
                    name: "id",
                    data_type: DataTypeKind::Uint32,
                    nullable: false,
                    primary_key: true,
                    foreign_key: None,
                },
                crate::dbms::value::Value::Uint32(id),
            ));
        }
        if let Some(name) = &self.name {
            values.push((
                ColumnDef {
                    name: "name",
                    data_type: DataTypeKind::Text,
                    nullable: false,
                    primary_key: false,
                    foreign_key: None,
                },
                crate::dbms::value::Value::Text(name.clone()),
            ));
        }
        values
    }

    fn where_clause(&self) -> Option<Filter> {
        self.where_clause.clone()
    }
}

impl TableRecord for UserRecord {
    type Schema = User;

    fn from_values(values: TableColumns) -> Self {
        let mut id = None;
        let mut name = None;

        let user_values = values
            .iter()
            .find(|(table_name, _)| *table_name == ValuesSource::This)
            .map(|(_, cols)| cols);

        for (col_def, value) in user_values.unwrap_or(&vec![]) {
            match col_def.name {
                "id" => {
                    if let crate::dbms::value::Value::Uint32(v) = value {
                        id = Some(*v);
                    }
                }
                "name" => {
                    if let crate::dbms::value::Value::Text(v) = value {
                        name = Some(v.clone());
                    }
                }
                _ => {}
            }
        }

        UserRecord { id, name }
    }

    fn to_values(&self) -> Vec<(ColumnDef, crate::dbms::value::Value)> {
        Self::Schema::columns()
            .iter()
            .zip(vec![
                match self.id {
                    Some(v) => Value::Uint32(v),
                    None => Value::Null,
                },
                match &self.name {
                    Some(v) => Value::Text(v.clone()),
                    None => Value::Null,
                },
            ])
            .map(|(col_def, value)| (*col_def, value))
            .collect()
    }
}

impl TableSchema for User {
    type Record = UserRecord;
    type Insert = UserInsertRequest;
    type Update = UserUpdateRequest;
    type ForeignFetcher = NoForeignFetcher;

    fn table_name() -> &'static str {
        "users"
    }

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
                name: "name",
                data_type: DataTypeKind::Text,
                nullable: false,
                primary_key: false,
                foreign_key: None,
            },
        ]
    }

    fn primary_key() -> &'static str {
        "id"
    }

    fn to_values(self) -> Vec<(ColumnDef, Value)> {
        vec![
            (Self::columns()[0], Value::Uint32(self.id)),
            (Self::columns()[1], Value::Text(self.name)),
        ]
    }
}

#[allow(clippy::module_inception)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_encode_decode() {
        let user = User {
            id: 42u32.into(),
            name: "Alice".to_string().into(),
        };
        let encoded = user.encode();
        let decoded = User::decode(encoded).unwrap();
        assert_eq!(user, decoded);
    }

    #[test]
    fn test_should_have_fingerprint() {
        let fingerprint = User::fingerprint();
        assert_ne!(fingerprint, 0);
    }
}
