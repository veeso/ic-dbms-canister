# IC DBMS Canister

![logo](./assets/images/cargo/logo-128.png)

[![license-mit](https://img.shields.io/crates/l/ic-dbms-canister.svg)](https://opensource.org/licenses/MIT)
[![repo-stars](https://img.shields.io/github/stars/veeso/ic-dbms-canister?style=flat)](https://github.com/veeso/ic-dbms-canister/stargazers)
[![downloads](https://img.shields.io/crates/d/ic-dbms-canister.svg)](https://crates.io/crates/ic-dbms-canister)
[![latest-version](https://img.shields.io/crates/v/ic-dbms-canister.svg)](https://crates.io/crates/ic-dbms-canister)
[![ko-fi](https://img.shields.io/badge/donate-ko--fi-red)](https://ko-fi.com/veeso)
[![conventional-commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)

[![ci](https://github.com/veeso/ic-dbms-canister/actions/workflows/ci.yml/badge.svg)](https://github.com/veeso/ic-dbms-canister/actions)
[![coveralls](https://coveralls.io/repos/github/veeso/ic-dbms-canister/badge.svg)](https://coveralls.io/github/veeso/ic-dbms-canister)
[![docs](https://docs.rs/ic-dbms-canister/badge.svg)](https://docs.rs/ic-dbms-canister)

This project is in a very early stage of development. The goal is to provide a framework for building database canisters on the Internet Computer.

## Overview

IC DBMS Canister is an Internet Computer framework which provides an easy way to implement a database canister by just providing the database schema.

The user can just define the data entity by defining the tables

```rust
#[derive(Table)]
#[table(name = "users")]
struct User {
    #[primary_key]
    id: Uint64,
    name: Text,
    email: Text,
    age: Nullable<Uint32>,
}
```

This will provide for the user the following API:

todo...

You can also define relationships between tables:

```rust
#[derive(Table)]
#[table(name = "posts")]
struct Post {
    #[primary_key]
    id: Uint32,
    title: Text,
    content: Text,
    #[foreign_key(table = "User", column = "id")]
    author_id: Uint32,
}
```

And once you have defined all your tables, you can instantiate the database canister:

```rust
ic_dbms_canister!(User, Post);
```

And you will have a fully functional database canister with all the CRUD operations implemented for you.

The canister API will be automatically generated based on the defined tables, with the following methods:

todo...

## Interacting with the Canister

To interact with a `ic-dbms-canister`, you first of all need to import `ic-dbms-api` in your project:

```toml
[dependencies]
ic-dbms-api = "0.1"
```

You can find the documentation for `ic-dbms-api` at <https://docs.rs/ic-dbms-api>.

todo...

## Features

- [x] Define tables with common attributes
- [x] CRUD operations
- [x] Complex queries with filtering and pagination
- [x] Relationships between tables with foreign keys
- [x] Transactions with commit and rollback
- [x] Access Control Lists (ACL) to restrict access to the database
- [ ] JOIN operations between tables (coming soon)
- [ ] Migrations to update the database schema without losing data (coming soon)
- [ ] Indexes on columns to optimize queries (coming soon)

## Documentation

- [Memory Management](./docs/memory.md)

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
