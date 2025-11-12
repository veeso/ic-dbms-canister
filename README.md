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
struct User {
    #[primary_key]
    id: Integer,
    name: Text,
    email: Text,
    age: Integer,
}
```

This will provide for the user the following API:

todo...

You can also define relationships between tables:

```rust
#[derive(Table)]
struct Post {
    #[primary_key]
    id: Integer,
    title: Text,
    content: Text,
    #[foreign_key(table = "User", column = "id")]
    author_id: Integer,
}
```

And once you have defined all your tables, you can instantiate the database canister:

```rust
ic_odbc_canister!(User, Post, Comment);
```

And you will have a fully functional database canister with all the CRUD operations implemented for you.

The canister API will be automatically generated based on the defined tables, with the following methods:

todo...

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
