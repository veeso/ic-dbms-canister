#![crate_name = "ic_dbms_api"]
#![crate_type = "lib"]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # IC DBMS API
//!
//! This crate exposes all the types which may be used by an external canister to interact with
//! an IC DBMS Canister instance.

#![doc(html_playground_url = "https://play.rust-lang.org")]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-128.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-512.png"
)]

// makes the crate accessible as `ic_dbms_api` in macros
extern crate self as ic_dbms_api;

mod dbms;
mod error;
mod memory;
pub mod prelude;
#[cfg(test)]
mod tests;
