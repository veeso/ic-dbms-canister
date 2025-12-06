#![crate_name = "ic_dbms_canister"]
#![crate_type = "lib"]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # IC DBMS Canister
//!
//! TODO

#![doc(html_playground_url = "https://play.rust-lang.org")]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-128.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-512.png"
)]

// makes the crate accessible as `ic_dbms_canister` in macros
extern crate self as ic_dbms_canister;

pub mod dbms;
pub mod memory;
pub mod prelude;
#[cfg(test)]
mod tests;
pub mod utils;
