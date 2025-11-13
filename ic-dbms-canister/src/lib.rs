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

pub mod data_types;
pub mod memory;
pub mod query;
pub mod table;
pub mod utils;
