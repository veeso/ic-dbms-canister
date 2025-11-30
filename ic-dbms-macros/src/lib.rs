#![crate_name = "ic_dbms_macros"]
#![crate_type = "lib"]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Macros and derive for ic-dbms-canister
//!
//! This crate provides procedural macros to automatically implement traits
//! required by the `ic-dbms-canister`.
//!
//! ## Provided Derive Macros
//!
//! - `Encode`: Automatically implements the `Encode` trait for structs.
//!

#![doc(html_playground_url = "https://play.rust-lang.org")]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-128.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/veeso/ic-dbms-canister/main/assets/images/cargo/logo-512.png"
)]

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod encode;
mod utils;

/// Automatically implements the `Encode`` trait for a struct.
///
/// This derive macro generates two methods required by the `Encode` trait:
///
/// - `fn data_size() -> DataSize`  
///   Computes the static size of the encoded type.  
///   If all fields implement `Encode::data_size()` returning  
///   `DataSize::Fixed(n)`, then the type is also considered fixed-size.  
///   Otherwise, the type is `DataSize::Dynamic`.
///
/// - `fn size(&self) -> MSize`  
///   Computes the runtime-encoding size of the value by summing the
///   sizes of all fields.
///
/// # What the macro generates
///
/// Given a struct like:
///
/// ```rust,ignore
/// #[derive(Encode)]
/// struct User {
///     id: Uint32,
///     name: Text,
/// }
/// ```
///
/// The macro expands into:
///
/// ```rust,ignore
/// impl Encode for User {
///     const DATA_SIZE: DataSize = DataSize::Dynamic; // or DataSize::Fixed(n) if applicable
///
///     fn size(&self) -> MSize {
///         self.id.size() + self.name.size()
///     }
///
///     fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
///         let mut encoded = Vec::with_capacity(self.size() as usize);
///         encoded.extend_from_slice(&self.id.encode());
///         encoded.extend_from_slice(&self.name.encode());
///         std::borrow::Cow::Owned(encoded)
///     }
///
///     fn decode(data: std::borrow::Cow<[u8]>) -> ::ic_dbms_canister::prelude::MemoryResult<Self> {
///         let mut offset = 0;
///         let id = Uint32::decode(std::borrow::Borrowed(&data[offset..]))?;
///         offset += id.size() as usize;
///         let name = Text::decode(std::borrow::Borrowed(&data[offset..]))?;
///         offset += name.size() as usize;
///         Ok(Self { id, name })
///     }
/// }
/// ```
/// # Requirements
///
/// - Each field type must implement `Encode`.
/// - Only works on `struct`s; enums and unions are not supported.
/// - All field identifiers must be valid Rust identifiers (no tuple structs).
///
/// # Notes
///
/// - It is intended for internal use within the `ic-dbms-canister` DBMS memory
///   system.
///
/// # Errors
///
/// The macro will fail to expand if:
///
/// - The struct has unnamed fields (tuple struct)
/// - A field type does not implement `Encode`
/// - The macro is applied to a non-struct item.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Encode, Debug, PartialEq, Eq)]
/// struct Position {
///     x: Int32,
///     y: Int32,
/// }
///
/// let pos = Position { x: 10.into(), y: 20.into() };
/// assert_eq!(Position::data_size(), DataSize::Fixed(8));
/// assert_eq!(pos.size(), 8);
/// let encoded = pos.encode();
/// let decoded = Position::decode(encoded).unwrap();
/// assert_eq!(pos, decoded);
/// ```
#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    self::encode::encode(input)
}
