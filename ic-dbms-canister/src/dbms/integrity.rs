//! This module exposes all the integrity validators for the DBMS.

mod insert;

pub use self::insert::InsertIntegrityValidator;
