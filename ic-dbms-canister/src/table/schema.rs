/// A type representing a unique fingerprint for a table schema.
pub type TableFingerprint = u64;

/// Table schema representation.
///
/// It is used to define the structure of a database table.
#[derive(Debug)]
pub struct TableSchema(pub TableFingerprint);

impl TableSchema {
    /// Returns a fingerprint uniquely identifying the table schema.
    ///
    /// This is used to identify schemas in the schema registry.
    pub const fn fingerprint(&self) -> TableFingerprint {
        self.0 // TODO: implement proper fingerprinting
    }
}
