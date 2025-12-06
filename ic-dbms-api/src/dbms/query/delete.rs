/// Defines the behavior for delete operations regarding foreign key constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteBehavior {
    /// Delete only the records matching the filter.
    Restrict,
    /// Cascade delete to related records.
    Cascade,
    /// Break the foreign key references.
    ///
    /// Don't use this option unless you are sure what you're doing!
    Break,
}
