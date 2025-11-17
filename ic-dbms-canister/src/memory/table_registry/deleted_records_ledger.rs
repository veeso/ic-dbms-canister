mod deleted_records;

pub use self::deleted_records::DeletedRecord;
use self::deleted_records::DeletedRecordsTable;
use crate::memory::{Encode, MEMORY_MANAGER, MSize, MemoryResult, Page, PageOffset};

/// The deleted records ledger keeps track of deleted records in the [`DeletedRecordsTable`] registry.
///
/// Each record tracks:
///
/// - The page number where the record was located
/// - The offset within that page
/// - The size of the deleted record
///
/// The responsibilities of this ledger include:
///
/// - Storing metadata about deleted records whenever a record is deleted
/// - Find a suitable location for new records by reusing space from deleted records
pub struct DeletedRecordsLedger {
    /// The page where the deleted records ledger is stored in memory.
    deleted_records_page: Page,
    /// Deleted records table that holds metadata about deleted records.
    table: DeletedRecordsTable,
}

impl DeletedRecordsLedger {
    /// Loads the deleted records ledger from memory
    pub fn load(deleted_records_page: Page) -> MemoryResult<Self> {
        // read from memory
        let table = MEMORY_MANAGER.with_borrow(|mm| mm.read_at(deleted_records_page, 0))?;

        Ok(Self {
            deleted_records_page,
            table,
        })
    }

    /// Inserts a new [`DeletedRecord`] into the ledger with the specified [`Page`], offset, and size.
    ///
    /// The table is then written back to memory.
    pub fn insert_deleted_record(
        &mut self,
        page: Page,
        offset: PageOffset,
        size: MSize,
    ) -> MemoryResult<()> {
        self.table.insert_deleted_record(page, offset, size);
        self.write()
    }

    /// Finds a reusable deleted record that can accommodate the size of the given record.
    ///
    /// If a suitable deleted record is found, it is returned as [`Some<DeletedRecord>`].
    /// If no suitable record is found, [`None`] is returned.
    pub fn find_reusable_record<E>(&self, record: &E) -> Option<DeletedRecord>
    where
        E: Encode,
    {
        let required_size = record.size();
        self.table.find(|r| r.size >= required_size)
    }

    /// Commits a deleted record by removing it from the ledger and updating it based on the used size.
    pub fn commit_reused_space<E>(
        &mut self,
        record: &E,
        DeletedRecord { page, offset, size }: DeletedRecord,
    ) -> MemoryResult<()>
    where
        E: Encode,
    {
        self.table.remove(page, offset, size, record.size());
        self.write()
    }

    /// Writes the current state of the deleted records table back to memory.
    fn write(&self) -> MemoryResult<()> {
        MEMORY_MANAGER.with_borrow_mut(|mm| mm.write_at(self.deleted_records_page, 0, &self.table))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::memory::{DataSize, MSize};

    #[test]
    fn test_should_load_deleted_records_ledger() {
        // allocate new page
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");

        let ledger = DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");
        assert_eq!(ledger.deleted_records_page, page);
        assert!(ledger.table.records.is_empty());
    }

    #[test]
    fn test_should_insert_record() {
        // allocate new page
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");

        let mut ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");

        ledger
            .insert_deleted_record(4, 0, 128)
            .expect("Failed to insert deleted record");

        let record = ledger
            .table
            .find(|r| r.page == 4 && r.offset == 0 && r.size == 128);
        assert!(record.is_some());

        // verify it's written (reload)
        let reloaded_ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");
        let record = reloaded_ledger
            .table
            .find(|r| r.page == 4 && r.offset == 0 && r.size == 128);
        assert!(record.is_some());
    }

    #[test]
    fn test_should_find_suitable_reusable_space() {
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");

        let mut ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");

        ledger
            .insert_deleted_record(4, 0, 128)
            .expect("Failed to insert deleted record");

        let record = TestRecord { data: [0; 100] };
        let reusable_space = ledger.find_reusable_record(&record);
        assert_eq!(
            reusable_space,
            Some(DeletedRecord {
                page: 4,
                offset: 0,
                size: 128
            })
        );
    }

    #[test]
    fn test_should_not_find_suitable_reusable_space() {
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");

        let mut ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");

        ledger
            .insert_deleted_record(4, 0, 56)
            .expect("Failed to insert deleted record");

        let record = TestRecord { data: [0; 100] };
        let reusable_space = ledger.find_reusable_record(&record);
        assert_eq!(reusable_space, None);
    }

    #[test]
    fn test_should_commit_reused_space_without_creating_a_new_record() {
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");

        let mut ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");

        ledger
            .insert_deleted_record(4, 0, 100)
            .expect("Failed to insert deleted record");

        let record = TestRecord { data: [0; 100] };
        let reusable_space = ledger
            .find_reusable_record(&record)
            .expect("should find reusable space");

        ledger
            .commit_reused_space(&record, reusable_space)
            .expect("Failed to commit reused space");

        // should be empty
        let record = ledger
            .table
            .find(|r| r.page == 4 && r.offset == 0 && r.size == 100);
        assert!(record.is_none());

        // reload
        let reloaded_ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");
        let record = reloaded_ledger
            .table
            .find(|r| r.page == 4 && r.offset == 0 && r.size == 100);
        assert!(record.is_none());
    }

    #[test]
    fn test_should_commit_reused_space_creating_a_new_record() {
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("Failed to allocate page");
        let mut ledger =
            DeletedRecordsLedger::load(page).expect("Failed to load DeletedRecordsLedger");

        ledger
            .insert_deleted_record(4, 0, 200)
            .expect("Failed to insert deleted record");

        let record = TestRecord { data: [0; 100] };
        let reusable_space = ledger
            .find_reusable_record(&record)
            .expect("should find reusable space");

        ledger
            .commit_reused_space(&record, reusable_space)
            .expect("Failed to commit reused space");

        // should have a new record for the remaining space
        let record = ledger
            .table
            .find(|r| r.page == 4 && r.offset == 100 && r.size == 100);
        assert!(record.is_some());
    }

    #[derive(Debug)]
    struct TestRecord {
        data: [u8; 100],
    }

    impl Encode for TestRecord {
        const SIZE: DataSize = DataSize::Fixed(100);

        fn size(&self) -> MSize {
            100
        }

        fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
            std::borrow::Cow::Borrowed(&self.data)
        }

        fn decode(data: std::borrow::Cow<[u8]>) -> crate::memory::MemoryResult<Self>
        where
            Self: Sized,
        {
            let mut record = TestRecord { data: [0; 100] };
            record.data.copy_from_slice(&data[0..100]);
            Ok(record)
        }
    }
}
