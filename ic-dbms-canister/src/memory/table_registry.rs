mod free_segments_ledger;
mod page_ledger;
mod raw_record;
mod table_reader;
mod write_at;

use std::marker::PhantomData;

use self::free_segments_ledger::FreeSegmentsLedger;
use self::page_ledger::PageLedger;
pub use self::table_reader::{NextRecord, TableReader};
use self::write_at::WriteAt;
use crate::memory::table_registry::raw_record::RawRecord;
use crate::memory::{
    Encode, MEMORY_MANAGER, MSize, MemoryResult, Page, PageOffset, TableRegistryPage,
};

/// Each record is prefixed with its length encoded in 2 bytes and a magic header byte.
const RAW_RECORD_HEADER_SIZE: MSize = 3;

/// The table registry takes care of storing the records for each table,
/// using the [`FreeSegmentsLedger`] and [`PageLedger`] to derive exactly where to read/write.
///
/// A registry is generic over a record, which must implement [`Encode`].
///
/// The CRUD operations provided by the table registry do NOT perform any logical checks,
/// but just allow to read/write records from/to memory.
/// So CRUD checks must be performed by a higher layer, prior to calling these methods.
pub struct TableRegistry<E>
where
    E: Encode,
{
    _marker: PhantomData<E>,
    free_segments_ledger: FreeSegmentsLedger,
    page_ledger: PageLedger,
}

impl<E> TableRegistry<E>
where
    E: Encode,
{
    /// Loads the table registry from memory
    pub fn load(table_pages: TableRegistryPage) -> MemoryResult<Self> {
        Ok(Self {
            _marker: PhantomData,
            free_segments_ledger: FreeSegmentsLedger::load(table_pages.free_segments_page)?,
            page_ledger: PageLedger::load(table_pages.pages_list_page)?,
        })
    }

    /// Inserts a new record into the table registry.
    ///
    /// NOTE: this function does NOT make any logical checks on the record being inserted.
    pub fn insert(&mut self, record: E) -> MemoryResult<()> {
        // get position to write the record
        let raw_record = RawRecord::new(record);
        let write_at = self.get_write_position(&raw_record)?;

        // write record
        MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.write_at(write_at.page(), write_at.offset(), &raw_record))?;

        // commit post-write actions
        self.post_write(write_at, &raw_record)
    }

    /// Creates a [`TableReader`] to read records from the table registry.
    ///
    /// Use [`TableReader::try_next`] to read records one by one.
    pub fn read(&self) -> TableReader<'_, E> {
        TableReader::new(&self.page_ledger)
    }

    /// Deletes a record at the given page and offset.
    ///
    /// The space occupied by the record is marked as free and zeroed.
    pub fn delete(&mut self, record: E, page: Page, offset: PageOffset) -> MemoryResult<()> {
        let raw_record = RawRecord::new(record);

        // zero the record in memory
        MEMORY_MANAGER.with_borrow_mut(|mm| mm.zero(page, offset, &raw_record))?;

        // insert a free segment for the deleted record
        self.free_segments_ledger
            .insert_free_segment(page, offset, &raw_record)
    }

    /// Updates a record at the given page and offset.
    ///
    /// The logic is the following:
    ///
    /// 1. If the new record has exactly the same size of the old record, overwrite it in place.
    /// 2. If the new record does not fit, delete the old record and insert the new record.
    pub fn update(
        &mut self,
        new_record: E,
        old_record: E,
        old_page: Page,
        old_offset: PageOffset,
    ) -> MemoryResult<()> {
        if new_record.size() == old_record.size() {
            self.update_in_place(new_record, old_page, old_offset)
        } else {
            self.update_by_realloc(new_record, old_record, old_page, old_offset)
        }
    }

    /// Update a [`RawRecord`] in place at the given page and offset.
    ///
    /// This must be used IF AND ONLY if the new record has the SAME size as the old record.
    fn update_in_place(&mut self, record: E, page: Page, offset: PageOffset) -> MemoryResult<()> {
        let raw_record = RawRecord::new(record);
        MEMORY_MANAGER.with_borrow_mut(|mm| mm.write_at(page, offset, &raw_record))
    }

    /// Updates a record by reallocating it.
    ///
    /// The old record is deleted and the new record is inserted.
    fn update_by_realloc(
        &mut self,
        new_record: E,
        old_record: E,
        old_page: Page,
        old_offset: PageOffset,
    ) -> MemoryResult<()> {
        // delete old record
        self.delete(old_record, old_page, old_offset)?;

        // insert new record
        self.insert(new_record)
    }

    /// Gets the position where to write a record of the given size.
    fn get_write_position(&mut self, record: &RawRecord<E>) -> MemoryResult<WriteAt> {
        // check if there is a free segment that can hold the record
        if let Some(segment) = self.free_segments_ledger.find_reusable_segment(record) {
            return Ok(WriteAt::ReusedSegment(segment));
        }

        // otherwise, write at the end of the table
        self.page_ledger
            .get_page_and_offset_for_record(record)
            .map(|(page, offset)| WriteAt::End(page, offset))
    }

    /// Commits the post-write actions after writing a record at the given position.
    ///
    /// - If the record was a [`WriteAt::ReusedSegment`], the free segment is marked as used.
    /// - If the record was a [`WriteAt::End`], the page ledger is updated.
    fn post_write(&mut self, write_at: WriteAt, record: &RawRecord<E>) -> MemoryResult<()> {
        match write_at {
            WriteAt::ReusedSegment(free_segment) => {
                // mark segment as used
                self.free_segments_ledger
                    .commit_reused_space(record, free_segment)
            }
            WriteAt::End(page, ..) => {
                // update page ledger
                self.page_ledger.commit(page, record)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::memory::table_registry::free_segments_ledger::FreeSegment;
    use crate::tests::User;

    #[test]
    fn test_should_create_table_registry() {
        let page_ledger_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let free_segments_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let table_pages = TableRegistryPage {
            pages_list_page: page_ledger_page,
            free_segments_page,
        };

        let registry: MemoryResult<TableRegistry<User>> = TableRegistry::load(table_pages);
        assert!(registry.is_ok());
    }

    #[test]
    fn test_should_get_write_at_end() {
        let mut registry = registry();

        let record = RawRecord::new(User {
            id: 1,
            name: "Test".to_string(),
        });
        let write_at = registry
            .get_write_position(&record)
            .expect("failed to get write at");

        assert!(matches!(write_at, WriteAt::End(_, 0)));
    }

    #[test]
    fn test_should_get_write_at_free_segment() {
        let mut registry = registry();

        let record = RawRecord::new(User {
            id: 1,
            name: "Test".to_string(),
        });
        // allocate a page to insert a free segment
        let (page, _) = registry
            .page_ledger
            .get_page_and_offset_for_record(&record)
            .expect("failed to get page and offset");
        registry
            .page_ledger
            .commit(page, &record)
            .expect("failed to commit page ledger");
        // insert data about a free segment
        registry
            .free_segments_ledger
            .insert_free_segment(page, 256, &record)
            .expect("failed to insert free segment");

        let write_at = registry
            .get_write_position(&record)
            .expect("failed to get write at");
        assert_eq!(
            write_at,
            WriteAt::ReusedSegment(FreeSegment {
                page,
                offset: 256,
                size: record.size(),
            })
        );
    }

    #[test]
    fn test_should_insert_record_into_table_registry() {
        let mut registry = registry();

        let record = User {
            id: 1,
            name: "Test".to_string(),
        };

        // insert record
        assert!(registry.insert(record).is_ok());
    }

    #[test]
    fn test_should_manage_to_insert_users_to_exceed_one_page() {
        let mut registry = registry();

        for id in 0..4000 {
            let record = User {
                id,
                name: format!("User {}", id),
            };
            registry.insert(record).expect("failed to insert record");
        }
    }

    #[test]
    fn test_should_delete_record() {
        let mut registry = registry();

        let record = User {
            id: 1,
            name: "Test".to_string(),
        };

        // insert record
        registry.insert(record.clone()).expect("failed to insert");

        // find where it was written
        let mut reader = registry.read();
        let next_record = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");
        let page = next_record.page;
        let offset = next_record.offset;
        let record = next_record.record;
        let raw_user = RawRecord::new(record.clone());
        let raw_user_size = raw_user.size();

        // delete record
        assert!(registry.delete(record, page, offset).is_ok());

        // should have been deleted
        let mut reader = registry.read();
        assert!(reader.try_next().expect("failed to read").is_none());

        // should have a free segment
        let free_segment = registry
            .free_segments_ledger
            .find_reusable_segment(&User {
                id: 2,
                name: "Test".to_string(),
            })
            .expect("could not find the free segment after free");
        assert_eq!(free_segment.page, page);
        assert_eq!(free_segment.offset, offset);
        assert_eq!(free_segment.size, raw_user_size);

        // should have zeroed the memory
        let mut buffer = vec![0u8; raw_user_size as usize];
        MEMORY_MANAGER
            .with_borrow(|mm| mm.read_at_raw(page, offset, &mut buffer))
            .expect("failed to read memory");
        assert!(buffer.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_should_update_record_in_place() {
        let mut registry = registry();

        let old_record = User {
            id: 1,
            name: "John".to_string(),
        };
        let new_record = User {
            id: 1,
            name: "Mark".to_string(), // same length as "John"
        };

        // insert old record
        registry
            .insert(old_record.clone())
            .expect("failed to insert");

        // find where it was written
        let mut reader = registry.read();
        let next_record = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");
        let page = next_record.page;
        let offset = next_record.offset;

        // update in place
        assert!(
            registry
                .update(new_record.clone(), next_record.record.clone(), page, offset)
                .is_ok()
        );

        // read back the record
        let mut reader = registry.read();
        let next_record = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(next_record.page, page); // should be same page
        assert_eq!(next_record.offset, offset); // should be same offset
        assert_eq!(next_record.record, new_record);
    }

    #[test]
    fn test_should_update_record_reallocating() {
        let mut registry = registry();

        let old_record = User {
            id: 1,
            name: "John".to_string(),
        };
        // this user creates a record with same size as old_record to avoid reusing the free segment
        let extra_user = User {
            id: 2,
            name: "Extra".to_string(),
        };
        let new_record = User {
            id: 1,
            name: "Alexander".to_string(), // longer than "John"
        };

        // insert old record
        registry
            .insert(old_record.clone())
            .expect("failed to insert");
        // insert extra record to avoid reusing the free segment
        registry
            .insert(extra_user.clone())
            .expect("failed to insert extra user");

        // find where it was written
        let mut reader = registry.read();
        let old_record = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");
        let page = old_record.page;
        let offset = old_record.offset;

        // update by reallocating
        assert!(
            registry
                .update(new_record.clone(), old_record.record.clone(), page, offset)
                .is_ok()
        );

        // read back the record
        let mut reader = registry.read();

        // find extra record first
        let _ = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");

        let updated_record = reader
            .try_next()
            .expect("failed to read")
            .expect("no record");
        assert_ne!(updated_record.offset, offset); // should be different offset
        assert_eq!(updated_record.record, new_record);
    }

    fn registry() -> TableRegistry<User> {
        let page_ledger_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let free_segments_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let table_pages = TableRegistryPage {
            pages_list_page: page_ledger_page,
            free_segments_page,
        };

        TableRegistry::load(table_pages).expect("failed to load")
    }
}
