mod free_segments_ledger;
mod page_ledger;
mod raw_record;
mod table_reader;
mod write_at;

use std::marker::PhantomData;

use self::free_segments_ledger::FreeSegmentsLedger;
use self::page_ledger::PageLedger;
pub use self::table_reader::TableReader;
use self::write_at::WriteAt;
use crate::memory::table_registry::raw_record::RawRecord;
use crate::memory::{Encode, MEMORY_MANAGER, MSize, MemoryResult, TableRegistryPage};

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

        let mut registry: TableRegistry<User> =
            TableRegistry::load(table_pages).expect("failed to load");

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

        let mut registry: TableRegistry<User> =
            TableRegistry::load(table_pages).expect("failed to load");

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

        let mut registry: TableRegistry<User> =
            TableRegistry::load(table_pages).expect("failed to load");

        let record = User {
            id: 1,
            name: "Test".to_string(),
        };

        // insert record
        assert!(registry.insert(record).is_ok());
    }

    #[test]
    fn test_should_manage_to_insert_users_to_exceed_one_page() {
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

        let mut registry: TableRegistry<User> =
            TableRegistry::load(table_pages).expect("failed to load");

        for id in 0..4000 {
            let record = User {
                id,
                name: format!("User {}", id),
            };
            registry.insert(record).expect("failed to insert record");
        }
    }
}
