mod page_table;

use self::page_table::PageTable;
use crate::memory::{Encode, MEMORY_MANAGER, MemoryResult, Page};

/// Takes care of storing the pages for each table
#[derive(Debug)]
pub struct PageLedger {
    /// The page where the ledger is stored in memory.
    ledger_page: Page,
    /// The pages table.
    pages: PageTable,
}

impl PageLedger {
    /// Load the page ledger from memory at the given [`Page`].
    pub fn load(page: Page) -> MemoryResult<Self> {
        Ok(Self {
            pages: MEMORY_MANAGER.with_borrow(|mm| mm.read_at(page, 0))?,
            ledger_page: page,
        })
    }

    /// Get the page number to store the next record.
    ///
    /// It usually returns the first page with enough free space.
    /// If the provided record is larger than any page's free space,
    /// it allocates a new page and returns it.
    pub fn get_page_for_record<R>(&mut self, record: &R) -> MemoryResult<Page>
    where
        R: Encode,
    {
        let required_size = record.size() as u64;
        let page_size = MEMORY_MANAGER.with_borrow(|mm| mm.page_size());
        // check if record can fit in a page
        if required_size > page_size {
            return Err(crate::memory::error::MemoryError::DataTooLarge {
                page_size: page_size,
                requested: required_size,
            });
        }

        // iter ledger pages to find a page with enough free space
        let next_page = self
            .pages
            .pages
            .iter()
            .find(|page_record| page_record.free + required_size <= page_size);
        // if page found, return it
        if let Some(page_record) = next_page {
            return Ok(page_record.page);
        }

        // otherwise allocate a new one
        let new_page = MEMORY_MANAGER.with_borrow_mut(|mm| mm.allocate_page())?;
        // add to ledger
        self.pages.pages.push(self::page_table::PageRecord {
            page: new_page,
            free: page_size, // NOTE: we commit later, so full free space
        });

        Ok(new_page)
    }

    /// Commits the allocation of a record in the given page.
    ///
    /// This will commit the eventual allocated page
    /// and decrease the free space available in the page and write the updated ledger to memory.
    pub fn commit<R>(&mut self, page: Page, record: &R) -> MemoryResult<()>
    where
        R: Encode,
    {
        if let Some(page_record) = self.pages.pages.iter_mut().find(|pr| pr.page == page) {
            let record_size = record.size() as u64;
            if page_record.free < record_size {
                return Err(crate::memory::error::MemoryError::DataTooLarge {
                    page_size: page_record.free,
                    requested: record_size,
                });
            }
            page_record.free = page_record.free.saturating_sub(record_size);
            self.write()?;
            return Ok(());
        }

        Err(crate::memory::error::MemoryError::OutOfBounds)
    }

    /// Write the page ledger to memory.
    fn write(&self) -> MemoryResult<()> {
        MEMORY_MANAGER.with_borrow_mut(|mm| mm.write_at(self.ledger_page, 0, &self.pages))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::memory::provider::{HeapMemoryProvider, MemoryProvider};
    use crate::memory::table_registry::page_ledger::page_table::PageRecord;
    use crate::memory::{DataSize, MSize};

    #[test]
    fn test_should_store_pages_and_load_back() {
        let page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .unwrap();
        let page_ledger = PageLedger {
            pages: PageTable {
                pages: vec![
                    PageRecord {
                        page: 10,
                        free: 100,
                    },
                    PageRecord {
                        page: 11,
                        free: 200,
                    },
                    PageRecord {
                        page: 12,
                        free: 300,
                    },
                ],
            },
            ledger_page: page,
        };
        page_ledger.write().expect("failed to write page ledger");
        let loaded_ledger = PageLedger::load(page).expect("failed to load page ledger");
        assert_eq!(page_ledger.pages.pages, loaded_ledger.pages.pages);
    }

    #[test]
    fn test_should_get_page_for_record() {
        // allocate page
        let ledger_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to allocate ledger page");
        let mut page_ledger = PageLedger::load(ledger_page).expect("failed to load page ledger");
        assert!(page_ledger.pages.pages.is_empty());

        // create test record
        let record = TestRecord { data: [1; 100] };
        // get page for record
        let page = page_ledger
            .get_page_for_record(&record)
            .expect("failed to get page for record");
        assert_eq!(page_ledger.pages.pages.len(), 1);
        assert_eq!(page_ledger.pages.pages[0].page, page);
        assert_eq!(
            page_ledger.pages.pages[0].free,
            HeapMemoryProvider::PAGE_SIZE
        );

        // commit record allocation
        page_ledger
            .commit(page, &record)
            .expect("failed to commit record allocation");
        assert_eq!(
            page_ledger.pages.pages[0].free,
            HeapMemoryProvider::PAGE_SIZE - 100
        );

        // reload
        let reloaded_ledger = PageLedger::load(ledger_page).expect("failed to load page ledger");
        assert_eq!(page_ledger.pages.pages, reloaded_ledger.pages.pages);
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
