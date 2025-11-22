use std::marker::PhantomData;

use crate::memory::error::DecodeError;
use crate::memory::table_registry::RAW_RECORD_HEADER_SIZE;
use crate::memory::table_registry::page_ledger::PageLedger;
use crate::memory::table_registry::raw_record::{RAW_RECORD_HEADER_MAGIC_NUMBER, RawRecord};
use crate::memory::{Encode, MEMORY_MANAGER, MSize, MemoryError, MemoryResult, Page, PageOffset};

/// Stores the current position to read/write in memory.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Position {
    page: Page,
    offset: PageOffset,
    size: u64,
}

/// Represents the next record to read from memory.
/// It also contains the new [`Position`] after reading the record.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct FoundRecord {
    page: Page,
    offset: PageOffset,
    length: MSize,
    new_position: Option<Position>,
}

/// Represents the next record read by the [`TableReader`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct NextRecord<E>
where
    E: Encode,
{
    pub record: E,
    pub page: Page,
    pub offset: PageOffset,
}

/// A reader for the table registry that allows reading records from memory.
///
/// The table reader provides methods to read records from the table registry one by one,
/// using the underlying [`PageLedger`] to locate the records in memory.
pub struct TableReader<'a, E>
where
    E: Encode,
{
    /// Buffer used to read records from memory.
    buffer: Vec<u8>,
    page_ledger: &'a PageLedger,
    page_size: usize,
    phantom: PhantomData<E>,
    /// Current position in the table registry.
    /// If `None`, the reader has reached the end of the table.
    position: Option<Position>,
}

impl<'a, E> TableReader<'a, E>
where
    E: Encode,
{
    /// Creates a new table reader starting from the beginning of the table registry.
    pub fn new(page_ledger: &'a PageLedger) -> Self {
        // init position
        let position = page_ledger.pages().first().map(|page_record| Position {
            page: page_record.page,
            offset: 0,
            size: MEMORY_MANAGER
                .with_borrow(|mm| mm.page_size())
                .saturating_sub(page_record.free),
        });
        let page_size = MEMORY_MANAGER.with_borrow(|mm| mm.page_size() as usize);
        Self {
            buffer: vec![0u8; page_size],
            page_ledger,
            phantom: PhantomData,
            position,
            page_size,
        }
    }

    /// Reads the next record from the table registry.
    pub fn try_next(&mut self) -> MemoryResult<Option<NextRecord<E>>> {
        let Some(Position { page, offset, size }) = self.position else {
            return Ok(None);
        };

        // find next record segment
        let Some(next_record) = self.find_next_record(page, offset, size)? else {
            // no more records
            self.position = None;
            return Ok(None);
        };

        // read raw record
        let record: RawRecord<E> =
            MEMORY_MANAGER.with_borrow(|mm| mm.read_at(next_record.page, next_record.offset))?;

        // update position
        self.position = next_record.new_position;

        Ok(Some(NextRecord {
            record: record.data,
            page: next_record.page,
            offset: next_record.offset,
        }))
    }

    /// Finds the next record starting from the given position.
    ///
    /// If a record is found, returns [`Some<NextRecord>`], otherwise returns [`None`].
    /// If [`None`] is returned, the reader has reached the end of the table.
    fn find_next_record(
        &mut self,
        mut page: Page,
        offset: PageOffset,
        mut page_size: u64,
    ) -> MemoryResult<Option<FoundRecord>> {
        loop {
            // get read_len (cannot read more than page_size)
            let read_len =
                std::cmp::min(self.page_size, page_size as usize).saturating_sub(offset as usize);
            // if offset is zero, read page; otherwise, just reuse buffer
            if offset == 0 {
                MEMORY_MANAGER
                    .with_borrow(|mm| mm.read_at_raw(page, 0, &mut self.buffer[..read_len]))?;
            }

            // find next record in buffer; if found, return it
            let buf_end = (page_size as usize)
                .saturating_sub(offset as usize)
                .max(offset as usize);
            if let Some((next_segment_offset, next_segment_size)) =
                self.find_next_record_position(&self.buffer[(offset as usize)..buf_end])?
            {
                // found a record; return it
                // sum the buffer offset to the current page offset to get the absolute offset
                let next_segment_offset = offset + next_segment_offset as PageOffset;
                let new_offset = next_segment_offset + next_segment_size as PageOffset;
                let new_position = if new_offset as u64 >= page_size {
                    // move to next page
                    self.next_page(page)
                } else {
                    Some(Position {
                        page,
                        offset: new_offset + RAW_RECORD_HEADER_SIZE,
                        size: page_size,
                    })
                };
                return Ok(Some(FoundRecord {
                    page,
                    offset: next_segment_offset,
                    length: next_segment_size,
                    new_position,
                }));
            }

            // read next page
            match self.next_page(page) {
                Some(pos) => {
                    page = pos.page;
                    page_size = pos.size;
                }
                None => break,
            }
        }

        Ok(None)
    }

    /// Gets the next page after the given current page.
    fn next_page(&self, current_page: Page) -> Option<Position> {
        self.page_ledger
            .pages()
            .iter()
            .find(|p| p.page > current_page)
            .map(|page_record| Position {
                page: page_record.page,
                offset: 0,
                size: (self.page_size as u64).saturating_sub(page_record.free),
            })
    }

    /// Finds the next record segment position.
    ///
    /// Returns the offset and size of the next record segment if found.
    fn find_next_record_position(&self, buf: &[u8]) -> MemoryResult<Option<(PageOffset, MSize)>> {
        // iter until we find a byte that is not 0
        let offset = match buf
            .iter()
            .position(|b| *b == RAW_RECORD_HEADER_MAGIC_NUMBER)
        {
            Some(offset) => offset,
            None => return Ok(None),
        };

        // get length
        if buf.len() < offset + 3 {
            return Err(MemoryError::DecodeError(DecodeError::TooShort));
        }

        let data_len = u16::from_le_bytes([buf[offset + 1], buf[offset + 2]]) as MSize;
        let data_offset = offset + 3;
        if buf.len() < data_offset + data_len as usize {
            return Err(MemoryError::DecodeError(DecodeError::TooShort));
        }

        Ok(Some((offset as PageOffset, data_len)))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::memory::{TableRegistry, TableRegistryPage};
    use crate::tests::User;

    #[test]
    fn test_should_read_all_records() {
        let table_registry = mock_table_registry(4_000);
        let mut reader = mocked(&table_registry);

        // should read all records
        let mut id = 0;
        while let Some(NextRecord { record: user, .. }) =
            reader.try_next().expect("failed to read user")
        {
            println!(
                "Read user: id={}, name={}; new position: {:?}",
                user.id, user.name, reader.position
            );
            assert_eq!(user.id, id);
            assert_eq!(user.name, format!("User {}", id));

            id += 1;
        }
    }

    #[test]
    fn test_should_find_next_page() {
        let table_registry = mock_table_registry(4_000);
        let reader = mocked(&table_registry);

        let page = reader.position.expect("should have position").page;

        let next_page = reader.next_page(page).expect("should have next page");
        assert_eq!(next_page.page, page + 1);
        let next_page = reader.next_page(next_page.page);
        assert!(next_page.is_none());
    }

    #[test]
    fn test_should_find_next_record_position() {
        let table_registry = mock_table_registry(1);
        let reader = mocked(&table_registry);

        let buf = [
            0u8,
            0u8,
            RAW_RECORD_HEADER_MAGIC_NUMBER,
            5u8,
            0u8,
            0u8,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        let (offset, size) = reader
            .find_next_record_position(&buf)
            .expect("failed to get next record")
            .expect("should have next record");

        assert_eq!(offset, 2);
        assert_eq!(size, 5);
    }

    #[test]
    fn test_should_not_find_next_record_position_none() {
        let table_registry = mock_table_registry(1);
        let reader = mocked(&table_registry);

        let buf = [0u8, 0u8, 0u8, 0u8, 0u8];
        let result = reader
            .find_next_record_position(&buf)
            .expect("failed to get next record");

        assert!(result.is_none());
    }

    #[test]
    fn test_should_not_find_next_record_position_too_short_for_length() {
        let table_registry = mock_table_registry(1);
        let reader = mocked(&table_registry);

        let buf = [0u8, RAW_RECORD_HEADER_MAGIC_NUMBER, 5u8];
        let result = reader.find_next_record_position(&buf);

        assert!(matches!(
            result,
            Err(MemoryError::DecodeError(DecodeError::TooShort))
        ));
    }

    #[test]
    fn test_should_not_find_next_record_position_too_short_for_data() {
        let table_registry = mock_table_registry(1);
        let reader = mocked(&table_registry);

        let buf = [
            0u8,
            0u8,
            RAW_RECORD_HEADER_MAGIC_NUMBER,
            5u8,
            0u8,
            0u8,
            0,
            0,
        ];
        let result = reader.find_next_record_position(&buf);

        assert!(matches!(
            result,
            Err(MemoryError::DecodeError(DecodeError::TooShort))
        ));
    }

    fn mock_table_registry(entries: u32) -> TableRegistry<User> {
        let page_ledger_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let free_segments_page = MEMORY_MANAGER
            .with_borrow_mut(|mm| mm.allocate_page())
            .expect("failed to get page");
        let mut registry = TableRegistry::load(TableRegistryPage {
            pages_list_page: page_ledger_page,
            free_segments_page,
        })
        .expect("failed to load registry");

        // insert `entries` records
        for id in 0..entries {
            let user = User {
                id,
                name: format!("User {}", id),
            };
            registry.insert(user).expect("failed to insert user");
        }

        registry
    }

    fn mocked<'a>(table_registry: &'a TableRegistry<User>) -> TableReader<'a, User> {
        TableReader::new(&table_registry.page_ledger)
    }
}
