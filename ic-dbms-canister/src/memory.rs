//! Memory module provides stable memory management for the IC DBMS Canister.

mod acl;
mod encode;
mod error;
mod provider;
mod schema_registry;
mod table_registry;

use std::cell::RefCell;

pub use self::acl::{ACL, AccessControlList};
pub use self::encode::{DataSize, Encode};
pub use self::error::MemoryError;
use self::provider::MemoryProvider;
pub use self::schema_registry::{SCHEMA_REGISTRY, SchemaRegistry, TableRegistryPage};
pub use self::table_registry::{NextRecord, TableReader, TableRegistry};

// instantiate a static memory manager with the stable memory provider
thread_local! {
    #[cfg(target_family = "wasm")]
    pub static MEMORY_MANAGER: RefCell<MemoryManager<provider::IcMemoryProvider>> = RefCell::new(MemoryManager::init(
        provider::IcMemoryProvider::default(),
    ));

    #[cfg(not(target_family = "wasm"))]
    pub static MEMORY_MANAGER: RefCell<MemoryManager<provider::HeapMemoryProvider>> = RefCell::new(MemoryManager::init(
        provider::HeapMemoryProvider::default()
    ));
}

/// Type identifying a memory page number.
pub type Page = u32;
/// Type identifying an offset within a memory page.
pub type PageOffset = u16;
/// Size type for memory operations.
pub type MSize = u16;

/// The result type for memory operations.
pub type MemoryResult<T> = Result<T, MemoryError>;

/// Schema page
const SCHEMA_PAGE: Page = 0;
/// The page for ACL
const ACL_PAGE: Page = 1;

/// The memory manager is the main struct responsible for handling the stable memory operations.
pub struct MemoryManager<P>
where
    P: MemoryProvider,
{
    provider: P,
}

impl<P> MemoryManager<P>
where
    P: MemoryProvider,
{
    /// Initializes the memory manager and allocates the header and reserved pages.
    ///
    /// Panics if the memory provider fails to initialize.
    fn init(provider: P) -> Self {
        let mut manager = MemoryManager { provider };

        // check whether two pages are already allocated
        if manager.provider.pages() >= 2 {
            return manager;
        }

        // request at least 2 pages for header and ACL
        if let Err(err) = manager.provider.grow(2) {
            crate::trap!("Failed to grow stable memory during initialization: {err}");
        }

        manager
    }

    /// Returns the size of a memory page.
    pub const fn page_size(&self) -> u64 {
        P::PAGE_SIZE
    }

    /// Returns the ACL page number.
    pub const fn acl_page(&self) -> Page {
        ACL_PAGE
    }

    /// Returns the schema page.
    pub const fn schema_page(&self) -> Page {
        SCHEMA_PAGE
    }

    /// Allocates an additional page in memory.
    ///
    /// In case of success returns the [`Page`] number.
    pub fn allocate_page(&mut self) -> MemoryResult<Page> {
        self.provider.grow(1)?;

        // zero page CHECK: is it really necessary?
        self.provider.write(
            self.absolute_offset(self.last_page().unwrap_or(0), 0),
            &vec![0u8; P::PAGE_SIZE as usize],
        )?;

        match self.last_page() {
            Some(page) => Ok(page),
            None => Err(MemoryError::FailedToAllocatePage),
        }
    }

    /// Read data as a [`Encode`] impl at the specified page and offset.
    pub fn read_at<D>(&self, page: Page, offset: PageOffset) -> MemoryResult<D>
    where
        D: Encode,
    {
        // read until end of the page (or fixed size)
        let mut buf = vec![
            0u8;
            match D::SIZE {
                DataSize::Fixed(size) => size as usize,
                DataSize::Variable => (P::PAGE_SIZE as usize) - (offset as usize),
            }
        ];

        self.read_at_raw(page, offset, &mut buf)?;

        D::decode(std::borrow::Cow::Owned(buf))
    }

    /// Write data as a [`Encode`] impl at the specified page and offset.
    pub fn write_at<E>(&mut self, page: Page, offset: PageOffset, data: &E) -> MemoryResult<()>
    where
        E: Encode,
    {
        // page must be allocated
        if self.last_page().is_none_or(|last_page| page > last_page) {
            return Err(MemoryError::SegmentationFault {
                page,
                offset,
                data_size: data.size(),
                page_size: P::PAGE_SIZE,
            });
        }

        let encoded = data.encode();

        // if page exists, the write must be within bounds
        if offset as u64 + encoded.len() as u64 > P::PAGE_SIZE {
            return Err(MemoryError::SegmentationFault {
                page,
                offset,
                data_size: data.size(),
                page_size: P::PAGE_SIZE,
            });
        }

        // get absolute offset
        let absolute_offset = self.absolute_offset(page, offset);
        self.provider.write(absolute_offset, encoded.as_ref())
    }

    /// Zeros out data at the specified page and offset.
    pub fn zero<E>(&mut self, page: Page, offset: PageOffset, data: &E) -> MemoryResult<()>
    where
        E: Encode,
    {
        // can't zero unallocated page
        if self.last_page().is_none_or(|last_page| page > last_page) {
            return Err(MemoryError::SegmentationFault {
                page,
                offset,
                data_size: data.size(),
                page_size: P::PAGE_SIZE,
            });
        }

        let length = data.size() as usize;

        if offset as u64 + (length as u64) > P::PAGE_SIZE {
            return Err(MemoryError::SegmentationFault {
                page,
                offset,
                data_size: data.size(),
                page_size: P::PAGE_SIZE,
            });
        }

        // get absolute offset
        let absolute_offset = self.absolute_offset(page, offset);
        let buffer = vec![0u8; length];
        self.provider.write(absolute_offset, buffer.as_ref())
    }

    /// Reads raw bytes into the provided buffer at the specified page and offset.
    pub fn read_at_raw(
        &self,
        page: Page,
        offset: PageOffset,
        buf: &mut [u8],
    ) -> MemoryResult<usize> {
        // page must be allocated
        if self.last_page().is_none_or(|last_page| page > last_page) {
            return Err(MemoryError::SegmentationFault {
                page,
                offset,
                data_size: buf.len() as MSize,
                page_size: P::PAGE_SIZE,
            });
        }

        // read up to buffer length or end of page
        let read_len = ((P::PAGE_SIZE - offset as u64) as usize).min(buf.len());

        // get absolute offset
        let absolute_offset = self.absolute_offset(page, offset);
        self.provider
            .read(absolute_offset, buf[..read_len].as_mut())?;

        Ok(read_len)
    }

    /// Gets the last allocated page number.
    fn last_page(&self) -> Option<Page> {
        match self.provider.pages() {
            0 => None,
            n => Some(n as Page - 1),
        }
    }

    /// Calculates the absolute offset in stable memory given a page number and an offset within that page.
    fn absolute_offset(&self, page: Page, offset: PageOffset) -> u64 {
        (page as u64)
            .checked_mul(P::PAGE_SIZE)
            .and_then(|page_offset| page_offset.checked_add(offset as u64))
            .expect("Overflow when calculating absolute offset")
    }
}

#[cfg(test)]
mod tests {

    use std::borrow::Cow;

    use super::*;
    use crate::memory::provider::HeapMemoryProvider;
    use crate::tests::User;

    #[test]
    fn test_should_init_memory_manager() {
        MEMORY_MANAGER.with_borrow(|manager| assert_eq!(manager.last_page(), Some(1)));
    }

    #[test]
    fn test_should_get_last_page() {
        MEMORY_MANAGER.with_borrow(|manager| {
            let last_page = manager.last_page();
            assert_eq!(last_page, Some(1)); // header and ACL pages
        });
    }

    #[test]
    fn test_should_get_memory_page_size() {
        MEMORY_MANAGER.with_borrow(|manager| {
            let page_size = manager.page_size();
            assert_eq!(page_size, HeapMemoryProvider::PAGE_SIZE);
        });
    }

    #[test]
    fn test_should_write_and_read_fixed_data_size() {
        // write to ACL page
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_write = FixedSizeData { a: 42, b: 1337 };
            manager
                .write_at(ACL_PAGE, 0, &data_to_write)
                .expect("Failed to write data to ACL page");

            let out: FixedSizeData = manager
                .read_at(ACL_PAGE, 0)
                .expect("Failed to read data from ACL page");

            assert_eq!(out, data_to_write);
        });
    }

    #[test]
    fn test_should_write_and_read_variable_data_size() {
        // write to ACL page
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_write = User {
                id: 30,
                name: "Alice".to_string(),
            };
            manager
                .write_at(ACL_PAGE, 10, &data_to_write)
                .expect("Failed to write data to ACL page");
        });
    }

    #[test]
    fn test_should_zero_data() {
        // write to ACL page
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_write = FixedSizeData { a: 100, b: 200 };
            manager
                .write_at(ACL_PAGE, 50, &data_to_write)
                .expect("Failed to write data to ACL page");

            // zero the data
            manager
                .zero(ACL_PAGE, 50, &data_to_write)
                .expect("Failed to zero data on ACL page");

            let mut buffer = vec![0; 50];

            manager
                .read_at_raw(ACL_PAGE, 50, &mut buffer)
                .expect("Failed to read data from ACL page");

            assert!(buffer.iter().all(|&b| b == 0));
        });
    }

    #[test]
    fn test_should_not_zero_unallocated_page() {
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_zero = FixedSizeData { a: 1, b: 2 };
            let result = manager.zero(10, 0, &data_to_zero);
            assert!(matches!(result, Err(MemoryError::SegmentationFault { .. })));
        });
    }

    #[test]
    fn test_should_not_zero_out_of_bounds() {
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_zero = FixedSizeData { a: 1, b: 2 };
            let result = manager.zero(
                ACL_PAGE,
                (HeapMemoryProvider::PAGE_SIZE - 3) as PageOffset,
                &data_to_zero,
            );
            assert!(matches!(result, Err(MemoryError::SegmentationFault { .. })));
        });
    }

    #[test]
    fn test_should_read_raw() {
        // write to ACL page
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_write = vec![1u8, 2, 3, 4, 5];
            manager
                .provider
                .write(manager.absolute_offset(ACL_PAGE, 20), &data_to_write)
                .expect("Failed to write raw data to ACL page");

            let mut buf = vec![0u8; 5];
            manager
                .read_at_raw(ACL_PAGE, 20, &mut buf)
                .expect("Failed to read raw data from ACL page");

            assert_eq!(buf, data_to_write);
        });
    }

    #[test]
    fn test_should_fail_out_of_bounds_access() {
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let data_to_write = FixedSizeData { a: 1, b: 2 };
            let result = manager.write_at(
                ACL_PAGE,
                (HeapMemoryProvider::PAGE_SIZE - 3) as PageOffset,
                &data_to_write,
            );
            assert!(matches!(result, Err(MemoryError::SegmentationFault { .. })));

            // try to access unallocated page
            let result: MemoryResult<FixedSizeData> = manager.read_at(10, 0);
            assert!(matches!(result, Err(MemoryError::SegmentationFault { .. })));
            let result = manager.write_at(10, 0, &data_to_write);
            assert!(matches!(result, Err(MemoryError::SegmentationFault { .. })));
        });
    }

    #[test]
    fn test_should_allocate_new_page() {
        MEMORY_MANAGER.with_borrow_mut(|manager| {
            let initial_last_page = manager.last_page().unwrap();
            let new_page = manager
                .allocate_page()
                .expect("Failed to allocate new page");
            assert_eq!(new_page, initial_last_page + 1);
            let updated_last_page = manager.last_page().unwrap();
            assert_eq!(updated_last_page, new_page);
        });
    }

    #[derive(Debug, PartialEq)]
    struct FixedSizeData {
        a: u16,
        b: u32,
    }

    impl Encode for FixedSizeData {
        const SIZE: DataSize = DataSize::Fixed(6);

        fn encode(&'_ self) -> Cow<'_, [u8]> {
            let mut buf = vec![0u8; self.size() as usize];
            buf[0..2].copy_from_slice(&self.a.to_le_bytes());
            buf[2..6].copy_from_slice(&self.b.to_le_bytes());
            Cow::Owned(buf)
        }

        fn decode(data: Cow<[u8]>) -> MemoryResult<Self>
        where
            Self: Sized,
        {
            let a = u16::from_le_bytes([data[0], data[1]]);
            let b = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
            Ok(FixedSizeData { a, b })
        }

        fn size(&self) -> MSize {
            6
        }
    }
}
