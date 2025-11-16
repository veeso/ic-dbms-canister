use crate::memory::{DataSize, Encode, Page};

/// The list of pages in the page ledger
#[derive(Debug, Default)]
pub struct PageTable {
    pub pages: Vec<PageRecord>,
}

/// A record in the page ledger
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRecord {
    pub page: Page,
    pub free: u64,
}

impl Encode for PageTable {
    const SIZE: DataSize = DataSize::Variable;

    fn size(&self) -> usize {
        // 4 bytes for len + (4 bytes per page)
        std::mem::size_of::<u32>()
            + (self.pages.len()
                * PageRecord::SIZE
                    .get_fixed_size()
                    .expect("Should be fixed size"))
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        // write length of pages
        let size = self.pages.len() as u32;
        let mut encoded = Vec::with_capacity(self.size());
        encoded.extend_from_slice(&size.to_le_bytes());
        for page in &self.pages {
            encoded.extend_from_slice(&page.encode());
        }
        std::borrow::Cow::Owned(encoded)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> crate::memory::MemoryResult<Self>
    where
        Self: Sized,
    {
        let size_bytes = &data[0..4];
        let size = u32::from_le_bytes(size_bytes.try_into()?) as usize;
        let mut pages = Vec::with_capacity(size);

        for i in 0..size {
            let start = std::mem::size_of::<u32>()
                + (i * PageRecord::SIZE
                    .get_fixed_size()
                    .expect("Should be fixed size"));
            let end = start
                + PageRecord::SIZE
                    .get_fixed_size()
                    .expect("Should be fixed size");
            let page_bytes = &data[start..end];
            let page = PageRecord::decode(std::borrow::Cow::Borrowed(page_bytes))?;
            pages.push(page);
        }
        Ok(PageTable { pages })
    }
}

impl Encode for PageRecord {
    const SIZE: DataSize =
        DataSize::Fixed(std::mem::size_of::<Page>() + std::mem::size_of::<u64>());

    fn size(&self) -> usize {
        std::mem::size_of::<Page>() + std::mem::size_of::<u64>()
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        let mut encoded = Vec::with_capacity(self.size());
        encoded.extend_from_slice(&self.page.to_le_bytes());
        encoded.extend_from_slice(&self.free.to_le_bytes());
        std::borrow::Cow::Owned(encoded)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> crate::memory::MemoryResult<Self>
    where
        Self: Sized,
    {
        let page_bytes = &data[0..std::mem::size_of::<Page>()];
        let page = Page::from_le_bytes(page_bytes.try_into()?);
        let free_bytes = &data
            [std::mem::size_of::<Page>()..std::mem::size_of::<Page>() + std::mem::size_of::<u64>()];
        let free = u64::from_le_bytes(free_bytes.try_into()?);
        Ok(PageRecord { page, free })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_should_encode_and_decode_page_table() {
        let original_ledger = PageTable {
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
        };

        let encoded = original_ledger.encode();
        let decoded_ledger = PageTable::decode(encoded).unwrap();

        assert_eq!(original_ledger.pages, decoded_ledger.pages);
    }
}
