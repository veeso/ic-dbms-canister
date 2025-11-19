use crate::memory::{DataSize, Encode, MSize, MemoryResult, Page, PageOffset};

/// [`Encode`]able representation of a table that keeps track of [`FreeSegment`]s.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct FreeSegmentsTable {
    pub records: Vec<FreeSegment>,
}

/// Represents a free segment's metadata.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct FreeSegment {
    /// The page where the free segment was located.
    pub page: Page,
    /// The offset within the page where the free segment was located.
    pub offset: PageOffset,
    /// The size of the free segment.
    pub size: MSize,
}

impl FreeSegmentsTable {
    /// Inserts a new [`FreeSegment`] into the table.
    pub fn insert_free_segment(&mut self, page: Page, offset: PageOffset, size: MSize) {
        let record = FreeSegment { page, offset, size };
        self.records.push(record);
        todo!("Merge adjacent free segments for optimization");
    }

    /// Finds a free segment that matches the given predicate.
    pub fn find<F>(&self, predicate: F) -> Option<FreeSegment>
    where
        F: Fn(&&FreeSegment) -> bool,
    {
        self.records.iter().find(predicate).copied()
    }

    /// Removes a free segment that matches the given parameters.
    ///
    /// If `used_size` is less than `size`, the old record is removed, but a new record is added
    /// for the remaining free space.
    pub fn remove(&mut self, page: Page, offset: PageOffset, size: MSize, used_size: MSize) {
        if let Some(pos) = self
            .records
            .iter()
            .position(|r| r.page == page && r.offset == offset && r.size == size)
        {
            self.records.swap_remove(pos);

            // If there is remaining space, add a new record for it.
            if used_size < size {
                let remaining_size = size.saturating_sub(used_size);
                let new_offset = offset.saturating_add(used_size);
                let new_record = FreeSegment {
                    page,
                    offset: new_offset,
                    size: remaining_size,
                };
                self.records.push(new_record);
            }
        }
    }
}

impl Encode for FreeSegmentsTable {
    const SIZE: DataSize = DataSize::Variable;

    fn size(&self) -> MSize {
        // 4 bytes for the length + size of each record.
        4 + self.records.iter().map(|r| r.size()).sum::<MSize>()
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        let mut buffer = Vec::with_capacity(self.size() as usize);

        // Encode the length of the records vector.
        let length = self.records.len() as u32;
        buffer.extend_from_slice(&length.to_le_bytes());

        // Encode each DeletedRecord.
        for record in &self.records {
            buffer.extend_from_slice(&record.encode());
        }

        std::borrow::Cow::Owned(buffer)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> MemoryResult<Self>
    where
        Self: Sized,
    {
        let length = u32::from_le_bytes(data[0..4].try_into()?);
        let mut records = Vec::with_capacity(length as usize);
        let record_size = FreeSegment::SIZE.get_fixed_size().expect("Should be fixed");

        let mut offset = 4;
        for _ in 0..length {
            let record_data = data[offset as usize..(offset + record_size) as usize]
                .to_vec()
                .into();
            let record = FreeSegment::decode(record_data)?;
            records.push(record);
            offset += record_size;
        }

        Ok(FreeSegmentsTable { records })
    }
}

impl Encode for FreeSegment {
    const SIZE: DataSize = DataSize::Fixed(8); // page (4) + offset (2) + size (2)

    fn size(&self) -> MSize {
        Self::SIZE.get_fixed_size().expect("Should be fixed")
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        let mut buffer = Vec::with_capacity(self.size() as usize);

        buffer.extend_from_slice(&self.page.to_le_bytes());
        buffer.extend_from_slice(&self.offset.to_le_bytes());
        buffer.extend_from_slice(&self.size.to_le_bytes());
        std::borrow::Cow::Owned(buffer)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> MemoryResult<Self>
    where
        Self: Sized,
    {
        let page = Page::from_le_bytes(data[0..4].try_into()?);
        let offset = PageOffset::from_le_bytes(data[4..6].try_into()?);
        let size = MSize::from_le_bytes(data[6..8].try_into()?);

        Ok(FreeSegment { page, offset, size })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_should_encode_and_decode_free_segment() {
        let original_record = FreeSegment {
            page: 42,
            offset: 1000,
            size: 256,
        };

        assert_eq!(original_record.size(), 8);
        let encoded = original_record.encode();
        let decoded = FreeSegment::decode(encoded).expect("Decoding failed");

        assert_eq!(original_record, decoded);
    }

    #[test]
    fn test_should_encode_and_decode_free_segments_table() {
        let original_table = FreeSegmentsTable {
            records: vec![
                FreeSegment {
                    page: 1,
                    offset: 100,
                    size: 50,
                },
                FreeSegment {
                    page: 2,
                    offset: 200,
                    size: 75,
                },
            ],
        };

        let encoded = original_table.encode();
        let decoded = FreeSegmentsTable::decode(encoded).expect("Decoding failed");

        assert_eq!(original_table, decoded);
    }

    #[test]
    fn test_should_insert_free_segment() {
        let mut table = FreeSegmentsTable::default();

        table.insert_free_segment(1, 100, 50);
        table.insert_free_segment(2, 200, 75);

        assert_eq!(table.records.len(), 2);
        assert_eq!(table.records[0].page, 1);
        assert_eq!(table.records[1].page, 2);
    }

    #[test]
    fn test_should_find_free_segment() {
        let mut table = FreeSegmentsTable::default();
        table.insert_free_segment(1, 100, 50);
        table.insert_free_segment(2, 200, 75);

        let record = table.find(|r| r.page == 2);
        assert!(record.is_some());
        assert_eq!(record.unwrap().offset, 200);
    }

    #[test]
    fn test_should_remove_free_segment_with_same_size() {
        let mut table = FreeSegmentsTable::default();
        table.insert_free_segment(1, 100, 50);

        table.remove(1, 100, 50, 50);

        assert!(table.records.is_empty());
    }

    #[test]
    fn test_should_remove_free_segment_and_create_remaining() {
        let mut table = FreeSegmentsTable::default();
        table.insert_free_segment(1, 100, 50);

        table.remove(1, 100, 50, 30);

        assert_eq!(table.records.len(), 1);
        assert_eq!(table.records[0].page, 1);
        assert_eq!(table.records[0].offset, 130);
        assert_eq!(table.records[0].size, 20);
    }
}
