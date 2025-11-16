use std::cell::RefCell;
use std::collections::HashMap;

use crate::memory::{DataSize, Encode, MEMORY_MANAGER, MemoryError, MemoryResult, Page};
use crate::table::{TableFingerprint, TableSchema};

thread_local! {
    /// The global schema registry.
    ///
    /// We allow failing because on first initialization the schema registry might not be present yet.
    pub static SCHEMA_REGISTRY: RefCell<SchemaRegistry> = RefCell::new(SchemaRegistry::load().unwrap_or_default());
}

/// Data regarding the table registry page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRegistryPage {
    pub pages_list_page: Page,
    pub deleted_records_page: Page,
}

/// The schema registry takes care of storing and retrieving table schemas from memory.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SchemaRegistry {
    tables: HashMap<TableFingerprint, TableRegistryPage>,
}

impl SchemaRegistry {
    /// Load the schema registry from memory.
    pub fn load() -> MemoryResult<Self> {
        let page = MEMORY_MANAGER.with_borrow(|m| m.schema_page());
        let registry: Self = MEMORY_MANAGER.with_borrow(|m| m.read_at(page, 0))?;
        Ok(registry)
    }

    /// Registers a table and allocates it registry page.
    pub fn register_table(&mut self, schema: &TableSchema) -> MemoryResult<TableRegistryPage> {
        // allocate table registry page
        let (pages_list_page, deleted_records_page) = MEMORY_MANAGER.with_borrow_mut(|m| {
            Ok::<(Page, Page), MemoryError>((m.allocate_page()?, m.allocate_page()?))
        })?;

        // insert into tables map
        let fingerprint = schema.fingerprint();
        let pages = TableRegistryPage {
            pages_list_page,
            deleted_records_page,
        };
        self.tables.insert(fingerprint, pages);

        // get schema page
        let page = MEMORY_MANAGER.with_borrow(|m| m.schema_page());
        // write self to schema page
        MEMORY_MANAGER.with_borrow_mut(|m| m.write_at(page, 0, self))?;

        Ok(pages)
    }

    /// Returns the table registry page for a given table schema.
    pub fn table_registry_page(&self, schema: &TableSchema) -> Option<TableRegistryPage> {
        self.tables.get(&schema.fingerprint()).copied()
    }
}

impl Encode for SchemaRegistry {
    const SIZE: DataSize = DataSize::Variable;

    fn size(&self) -> usize {
        // 8 bytes for len + (8 + (4 * 2)) bytes for each entry
        8 + (self.tables.len() * (4 * 2 + 8))
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        // prepare buffer; size is 8 bytes for len + (8 + (4 * 2)) bytes for each entry
        let mut buffer = Vec::with_capacity(self.size());
        // write 8 bytes len of map
        buffer.extend_from_slice(&(self.tables.len() as u64).to_le_bytes());
        // write each entry
        for (fingerprint, page) in &self.tables {
            buffer.extend_from_slice(&fingerprint.to_le_bytes());
            buffer.extend_from_slice(&page.pages_list_page.to_le_bytes());
            buffer.extend_from_slice(&page.deleted_records_page.to_le_bytes());
        }
        std::borrow::Cow::Owned(buffer)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> MemoryResult<Self>
    where
        Self: Sized,
    {
        let mut offset = 0;
        // read len
        let len = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .expect("failed to read length"),
        ) as usize;
        offset += 8;
        let mut tables = HashMap::with_capacity(len);
        // read each entry
        for _ in 0..len {
            let fingerprint = u64::from_le_bytes(data[offset..offset + 8].try_into()?);
            offset += 8;
            let pages_list_page = Page::from_le_bytes(data[offset..offset + 4].try_into()?);
            offset += 4;
            let deleted_records_page = Page::from_le_bytes(data[offset..offset + 4].try_into()?);
            offset += 4;
            tables.insert(
                fingerprint,
                TableRegistryPage {
                    pages_list_page,
                    deleted_records_page,
                },
            );
        }
        Ok(Self { tables })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_should_encode_and_decode_schema_registry() {
        // load
        let mut registry = SchemaRegistry::load().expect("failed to load init schema registry");

        // create table schema
        let table_schema = TableSchema(42);
        // register table
        let registry_page = registry
            .register_table(&table_schema)
            .expect("failed to register table");

        // get table registry page
        let fetched_page = registry
            .table_registry_page(&table_schema)
            .expect("failed to get table registry page");
        assert_eq!(registry_page, fetched_page);

        // encode
        let encoded = registry.encode();
        // decode
        let decoded = SchemaRegistry::decode(encoded).expect("failed to decode");
        assert_eq!(registry, decoded);

        // try to actually add another
        let another_table_schema = TableSchema(84);
        let another_registry_page = registry
            .register_table(&another_table_schema)
            .expect("failed to register another table");
        let another_fetched_page = registry
            .table_registry_page(&another_table_schema)
            .expect("failed to get another table registry page");
        assert_eq!(another_registry_page, another_fetched_page);

        // re-init
        let reloaded = SchemaRegistry::load().expect("failed to reload schema registry");
        assert_eq!(registry, reloaded);
        // should have two
        assert_eq!(reloaded.tables.len(), 2);
        assert_eq!(
            reloaded
                .table_registry_page(&table_schema)
                .expect("failed to get first table registry page after reload"),
            registry_page
        );
        assert_eq!(
            reloaded
                .table_registry_page(&another_table_schema)
                .expect("failed to get second table registry page after reload"),
            another_registry_page
        );
    }
}
