use std::cell::RefCell;

use candid::Principal;

use super::MEMORY_MANAGER;
use crate::memory::{DataSize, Encode, MSize, MemoryResult};

thread_local! {
    /// The global ACL.
    ///
    /// We allow failing because on first initialization the ACL might not be present yet.
    pub static ACL: RefCell<AccessControlList> = RefCell::new(AccessControlList::load().unwrap_or_default());
}

/// Access control list module.
///
/// Takes care of storing and retrieving the list of principals that have access to the database.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccessControlList {
    allowed: Vec<Principal>,
}

impl AccessControlList {
    /// Load [`AccessControlList`] from memory.
    pub fn load() -> MemoryResult<Self> {
        // read memory location from MEMORY_MANAGER
        MEMORY_MANAGER.with_borrow(|m| m.read_at(m.acl_page(), 0))
    }

    /// Get the list of allowed principals.
    pub fn allowed_principals(&self) -> &[Principal] {
        &self.allowed
    }

    /// Get whether a principal is allowed.
    pub fn is_allowed(&self, principal: &Principal) -> bool {
        self.allowed.contains(principal)
    }

    /// Add a principal to the allowed list.
    ///
    /// If the principal is already present, do nothing.
    /// Otherwise, add the principal and write the updated ACL to memory.
    pub fn add_principal(&mut self, principal: Principal) -> MemoryResult<()> {
        if !self.is_allowed(&principal) {
            self.allowed.push(principal);
            self.write()?;
        }

        Ok(())
    }

    /// Remove a principal from the allowed list.
    ///
    /// If the principal is not present, do nothing.
    /// Otherwise, remove the principal and write the updated ACL to memory.
    pub fn remove_principal(&mut self, principal: &Principal) -> MemoryResult<()> {
        if let Some(pos) = self.allowed.iter().position(|p| p == principal) {
            self.allowed.swap_remove(pos);
            self.write()?;
        }

        Ok(())
    }

    /// Write [`AccessControlList`] to memory.
    fn write(&self) -> MemoryResult<()> {
        // write to memory location from MEMORY_MANAGER
        MEMORY_MANAGER.with_borrow_mut(|m| m.write_at(m.acl_page(), 0, self))
    }
}

impl Encode for AccessControlList {
    const SIZE: DataSize = DataSize::Variable;

    fn size(&self) -> MSize {
        // 4 bytes for len + sum of each principal's length (1 byte for length + bytes)
        4 + self
            .allowed
            .iter()
            .map(|p| 1 + p.as_slice().len() as MSize)
            .sum::<MSize>()
    }

    fn encode(&'_ self) -> std::borrow::Cow<'_, [u8]> {
        // write the number of principals as u32 followed by each principal's bytes
        let mut bytes = Vec::with_capacity(self.size() as usize);
        let len = self.allowed.len() as u32;
        bytes.extend_from_slice(&len.to_le_bytes());
        for principal in &self.allowed {
            let principal_bytes = principal.as_slice();
            let principal_len = principal_bytes.len() as u8;
            bytes.extend_from_slice(&principal_len.to_le_bytes());
            bytes.extend_from_slice(principal_bytes);
        }
        std::borrow::Cow::Owned(bytes)
    }

    fn decode(data: std::borrow::Cow<[u8]>) -> MemoryResult<Self>
    where
        Self: Sized,
    {
        // read the number of principals as u32 followed by each principal's bytes
        let mut offset = 0;
        let len_bytes = &data[offset..offset + 4];
        offset += 4;
        let len = u32::from_le_bytes(len_bytes.try_into()?) as usize;

        // init vec
        let mut allowed = Vec::with_capacity(len);
        for _ in 0..len {
            let principal_len_bytes = &data[offset..offset + 1];
            offset += 1;
            let principal_len = u8::from_le_bytes(principal_len_bytes.try_into()?) as usize;

            let principal_bytes = &data[offset..offset + principal_len];
            offset += principal_len;

            let principal = Principal::from_slice(principal_bytes);
            allowed.push(principal);
        }
        Ok(AccessControlList { allowed })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_acl_encode_decode() {
        let acl = AccessControlList {
            allowed: vec![
                Principal::anonymous(),
                Principal::from_text("aaaaa-aa").unwrap(),
                Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap(),
                Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap(),
            ],
        };

        let encoded = acl.encode();
        let decoded = AccessControlList::decode(encoded).unwrap();

        assert_eq!(acl, decoded);
    }

    #[test]
    fn test_acl_add_remove_principal() {
        let mut acl = AccessControlList::default();
        let principal = Principal::from_text("aaaaa-aa").unwrap();
        assert!(!acl.is_allowed(&principal));
        acl.add_principal(principal).unwrap();
        assert!(acl.is_allowed(&principal));
        assert_eq!(acl.allowed.len(), 1); // only one principal added
        acl.remove_principal(&principal).unwrap();
        assert!(!acl.is_allowed(&principal));
        assert_eq!(acl.allowed.len(), 0); // principal removed
    }

    #[test]
    fn test_should_add_more_principals() {
        let mut acl = AccessControlList::default();
        let principal1 = Principal::from_text("aaaaa-aa").unwrap();
        let principal2 = Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap();
        acl.add_principal(principal1).unwrap();
        acl.add_principal(principal2).unwrap();
        assert!(acl.is_allowed(&principal1));
        assert!(acl.is_allowed(&principal2));
        assert_eq!(acl.allowed_principals(), &[principal1, principal2]);
    }

    #[test]
    fn test_add_principal_should_write_to_memory() {
        let mut acl = AccessControlList::default();
        let principal = Principal::from_text("aaaaa-aa").unwrap();
        acl.add_principal(principal).unwrap();

        // Load from memory and check if the principal is present
        let loaded_acl = AccessControlList::load().unwrap();
        assert!(loaded_acl.is_allowed(&principal));
    }
}
