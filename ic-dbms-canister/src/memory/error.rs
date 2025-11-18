use std::array::TryFromSliceError;

use thiserror::Error;

use crate::memory::{MSize, Page, PageOffset};

/// An enum representing possible memory-related errors.
#[derive(Debug, Error)]
pub enum MemoryError {
    /// Error when the data to be written is too large for the page.
    #[error("Data too large for page (page size: {page_size}, requested: {requested})")]
    DataTooLarge { page_size: u64, requested: u64 },
    /// Error when failing to decode data from bytes.
    #[error("Failed to decode data from bytes: {0}")]
    DecodeError(#[from] DecodeError),
    /// Error when failing to allocate a new page.
    #[error("Failed to allocate a new page")]
    FailedToAllocatePage,
    /// Error when attempting to access stable memory out of bounds.
    #[error("Stable memory access out of bounds")]
    OutOfBounds,
    /// Error when attempting to write out of the allocated page.
    #[error(
        "Tried to write out of the allocated page (page: {page}, offset: {offset}, data size: {data_size}, page size: {page_size})"
    )]
    SegmentationFault {
        page: Page,
        offset: PageOffset,
        data_size: MSize,
        page_size: u64,
    },
    /// Error when failing to grow stable memory.
    #[error("Failed to grow stable memory: {0}")]
    StableMemoryError(#[from] ic_cdk::stable::StableMemoryError),
}

impl From<TryFromSliceError> for MemoryError {
    fn from(err: TryFromSliceError) -> Self {
        MemoryError::DecodeError(DecodeError::from(err))
    }
}

impl From<std::string::FromUtf8Error> for MemoryError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        MemoryError::DecodeError(DecodeError::from(err))
    }
}

/// An enum representing possible decoding errors.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Error when the raw record header is invalid.
    #[error("Bad raw record header")]
    BadRawRecordHeader,
    /// Error when failing to convert from slice.
    #[error("Failed to convert from slice: {0}")]
    TryFromSliceError(#[from] TryFromSliceError),
    /// Error when failing to convert from UTF-8 string.
    #[error("Failed to convert from UTF-8 string: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    /// Error when the data is too short to decode.
    #[error("Data too short to decode")]
    TooShort,
}
