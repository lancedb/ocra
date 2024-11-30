//! Trait for page cache
//!
//! A Page cache caches data in fixed-size pages.

use std::fmt::Debug;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;

/// [PageCache] trait.
///
/// Caching fixed-size pages. Each page has a unique ID.
#[async_trait]
pub trait PageCache: Sync + Send + Debug {
    /// The size of each page.
    fn page_size(&self) -> usize;

    /// Cache capacity, in number of pages.
    fn capacity(&self) -> usize;

    /// How many pages are cached.
    fn len(&self) -> usize;

    /// Returns true if the cache is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read data from a Page.
    ///
    /// # Returns
    /// - `Ok(Some(Bytes))` if the page exists and the data was read successfully.
    /// - `Ok(None)` if the page does not exist.
    /// - `Err(Error)` if an error occurred.
    async fn get(&self, id: [u8; 32]) -> Result<Option<Bytes>>;

    /// Get range of data in the page.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `range`: The range of data to read from the page. The range must be within the page size.
    ///
    /// # Returns
    /// See [Self::get()].
    async fn get_range(&self, id: [u8; 32], range: Range<usize>) -> Result<Option<Bytes>>;

    /// Put a page in the cache.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `page`: The data to put in the page. The page must not be larger than the page size.
    ///           If the page is smaller than the page size, the remaining space will be zeroed.
    ///
    async fn put(&self, id: [u8; 32], page: Bytes) -> Result<()>;
}
