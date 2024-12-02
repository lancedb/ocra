//! Trait for page cache
//!
//! A Page cache caches data in fixed-size pages.

use std::fmt::Debug;
use std::future::Future;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;

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

    /// Read data of a page.
    ///
    /// # Parameters
    /// - `location`: the path of the object.
    /// - `page_id`: the ID of the page.
    ///
    /// # Returns
    /// - `Ok(Some(Bytes))` if the page exists and the data was read successfully.
    /// - `Ok(None)` if the page does not exist.
    /// - `Err(Error)` if an error occurred.
    async fn get_with(
        &self,
        location: &Path,
        page_id: u32,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes>;

    /// Get range of data in the page.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `range`: The range of data to read from the page. The range must be within the page size.
    ///
    /// # Returns
    /// See [Self::get_with()].
    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes>;

    /// Remove a page from the cache.
    async fn invalidate(&self, location: &Path, page_id: u32) -> Result<()>;
}
