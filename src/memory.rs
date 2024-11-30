//! In-memory page cache
//!

use std::ops::Range;

use bytes::Bytes;

use crate::{paging::PageCache, Result};

/// In-memory Page Cache
#[derive(Debug)]
pub struct InMemoryCache {}

impl InMemoryCache {
    /// Create a new cache with a size that is a fraction of the system memory
    ///
    /// warning: does NOT panic if the fraction is greater than 1
    /// but you are responsible for making sure there is
    /// 1. no OOM killer, i.e. swap enabled
    /// 2. you are okay with the performance of swapping to disk
    pub fn with_sys_memory(_fraction: f32, _page_size: usize) -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl PageCache for InMemoryCache {
    /// The size of each page.
    fn page_size(&self) -> usize {
        todo!()
    }

    /// Cache capacity, in number of pages.
    fn capacity(&self) -> usize {
        todo!()
    }

    /// How many pages are cached.
    fn len(&self) -> usize {
        todo!()
    }

    async fn get(&self, id: [u8; 32]) -> Result<Option<Bytes>> {
        todo!()
    }

    /// Get range of data in the page.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `range`: The range of data to read from the page. The range must be within the page size.
    ///
    /// # Returns
    /// See [Self::get()].
    async fn get_range(&self, id: [u8; 32], range: Range<usize>) -> Result<Option<Bytes>> {
        todo!()
    }

    /// Put a page in the cache.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `page`: The data to put in the page. The page must not be larger than the page size.
    ///           If the page is smaller than the page size, the remaining space will be zeroed.
    ///
    async fn put(&self, id: [u8; 32], page: Bytes) -> Result<()> {
        todo!()
    }
}
