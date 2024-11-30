//! In-memory [PageCache] implementation
//!
//! User can specify the capacity of the cache, or specify
//! how much percentage of memory should be allocated to it.
//!
//! ```
//! use ocra::memory::InMemoryCache;
//!
//! // Use 60% of system memory
//! let cache = InMemoryCache::with_sys_memory(0.6).build();
//!
//! // Use 32 GB of memory
//! let cache = InMemoryCache::builder(32 * 1024 * 1024 * 1024).build();
//! ```

use std::{ops::Range, time::Duration};

use bytes::Bytes;
use moka::future::Cache;
use sysinfo::{MemoryRefreshKind, RefreshKind};

mod builder;

pub use self::builder::InMemoryCacheBuilder;
use crate::{paging::PageCache, Result};

/// Default memory page size is 8 MB
pub const DEFAULT_PAGE_SIZE: u64 = 8 * 1024 * 1024;
const DEFAULT_TIME_TO_LIVE: Duration = Duration::from_secs(60 * 30); // 30 minutes

/// In-memory [PageCache] implementation.
///
/// This is a LRU mapping of page IDs to page data, with TTL eviction.
///
#[derive(Debug)]
pub struct InMemoryCache {
    /// Capacity in bytes
    capacity: u64,

    /// Size of each page
    page_size: u64,

    /// Page cache: a mapping from `(path id, offset)` to data / bytes.
    cache: Cache<(u32, u32), Bytes>,
}

impl InMemoryCache {
    /// Create a [`Builder`](InMemoryCacheBuilder) to construct [InMemoryCache].
    ///
    /// # Parameters:
    /// - *capacity*: capacity in bytes
    ///
    /// ```
    /// # use std::time::Duration;
    /// use ocra::memory::InMemoryCache;
    ///
    /// let cache = InMemoryCache::builder(8*1024*1024)
    ///     .page_size(4096)
    ///     .time_to_idle(Duration::from_secs(60))
    ///     .build();
    /// ```
    pub fn builder(capacity_bytes: u64) -> InMemoryCacheBuilder {
        InMemoryCacheBuilder::new(capacity_bytes)
    }

    /// Explicitly create a new [InMemoryCache] with a fixed capacity and page size.
    ///
    /// # Parameters:
    /// - `capacity_bytes`: Max capacity in bytes.
    /// - `page_size`: The maximum size of each page.
    ///
    pub fn new(capacity_bytes: u64, page_size: u64) -> Self {
        Self::with_params(capacity_bytes, page_size, DEFAULT_TIME_TO_LIVE)
    }

    /// Create a new cache with a size that is a fraction of the system memory
    ///
    /// warning: does NOT panic if the fraction is greater than 1
    /// but you are responsible for making sure there is
    /// 1. no OOM killer, i.e. swap enabled
    /// 2. you are okay with the performance of swapping to disk
    pub fn with_sys_memory(fraction: f32) -> InMemoryCacheBuilder {
        let sys = sysinfo::System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::everything()),
        );
        let capacity = (sys.total_memory() as f32 * fraction) as u64;
        Self::builder(capacity)
    }

    fn with_params(capacity_bytes: u64, page_size: u64, time_to_idle: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity_bytes)
            // weight each key using the size of the value
            .weigher(|_key, value: &Bytes| -> u32 { value.len() as u32 })
            .time_to_idle(time_to_idle)
            // .eviction_listener(eviction_listener)
            .build();
        Self {
            capacity: capacity_bytes,
            page_size: page_size,
            cache,
        }
    }
}

#[async_trait::async_trait]
impl PageCache for InMemoryCache {
    /// The size of each page.
    fn page_size(&self) -> u64 {
        self.page_size
    }

    /// Cache capacity in bytes.
    fn capacity(&self) -> u64 {
        self.capacity
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
