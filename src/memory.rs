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

use std::{future::Future, ops::Range, time::Duration};

use bytes::Bytes;
use moka::future::Cache;
use object_store::path::Path;
use sysinfo::{MemoryRefreshKind, RefreshKind};

mod builder;

pub use self::builder::InMemoryCacheBuilder;
use crate::{
    paging::{to_page_key, PageCache, PageKey},
    Error, Result,
};

/// Default memory page size is 8 MB
pub const DEFAULT_PAGE_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_TIME_TO_LIVE: Duration = Duration::from_secs(60 * 30); // 30 minutes

/// In-memory [PageCache] implementation.
///
/// This is a LRU mapping of page IDs to page data, with TTL eviction.
///
#[derive(Debug)]
pub struct InMemoryCache {
    /// Capacity in bytes
    capacity: usize,

    /// Size of each page
    page_size: usize,

    /// In memory page cache: a mapping from `(path id, offset)` to data / bytes.
    cache: Cache<PageKey, Bytes>,
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
    pub fn builder(capacity_bytes: usize) -> InMemoryCacheBuilder {
        InMemoryCacheBuilder::new(capacity_bytes)
    }

    /// Explicitly create a new [InMemoryCache] with a fixed capacity and page size.
    ///
    /// # Parameters:
    /// - `capacity_bytes`: Max capacity in bytes.
    /// - `page_size`: The maximum size of each page.
    ///
    pub fn new(capacity_bytes: usize, page_size: usize) -> Self {
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
        let capacity = (sys.total_memory() as f32 * fraction) as usize;
        Self::builder(capacity)
    }

    fn with_params(capacity: usize, page_size: usize, time_to_idle: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity as u64)
            // weight each key using the size of the value
            .weigher(|_key, value: &Bytes| -> u32 { value.len() as u32 })
            .time_to_idle(time_to_idle)
            // .eviction_listener(eviction_listener)
            .build();
        Self {
            capacity,
            page_size,
            cache,
        }
    }
}

#[async_trait::async_trait]
impl PageCache for InMemoryCache {
    /// The size of each page.
    fn page_size(&self) -> usize {
        self.page_size
    }

    /// Cache capacity in bytes.
    fn capacity(&self) -> usize {
        self.capacity
    }

    async fn get_with(
        &self,
        location: &Path,
        page_id: u64,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        let key = to_page_key(location, page_id);
        match self.cache.try_get_with(key, loader).await {
            Ok(bytes) => Ok(bytes),
            Err(e) => match e.as_ref() {
                Error::NotFound { .. } => Err(Error::NotFound {
                    path: location.to_string(),
                    source: Box::new(e),
                }),
                _ => Err(Error::Generic {
                    store: "InMemoryCache",
                    source: Box::new(e),
                }),
            },
        }
    }

    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u64,
        range: Range<usize>,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        assert!(range.start <= range.end && range.end <= self.page_size());
        let bytes = self.get_with(location, page_id, loader).await?;
        Ok(bytes.slice(range))
    }

    async fn invalidate(&self, location: &Path, page_id: u64) -> Result<()> {
        let key = to_page_key(location, page_id);
        self.cache.remove(&key).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        io::Write,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use bytes::{BufMut, BytesMut};
    use object_store::{local::LocalFileSystem, ObjectStore};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_get_range() {
        let cache = InMemoryCache::new(1024, 512);
        let local_fs = Arc::new(LocalFileSystem::new());

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.bin");
        std::fs::write(&file_path, "test data").unwrap();
        let location = Path::from(file_path.as_path().to_str().unwrap());

        let miss = Arc::new(AtomicUsize::new(0));

        let data = cache
            .get_with(&location, 0, {
                let miss = miss.clone();
                let local_fs = local_fs.clone();
                let location = location.clone();
                async move {
                    miss.fetch_add(1, Ordering::SeqCst);
                    local_fs.get(&location).await.unwrap().bytes().await
                }
            })
            .await
            .unwrap();
        assert_eq!(miss.load(Ordering::SeqCst), 1);
        assert_eq!(data, Bytes::from("test data"));

        let data = cache
            .get_with(&location, 0, {
                let miss = miss.clone();
                let location = location.clone();
                async move {
                    miss.fetch_add(1, Ordering::SeqCst);
                    local_fs.get(&location).await.unwrap().bytes().await
                }
            })
            .await
            .unwrap();
        assert_eq!(miss.load(Ordering::SeqCst), 1);
        assert_eq!(data, Bytes::from("test data"));
    }

    #[tokio::test]
    async fn test_eviction() {
        const PAGE_SIZE: usize = 512;
        let cache = InMemoryCache::new(1024, PAGE_SIZE);
        let local_fs = Arc::new(LocalFileSystem::new());

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.bin");
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            for i in 0_u64..1024 {
                file.write_all(&i.to_be_bytes()).unwrap();
            }
        }
        let location = Path::from(file_path.as_path().to_str().unwrap());
        cache.cache.run_pending_tasks().await;

        let miss = Arc::new(AtomicUsize::new(0));

        for (page_id, expected_miss, expected_size) in
            [(0, 1, 1), (0, 1, 1), (1, 2, 2), (4, 3, 2), (5, 4, 2)].iter()
        {
            let data = cache
                .get_with(&location, *page_id, {
                    let miss = miss.clone();
                    let local_fs = local_fs.clone();
                    let location = location.clone();
                    async move {
                        miss.fetch_add(1, Ordering::SeqCst);
                        local_fs
                            .get_range(
                                &location,
                                PAGE_SIZE * (*page_id as usize)..PAGE_SIZE * (page_id + 1) as usize,
                            )
                            .await
                    }
                })
                .await
                .unwrap();
            assert_eq!(miss.load(Ordering::SeqCst), *expected_miss);
            assert_eq!(data.len(), PAGE_SIZE);

            cache.cache.run_pending_tasks().await;
            assert_eq!(cache.cache.entry_count(), *expected_size);

            let mut buf = BytesMut::with_capacity(PAGE_SIZE);
            for i in page_id * PAGE_SIZE as u64 / 8..(page_id + 1) * PAGE_SIZE as u64 / 8 {
                buf.put_u64(i);
            }
            assert_eq!(data, buf);
        }
    }
}
