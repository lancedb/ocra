use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path, Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::{paging::PageCache, stats::CacheStats, Error, Result};

/// Read-through Page Cache.
///
#[derive(Debug, Clone)]
pub struct ReadThroughCache<C: PageCache> {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<C>,

    parallelism: usize,

    stats: Arc<dyn CacheStats>,
}

impl<C: PageCache> std::fmt::Display for ReadThroughCache<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadThroughCache(inner={}, cache={:?})",
            self.inner, self.cache
        )
    }
}

impl<C: PageCache> ReadThroughCache<C> {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<C>) -> Self {
        Self::new_with_stats(
            inner,
            cache,
            Arc::new(crate::stats::AtomicIntCacheStats::new()),
        )
    }

    pub fn new_with_stats(
        inner: Arc<dyn ObjectStore>,
        cache: Arc<C>,
        stats: Arc<dyn CacheStats>,
    ) -> Self {
        Self {
            inner,
            cache,
            parallelism: num_cpus::get(),
            stats,
        }
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        self.cache.invalidate(location).await
    }
}

async fn get_range<C: PageCache>(
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,
    stats: Arc<dyn CacheStats>,
    location: &Path,
    range: Range<u64>,
    parallelism: usize,
) -> Result<Bytes> {
    let page_size = cache.page_size();
    let page_size_u64 = page_size as u64;
    let range_start = range.start;
    let range_end = range.end;
    let start = (range_start / page_size_u64) * page_size_u64;
    let meta = cache.head(location, store.head(location)).await?;
    let meta_size = meta.size;

    let pages = stream::iter((start..range_end).step_by(page_size))
        .map(|offset| {
            let page_cache = cache.clone();
            let store = store.clone();
            let stats = stats.clone();
            let loc = location.clone();

            async move {
                stats.inc_total_reads();

                let page_id =
                    u32::try_from(offset / page_size_u64).map_err(|e| Error::Generic {
                        store: "ReadThroughCache",
                        source: Box::new(e),
                    })?;

                let page_end = std::cmp::min(offset + page_size_u64, meta_size);
                let intersection_start = std::cmp::max(offset, range_start);
                let intersection_end = std::cmp::min(page_end, range_end);

                let start_in_page =
                    usize::try_from(intersection_start - offset).map_err(|e| Error::Generic {
                        store: "ReadThroughCache",
                        source: Box::new(e),
                    })?;
                let end_in_page =
                    usize::try_from(intersection_end - offset).map_err(|e| Error::Generic {
                        store: "ReadThroughCache",
                        source: Box::new(e),
                    })?;

                let range_in_page = start_in_page..end_in_page;
                let stats_for_miss = stats.clone();
                let store_for_loader = store.clone();
                let loc_for_loader = loc.clone();

                page_cache
                    .get_range_with(&loc, page_id, range_in_page, async move {
                        stats_for_miss.inc_total_misses();
                        store_for_loader
                            .get_range(&loc_for_loader, offset..page_end)
                            .await
                    })
                    .await
            }
        })
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;

    if pages.len() == 1 {
        return Ok(pages.into_iter().next().unwrap());
    }

    let range_len = usize::try_from(range_end - range_start).map_err(|e| Error::Generic {
        store: "ReadThroughCache",
        source: Box::new(e),
    })?;

    // stick all bytes together.
    let mut buf = BytesMut::with_capacity(range_len);
    for page in pages {
        buf.extend_from_slice(&page);
    }
    Ok(buf.into())
}

#[async_trait]
impl<C: PageCache> ObjectStore for ReadThroughCache<C> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.cache.invalidate(location).await?;

        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.invalidate(location).await?;

        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        todo!()
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let meta = self.head(location).await?;
        let file_size = meta.size;
        let page_size = self.cache.page_size();
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let location = location.clone();
        let parallelism = self.parallelism;

        // TODO: This might yield too many small reads.
        let s = stream::iter((0..file_size).step_by(page_size))
            .map(move |offset| {
                let loc = location.clone();
                let store = inner.clone();
                let stats = stats.clone();
                let c = cache.clone();
                let page_size = cache.page_size() as u64;

                async move {
                    get_range(
                        store,
                        c,
                        stats,
                        &loc,
                        offset..offset + page_size,
                        parallelism,
                    )
                    .await
                }
            })
            .buffered(self.parallelism)
            .boxed();

        let payload = GetResultPayload::Stream(s);
        Ok(GetResult {
            payload,
            meta: meta.clone(),
            range: 0..meta.size,
            attributes: Attributes::default(),
        })
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        get_range(
            self.inner.clone(),
            self.cache.clone(),
            self.stats.clone(),
            location,
            range,
            self.parallelism,
        )
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.cache.head(location, self.inner.head(location)).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.invalidate(location).await?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.invalidate(to).await?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.invalidate(to).await?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::InMemoryCache;

    use super::*;

    #[tokio::test]
    async fn test_get_end_of_file() {
        let cache = Arc::new(InMemoryCache::new(1024 * 1024, 1024));
        let store = Arc::new(object_store::local::LocalFileSystem::new());
        let cache = Arc::new(ReadThroughCache::new(store, cache));

        let temp_file = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        {
            std::fs::write(temp_file.to_str().unwrap(), "this is a long text").unwrap();
        }
        let path = Path::from(temp_file.to_str().unwrap());
        let meta = cache.head(&path).await.unwrap();

        let data = cache.get_range(&path, 10..meta.size).await.unwrap();
        assert_eq!(data.len(), 9);
        assert_eq!(data, "long text".as_bytes());
    }
}
