use std::sync::Arc;
use std::{ops::Range, time::Duration};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use moka::future::Cache;
use object_store::{
    path::Path, Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::{paging::PageCache, Error, Result};

/// Read-through Page Cache.
///
#[derive(Debug, Clone)]
pub struct ReadThroughCache<C: PageCache> {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<C>,

    global_loader: Cache<(Path, usize), bool>,

    parallelism: usize,
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
        Self {
            inner,
            cache,
            global_loader: Cache::builder()
                .max_capacity(32)
                .time_to_live(Duration::from_secs(60))
                .build(),
            parallelism: num_cpus::get(),
        }
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        self.cache.invalidate(location).await
    }
}

async fn get_range<C: PageCache>(
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,
    location: &Path,
    range: Range<usize>,
    parallelism: usize,
) -> Result<Bytes> {
    let page_size = cache.page_size();
    let start = (range.start / page_size as usize) * page_size;
    let meta = cache.head(location, store.head(location)).await?;

    let pages = stream::iter((start..range.end).step_by(page_size))
        .map(|offset| {
            let page_cache = cache.clone();
            let page_id = offset / page_size;
            let page_end = std::cmp::min(offset + page_size, meta.size);
            async move {
                // Actual range in the file.
                let range_in_file = std::cmp::max(offset, range.start)
                    ..std::cmp::min(offset + page_size, range.end);
                let range_in_page = range_in_file.start - offset..range_in_file.end - offset;
                let page = page_cache
                    .get_range(location, page_id as u32, range_in_page)
                    .await?;
                Ok::<_, Error>((page, offset..page_end))
            }
        })
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;

    let missed_pages = pages
        .iter()
        .filter(|(page, _)| page.is_none())
        .map(|(_, range)| range.clone())
        .collect::<Vec<_>>();

    // TODO: handle parallel loading of missed ranges next.
    let uncached_pages = store.get_ranges(location, &missed_pages).await?;

    // stick all bytes together.
    let mut buf = BytesMut::with_capacity(range.len());
    let mut uncached_idx = 0;
    for (bytes, page_range) in pages {
        if let Some(bytes) = bytes {
            buf.extend_from_slice(&bytes);
        } else {
            let page = &uncached_pages[uncached_idx];
            let intersection = std::cmp::max(page_range.start, range.start)
                ..std::cmp::min(page_range.end, range.end);
            let bytes =
                &page[intersection.start - page_range.start..intersection.end - page_range.start];
            buf.extend_from_slice(bytes);
            uncached_idx += 1;
        }
    }

    // Put them back
    for (bytes, range) in uncached_pages.into_iter().zip(missed_pages.iter()) {
        let page_id = range.start / page_size;
        cache.put(location, page_id as u32, bytes).await?;
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
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.invalidate(location).await?;

        self.inner.put_multipart_opts(location, _opts).await
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
        let location = location.clone();
        let parallelism = self.parallelism;

        // TODO: This might yield too many small reads.
        let s =
            stream::iter((0..file_size).step_by(page_size))
                .map(move |offset| {
                    let loc = location.clone();
                    let store = inner.clone();
                    let c = cache.clone();
                    let page_size = cache.page_size();

                    async move {
                        get_range(store, c, &loc, offset..offset + page_size, parallelism).await
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

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        get_range(
            self.inner.clone(),
            self.cache.clone(),
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

    fn list(&'_ self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
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
        println!("Data: {:?}", data);
        assert_eq!(data, "long text".as_bytes());
    }
}
