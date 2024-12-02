use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::{paging::PageCache, Result};

/// Read-through Page Cache.
///
#[derive(Debug)]
pub struct ReadThroughCache<C: PageCache> {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<C>,
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
        Self { inner, cache }
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        self.cache.invalidate(location).await
    }
}

#[async_trait]
impl<C: PageCache + 'static> ObjectStore for ReadThroughCache<C> {
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
