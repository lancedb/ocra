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
}

#[async_trait]
impl<C: PageCache + 'static> ObjectStore for ReadThroughCache<C> {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> Result<PutResult> {
        todo!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        todo!()
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        todo!()
    }

    fn list(&'_ self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
