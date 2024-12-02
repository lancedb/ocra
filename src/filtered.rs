//! Filtered [`ObjectStore`]
//!

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

use crate::{traits::CachedObjectStore, Result};

/// Filtered by [object_store::path::Path]
///
pub trait PathFilter: Send + Sync + Debug {
    /// Returns true if the path should skip the cache and read the object store directly
    fn skip_cache(&self, path: &Path) -> bool;
}

/// Filter based on file extensions.
#[derive(Debug)]
pub struct ExtensionFilter {
    extensions: Vec<String>,
}

impl ExtensionFilter {
    pub fn new<S: AsRef<str>>(extensions: &[S]) -> Self {
        Self {
            extensions: extensions.iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }
}

impl PathFilter for ExtensionFilter {
    fn skip_cache(&self, path: &Path) -> bool {
        if let Some(ext) = path.extension() {
            self.extensions.iter().any(|e| e == ext)
        } else {
            false
        }
    }
}

/// Path filter
#[derive(Debug)]
pub struct FilteringStore {
    filters: Vec<Box<dyn PathFilter>>,
    inner: Arc<dyn CachedObjectStore>,
}

impl FilteringStore {
    pub fn new(
        read_through: Arc<dyn CachedObjectStore>,
        filters: Vec<Box<dyn PathFilter>>,
    ) -> Self {
        Self {
            filters,
            inner: read_through,
        }
    }

    /// Returns true if the path should skip the cache and read the object store directly.
    fn should_skip_cache(&self, path: &Path) -> bool {
        self.filters.iter().any(|f| f.skip_cache(path))
    }
}

impl Display for FilteringStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FilteringStore(filters={:?}, inner={})",
            self.filters, self.inner
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for FilteringStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if self.should_skip_cache(location) {
            self.inner.inner().put_opts(location, payload, opts).await
        } else {
            self.inner.put_opts(location, payload, opts).await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        if self.should_skip_cache(location) {
            self.inner.inner().put_multipart_opts(location, opts).await
        } else {
            self.inner.put_multipart_opts(location, opts).await
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if self.should_skip_cache(location) {
            self.inner.inner().get_opts(location, options).await
        } else {
            self.inner.get_opts(location, options).await
        }
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        if self.should_skip_cache(location) {
            self.inner.inner().head(location).await
        } else {
            self.inner.head(location).await
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        if self.should_skip_cache(location) {
            self.inner.inner().delete(location).await
        } else {
            self.inner.delete(location).await
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_skip_cache(from) {
            self.inner.inner().copy_if_not_exists(from, to).await
        } else {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if self.should_skip_cache(from) {
            self.inner.inner().copy(from, to).await
        } else {
            self.inner.copy(from, to).await
        }
    }
}
