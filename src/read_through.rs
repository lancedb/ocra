use std::sync::Arc;

use object_store::ObjectStore;

use crate::paging::PageCache;

/// Read-through Page Cache.
///
#[derive(Debug)]
pub struct ReadThroughCache {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<dyn PageCache>,
}

impl std::fmt::Display for ReadThroughCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadThroughCache(inner={}, cache={:?})",
            self.inner, self.cache
        )
    }
}

impl ReadThroughCache {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<dyn PageCache>) -> Self {
        Self { inner, cache }
    }
}
