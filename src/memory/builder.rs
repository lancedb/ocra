//! Memory Cache Builder
//!

use std::time::Duration;

use super::{InMemoryCache, DEFAULT_PAGE_SIZE, DEFAULT_TIME_TO_LIVE};

/// Builder for [InMemoryCache]
pub struct InMemoryCacheBuilder {
    capacity: usize,
    page_size: usize,

    time_to_idle: Duration,
}

impl InMemoryCacheBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            page_size: DEFAULT_PAGE_SIZE,
            time_to_idle: DEFAULT_TIME_TO_LIVE,
        }
    }

    /// Set the page size.
    pub fn page_size(&mut self, size: usize) -> &mut Self {
        self.page_size = size;
        self
    }

    /// If an entry has been idle longer than `time_to_idle` seconds,
    /// it will be evicted.
    ///
    /// Default is 30 minutes.
    pub fn time_to_idle(&mut self, tti: Duration) -> &mut Self {
        self.time_to_idle = tti;
        self
    }

    pub fn build(&self) -> InMemoryCache {
        InMemoryCache::with_params(self.capacity, self.page_size, self.time_to_idle)
    }
}
