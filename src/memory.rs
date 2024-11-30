//! In-memory page cache
//!

/// In-memory Page Cache
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
