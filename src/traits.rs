//! Traits
//!

use std::sync::Arc;

use object_store::ObjectStore;

pub trait CachedObjectStore: ObjectStore {
    /// Returns the cache used by this store
    fn inner(&self) -> &Arc<dyn ObjectStore>;
}
