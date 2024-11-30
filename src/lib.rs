//! **OCRA**: an *arrow-rs* [**O**bjectStore](object_store::ObjectStore)
//! implementation with **C**ache in **R**ust for **A**ll
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use tokio::runtime::Runtime;
//! use object_store::{ObjectStore, local::LocalFileSystem, path::Path};
//! use ocra::{ReadThroughCache, memory::InMemoryCache};
//!
//! # let mut rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! let fs = Arc::new(LocalFileSystem::new());
//! // Use 75% of system memory for cache
//! let memory_cache = Arc::new(
//!     InMemoryCache::with_sys_memory(0.75, 4 * 1024 * 1024));
//! let cached: Arc<dyn ObjectStore> =
//!     Arc::new(ReadThroughCache::new(fs, memory_cache));
//!
//! // Now you can use `cached` as a regular ObjectStore
//! let path = Path::from("my-key");
//! let data = cached.get_range(&path, 1024..2048).await.unwrap();
//! # })
//! ```

pub mod memory;
mod paging;
mod read_through;

// We reuse `object_store` Error and Result to make this crate work well
// with the rest of object_store implementations.
pub use object_store::{Error, Result};

pub use read_through::ReadThroughCache;
