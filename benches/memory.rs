//! Benchmark for in-memory page cache.
//!
use criterion::{criterion_group, criterion_main, Criterion};

use ocra::memory::InMemoryCache;

fn memory_cache_bench(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    const FILE_SiZE: usize = 1024 * 1024 * 1024;
    // TODO: support other object store later
    let local_fs = object_store::local::LocalFileSystem::new();
    

    c.bench_function("memory_cache,warm", |b| {
        b.to_async(&rt).iter(|| async {});
    });
}

criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = memory_cache_bench);

criterion_main!(benches);
