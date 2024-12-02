//! Benchmark for in-memory page cache.
//!
//!

use std::{fs::File, io::Write, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use object_store::{path::Path, ObjectStore};
use rand::Rng;

use ocra::{memory::InMemoryCache, paging::PageCache};

fn memory_cache_bench(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut rng = rand::thread_rng();

    const FILE_SIZE: usize = 1024 * 1024 * 1024;
    // TODO: support other object stores later
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let temp_file = tempfile::NamedTempFile::new().unwrap().into_temp_path();
    {
        let mut writer = File::create(temp_file.to_str().unwrap()).unwrap();
        let mut buf = vec![0_u8; 128 * 1024];

        for _ in 0..FILE_SIZE / (128 * 1024) {
            rng.fill(&mut buf[..]);
            writer.write_all(&buf).unwrap();
        }
    }

    for page_size in &[1024 * 1024, 8 * 1024 * 1024] {
        let cache = Arc::new(InMemoryCache::new(FILE_SIZE + 32 * 1024, *page_size));
        let location = Path::from(temp_file.to_str().unwrap());

        // Warm up the cache
        println!("Starting warm up cache with page size: {}", page_size);
        rt.block_on(async {
            let loc = location.clone();
            for i in 0..FILE_SIZE / page_size {
                let data = cache
                    .get_with(&loc, i as u32, {
                        let store = store.clone();
                        let location = loc.clone();
                        async move {
                            store
                                .get_range(&location, i * page_size..(i + 1) * page_size)
                                .await
                        }
                    })
                    .await
                    .unwrap();
                assert!(!data.is_empty());
            }
            cache.cache.run_pending_tasks().await;
        });

        c.bench_function(
            format!("memory_cache,warm,page_size={}", page_size).as_str(),
            |b| {
                b.to_async(&rt).iter(|| {
                    let mut rng = rand::thread_rng();
                    let cache = cache.clone();
                    let loc = location.clone();
                    async move {
                        let page_id = rng.gen_range(0..FILE_SIZE / page_size);

                        let _data = cache
                            .get_with(&loc, page_id as u32, async {
                                panic!("Should not be called page_id={}", page_id)
                            })
                            .await
                            .unwrap();
                    }
                })
            },
        );
    }
}

criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = memory_cache_bench);

criterion_main!(benches);
