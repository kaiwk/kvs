use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use once_cell::sync::Lazy;
use rand::{distributions::Alphanumeric, Rng};

use kvs::KvStore;
use kvs::KvsEngine;
use kvs::SledEngine;

static RANDOM_KEYS: Lazy<Vec<String>> = Lazy::new(|| rand_generate_vec(100));
static RANDOM_VALUES: Lazy<Vec<String>> = Lazy::new(|| rand_generate_vec(100));

fn rand_generate_vec(size: usize) -> Vec<String> {
    let mut random_values = vec![];
    let mut r = rand::thread_rng();

    for _ in 0..size {
        random_values.push(rand_generate(r.gen_range(0, 100000)));
    }

    random_values
}

fn rand_generate(size: usize) -> String {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect();
    s
}

fn kvs_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("kvs write group");
    group.sample_size(10);

    group.bench_function("kvs write", |b| {
        b.iter(|| {
            let kvs = KvStore::open(std::env::current_dir().unwrap()).unwrap();
            for (key, value) in RANDOM_KEYS.iter().zip(RANDOM_VALUES.iter()) {
                kvs.set(key.to_owned(), value.to_owned()).unwrap();
            }
        });
    });

    group.finish();
}

fn sled_write(c: &mut Criterion) {
    c.bench_function("sled write", |b| {
        b.iter(|| {
            let sled_engine = SledEngine::open(std::env::current_dir().unwrap()).unwrap();
            for (key, value) in RANDOM_KEYS.iter().zip(RANDOM_VALUES.iter()) {
                sled_engine.set(key.to_owned(), value.to_owned()).unwrap();
            }
        });
    });
}

fn kvs_read(c: &mut Criterion) {
    c.bench_function("kvs read", |b| {
        b.iter(|| {
            let kvs = KvStore::open(std::env::current_dir().unwrap()).unwrap();
            for key in RANDOM_KEYS.iter() {
                let _ = kvs.get(key.to_owned());
            }
        });
    });
}

fn sled_read(c: &mut Criterion) {
    c.bench_function("sled read", |b| {
        b.iter(|| {
            let sled_engine = SledEngine::open(std::env::current_dir().unwrap()).unwrap();
            for key in RANDOM_KEYS.iter() {
                let _ = sled_engine.get(key.to_owned());
            }
        });
    });
}

criterion_group!(benches, kvs_write, sled_write, kvs_read, sled_read);
criterion_main!(benches);
