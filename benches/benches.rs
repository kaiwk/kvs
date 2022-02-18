use std::process::Command;

use criterion::black_box;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::fibonacci;
use rand::{distributions::Alphanumeric, Rng};

// let s: String = rand::thread_rng()
//     .sample_iter(&Alphanumeric)
//     .take(7)
//     .map(char::from)
//     .collect();

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
