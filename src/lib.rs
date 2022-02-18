#![deny(missing_docs)]

//! KvStore library

pub mod engine;

pub use engine::kvs;
pub use engine::KvStore;
pub use engine::KvsEngine;
pub use engine::Result;
pub use engine::SledEngine;

/// Fib
pub fn fibonacci(n: u64) -> u64 {
    let mut a = 0;
    let mut b = 1;

    match n {
        0 => b,
        _ => {
            for _ in 0..n {
                let c = a + b;
                a = b;
                b = c;
            }
            b
        }
    }
}
