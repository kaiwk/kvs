#![deny(missing_docs)]

//! KvStore library

pub mod engine;

pub use engine::kvs;
pub use engine::KvStore;
pub use engine::KvsEngine;
pub use engine::Result;
