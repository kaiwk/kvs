//! kvs engine

pub mod kvs;
pub mod sled_engine;

pub use crate::engine::kvs::EngineError;
pub use crate::engine::kvs::KvStore;
pub use crate::engine::kvs::Result;
pub use sled_engine::SledEngine;

/// Storage interface called by KvsServer
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get the value of a string key.
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Remove the given string key.
    /// Return an error if the key does not exist, or value is not read successfully.
    fn remove(&self, key: String) -> Result<()>;
}
