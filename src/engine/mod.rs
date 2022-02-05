//! kvs engine

pub mod kvs;

pub use crate::engine::kvs::KvStore;
pub use crate::engine::kvs::Result;

/// Storage interface called by KvsServer
pub trait KvsEngine {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the value of a string key.
    /// Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove the given string key.
    /// Return an error if the key does not exist, or value is not read successfully.
    fn remove(&mut self, key: String) -> Result<()>;
}
