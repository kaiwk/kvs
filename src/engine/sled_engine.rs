//! sled engine

use std::path::PathBuf;

use anyhow::anyhow;
use sled;

use crate::engine::KvsEngine;
use crate::kvs::EngineError;
use crate::Result;

/// Sled Engine
pub struct SledEngine {
    inner: sled::Db,
}

impl SledEngine {
    /// Open a Sled database
    pub fn open(path: impl Into<PathBuf>) -> Result<SledEngine> {
        let db = sled::open(path.into()).map_err(|e| anyhow!(e))?;

        Ok(SledEngine { inner: db })
    }
}

impl KvsEngine for SledEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _ = self
            .inner
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| anyhow!(e))?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let result = self
            .inner
            .get(key)
            .map_err(|e| anyhow!(e))?
            .map(|iv| String::from_utf8_lossy(&iv).to_string());

        Ok(result)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let _ = self
            .inner
            .remove(key.clone())
            .map_err(|e| anyhow!(e))?
            .ok_or(EngineError::NotFound(key))?;
        Ok(())
    }
}
