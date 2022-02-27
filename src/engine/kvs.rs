#![deny(missing_docs)]

//! KvStore library

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
// use bincode;
use serde::{Deserialize, Serialize};
use serde_json;
use walkdir::WalkDir;

pub use crate::engine::KvsEngine;

/// Result for engine
pub type Result<T> = std::result::Result<T, EngineError>;

/// Log entry
#[derive(Serialize, Deserialize, Debug)]
pub enum Entry {
    /// Set
    Set {
        /// Key
        key: String,
        /// value
        value: String,
    },

    /// Remove
    Remove {
        /// Key
        key: String,
    },
}

/// Log path + offset
#[derive(Debug, Clone)]
pub struct LogPointer {
    // Log Path
    path: PathBuf,

    // offset
    offset: usize,
}

/// Store key-value pair
#[derive(Clone)]
pub struct KvStore {
    keydir: Arc<Mutex<HashMap<String, LogPointer>>>,
    active_file: Arc<Mutex<File>>,
    active_file_path: PathBuf,
    dir_path: PathBuf,
    file_threshold: usize,
    max_size: usize,
}

impl KvStore {
    /// Create KvStore instance.
    pub fn new(dir_path: PathBuf) -> Result<Self> {
        let active_file_path = dir_path.join("db.log");
        let active_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .append(true)
                .open(active_file_path.clone())?,
        ));

        Ok(KvStore {
            keydir: Arc::new(Mutex::new(HashMap::new())),
            active_file,
            active_file_path,
            dir_path,
            file_threshold: 10 * 1024,
            max_size: 5 * 10 * 1024,
        })
    }

    /// Truncate current active file and create data file.
    fn truncate_active_file(&self) -> Result<()> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let data_file_path = self.dir_path.join(format!(
            "data-{}.log",
            since_the_epoch.as_millis().to_string()
        ));

        std::fs::rename(self.active_file_path.clone(), data_file_path.clone())?;

        self.scan_file(data_file_path)?;

        let mut active_file = self.active_file.lock().unwrap();
        *active_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(self.active_file_path.clone())?;

        Ok(())
    }

    fn total_size(&self) -> usize {
        let entries = WalkDir::new(self.dir_path.clone()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size") as usize
    }

    /// Create KvStore from file.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        let mut compacted_paths = vec![];
        for entry in std::fs::read_dir(path.clone())? {
            let p = entry?.path();
            if p.is_file() {
                if let Some(file_name) = p.file_name().map(|s| s.to_string_lossy()) {
                    if file_name.starts_with("compact") {
                        compacted_paths.push(p);
                    }
                }
            }
        }
        compacted_paths.sort_by(|a, b| b.cmp(a));
        let compacted_path = compacted_paths
            .first()
            .cloned()
            .unwrap_or(path.join("compact.log"));

        let kv_store = KvStore::new(path)?;

        if compacted_path.exists() {
            kv_store.scan_file(compacted_path)?;
        }

        kv_store.scan_active_file()?;

        Ok(kv_store)
    }

    fn compact(&self) -> Result<()> {
        // compact
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let compact_path = self.dir_path.join(format!(
            "compact-{}.log",
            since_the_epoch.as_millis().to_string()
        ));
        let mut compact_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(compact_path.clone())?;

        let keydir = self.keydir.lock().unwrap();
        for (key, _) in keydir.iter() {
            if let Some(val) = self.read_value(&*keydir, key.to_owned())? {
                let entry = Entry::Set {
                    key: key.clone(),
                    value: val.clone(),
                };

                writeln!(compact_file, "{}", serde_json::to_string(&entry)?)?;
            }
        }
        drop(keydir);

        compact_file.sync_all()?;

        // remove stale data files and compact files
        for entry in std::fs::read_dir(self.dir_path.clone())? {
            let p = entry?.path();
            if p.is_file() && p != compact_path {
                if let Some(file_name) = p.file_name().map(|s| s.to_string_lossy()) {
                    if file_name.starts_with("compact") || file_name.starts_with("data") {
                        std::fs::remove_file(p)?;
                    }
                }
            }
        }

        // clear key-dir and active file
        let mut keydir = self.keydir.lock().unwrap();
        keydir.clear();
        drop(keydir);

        let mut active_file = self.active_file.lock().unwrap();
        active_file.seek(SeekFrom::Start(0))?;
        active_file.set_len(0)?;
        active_file.sync_all()?;
        drop(active_file);

        // scan
        self.scan_file(compact_path)?;

        Ok(())
    }

    fn scan_active_file(&self) -> Result<()> {
        self.scan_file(self.active_file_path.clone())
    }

    /// Scan file and refresh inner
    fn scan_file(&self, path: PathBuf) -> Result<()> {
        let mut bytes_len = 0;
        let reader = BufReader::new(OpenOptions::new().read(true).open(path.clone())?);
        for line in reader.lines() {
            let line_string = line?;
            let entry: Entry = serde_json::from_str(&line_string)?;
            match entry {
                Entry::Set { key, .. } => {
                    let mut keydir = self.keydir.lock().unwrap();
                    keydir.insert(
                        key,
                        LogPointer {
                            path: path.clone(),
                            offset: bytes_len,
                        },
                    );
                }
                Entry::Remove { key } => {
                    let mut keydir = self.keydir.lock().unwrap();
                    keydir.remove(&key);
                }
            }
            bytes_len += line_string.as_bytes().len() + 1;
        }

        Ok(())
    }

    fn read_value(
        &self,
        keydir: &HashMap<String, LogPointer>,
        key: String,
    ) -> Result<Option<String>> {
        if let Some(log_pointer) = keydir.get(&key) {
            let file = OpenOptions::new()
                .read(true)
                .open(log_pointer.path.clone())?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(log_pointer.offset as u64))?;
            let mut entry_string = String::new();
            reader.read_line(&mut entry_string)?;
            let entry: Entry = serde_json::from_str(&entry_string)?;
            if let Entry::Set { value, .. } = entry {
                Ok(Some(value))
            } else {
                return Err(EngineError::NotFound(
                    "DB log error, there should be a Set entry".to_owned(),
                ));
            }
        } else {
            Ok(None)
        }
    }
}

/// Error for engine
#[derive(Error, Debug)]
pub enum EngineError {
    /// Not found data for the given key
    #[error("Key not found, `{0}` is not found")]
    NotFound(String),

    /// Io error
    #[error("Io Error")]
    Io(#[from] std::io::Error),

    /// serde json error
    #[error("serealize json failed")]
    Serde(#[from] serde_json::Error),

    /// Unknown error
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut active_file = self.active_file.lock().unwrap();
        let file_size = active_file.metadata()?.len() as usize;

        let entry = Entry::Set {
            key: key.clone(),
            value: value.clone(),
        };
        writeln!(active_file, "{}", serde_json::to_string(&entry)?)?;
        active_file.sync_all()?;
        drop(active_file);

        let mut keydir = self.keydir.lock().unwrap();
        keydir.insert(
            key,
            LogPointer {
                path: self.dir_path.join("db.log"),
                offset: file_size,
            },
        );
        drop(keydir);

        if file_size > self.file_threshold {
            self.truncate_active_file()?;
        }

        if self.total_size() > self.max_size {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let keydir = self.keydir.lock().unwrap();
        self.read_value(&*keydir, key)
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut keydir = self.keydir.lock().unwrap();
        if !keydir.contains_key(&key) {
            return Err(EngineError::NotFound(key));
        }

        let mut active_file = self.active_file.lock().unwrap();
        let entry = Entry::Remove { key: key.clone() };
        writeln!(active_file, "{}", serde_json::to_string(&entry)?)?;
        active_file.sync_all()?;
        drop(active_file);

        keydir.remove(&key);

        Ok(())
    }
}
