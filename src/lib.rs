#![deny(missing_docs)]

//! KvStore library

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::{prelude::*, BufReader};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::bail;
use anyhow::Result;
// use bincode;
use serde::{Deserialize, Serialize};
use serde_json;

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
#[derive(Debug)]
pub struct LogPointer {
    // Log Path
    path: PathBuf,

    // offset
    offset: usize,
}

/// Store key-value pair
pub struct KvStore {
    keydir: HashMap<String, LogPointer>,
    active_file: File,
    dir_path: PathBuf,
    max_size: usize,
}

impl KvStore {
    /// Create KvStore instance.
    pub fn new(file: File, dir_path: PathBuf) -> Self {
        KvStore {
            keydir: HashMap::new(),
            active_file: file,
            dir_path,
            max_size: 10 * 1024,
        }
    }

    /// Insert a key-value pair.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let file_size = self.active_file.metadata()?.len() as usize;

        let entry = Entry::Set {
            key: key.clone(),
            value: value.clone(),
        };
        writeln!(self.active_file, "{}", serde_json::to_string(&entry)?)?;
        self.active_file.sync_all()?;

        self.keydir.insert(
            key,
            LogPointer {
                path: self.dir_path.join("db.log"),
                offset: file_size,
            },
        );

        // compact
        if file_size > self.max_size {
            self.compact();
        }

        Ok(())
    }

    /// Get a value with `key`.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(log_pointer) = self.keydir.get(&key) {
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
                bail!("DB log error, there should be a Set entry");
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a key-value pair.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.keydir.contains_key(&key) {
            println!("Key not found");
            bail!("Key not found");
        }

        let entry = Entry::Remove { key: key.clone() };
        writeln!(self.active_file, "{}", serde_json::to_string(&entry)?)?;
        self.active_file.sync_all()?;

        self.keydir.remove(&key);

        Ok(())
    }

    /// Create KvStore from file.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        let log_path = path.join("db.log");

        let mut compact_files = vec![];
        for entry in std::fs::read_dir(path.clone())? {
            let p = entry?.path();
            if p.is_file() {
                if let Some(file_name) = p.file_name().map(|s| s.to_string_lossy()) {
                    if file_name.starts_with("compact") {
                        compact_files.push(p);
                    }
                }
            }
        }
        compact_files.sort_by(|a, b| b.cmp(a));
        let compact_log_path = compact_files
            .first()
            .cloned()
            .unwrap_or(path.join("compact.log"));

        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(log_path.clone())?;

        let mut kv_store = KvStore::new(log_file, path);

        if compact_log_path.exists() {
            kv_store.scan_file(compact_log_path)?
        }

        kv_store.scan_file(log_path);

        Ok(kv_store)
    }

    fn compact(&mut self) -> Result<()> {
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

        for (key, _) in self.keydir.iter() {
            if let Some(val) = self.get(key.to_owned())? {
                let entry = Entry::Set {
                    key: key.clone(),
                    value: val.clone(),
                };

                writeln!(compact_file, "{}", serde_json::to_string(&entry)?)?;
            }
        }

        compact_file.sync_all()?;

        // clear key-dir and active file
        self.keydir.clear();
        self.active_file.seek(SeekFrom::Start(0));
        self.active_file.set_len(0);
        self.active_file.sync_all()?;

        // scan
        self.scan_file(compact_path)?;

        Ok(())
    }

    /// Scan file and refresh inner
    fn scan_file(&mut self, path: PathBuf) -> Result<()> {
        let mut bytes_len = 0;
        let reader = BufReader::new(OpenOptions::new().read(true).open(path.clone())?);
        for line in reader.lines() {
            let line_string = line?;
            let entry: Entry = serde_json::from_str(&line_string)?;
            match entry {
                Entry::Set { key, .. } => {
                    self.keydir.insert(
                        key,
                        LogPointer {
                            path: path.clone(),
                            offset: bytes_len,
                        },
                    );
                }
                Entry::Remove { key } => {
                    self.keydir.remove(&key);
                }
            }
            bytes_len += line_string.as_bytes().len() + 1;
        }

        Ok(())
    }
}
