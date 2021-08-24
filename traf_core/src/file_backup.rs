use super::interpreter::Command;
use crate::storage::Storage;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::hash::Hasher;
use std::io::prelude::*;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

// IDEA: Sharding:
//        - eg when the values file reaches a certain size: half it
//        - what if an update adds more than 2,3,.. size of max shard size?
//        - should it be alphabet? hash?

// IDEA: What is the good frequency/schedule to write backup
//        - maybe every 100 log?
//        - maybe some time based?

#[derive(Default)]
struct Changeset {
  updates: HashMap<String, Vec<u8>>,
  removals: HashSet<String>,
}

#[derive(Default)]
struct ChangesetCollection(HashMap<String, Changeset>);

#[derive(Serialize, Deserialize)]
struct BackupKeyInfo {
  content_size: usize,
  capacity: usize,
  pos: usize,
}

impl BackupKeyInfo {
  fn new(content_size: usize, capacity: usize, pos: usize) -> Self {
    Self {
      content_size,
      capacity,
      pos,
    }
  }

  fn value_range(&self) -> Range<usize> {
    self.pos..(self.pos + self.content_size)
  }
}

#[derive(Default, Serialize, Deserialize)]
struct BackupKeys(HashMap<String, BackupKeyInfo>);

#[derive(Serialize, Deserialize)]
struct ShardFileInfo {
  mod_base: u64,
  mod_value: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct ShardRegistry {
  files: HashMap<String, ShardFileInfo>,
}

impl ShardRegistry {
  fn filehash_for_key(&self, key: &String) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    let key_hash = hasher.finish();

    let (filehash, _) = self
      .files
      .iter()
      .find(|(_, fileinfo)| key_hash % fileinfo.mod_base == fileinfo.mod_value)
      .unwrap();

    filehash.clone()
  }
}

pub struct FileBackup {
  changesets: ChangesetCollection,
  dir: String,
  shard_registry: ShardRegistry,
}

impl FileBackup {
  pub fn new(dir: String) -> Self {
    let shard_registry = Self::fetch_shard_registry(&dir);
    Self {
      changesets: ChangesetCollection::default(),
      dir,
      shard_registry,
    }
  }

  pub fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Delete { key } => {
        let filehash = self.shard_registry.filehash_for_key(key);
        let changeset = self
          .changesets
          .0
          .entry(filehash)
          .or_insert(Changeset::default());

        changeset.removals.insert(key.clone());
        changeset
          .updates
          .remove(key.as_str())
          .expect("Cannot delete from changeset updates");
      }
      Command::Set { key, value } => {
        let filehash = self.shard_registry.filehash_for_key(key);
        let changeset = self
          .changesets
          .0
          .entry(filehash)
          .or_insert(Changeset::default());

        changeset.updates.insert(key.clone(), value.clone());
      }
      _ => (),
    };

    // FIXME: this is temporary, should be called moderately.
    self.backup();
  }

  pub fn restore(&self, storage: Arc<Mutex<Storage>>) {
    let shard_registry = Self::fetch_shard_registry(&self.dir);

    let mut storage = storage.lock().expect("Cannot gain lock to storage");

    for (filehash, _) in &shard_registry.files {
      let registered_backup_keys = self.fetch_backup_keys(filehash);
      let value_file_content: Vec<u8> = self.fetch_backup_values(filehash);

      for (key, info) in &registered_backup_keys.0 {
        let value_range = info.value_range();
        let value = &value_file_content[value_range];
        storage.set(key.clone(), value.to_vec());
      }
    }
  }

  fn backup(&mut self) {
    self.changesets.0.iter().for_each(|(filehash, changeset)| {
      let mut registered_backup_keys = self.fetch_backup_keys(filehash);
      let mut value_file_content: Vec<u8> = self.fetch_backup_values(filehash);

      // Remove all removals;
      for key_to_remove in &changeset.removals {
        registered_backup_keys
          .0
          .remove(key_to_remove)
          .expect("Cannot remove key");
      }

      // Update all updates.
      for (key, bytes) in &changeset.updates {
        // We already have that key/v.
        if registered_backup_keys.0.contains_key(key) {
          let mut elem = registered_backup_keys.0.get_mut(key).unwrap();
          // The change fits in the current slot.
          if elem.capacity >= bytes.len() {
            elem.content_size = bytes.len();
            let override_range = elem.pos..(elem.pos + bytes.len());
            value_file_content.splice(override_range, bytes.iter().cloned());
          // Change needs a bigger spot.
          } else {
            elem.content_size = bytes.len();
            elem.pos = value_file_content.len();
            value_file_content.append(&mut bytes.clone());
            let mut padding: Vec<u8> = vec![0; bytes.len()];
            value_file_content.append(&mut padding);
          }
        // We do not have this key/v.
        } else {
          let new_key_info =
            BackupKeyInfo::new(bytes.len(), bytes.len() << 1, value_file_content.len());
          registered_backup_keys.0.insert(key.clone(), new_key_info);

          value_file_content.append(&mut bytes.clone());
          let mut padding: Vec<u8> = vec![0; bytes.len()];
          value_file_content.append(&mut padding);
        }
      }

      self.save_backup_keys(filehash, registered_backup_keys);
      self.save_backup_values(filehash, value_file_content);
    });

    // Reset changelog.
    self.changesets = ChangesetCollection::default();
  }

  fn key_file_path(&self, filehash: &str) -> PathBuf {
    let mut filename = String::new();
    filename.push_str("__traf_keys_");
    filename.push_str(&filehash);
    filename.push_str(".db");

    Path::new(&self.dir).join(filename)
  }

  fn value_file_path(&self, filehash: &str) -> PathBuf {
    let mut filename = String::new();
    filename.push_str("__traf_values_");
    filename.push_str(&filehash);
    filename.push_str(".db");

    Path::new(&self.dir).join(filename)
  }

  fn shard_registry_file_path(dir: &str) -> PathBuf {
    Path::new(dir).join("__traf_shards.db")
  }

  // IDEA: File ops could be tokio::fs.

  fn fetch_shard_registry(dir: &str) -> ShardRegistry {
    let shard_registry_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(FileBackup::shard_registry_file_path(dir))
      .expect("Cannot open shard registry file for read");
    serde_json::from_reader(shard_registry_file).unwrap_or(ShardRegistry::default())
  }

  fn fetch_backup_keys(&self, filehash: &str) -> BackupKeys {
    let key_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(self.key_file_path(filehash))
      .expect("Cannot open key file for read");
    serde_json::from_reader(key_file).unwrap_or(BackupKeys::default())
  }

  fn fetch_backup_values(&self, filehash: &str) -> Vec<u8> {
    let mut value_file_content: Vec<u8> = vec![];

    let mut value_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(self.value_file_path(filehash))
      .expect("Cannot open value file for read");

    value_file
      .read_to_end(&mut value_file_content)
      .expect("Cannot read value file");

    value_file_content
  }

  fn save_backup_keys(&self, filehash: &str, keys: BackupKeys) {
    let mut key_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(false)
      .truncate(true)
      .open(self.key_file_path(filehash))
      .expect("Cannot open key file for write");
    let key_blob = serde_json::to_string(&keys).expect("Cannot serialize keys");
    key_file
      .write_all(key_blob.as_bytes())
      .expect("Cannot write keys");
  }

  fn save_backup_values(&self, filehash: &str, values: Vec<u8>) {
    let mut value_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(false)
      .truncate(true)
      .open(self.value_file_path(filehash))
      .expect("Cannot open key file for write");
    value_file
      .write_all(&values[..])
      .expect("Cannot write values");
  }
}
