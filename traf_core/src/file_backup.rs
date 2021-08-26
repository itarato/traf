use crate::interpreter::Command;
use crate::storage::Storage;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
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

// IDEA: The current concept splits a shard on a size limit. This is a problem when a single item
//        can allocate a lot of space -> may result in a forever split.

fn generate_random_name() -> String {
  let mut rng = rand::thread_rng();
  let mut bytes: Vec<u8> = vec![];
  for _ in 0..16 {
    let byte: u8 = rng.gen_range(b'a'..b'z');
    bytes.push(byte);
  }
  String::from_utf8(bytes).expect("Failed random string generation")
}

fn hash_for_key(key: &String) -> u64 {
  let mut hasher = DefaultHasher::new();
  hasher.write(key.as_bytes());
  hasher.finish()
}

fn append_bytes_with_same_size_padding(v: &mut Vec<u8>, bytes: &[u8]) {
  let mut padding: Vec<u8> = vec![0; bytes.len()];
  let mut bytes = Vec::from(bytes);
  v.append(&mut bytes);
  v.append(&mut padding);
}

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

impl BackupKeys {
  fn size(&self) -> usize {
    self
      .0
      .values()
      .max_by(|lhs, rhs| lhs.pos.cmp(&rhs.pos))
      .map(|last_key_info| last_key_info.pos + last_key_info.capacity)
      .unwrap_or(0usize)
  }
}

#[derive(Serialize, Deserialize, Debug)]
struct ShardFileInfo {
  mod_base: u64,
  mod_value: u64,
}

impl ShardFileInfo {
  fn new(mod_base: u64, mod_value: u64) -> Self {
    Self {
      mod_base,
      mod_value,
    }
  }
}

#[derive(Serialize, Deserialize)]
struct ShardRegistry {
  files: HashMap<String, ShardFileInfo>,
  shard_break_limit: usize,
}

impl ShardRegistry {
  fn new(shard_break_limit: usize) -> Self {
    let mut files = HashMap::new();
    let fileinfo = ShardFileInfo::new(1, 0);
    files.insert(generate_random_name(), fileinfo);

    ShardRegistry {
      files,
      shard_break_limit,
    }
  }

  fn filehash_for_key(&self, key: &String) -> String {
    let key_hash = hash_for_key(key);

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

    let instance = Self {
      changesets: ChangesetCollection::default(),
      dir,
      shard_registry,
    };

    // IDEA: have a dirty indicator, so only save when needed.
    instance.save_shard_registry();

    instance
  }

  // FIXME: probably we need a module level lock - for log/restore/backup/shard

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
    self.shard();
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
            append_bytes_with_same_size_padding(&mut value_file_content, &bytes[..]);
          }
        // We do not have this key/v.
        } else {
          let new_key_info =
            BackupKeyInfo::new(bytes.len(), bytes.len() << 1, value_file_content.len());
          registered_backup_keys.0.insert(key.clone(), new_key_info);
          append_bytes_with_same_size_padding(&mut value_file_content, &bytes[..]);
        }
      }

      self.save_backup_keys(filehash, registered_backup_keys);
      self.save_backup_values(filehash, value_file_content);
    });

    self.save_shard_registry();

    // Reset changelog.
    self.changesets = ChangesetCollection::default();
  }

  fn shard(&mut self) {
    let mut change_required: Vec<String> = vec![];

    self.shard_registry.files.keys().for_each(|filehash| {
      let key_db = self.fetch_backup_keys(filehash);
      if key_db.size() >= self.shard_registry.shard_break_limit {
        change_required.push(filehash.clone());
      }
    });

    for filehash_to_split in &change_required {
      let old_file_info = self
        .shard_registry
        .files
        .remove(filehash_to_split)
        .expect("Key not found");

      let new_filehash_lhs = generate_random_name();
      let new_filehash_rhs = generate_random_name();

      let new_mod = old_file_info.mod_base * 2;
      let new_mod_value_lhs = old_file_info.mod_value;
      let new_mod_value_rhs = old_file_info.mod_value + old_file_info.mod_base;

      let new_file_info_lhs = ShardFileInfo::new(new_mod, new_mod_value_lhs);
      let new_file_info_rhs = ShardFileInfo::new(new_mod, new_mod_value_rhs);

      self
        .shard_registry
        .files
        .insert(new_filehash_lhs.clone(), new_file_info_lhs);
      self
        .shard_registry
        .files
        .insert(new_filehash_rhs.clone(), new_file_info_rhs);

      let old_value = self.fetch_backup_values(filehash_to_split);
      let old_keys = self.fetch_backup_keys(filehash_to_split);

      let mut new_content_lhs: Vec<u8> = vec![];
      let mut new_content_rhs: Vec<u8> = vec![];

      let mut new_keys_lhs = BackupKeys::default();
      let mut new_keys_rhs = BackupKeys::default();

      // IDEA: inserting newly values and right after the padding is very weak pattern - easy to miss. Fix this.

      // This block:
      //  - takes all values of the current shard (that will be split)
      //  - and finds which new shard it belongs to (via mod breaks)
      //  - moves the value
      //    - values file
      //    - keys file
      for (key, value_info) in old_keys.0 {
        let key_hash = hash_for_key(&key);
        let old_value_part = &old_value[value_info.value_range()];

        if key_hash % new_mod == new_mod_value_lhs {
          let new_key_info = BackupKeyInfo::new(
            value_info.content_size,
            value_info.capacity,
            new_content_lhs.len(),
          );
          append_bytes_with_same_size_padding(&mut new_content_lhs, old_value_part);

          new_keys_lhs.0.insert(key.clone(), new_key_info);
        } else if key_hash % new_mod == new_mod_value_rhs {
          let new_key_info = BackupKeyInfo::new(
            value_info.content_size,
            value_info.capacity,
            new_content_rhs.len(),
          );
          append_bytes_with_same_size_padding(&mut new_content_rhs, old_value_part);

          new_keys_rhs.0.insert(key.clone(), new_key_info);
        } else {
          println!(
            "key: {:?}\nkeyhash: {:?}\nold file info: {:#?}\nnew mods: {}/{}/{}",
            &key, key_hash, &old_file_info, new_mod, new_mod_value_lhs, new_mod_value_rhs
          );
          panic!("Error during shard split distribution");
        }
      }

      self.save_backup_keys(&new_filehash_lhs, new_keys_lhs);
      self.save_backup_keys(&new_filehash_rhs, new_keys_rhs);

      self.save_backup_values(&new_filehash_lhs, new_content_lhs);
      self.save_backup_values(&new_filehash_rhs, new_content_rhs);

      self.delete_backup_keys(&filehash_to_split);
      self.delete_backup_values(&filehash_to_split);
    }

    self.save_shard_registry();
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

    // FIXME: do not hardcode shard break limit.
    serde_json::from_reader(shard_registry_file).unwrap_or(ShardRegistry::new(1 << 5))
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
      .create(true)
      .truncate(true)
      .open(self.key_file_path(filehash))
      .expect("Cannot open key file for write");
    let key_blob = serde_json::to_string(&keys).expect("Cannot serialize keys");
    key_file
      .write_all(key_blob.as_bytes())
      .expect("Cannot write keys");
  }

  fn delete_backup_keys(&self, filehash: &str) {
    fs::remove_file(self.key_file_path(filehash)).expect("Cannot delete keys file");
  }

  fn save_backup_values(&self, filehash: &str, values: Vec<u8>) {
    let mut value_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(true)
      .truncate(true)
      .open(self.value_file_path(filehash))
      .expect("Cannot open key file for write");
    value_file
      .write_all(&values[..])
      .expect("Cannot write values");
  }

  fn delete_backup_values(&self, filehash: &str) {
    fs::remove_file(self.value_file_path(filehash)).expect("Cannot delete value file");
  }

  fn save_shard_registry(&self) {
    let mut shard_registry_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(true)
      .truncate(true)
      .open(Self::shard_registry_file_path(&self.dir))
      .expect("Cannot open shard registry file for write");
    let blob =
      serde_json::to_string(&self.shard_registry).expect("Cannot serialize shard registry");
    shard_registry_file
      .write_all(blob.as_bytes())
      .expect("Cannot write shard registry");
  }
}
