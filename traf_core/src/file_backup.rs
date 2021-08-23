use super::interpreter::Command;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Default)]
struct Changeset {
  updates: HashMap<String, Vec<u8>>,
  removals: HashSet<String>,
}

impl Changeset {
  fn new() -> Self {
    Default::default()
  }
}

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
}

#[derive(Default, Serialize, Deserialize)]
struct BackupKeys(HashMap<String, BackupKeyInfo>);

pub struct FileBackup {
  changeset: Changeset,
  dir: String,
}

impl FileBackup {
  pub fn new(dir: String) -> Self {
    Self {
      changeset: Changeset::new(),
      dir,
    }
  }

  pub fn start(&self) {}

  pub fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Delete { key } => {
        self.changeset.removals.insert(key.clone());
        self
          .changeset
          .updates
          .remove(key.as_str())
          .expect("Cannot delete from changeset updates");
      }
      Command::Set { key, value } => {
        self.changeset.updates.insert(key.clone(), value.clone());
      }
      _ => (),
    };

    self.backup();
  }

  pub fn backup(&mut self) {
    let mut registered_backup_keys: BackupKeys;
    {
      let key_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(self.key_file_path())
        .expect("Cannot open key file for read");
      registered_backup_keys = serde_json::from_reader(key_file).unwrap_or(BackupKeys::default());
    }

    let mut value_file_content: Vec<u8> = vec![];
    {
      let mut value_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(self.value_file_path())
        .expect("Cannot open value file for read");

      value_file
        .read_to_end(&mut value_file_content)
        .expect("Cannot read value file");
    }

    // Remove all removals;
    for key_to_remove in &self.changeset.removals {
      registered_backup_keys
        .0
        .remove(key_to_remove)
        .expect("Cannot remove key");
    }

    // Update all updates.
    for (key, bytes) in &self.changeset.updates {
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

    // Save keys.
    let mut key_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(false)
      .truncate(true)
      .open(self.key_file_path())
      .expect("Cannot open key file for write");
    let key_blob = serde_json::to_string(&registered_backup_keys).expect("Cannot serialize keys");
    key_file
      .write_all(key_blob.as_bytes())
      .expect("Cannot write keys");

    // Save values.
    let mut value_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(false)
      .truncate(true)
      .open(self.value_file_path())
      .expect("Cannot open key file for write");
    value_file
      .write_all(&value_file_content[..])
      .expect("Cannot write values");

    self.changeset = Changeset::new();
  }

  fn key_file_path(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_keys.db")
  }

  fn value_file_path(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_values.db")
  }
}
