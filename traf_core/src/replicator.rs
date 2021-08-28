use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use crate::interpreter::Command;

type EventPtrT = u64;

pub struct Replicator {
  dir: String,
}

// IDEA: the sync to readers probably better do batches to avoid always being networked.

impl Replicator {
  pub fn new(dir: String) -> Self {
    Self { dir }
  }

  pub fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Set { .. } | Command::Delete { .. } => {
        let bytes = cmd.as_bytes().unwrap();
        let pos = self.event_log_file_size().unwrap_or(0);

        self.append_event_log(bytes);
        self.append_event_log_pointers(pos);
      }
      _ => (),
    };
  }

  fn append_event_log(&self, bytes: Vec<u8>) {
    let mut event_log_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(true)
      .truncate(false)
      .append(true)
      .open(self.event_log_file_path())
      .expect("Cannot open event log file for write");

    event_log_file
      .write(&bytes.len().to_ne_bytes())
      .expect("Cannot write event log size");
    event_log_file
      .write_all(&bytes[..])
      .expect("Cannot write event log");
  }

  fn append_event_log_pointers(&self, pos: EventPtrT) {
    let mut event_log_pointers_file = OpenOptions::new()
      .read(false)
      .write(true)
      .create(true)
      .truncate(false)
      .append(true)
      .open(self.event_log_pointers_file_path())
      .expect("Cannot open event log file for write");

    event_log_pointers_file
      .write(&pos.to_ne_bytes())
      .expect("Cannot write event log pointers");
  }

  fn event_log_pointers_file_path(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_replicator_event_log_pointers.db")
  }

  fn event_log_file_path(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_replicator_event_log.db")
  }

  fn event_log_file_size(&self) -> Option<EventPtrT> {
    fs::metadata(self.event_log_file_path())
      .map(|metadata| Some(metadata.len()))
      .unwrap_or(None)
  }
}
