use std::fs;
use std::path::{Path, PathBuf};

use crate::interpreter::Command;

// struct EventLogPointer {
//   id: u64,
//   pos: usize,
// }

// struct EventLogPointerList(Vec<EventLogPointer>);

pub struct Replicator {
  dir: String,
}

impl Replicator {
  fn new(dir: String) -> Self {
    Self { dir }
  }

  fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Set { .. } => {
        let bytes = cmd.as_bytes();
        let pos = self.event_log_file_size().unwrap_or(0);
      }
      Command::Delete { .. } => unimplemented!(),
      _ => (),
    };
  }

  fn append_event_log(&self, bytes: Vec<u8>) {}

  fn event_log_pointers_file_name(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_replicator_event_log_pointers.db")
  }

  fn event_log_file_name(&self) -> PathBuf {
    Path::new(&self.dir).join("__traf_replicator_event_log.db")
  }

  fn event_log_file_size(&self) -> Option<u64> {
    fs::metadata(self.event_log_file_name())
      .map(|metadata| Some(metadata.len()))
      .unwrap_or(None)
  }
}
