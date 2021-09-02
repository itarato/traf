use crate::interpreter::Interpreter;
use std::convert::{TryFrom, TryInto};
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use traf_client::Client;

use tokio::spawn;

use crate::interpreter::Command;
use crate::storage::Storage;

type EventPtrT = u64;

#[derive(Debug)]
struct Reader {
  addr: String,
}

impl Reader {
  fn new(addr: String) -> Self {
    Self { addr }
  }
}

impl TryFrom<&str> for Reader {
  type Error = ();

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    // FIXME: add validation
    Ok(Reader::new(s.into()))
  }
}

#[derive(Debug)]
pub struct ReaderList(Vec<Reader>);

impl ReaderList {
  fn new(readers: Vec<Reader>) -> Self {
    Self(readers)
  }
}

impl TryFrom<&str> for ReaderList {
  type Error = ();

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    let reader_raw_list: Vec<&str> = s.split(",").collect();

    let mut readers: Vec<Reader> = vec![];
    for reader_raw in reader_raw_list {
      readers.push(Reader::try_from(reader_raw)?);
    }

    Ok(ReaderList::new(readers))
  }
}

struct SyncChunkList(Vec<Command>);

impl TryFrom<Vec<u8>> for SyncChunkList {
  type Error = ();

  fn try_from(mut bytes: Vec<u8>) -> Result<Self, Self::Error> {
    let mut commands: Vec<Command> = vec![];

    loop {
      if bytes.len() == 0 {
        break;
      }

      // Missing size bytes.
      if bytes.len() < 8 {
        return Err(());
      }

      let size_marker: Vec<u8> = bytes.drain(..8).collect();
      let chunk_size: u64 = match size_marker.try_into() {
        Ok(size_bytes) => u64::from_be_bytes(size_bytes),
        Err(_) => return Err(()),
      };

      if bytes.len() < chunk_size as usize {
        return Err(());
      }

      let command_bytes: Vec<u8> = bytes.drain(..chunk_size as usize).collect();
      let command = Interpreter::new().read(command_bytes);

      commands.push(command);
    }

    Ok(SyncChunkList(commands))
  }
}

pub struct Replicator {
  dir: String,
  readers: ReaderList,
}

// IDEA: the sync to readers probably better do batches to avoid always being networked.

impl Replicator {
  pub fn new(dir: String, readers: ReaderList) -> Self {
    Self { dir, readers }
  }

  pub async fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Set { .. } | Command::Delete { .. } => {
        let bytes = cmd.as_bytes().unwrap();
        let pos = self.event_log_file_size().unwrap_or(0);

        self.append_event_log(bytes);
        self.append_event_log_pointers(pos);

        if self.should_sync() {
          self.sync().await;
        }
      }
      _ => (),
    };
  }

  async fn sync(&self) {
    /*
      collect all last ids from all readers
      send a big chunk payload to readers

      Missing:
      - command to accept sync
      - arglist to accept reader
    */

    for reader in &self.readers.0 {
      let addr = reader.addr.clone();
      spawn(async move {
        // FIXME: error handling
        let mut client = Client::connect(addr)
          .await
          .expect("Failed connecting to reader");

        client
          .last_replication_id()
          .await
          .and_then(|last_replication_id_result| {
            let replication_id_start = last_replication_id_result.map(|id| id + 1).unwrap_or(0);

            // #1 Fetch range [replication_id_start..] from event log
            // #2 Sent it to client.sync
            unimplemented!();

            Ok(())
          });
      });
    }
  }

  pub fn restore(&self, storage: Arc<Mutex<Storage>>, dump: Vec<u8>) {
    unimplemented!();
  }

  fn should_sync(&self) -> bool {
    // IDEA: figure out some reasonable frequency/rule for replication.
    true
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
      .write(&bytes.len().to_be_bytes())
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
      .write(&pos.to_be_bytes())
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
