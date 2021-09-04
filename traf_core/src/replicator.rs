use crate::interpreter::Command;
use crate::interpreter::Interpreter;
use crate::storage::Storage;
use crate::Executor;
use std::convert::{TryFrom, TryInto};
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::spawn;
use traf_client::{Client};
use traf_lib::response_frame::ResponseFrame;

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
    if s.len() == 0 {
      Err(())
    } else {
      // FIXME: add validation
      Ok(Reader::new(s.into()))
    }
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
      match Reader::try_from(reader_raw) {
        Ok(reader) => readers.push(reader),
        _ => (),
      };
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

        // FIXME: This 2 should be atomic
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

    let event_log_pointers: Arc<Vec<EventPtrT>> = Arc::new(self.fetch_event_log_pointers());
    let event_logs: Arc<Vec<u8>> = Arc::new(self.fetch_event_logs());

    for reader in &self.readers.0 {
      let addr = reader.addr.clone();

      let event_log_pointers = event_log_pointers.clone();
      let event_logs = event_logs.clone();

      spawn(async move {
        // FIXME: error handling
        let mut client = Client::connect(addr.clone())
          .await
          .expect("Failed connecting to reader");

        match client.last_replication_id().await {
          Ok(last_replication_id_result) => {
            let replication_id_start = last_replication_id_result.map(|id| id + 1).unwrap_or(0);
            info!("Writer init sync with reader from ID: {}", replication_id_start);

            // !!! BUG !!!
            // thread 'tokio-runtime-worker' panicked at 'index out of bounds: the len is 101 but the index is 725',
            // traf_core/src/replicator.rs:163:31 stack backtrace:
            let range_start = event_log_pointers[replication_id_start as usize];
            let sync_payload = Vec::from(&event_logs[range_start as usize..]);

            client
              .batch_sync(sync_payload)
              .await
              .unwrap_or_else(|err| warn!("Failed syncing {:?} due to {:?}", addr, err));
          }
          Err(err) => {
            warn!(
              "Failed reading last replication id of {:?} due to {:?}",
              addr, err
            );
          }
        };
      });
    }
  }

  // Returns the number or replica changes.
  pub fn restore(&self, storage: Arc<Mutex<Storage>>, dump: Vec<u8>) -> EventPtrT {
    match SyncChunkList::try_from(dump) {
      Ok(list) => {
        let changes_count = list.0.len() as EventPtrT;

        list.0.into_iter().for_each(|cmd| {
          match storage
            .lock()
            .expect("Failed gaining storage lock")
            .execute(&cmd) {
              ResponseFrame::ErrorInvalidCommand => panic!("Failed executing batch sync cmd"),
              _ => (),
            };
        });

        changes_count
      }
      Err(_) => {
        error!("Failed decoding commands from chunk bytes");
        0
      }
    }
  }

  fn should_sync(&self) -> bool {
    // IDEA: figure out some reasonable frequency/rule for replication.
    true
  }

  // FIXME: There might be code parts where EventPtrT serialization is hardcoded to 8s of bytes.
  //        We should use `size_of` everywhere.
  fn fetch_event_log_pointers(&self) -> Vec<EventPtrT> {
    let mut event_log_pointers_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(self.event_log_pointers_file_path())
      .expect("Cannot open event log pointer file for read");

    let mut buf: Vec<u8> = vec![];
    event_log_pointers_file
      .read_to_end(&mut buf)
      .expect("Failed reading event log pointer file");

    let ptr_size: usize = size_of::<EventPtrT>();

    buf[..]
      .chunks(ptr_size)
      .map(|chunk| {
        if chunk.len() < ptr_size {
          panic!("Invalid bytes");
        }

        EventPtrT::from_be_bytes(chunk.try_into().expect("Cannot create fixed array"))
      })
      .collect()
  }

  // FIXME: This is grossly inefficient to load a designed-to-be-huge file. Rather seek and fetch the fragment.
  fn fetch_event_logs(&self) -> Vec<u8> {
    let mut event_log_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(self.event_log_file_path())
      .expect("Cannot open event log file for read");

    let mut buf: Vec<u8> = vec![];
    event_log_file
      .read_to_end(&mut buf)
      .expect("Failed reading event log pointer file");

    buf
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
