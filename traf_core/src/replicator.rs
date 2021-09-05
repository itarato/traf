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
use traf_client::Client;
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

struct SyncChunk {
  command: Command,
  number: EventPtrT,
}

impl SyncChunk {
  fn new(command: Command, number: EventPtrT) -> Self {
    SyncChunk { command, number }
  }
}

struct SyncChunkList(Vec<SyncChunk>);

impl TryFrom<Vec<u8>> for SyncChunkList {
  type Error = ();

  fn try_from(mut bytes: Vec<u8>) -> Result<Self, Self::Error> {
    let mut chunks: Vec<SyncChunk> = vec![];
    let ptr_size = size_of::<EventPtrT>();

    loop {
      if bytes.len() == 0 {
        break;
      }

      // Missing size bytes.
      if bytes.len() < ptr_size {
        return Err(());
      }

      let size_marker: Vec<u8> = bytes.drain(..ptr_size).collect();
      let chunk_size: EventPtrT = match size_marker.try_into() {
        Ok(size_bytes) => EventPtrT::from_be_bytes(size_bytes),
        Err(_) => return Err(()),
      };

      if bytes.len() < ptr_size {
        return Err(());
      }

      let chunk_number_marker: Vec<u8> = bytes.drain(..ptr_size).collect();
      let chunk_number: EventPtrT = match chunk_number_marker.try_into() {
        Ok(chunk_number_bytes) => EventPtrT::from_be_bytes(chunk_number_bytes),
        Err(_) => return Err(()),
      };

      if bytes.len() < chunk_size as usize {
        return Err(());
      }

      let command_bytes: Vec<u8> = bytes.drain(..chunk_size as usize).collect();
      let command = Interpreter::new().read(command_bytes);

      chunks.push(SyncChunk::new(command, chunk_number));
    }

    Ok(SyncChunkList(chunks))
  }
}

pub struct Replicator {
  dir: String,
  readers: ReaderList,
  event_log_mutex: Mutex<()>,
}

// IDEA: the sync to readers probably better do batches to avoid always being networked.

impl Replicator {
  pub fn new(dir: String, readers: ReaderList) -> Self {
    Self {
      dir,
      readers,
      event_log_mutex: Mutex::new(()),
    }
  }

  pub async fn log(&mut self, cmd: &Command) {
    match cmd {
      Command::Set { .. } | Command::Delete { .. } => {
        let bytes = cmd.as_bytes().unwrap();

        {
          let _event_mutex = self
            .event_log_mutex
            .lock()
            .expect("Failed locking event ops");

          let next_event_number = self.next_event_log_number();
          let pos = self.event_log_file_size().unwrap_or(0);

          // FIXME: This 2 should be atomic
          self.append_event_log(bytes, next_event_number);
          self.append_event_log_pointers(pos);
        }

        if self.should_sync() {
          self.sync().await;
        }
      }
      _ => (),
    };
  }

  // IDEA: There should be a sync at the beginning too -> maybe not? (Should a start state reader/writer)
  //        be different?
  async fn sync(&self) {
    /*
      collect all last ids from all readers
      send a big chunk payload to readers

      Missing:
      - command to accept sync
      - arglist to accept reader
    */
    let mut join_handles: Vec<_> = vec![];

    for reader in &self.readers.0 {
      let addr = reader.addr.clone();
      let event_log_pointers_file_path = self.event_log_pointers_file_path();
      let event_log_file_path = self.event_log_file_path();

      let join_handle = spawn(async move {
        // FIXME: error handling
        let mut client = Client::connect(addr.clone())
          .await
          .expect("Failed connecting to reader");

        match client.last_replication_id().await {
          Ok(last_replication_id_result) => {
            let replication_id_start = last_replication_id_result.map(|id| id + 1).unwrap_or(0);
            info!(
              "Writer init sync with reader from ID: {}",
              replication_id_start
            );

            // FIXME: This is horribly inefficient to load these always. The problem is that
            //        if this happens once, and we ask for the reader's latest event id after getting it,
            //        the reader might already got a newer update which would result a last-id
            //        greater than our event registry.
            //        At least we should only load partial file data, if that helps.
            let event_log_pointers: Vec<EventPtrT> =
              Self::fetch_event_log_pointers(event_log_pointers_file_path);
            let event_logs: Vec<u8> = Self::fetch_event_logs(event_log_file_path);

            // !!! BUG !!!
            // thread 'tokio-runtime-worker' panicked at 'index out of bounds: the len is 101 but the index is 725',
            // traf_core/src/replicator.rs:163:31 stack backtrace:
            // Foundings:
            //  - Seems that due to event files being populated async a sync can send over more than the\
            //    intended batch -> this results some changes arriving twice and counted twice
            //    which bumps the last_replication_id uncontrollably
            //    Proposed solution:
            //    Make sure a sync is always clear on the range it syncs
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
      join_handles.push(join_handle);
    }

    for join_handle in join_handles {
      join_handle.await.expect("Failed closing thread");
    }
  }

  // Returns the last applied event ID.
  // FIXME: Pass the current app latest event ID and only apply the missing ones.
  pub fn restore(
    &self,
    storage: Arc<Mutex<Storage>>,
    dump: Vec<u8>,
    current_committed_event_id: Option<EventPtrT>,
  ) -> Option<EventPtrT> {
    match SyncChunkList::try_from(dump) {
      Ok(list) => {
        let mut last_successful_event_id: Option<EventPtrT> = None;

        list.0.into_iter().for_each(|chunk| {
          // If the reader instance already have this event, skip it.
          if current_committed_event_id.is_some()
            && current_committed_event_id.unwrap() >= chunk.number
          {
            return;
          }

          match storage
            .lock()
            .expect("Failed gaining storage lock")
            .execute(&chunk.command)
          {
            ResponseFrame::ErrorInvalidCommand => panic!("Failed executing batch sync cmd"),
            _ => last_successful_event_id = Some(chunk.number),
          };
        });

        last_successful_event_id
      }
      Err(_) => {
        error!("Failed decoding commands from chunk bytes");
        None
      }
    }
  }

  fn should_sync(&self) -> bool {
    // IDEA: figure out some reasonable frequency/rule for replication.
    true
  }

  // FIXME: There might be code parts where EventPtrT serialization is hardcoded to 8s of bytes.
  //        We should use `size_of` everywhere.
  fn fetch_event_log_pointers(event_log_pointers_file_path: PathBuf) -> Vec<EventPtrT> {
    let mut event_log_pointers_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(event_log_pointers_file_path)
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
  fn fetch_event_logs(event_log_file_path: PathBuf) -> Vec<u8> {
    let mut event_log_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(event_log_file_path)
      .expect("Cannot open event log file for read");

    let mut buf: Vec<u8> = vec![];
    event_log_file
      .read_to_end(&mut buf)
      .expect("Failed reading event log pointer file");

    buf
  }

  fn append_event_log(&self, bytes: Vec<u8>, count_number: EventPtrT) {
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
      .write(&count_number.to_be_bytes())
      .expect("Cannot write count number");
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

  fn event_log_file_size(&self) -> Option<u64> {
    fs::metadata(self.event_log_file_path())
      .map(|metadata| Some(metadata.len()))
      .unwrap_or(None)
  }

  fn next_event_log_number(&self) -> EventPtrT {
    fs::metadata(self.event_log_pointers_file_path())
      .map(|metadata| metadata.len() / size_of::<EventPtrT>() as EventPtrT)
      .unwrap_or(0)
  }
}
