use crate::file_backup::FileBackup;
use crate::replicator::{ReaderList, Replicator};
use crate::storage::*;
use crate::FrameAndChannel;
use crate::{interpreter::*, Executor};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use traf_lib::response_frame::ResponseFrame;

#[derive(PartialEq)]
pub enum InstanceType {
  Reader,
  Writer,
}

pub struct App {
  interpreter: Interpreter,
  storage: Arc<Mutex<Storage>>,
  rx: Receiver<FrameAndChannel>,
  backup: FileBackup,
  instance_type: InstanceType,
  replicator: Replicator,
  last_replica_id: Option<u64>,
}

// IDEA: Something smells with the App being either writer or reader and some behaviour divides on this.
//        Almost like it should be 2 types. Somehow this should be way safer.

impl App {
  pub fn new(
    instance_type: InstanceType,
    last_replica_id: Option<u64>,
    readers: ReaderList,
    rx: Receiver<FrameAndChannel>,
  ) -> Self {
    let storage = Arc::new(Mutex::new(Storage::new()));
    let backup = FileBackup::new("/tmp".into());

    backup.restore(storage.clone());

    App {
      interpreter: Interpreter::new(),
      storage: storage.clone(),
      rx,
      backup,
      instance_type,
      replicator: Replicator::new("/tmp".into(), readers),
      last_replica_id,
    }
  }

  pub async fn listen(&mut self) {
    info!("app start listening");
    while let Some(frame) = self.rx.recv().await {
      info!("app channel got message");
      let res = self.execute(frame.frame.bytes).await;

      frame
        .channel
        .send(res.into())
        .expect("Failed sending response");
    }
  }

  // IDEA: More commands:
  // - inc int / dec int
  // - key defined?

  async fn execute(&mut self, input: Vec<u8>) -> ResponseFrame {
    let ref cmd = self.interpreter.read(input);

    // FIXME: cloning a SET command with value can be expensive. Try to avoid it.

    // IDEA: The Executor trait (used by Storage) doesn't seem too strong as not all commands
    //        are owned by a single struct (like the way Storage does). Can we do better?

    let result = match cmd {
      Command::Set { .. } | Command::Delete { .. } => match self.instance_type {
        InstanceType::Reader => ResponseFrame::ErrorInvalidCommand,
        InstanceType::Writer => self.storage.lock().unwrap().execute(cmd),
      },
      Command::Get { .. } => self.storage.lock().unwrap().execute(cmd),
      Command::GetLastReplicationId => match self.instance_type {
        // IDEA: For a reader not having a last replication id is valid - it might be the beginning.
        //        Though it's also a weakness as we cannot really tell if that's legitimate or not.
        InstanceType::Reader => match self.last_replica_id {
          Some(id) => ResponseFrame::Value(Vec::from(id.to_be_bytes())),
          None => ResponseFrame::ValueMissing,
        },
        InstanceType::Writer => ResponseFrame::ErrorInvalidCommand,
      },
      Command::Sync { dump } => {
        let changes_count = self.replicator.restore(self.storage.clone(), dump.clone());

        info!("Reader replica ID before: {:?} + changes: {}", self.last_replica_id, changes_count);

        self.last_replica_id = self
          .last_replica_id
          .map(|count| count + changes_count)
          .or_else(|| {
            if changes_count == 0 {
              None
            } else {
              Some(changes_count - 1)
            }
          });

        // FIXME: Do a proper result
        ResponseFrame::Success
      }
      Command::Invalid => ResponseFrame::ErrorInvalidCommand,
    };

    match &result {
      // Mutating operations have a result (for now) of ::Success - which is the only case
      // when we need replica/backup tracking.
      &ResponseFrame::Success => {
        self.backup.log(&cmd);

        if !self.is_read_only() {
          self.replicator.log(&cmd).await;
        }
      }
      _ => (),
    };

    result
  }

  fn is_read_only(&self) -> bool {
    match self.instance_type {
      InstanceType::Reader => true,
      InstanceType::Writer => false,
    }
  }
}
