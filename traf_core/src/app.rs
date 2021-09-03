use crate::file_backup::FileBackup;
use crate::interpreter::*;
use crate::replicator::{ReaderList, Replicator};
use crate::storage::*;
use crate::FrameAndChannel;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use traf_lib::response_frame::ResponseFrame;

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
    let cmd = self.interpreter.read(input);

    // FIXME: cloning a SET command with value can be expensive. Try to avoid it.

    let result = match cmd.clone() {
      Command::Set { key, value } => {
        if self.is_read_only() {
          ResponseFrame::ErrorInvalidCommand
        } else {
          info!("SET {:?} {:?}", key, value);
          self.storage.lock().unwrap().set(key, value);
          ResponseFrame::Success
        }
      }
      Command::Get { key } => {
        info!("GET {:?}", key);
        match self.storage.lock().unwrap().get(key) {
          Some(v) => ResponseFrame::Value(v.clone()),
          None => ResponseFrame::ValueMissing,
        }
      }
      Command::Delete { key } => {
        if self.is_read_only() {
          ResponseFrame::ErrorInvalidCommand
        } else {
          info!("DELETE {:?}", key);
          if self.storage.lock().unwrap().delete(key) {
            ResponseFrame::Success
          } else {
            ResponseFrame::ValueMissing
          }
        }
      }
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
        self.replicator.restore(self.storage.clone(), dump);
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
