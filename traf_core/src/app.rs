use super::file_backup::FileBackup;
use super::interpreter::*;
use super::storage::*;
use super::FrameAndChannel;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use traf_lib::response_frame::ResponseFrame;

// IDEA: Make storage permanent -> write to disk.

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
}

impl App {
  pub fn new(instance_type: InstanceType, rx: Receiver<FrameAndChannel>) -> Self {
    let storage = Arc::new(Mutex::new(Storage::new()));
    let backup = FileBackup::new("/tmp".into());

    backup.restore(storage.clone());

    App {
      interpreter: Interpreter::new(),
      storage: storage.clone(),
      rx,
      backup,
      instance_type,
    }
  }

  pub async fn listen(&mut self) {
    info!("app start listening");
    while let Some(frame) = self.rx.recv().await {
      info!("app channel got message");
      let res = self.execute(frame.frame.bytes);

      frame
        .channel
        .send(res.into())
        .expect("Failed sending response");
    }
  }

  // IDEA: More commands:
  // - inc int / dec int
  // - key defined?

  fn execute(&mut self, input: Vec<u8>) -> ResponseFrame {
    let cmd = self.interpreter.read(input);

    self.backup.log(&cmd);

    match cmd {
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
      Command::Invalid => ResponseFrame::ErrorInvalidCommand,
    }
  }

  fn is_read_only(&self) -> bool {
    match self.instance_type {
      InstanceType::Reader => true,
      InstanceType::Writer => false,
    }
  }
}
