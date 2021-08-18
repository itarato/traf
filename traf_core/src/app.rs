use super::interpreter::*;
use super::storage::*;
use super::FrameAndChannel;

use tokio::sync::mpsc::Receiver;

// IDEA: Make storage permanent -> write to disk.

enum ResponseFrame {
  Success,
  ErrorInvalidCommand,
  Value(Vec<u8>),
  ValueMissing,
}

impl Into<Vec<u8>> for ResponseFrame {
  fn into(self) -> Vec<u8> {
    match self {
      ResponseFrame::Success => vec![0],
      ResponseFrame::ErrorInvalidCommand => vec![1],
      ResponseFrame::Value(v) => {
        let mut v = v.clone();
        v.insert(0, 2);
        v
      }
      ResponseFrame::ValueMissing => vec![3],
    }
  }
}

pub struct App {
  interpreter: Interpreter,
  storage: Storage,
  rx: Receiver<FrameAndChannel>,
}

impl App {
  pub fn new(rx: Receiver<FrameAndChannel>) -> Self {
    App {
      interpreter: Interpreter::new(),
      storage: Storage::new(),
      rx,
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

  // IDEA: Type wrapping: client lib to to be able to push typed variables and same lib used unpacking them
  //       (probably only on the client side)

  fn execute(&mut self, input: Vec<u8>) -> ResponseFrame {
    let cmd = self.interpreter.read(input);
    match cmd {
      Command::Set { key, value } => {
        info!("SET {:?} {:?}", key, value);
        self.storage.set(key, value);
        ResponseFrame::Success
      }
      Command::Get { key } => {
        info!("GET {:?}", key);
        match self.storage.get(key) {
          Some(v) => ResponseFrame::Value(v.clone()),
          None => ResponseFrame::ValueMissing,
        }
      }
      Command::Delete { key } => {
        info!("DELETE {:?}", key);
        if self.storage.delete(key) {
          ResponseFrame::Success
        } else {
          ResponseFrame::ValueMissing
        }
      }
      Command::Invalid => ResponseFrame::ErrorInvalidCommand,
    }
  }
}
