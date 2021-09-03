use traf_lib::response_frame::ResponseFrame;

use crate::{interpreter::Command, Executor};
use std::collections::HashMap;

type KeyT = String;
type ValueT = Vec<u8>;

pub struct Storage {
  data: HashMap<KeyT, ValueT>,
  read_only: bool,
}

impl Storage {
  pub fn new(read_only: bool) -> Self {
    Storage {
      data: Default::default(),
      read_only,
    }
  }

  pub fn set(&mut self, key: KeyT, value: ValueT) {
    self.data.insert(key, value);
  }

  pub fn get(&self, key: KeyT) -> Option<&ValueT> {
    self.data.get(&key)
  }

  pub fn delete(&mut self, key: KeyT) -> bool {
    self.data.remove(&key).is_some()
  }
}

impl Executor for Storage {
  fn execute(&mut self, command: &Command) -> ResponseFrame {
    match command.clone() {
      Command::Set { key, value } => {
        if self.read_only {
          ResponseFrame::ErrorInvalidCommand
        } else {
          info!("SET {:?} {:?}", key, value);
          self.set(key, value);
          ResponseFrame::Success
        }
      }
      Command::Get { key } => {
        info!("GET {:?}", key);
        match self.get(key) {
          Some(v) => ResponseFrame::Value(v.clone()),
          None => ResponseFrame::ValueMissing,
        }
      }
      Command::Delete { key } => {
        if self.read_only {
          ResponseFrame::ErrorInvalidCommand
        } else {
          info!("DELETE {:?}", key);
          if self.delete(key) {
            ResponseFrame::Success
          } else {
            ResponseFrame::ValueMissing
          }
        }
      }
      _ => ResponseFrame::ErrorInvalidCommand,
    }
  }
}
