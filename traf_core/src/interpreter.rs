pub struct Interpreter {}

impl Interpreter {
  pub fn new() -> Self {
    Interpreter {}
  }

  pub fn read(&self, input: Vec<u8>) -> Command {
    let parts = input.split(|&c| c == b' ').collect::<Vec<_>>();

    if parts[0] == b"SET" {
      Command::Set {
        key: String::from_utf8(parts[1][..].into()).unwrap(),
        value: parts[2][..].into(),
      }
    } else if parts[0] == b"GET" {
      Command::Get {
        key: String::from_utf8(parts[1][..].into()).unwrap(),
      }
    } else if parts[0] == b"DELETE" {
      Command::Delete {
        key: String::from_utf8(parts[1][..].into()).unwrap(),
      }
    } else {
      Command::Invalid
    }
  }
}

pub enum Command {
  Set { key: String, value: Vec<u8> },
  Get { key: String },
  Delete { key: String },
  Invalid,
}
