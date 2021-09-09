#[derive(Clone)]
pub enum Command {
  Set { key: String, value: Vec<u8> },
  Get { key: String },
  Delete { key: String },
  GetLastReplicationId,
  Invalid,
  Sync { dump: Vec<u8> },
}

impl From<Vec<u8>> for Command {
  fn from(input: Vec<u8>) -> Command {
    let after_cmd_space_pos = input
      .iter()
      .position(|ch| ch == &b' ')
      .unwrap_or(input.len());

    let (cmd, suffix_padded) = input.split_at(after_cmd_space_pos);

    if cmd == b"SET" {
      let suffix = &suffix_padded[1..];
      let after_key_space_pos = suffix.iter().position(|ch| ch == &b' ');
      if after_key_space_pos.is_none() {
        return Command::Invalid;
      }

      let (key, value_padded) = suffix.split_at(after_key_space_pos.unwrap());
      let value = &value_padded[1..];
      Command::Set {
        key: String::from_utf8(key.into()).unwrap(),
        value: value.into(),
      }
    } else if cmd == b"GET" {
      let suffix = &suffix_padded[1..];
      Command::Get {
        key: String::from_utf8(suffix.into()).unwrap(),
      }
    } else if cmd == b"DELETE" {
      let suffix = &suffix_padded[1..];
      Command::Delete {
        key: String::from_utf8(suffix.into()).unwrap(),
      }
    } else if cmd == b"LAST_REPLICATION_ID" {
      Command::GetLastReplicationId
    } else if cmd == b"SYNC" {
      let suffix = &suffix_padded[1..];
      Command::Sync {
        dump: suffix.into(),
      }
    } else {
      Command::Invalid
    }
  }
}

impl Command {
  // FIXME: This looks like ::Into<Vec<u8>>
  pub fn as_bytes(&self) -> Option<Vec<u8>> {
    let mut bytes: Vec<u8> = vec![];

    match self {
      Command::Set { key, value } => {
        bytes.append(&mut Vec::from(&b"SET "[..]));
        bytes.append(&mut Vec::from(&key[..]));
        bytes.push(b' ');
        bytes.append(&mut value.clone());
      }
      Command::Delete { key } => {
        bytes.append(&mut Vec::from(&b"DELETE "[..]));
        bytes.append(&mut Vec::from(&key[..]));
      }
      Command::Get { key } => {
        bytes.append(&mut Vec::from(&b"GET "[..]));
        bytes.append(&mut Vec::from(&key[..]));
      }
      Command::Invalid | Command::GetLastReplicationId | Command::Sync { .. } => return None,
    }

    Some(bytes)
  }
}
