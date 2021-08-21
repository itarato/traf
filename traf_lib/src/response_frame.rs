use std::convert::TryFrom;

pub enum ResponseFrame {
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

impl TryFrom<Vec<u8>> for ResponseFrame {
  type Error = ();

  fn try_from(mut v: Vec<u8>) -> Result<ResponseFrame, Self::Error> {
    let type_byte: u8 = v.drain(..1).collect::<Vec<u8>>()[0];

    match type_byte {
      0 => Ok(Self::Success),
      1 => Ok(Self::ErrorInvalidCommand),
      2 => Ok(Self::Value(v)),
      3 => Ok(Self::ValueMissing),
      _ => Err(()),
    }
  }
}
