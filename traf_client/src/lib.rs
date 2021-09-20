#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};

use bincode::{deserialize, serialize};
use std::convert::TryFrom;
use std::convert::TryInto;
use tokio::io::{self};
use tokio::net::{TcpStream, ToSocketAddrs};
use traf_lib::{
  frame_reader::{Frame, FramedTcpStream},
  response_frame::ResponseFrame
};

#[derive(Debug)]
pub struct Get {
  bytes: Vec<u8>,
}

impl Get {
  fn new(bytes: Vec<u8>) -> Self {
    Get { bytes }
  }

  pub fn try_decode<'a, D: Deserialize<'a>>(&'a self) -> Option<D> {
    deserialize(&self.bytes[..]).ok()
  }
}

#[derive(Debug)]
pub enum ClientError {
  IoError(io::Error),
  DataError,
  Failure,
}

pub struct Client {
  framed_stream: FramedTcpStream,
}

impl Client {
  pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
    Ok(Client {
      framed_stream: FramedTcpStream::new(TcpStream::connect(addr).await?),
    })
  }

  pub async fn set<S: Serialize>(&mut self, key: &str, val: S) -> Result<(), ClientError> {
    let mut part_command: Vec<u8> = Vec::from(&b"SET "[..]);
    part_command.append(&mut Vec::from(key));
    part_command.push(b' ');

    let mut encoded = serialize(&val).unwrap();
    part_command.append(&mut encoded);

    self
      .send(part_command)
      .await
      .map_err(|e| ClientError::IoError(e))
      .and_then(|bytes| ResponseFrame::try_from(bytes).map_err(|_| ClientError::DataError))
      .and_then(|success| match success {
        ResponseFrame::Success => Ok(()),
        _ => Err(ClientError::Failure),
      })
  }

  pub async fn get(&mut self, key: &str) -> Result<Get, ClientError> {
    let mut part_command: Vec<u8> = Vec::from(&b"GET "[..]);
    part_command.append(&mut Vec::from(key));

    self
      .send(part_command)
      .await
      .map_err(|e| ClientError::IoError(e))
      .and_then(|bytes| ResponseFrame::try_from(bytes).map_err(|_| ClientError::DataError))
      .and_then(|frame| match frame {
        ResponseFrame::ValueMissing => Err(ClientError::Failure),
        ResponseFrame::Value(v) => Ok(Get::new(v)),
        _ => Err(ClientError::DataError),
      })
  }

  pub async fn delete(&mut self, key: &str) -> Result<(), ClientError> {
    let mut part_command: Vec<u8> = Vec::from(&b"DELETE "[..]);
    part_command.append(&mut Vec::from(key));

    self
      .send(part_command)
      .await
      .map_err(|e| ClientError::IoError(e))
      .and_then(|bytes| ResponseFrame::try_from(bytes).map_err(|_| ClientError::DataError))
      .and_then(|frame| match frame {
        ResponseFrame::ValueMissing => Err(ClientError::Failure),
        ResponseFrame::Success => Ok(()),
        _ => Err(ClientError::DataError),
      })
  }

  pub async fn last_replication_id(&mut self) -> Result<Option<u64>, ClientError> {
    self
      .send(Vec::from(&b"LAST_REPLICATION_ID"[..]))
      .await
      .map_err(|e| ClientError::IoError(e))
      .and_then(|bytes| ResponseFrame::try_from(bytes).map_err(|_| ClientError::DataError))
      .and_then(|frame| match frame {
        ResponseFrame::ValueMissing => Ok(None),
        ResponseFrame::Value(bytes) => match bytes.try_into() {
          Ok(partial_bytes) => Ok(Some(u64::from_be_bytes(partial_bytes))),
          Err(_) => return Err(ClientError::DataError),
        },
        _ => Err(ClientError::DataError),
      })
  }

  // FIXME (?) (hard): This is a bad pattern to expect a generic blob as batch command list. We should not expect
  //        a client user to know how to craft it.
  //        Problem: this is how its stored in logs - would be quite a waste to back and forth convert.
  //
  // Request a batch sync from the server.
  //
  // param blob Byte sequence of list of commands: ([8 bytes: u64 size of command][bytes: command])*
  pub async fn batch_sync(&mut self, mut blob: Vec<u8>) -> Result<(), ClientError> {
    let mut part_command: Vec<u8> = Vec::from(&b"SYNC "[..]);
    part_command.append(&mut blob);

    self
      .send(part_command)
      .await
      .map_err(|e| ClientError::IoError(e))
      .and_then(|bytes| ResponseFrame::try_from(bytes).map_err(|_| ClientError::DataError))
      .and_then(|frame| match frame {
        ResponseFrame::Success => Ok(()),
        _ => Err(ClientError::DataError),
      })
  }

  async fn send(&mut self, msg: Vec<u8>) -> io::Result<Vec<u8>> {
    info!("{} bytes to send", msg.len());
    self.framed_stream.write_frame(msg).await?;

    let msg_in: Frame = self
      .framed_stream
      .read_frame()
      .await
      .ok_or(io::Error::new(io::ErrorKind::InvalidData, "unexpected end"))?;
    info!("{} bytes received", msg_in.bytes.len());

    Ok(msg_in.bytes)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bincode::serialize;

  #[test]
  fn get_works() {
    let subject = 123i32;
    let encoded = serialize(&subject).unwrap();
    let get = Get::new(encoded);
    let decoded: i32 = get.try_decode().unwrap();
    assert_eq!(123i32, decoded);
  }
}
