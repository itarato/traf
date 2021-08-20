#[macro_use]
extern crate log;

use serde::{Deserialize, Serialize};

use bincode::{deserialize, serialize};
use std::convert::TryFrom;
use tokio::io::{self};
use tokio::net::{TcpStream, ToSocketAddrs};
use traf_lib::frame_reader::{Frame, FramedTcpStream};

#[derive(Debug)]
pub enum ResponseFrame {
  Success,
  ErrorInvalidCommand,
  Value(Vec<u8>),
  ValueMissing,
}

impl TryFrom<Vec<u8>> for ResponseFrame {
  type Error = ();

  fn try_from(mut v: Vec<u8>) -> Result<Self, ()> {
    if v.len() == 0 {
      return Err(());
    }

    match v.remove(0) {
      0 => Ok(ResponseFrame::Success),
      1 => Ok(ResponseFrame::ErrorInvalidCommand),
      2 => Ok(ResponseFrame::Value(v)),
      3 => Ok(ResponseFrame::ValueMissing),
      _ => Err(()),
    }
  }
}

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
