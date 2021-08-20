use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Frame {
  pub bytes: Vec<u8>,
}

impl Frame {
  fn new(bytes: Vec<u8>) -> Self {
    Frame { bytes }
  }
}

pub struct FramedTcpStream {
  buffer: Vec<u8>,
  stream: TcpStream,
}

impl FramedTcpStream {
  pub fn new(stream: TcpStream) -> Self {
    Self {
      buffer: vec![],
      stream,
    }
  }

  pub async fn read_frame(&mut self) -> Option<Frame> {
    let read_len_res = self.read_frame_size().await;
    if read_len_res.is_none() {
      return None;
    }

    let read_len = read_len_res.unwrap();

    if !self.read_until_buffer_size(read_len).await {
      return None;
    }

    let frame_msg: Vec<_> = self.buffer.drain(..read_len).collect();

    Some(Frame::new(frame_msg))
  }

  pub async fn write_frame(&mut self, mut bytes: Vec<u8>) -> io::Result<()> {
    let len = bytes.len();
    if len <= 0xff as usize {
      bytes.insert(0, len as u8);
      bytes.insert(0, 1u8);
    } else if len <= 0xffff {
      bytes.insert(0, (len & 0xff) as u8);
      bytes.insert(0, ((len >> 8) & 0xff) as u8);
      bytes.insert(0, 2u8);
    } else if len <= 0xffff_ffff {
      bytes.insert(0, (len & 0xff) as u8);
      bytes.insert(0, ((len >> 8) & 0xff) as u8);
      bytes.insert(0, ((len >> 16) & 0xff) as u8);
      bytes.insert(0, ((len >> 24) & 0xff) as u8);
      bytes.insert(0, 4u8);
    } else {
      panic!("Too large sequence: {}", len);
    }

    self.stream.write_all(bytes.as_slice()).await
  }

  async fn read_frame_size(&mut self) -> Option<usize> {
    if !self.read_until_buffer_size(1).await {
      return None;
    }

    let byte_size = self.buffer.drain(..1).collect::<Vec<_>>()[0] as usize;

    match byte_size {
      1 | 2 | 4 => {
        if !self.read_until_buffer_size(byte_size).await {
          None
        } else {
          let len_bytes = self.buffer.drain(..byte_size).collect::<Vec<_>>();
          match byte_size {
            1 => Some(len_bytes[0] as usize),
            2 => Some(((len_bytes[0] as usize) << 8) | len_bytes[1] as usize),
            4 => Some(
              ((len_bytes[0] as usize) << 24)
                | ((len_bytes[1] as usize) << 16)
                | ((len_bytes[2] as usize) << 8)
                | len_bytes[3] as usize,
            ),
            _ => unreachable!(),
          }
        }
      }
      _ => panic!("Incompatible stream size: {}", byte_size),
    }
  }

  async fn read_until_buffer_size(&mut self, limit: usize) -> bool {
    while self.buffer.len() < limit {
      let mut buf: [u8; 1024] = [0; 1024];
      let read_res = self.stream.read(&mut buf).await;

      if read_res.is_err() {
        return false;
      }

      let n = read_res.unwrap();
      info!("received {} bytes: {:?}", n, &buf[..n]);

      if n == 0 {
        return false;
      }

      self.buffer.append(&mut Vec::from(&buf[..n]));
    }

    true
  }
}
