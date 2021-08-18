use tokio::net::TcpStream;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

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

// IDEA: Make it own the stream and be responsible of read/write.
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
    assert!(bytes.len() <= 0xff);
    bytes.insert(0, bytes.len() as u8);

    self.stream.write_all(bytes.as_slice()).await
  }

  // Fixme: 1 byte limits max frame size to 256 bytes.
  // IDEA: allow long messages
  async fn read_frame_size(&mut self) -> Option<usize> {
    if !self.read_until_buffer_size(1).await {
      return None;
    }

    let len = self.buffer.drain(..1).collect::<Vec<_>>()[0] as usize;

    Some(len)
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
