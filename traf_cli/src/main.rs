use tokio::net::{TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use std::io::{stdin, stdout};

#[macro_use] extern crate log;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let in_stream = stdin();
    let mut stream = TcpStream::connect("127.0.0.1:4567").await.expect("Cannot connect to stream.");

    loop {
        let mut stdin_buf = String::new();

        print!("traf> ");
        std::io::Write::flush(&mut stdout()).expect("Cannot flush to out");
        
        in_stream.read_line(&mut stdin_buf).expect("Failed reading from input stream");

        let input = stdin_buf.trim();

        match input {
            "q" => break,
            _ => {
                stream.write(&[input.len() as u8]).await.expect("Cannot write to stream");
                stream.write(input.as_bytes()).await.expect("cannot write to stream");

                info!("data sent");

                let mut buf_in: [u8; 256] = [0; 256];
                let buf_in_len = stream.read(&mut buf_in).await.expect("Cannot read server response");

                info!("data received data:{:?}", &buf_in[..buf_in_len]);
            },
        };
    }
}
