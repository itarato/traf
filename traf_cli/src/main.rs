use std::convert::TryFrom;
use std::io::{stdin, stdout};
use tokio::io;
use tokio::net::TcpStream;
use traf_lib::frame_reader::FramedTcpStream;
use traf_lib::response_frame::ResponseFrame;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() -> io::Result<()> {
    pretty_env_logger::init();

    let in_stream = stdin();
    let stream = TcpStream::connect("127.0.0.1:4567").await?;
    let mut framed_stream = FramedTcpStream::new(stream);

    loop {
        let mut stdin_buf = String::new();

        print!("traf> ");
        std::io::Write::flush(&mut stdout()).expect("Cannot flush to out");

        in_stream
            .read_line(&mut stdin_buf)
            .expect("Failed reading from input stream");
        let input = stdin_buf.trim();

        match input {
            "q" => break,
            _ => {
                framed_stream.write_frame(input.as_bytes().to_vec()).await?;
                info!("data sent");

                let bytes_in = framed_stream
                    .read_frame()
                    .await
                    .expect("Cannot read response");

                match ResponseFrame::try_from(bytes_in.bytes) {
                    Ok(response_frame) => match response_frame {
                        ResponseFrame::Success => println!("[success]"),
                        ResponseFrame::ErrorInvalidCommand => println!("[invalid command]"),
                        ResponseFrame::ValueMissing => println!("[value missing]"),
                        ResponseFrame::Value(v) => {
                            match String::from_utf8(v.clone()) {
                                Ok(s) => println!("{:?}", s),
                                Err(_) => println!("{:?}", v),
                            };
                        }
                    },
                    Err(_) => break,
                }
            }
        };
    }

    Ok(())
}
