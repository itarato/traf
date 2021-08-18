use tokio::net::{TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use clap::{Arg, App};

#[macro_use] extern crate log;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let matches = App::new("Traf Benchmark + Client")
        .arg(
            Arg::with_name("iteration")
                .short("i")
                .takes_value(true)
                .default_value("1")
        )
        .arg(
            Arg::with_name("concurrency")
                .short("c")
                .takes_value(true)
                .default_value("1")
        )
        .get_matches();

    let raw_iteration = matches.value_of("iteration").unwrap();
    let iteration = u32::from_str_radix(raw_iteration, 10).unwrap();

    let raw_concurrency = matches.value_of("concurrency").unwrap();
    let concurrency = u32::from_str_radix(raw_concurrency, 10).unwrap();

    info!("Iterations: {}", iteration);
    info!("Concurrency: {}", concurrency);

    let mut join_handles = Vec::new();

    for c in 0..concurrency {
        let join_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect("127.0.0.1:4567").await.expect("Cannot connect to stream.");

            for i in 0..iteration {
                let msg = b"SET foo 123";
                stream.write(&[msg.len() as u8]).await.expect("Cannot write to stream");
                stream.write(msg).await.expect("cannot write to stream");

                info!("data sent c:{} i:{}", c, i);

                let mut buf_in: [u8; 256] = [0; 256];
                let buf_in_len = stream.read(&mut buf_in).await.expect("Cannot read server response");

                info!("data received c:{} i:{} data:{:?}", c, i, &buf_in[..buf_in_len]);
            }
        });
        join_handles.push(join_handle);
    }

    for join_handle in join_handles {
        join_handle.await.expect("Failed closing thread");
    }
}
