use clap::{self, Arg};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::app::{App, InstanceType};
use traf_lib::frame_reader::{Frame, FramedTcpStream};

#[macro_use]
extern crate log;

mod app;
mod file_backup;
mod interpreter;
mod replicator;
mod storage;

pub struct FrameAndChannel {
  frame: Frame,
  channel: oneshot::Sender<Vec<u8>>,
}

impl FrameAndChannel {
  fn new(frame: Frame, channel: oneshot::Sender<Vec<u8>>) -> Self {
    FrameAndChannel { frame, channel }
  }
}

// IDEA: (BIG) distributed layout
//  there can be any number of instances running on the network

#[tokio::main]
async fn main() -> Result<(), String> {
  pretty_env_logger::init();

  info!("traf core start");

  let arg_matches = clap::App::new("Traf Core")
    .arg(
      Arg::with_name("type")
        .short("t")
        .value_name("TYPE")
        .takes_value(true)
        .default_value("reader"),
    )
    .arg(
      Arg::with_name("last_reader_receiver_replica_id")
        .short("last_replica_id")
        .value_name("LAST_READER_RECEIVED_REPLICA_ID")
        .takes_value(true)
        .default_value(""),
    )
    .get_matches();

  let instance_type = match arg_matches.value_of("type") {
    Some("writer") => InstanceType::Writer,
    Some("reader") => InstanceType::Reader,
    _ => panic!("instance type can be either reader or writer"),
  };

  let last_replica_id: u64 = u64::from_str_radix(
    arg_matches
      .value_of("last_reader_receiver_replica_id")
      .expect("Cannot obtain last replica id argument"),
    10,
  )
  .unwrap();

  let listener = TcpListener::bind("127.0.0.1:4567").await.unwrap();
  let (tx, rx): (Sender<FrameAndChannel>, Receiver<FrameAndChannel>) = mpsc::channel(32);
  let mut app: App = App::new(instance_type, last_replica_id, rx);

  let _app_join_handle = spawn(async move {
    app.listen().await;
  });

  loop {
    let (socket, _) = listener.accept().await.unwrap();
    let tx = tx.clone();

    spawn(async move {
      info!("socket connected");
      process(socket, tx).await.expect("Failed processing");
      info!("socket disconnected");
    });

    // IDEA: should we have a server killer?
  }
}

async fn process(stream: TcpStream, tx: Sender<FrameAndChannel>) -> Result<(), String> {
  let mut framed_stream = FramedTcpStream::new(stream);
  loop {
    let (feedback_tx, feedback_rx): (oneshot::Sender<Vec<u8>>, oneshot::Receiver<Vec<u8>>) =
      oneshot::channel();

    let msg_in = framed_stream.read_frame().await;
    if msg_in.is_none() {
      info!("Socket ended");
      return Ok(());
    }

    let msg_in = msg_in.unwrap();
    let frame_and_channel = FrameAndChannel::new(msg_in, feedback_tx);
    tx.send(frame_and_channel)
      .await
      .unwrap_or_else(|_| panic!("Failed sending input to app channel"));

    let feedback = feedback_rx.await.expect("Failed getting process feedback");
    framed_stream
      .write_frame(feedback)
      .await
      .expect("Failed sending message back to client");

    info!("socket completed");
  }
}
