use std::convert::TryFrom;

use clap::{self, Arg};
use interpreter::Command;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::app::{App, InstanceType};
use crate::replicator::ReaderList;
use traf_lib::{
  frame_reader::{Frame, FramedTcpStream},
  response_frame::ResponseFrame,
};

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

pub trait Executor {
  fn execute(&mut self, command: &Command) -> ResponseFrame;
}

// IDEA: (BIG) distributed layout
//  there can be any number of instances running on the network

#[tokio::main]
async fn main() -> Result<(), String> {
  pretty_env_logger::init();

  info!("traf core start");

  let arg_matches = clap::App::new("Traf Core")
    .arg(
      Arg::with_name("address")
        .short("a")
        .value_name("ADDRESS")
        .takes_value(true)
        .default_value("0.0.0.0:4567"),
    )
    .arg(
      Arg::with_name("type")
        .short("t")
        .value_name("TYPE")
        .takes_value(true)
        .default_value("reader"),
    )
    .arg(
      Arg::with_name("last_reader_receiver_replica_id")
        .short("l")
        .value_name("LAST_READER_RECEIVED_REPLICA_ID")
        .takes_value(true),
    )
    .arg(
      Arg::with_name("readers")
        .short("r")
        .value_name("READERS")
        .takes_value(true)
        .default_value(""),
    )
    .get_matches();

  let instance_type = match arg_matches.value_of("type") {
    Some("writer") => InstanceType::Writer,
    Some("reader") => InstanceType::Reader,
    _ => panic!("instance type can be either reader or writer"),
  };

  let last_replica_id: Option<u64> = arg_matches
    .value_of("last_reader_receiver_replica_id")
    .map(|raw| u64::from_str_radix(raw, 10).expect("Invalid number format"));

  let readers_raw = arg_matches
    .value_of("readers")
    .expect("Cannot find readers input");

  let readers = ReaderList::try_from(readers_raw)
    .expect("Incorrect readers input. Expected: -r IP1:PORT1,IP2:PORT2...");

  let address = arg_matches.value_of("address").unwrap();

  let listener = TcpListener::bind(address).await.unwrap();
  let (tx, rx): (Sender<FrameAndChannel>, Receiver<FrameAndChannel>) = mpsc::channel(32);
  let mut app: App = App::new(instance_type, last_replica_id, readers, rx);

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
