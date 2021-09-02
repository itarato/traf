use clap::{App, Arg};
use traf_client::Client;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
  pretty_env_logger::init();

  let matches = App::new("Traf Benchmark")
    .arg(
      Arg::with_name("iteration")
        .short("i")
        .takes_value(true)
        .default_value("1"),
    )
    .arg(
      Arg::with_name("concurrency")
        .short("c")
        .takes_value(true)
        .default_value("1"),
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
      let mut client = Client::connect("0.0.0.0:4567")
        .await
        .expect("Failed creating a client");

      for i in 0..iteration {
        client.set("foo", 123u8).await.expect("Cannot set value");
        info!("data sent c:{} i:{}", c, i);

        let get_result = client.get("foo").await.expect("Cannot get value");
        info!(
          "data received c:{} i:{} data:{:?}",
          c,
          i,
          get_result
            .try_decode::<u8>()
            .expect("Cannot decode response")
        );
      }
    });
    join_handles.push(join_handle);
  }

  for join_handle in join_handles {
    join_handle.await.expect("Failed closing thread");
  }
}
