use clap::{App, Arg};
use rand::Rng;
use traf_client::Client;

#[macro_use]
extern crate log;

fn generate_random_name(len: usize) -> String {
  let mut rng = rand::thread_rng();
  let mut bytes: Vec<u8> = vec![];
  for _ in 0..len {
    let byte: u8 = rng.gen_range(b'a'..b'z');
    bytes.push(byte);
  }
  String::from_utf8(bytes).expect("Failed random string generation")
}

#[tokio::main]
async fn main() {
  pretty_env_logger::init();

  let matches = App::new("Traf Benchmark + Client")
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
      let mut client = Client::connect("127.0.0.1:4567")
        .await
        .expect("Failed creating a client");

      for i in 0..iteration {
        let action_random_code: u8 = rand::random();
        let key = generate_random_name(4);

        if action_random_code < 200 {
          let value = generate_random_name((rand::random::<u8>() % 20 + 5) as usize);
          info!("[Thread #{} i:#{}] SET {:?} {:?}", c, i, key, &value);
          client.set(&key, value).await.expect("Failed at set");
        } else {
          info!("[Thread #{} i:#{}DELETE {:?}", c, i, key);
          let _ = client.delete(&key).await;
        }
      }
    });
    join_handles.push(join_handle);
  }

  for join_handle in join_handles {
    join_handle.await.expect("Failed closing thread");
  }
}
