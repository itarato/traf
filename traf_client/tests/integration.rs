use std::matches;
use traf_client::*;

// Requires TRAF-CORE running on 127.0.0.1:4567
#[tokio::test]
async fn test_basic_set_get_delete_flow() {
  let mut client = Client::connect("127.0.0.1:4567").await.unwrap();
  let set_result = client.set("foo", 123i32).await;

  assert!(set_result.is_ok());

  let get_result = client.get("foo").await;

  assert!(get_result.is_ok());

  let get = get_result.unwrap();
  let value_result: Option<i32> = get.try_decode();

  assert!(value_result.is_some());
  assert_eq!(123i32, value_result.unwrap());

  let delete_result = client.delete("foo").await;
  assert!(delete_result.is_ok());

  let get_again_result = client.get("foo").await;

  assert!(get_again_result.is_err());
  assert!(matches!(
    get_again_result.err().unwrap(),
    ClientError::Failure
  ));
}

#[tokio::test]
async fn test_missing_get_flow() {
  let mut client = Client::connect("127.0.0.1:4567").await.unwrap();
  let get_result = client.get("nofoo").await;

  assert!(get_result.is_err());
  assert!(matches!(get_result.err().unwrap(), ClientError::Failure));
}
