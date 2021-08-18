use std::collections::HashMap;

type KeyT = String;
type ValueT = Vec<u8>;

pub struct Storage {
  data: HashMap<KeyT, ValueT>,
}

impl Storage {
  pub fn new() -> Self {
    Storage {
      data: Default::default(),
    }
  }

  pub fn set(&mut self, key: KeyT, value: ValueT) {
    self.data.insert(key, value);
  }

  pub fn get(&self, key: KeyT) -> Option<&ValueT> {
    self.data.get(&key)
  }

  pub fn delete(&mut self, key: KeyT) -> bool {
    self.data.remove(&key).is_some()
  }
}
