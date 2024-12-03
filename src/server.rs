use  std::collections::HashMap;
use std::sync::{Arc, Mutex};


pub struct RedisServer {
    // Need to make thread safe for concurrent access
    pub db: Arc<Mutex<HashMap<String, String>>>,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer {
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let db = self.db.lock().unwrap();
        db.get(key).cloned()
    }

    pub fn set(&self, key: &str, value: &str) {
        let mut db = self.db.lock().unwrap();
        db.insert(key.to_string(), value.to_string());
    }
}