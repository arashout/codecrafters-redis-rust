use  std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};


pub struct RedisServer {
    // Need to make thread safe for concurrent access
    pub db: Mutex<HashMap<String, (String, Option<Instant>)>>,
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer {
            db: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut db = self.db.lock().unwrap();
        let res = db.get(key).cloned();
        // Check  if the key has an expiration time and if it has expired
        if let Some((value, expiration)) = res.as_ref() {
            if let Some(expiration) = expiration {
                if Instant::now() > *expiration {
                    // Key has expired, remove it from the database
                    db.remove(key);
                    return None;
                }
                return Some(value.to_string())
            }
            else {
                return Some(value.to_string())
            }
        }
        None
    }

    pub fn set(&self, key: &str, value: &str, ttl: Option<Duration>) {
        let mut db = self.db.lock().unwrap();
        if ttl.is_some() {
            db.insert(key.to_string(), (value.to_string(), Some(Instant::now() + ttl.unwrap())));
        } else {
            db.insert(key.to_string(), (value.to_string(), None));
        }
    }
}