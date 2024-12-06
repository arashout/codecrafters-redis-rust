use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Logger {
    pub kv: HashMap<String, String>,
}

impl Logger {
    pub fn new() -> Self {
        Logger {
            kv: HashMap::new(),
        }
    }

    pub fn log(&self, msg: &str) {
        let mut s = String::new();
        for (k, v) in self.kv.iter() {
            s.push_str(&format!("[{}:{}] ", k, v));
        }
        s.push_str(msg);
        println!("{}", s);
    }

    pub fn with(&self, key: &str, value: &str) -> Logger{
        let mut kv = self.kv.clone();
        kv.insert(key.to_string(), value.to_string());
        Logger { kv }
    }
}
