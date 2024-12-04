use core::panic;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::fmt::Display;
use crate::parser::RedisBufSplit;
use std::io::Write;

#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    String(String),
    Int(i64),
    Array(Vec<RedisValue>),
    Null,
}

impl RedisValue {
    pub fn to_response(&self) -> String {
        match self {
            RedisValue::String(s) => format!("+{}\r\n", s),
            RedisValue::Int(i) => format!(":{}\r\n", i),
            RedisValue::Array(a) => {
                let mut response = String::from("*");
                response.push_str(format!("{}\r\n", a.len()).as_str());
                for (i, v) in a.iter().enumerate() {
                    response.push_str(&v.to_response());
                }
                response
            }
            RedisValue::Null => String::from("$-1\r\n"),
        }
    }
}
impl Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::String(s) => write!(f, "{}", s),
            RedisValue::Int(i) => write!(f, "{}", i),
            RedisValue::Array(a) => {
                let mut s = String::new();
                s.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    s.push_str(&v.to_string());
                }
                s.push(']');
                write!(f, "{}", s)
            }
            RedisValue::Null => write!(f, "null"),
        }
    }
}
pub struct RedisServer {
    // Need to make thread safe for concurrent access
    pub db: Mutex<HashMap<String, (RedisValue, Option<Instant>)>>,
    config: Mutex<HashMap<String, RedisValue>>,
}

impl RedisServer {
    pub fn new(args: Vec<String>) -> RedisServer {
        let mut rs = RedisServer {
            db: Mutex::new(HashMap::new()),
            config: Mutex::new(HashMap::new()),
        };
        rs.parse_command_line(&args);

        rs.handshake_master();

        rs
    }
    
    pub fn get(&self, key: &str) -> Option<RedisValue> {
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
                return Some(value.clone());
            } else {
                return Some(value.clone());
            }
        }
        None
    }

    pub fn get_config(&self, key: &str) -> Option<RedisValue> {
        let config = self.config.lock().unwrap();
        config.get(key).cloned()
    }
    pub fn set_config(&self, key: &str, value: RedisValue) {
        let mut config = self.config.lock().unwrap();
        config.insert(key.to_string(), value);
    }

    pub fn handshake_master(&self) {
        let replicaof = self.get_config("replicaof");
        if replicaof.is_none() {
            return;
        }
        let mut master_host = String::new();
        let mut master_port = 0;
        let replicaof = replicaof.unwrap();
        if let RedisValue::Array(ref array) = replicaof {
            if array.len() == 2 {
                master_host = array[0].to_string();
                master_port = array[1].to_string().parse::<u16>().unwrap();
                
            } else {
                panic!("replicaof format is wrong: {}", replicaof.to_string());
            }
        }
        // Use basic TCP connection to send PING command to master
        let mut stream = match std::net::TcpStream::connect((master_host.to_string(), master_port.to_string().parse::<u16>().unwrap())) {
            Ok(stream) => stream,
            Err(_) => panic!("Failed to connect to master"),
        };
        let command = RedisValue::Array(vec![RedisValue::String("PING".to_string())]);
        stream.write_all(command.to_response().as_bytes()).unwrap();
    }
    
    pub fn set(&self, key: &str, value: RedisValue, ttl: Option<Duration>) {
        let mut db = self.db.lock().unwrap();
        if ttl.is_some() {
            db.insert(
                key.to_string(),
                (value, Some(Instant::now() + ttl.unwrap())),
            );
        } else {
            db.insert(key.to_string(), (value, None));
        }
    }

    pub fn info(&self, section: &str)-> RedisValue {
        match section {
            "replication" => {
                let role = self.get_config("role").unwrap_or(RedisValue::String("master".to_string()));
                RedisValue::String(format!("role:{}\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0", role))
            }
            _ => RedisValue::Null,
        }
    }

    fn parse_command_line(&mut self, args: &Vec<String>) {
        let mut args_iter = args.iter();
        while let Some(arg) = args_iter.next() {
            match arg.as_str() {
                "--dir" => {
                    if let Some(dir) = args_iter.next() {
                       self.set_config("dir", RedisValue::Array(vec![RedisValue::String("dir".to_string()), RedisValue::String(dir.clone())]));
                    } else {
                        eprintln!("Expected a directory after --dir");
                        return;
                    }
                }
                "--dbfilename" => {
                    if let Some(filename) = args_iter.next() {
                       self.set_config("dbfilename", RedisValue::Array(vec![RedisValue::String("dbfilename".to_string()), RedisValue::String(filename.clone())]));
                    } else {
                        eprintln!("Expected a filename after --dbfilename");
                        return;
                    }
                }
                "--port" => {
                    if let Some(port) = args_iter.next() {
                       self.set_config("port",  RedisValue::String(port.clone()));
                    } else {
                        eprintln!("Expected a port number after --port");
                        return;
                    }
                }
                "--replicaof" => {
                    // --replicaof "<MASTER_HOST> <MASTER_PORT>"
                    if let Some(host_and_port) = args_iter.next() {
                        let (host, port) = host_and_port.split_once(" ").expect("Invalid replicaof format");
                        self.set_config("replicaof",  RedisValue::Array(vec![RedisValue::String(host.to_string()), RedisValue::String(port.to_string())]));
                        self.set_config("role",  RedisValue::String("slave".to_string()));
                    } else {
                        eprintln!("Expected a host after --replicaof");
                        return;
                    }
                }
                _ => {}
            }
        }
    }
}
