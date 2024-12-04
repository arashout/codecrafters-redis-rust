use crate::parser::RedisBufSplit;
use core::panic;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

pub struct RedisConfig {
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub master_host_port: Option<(String, u16)>,
    pub master_replid: String,
    pub master_reploffset: i64,
}
pub struct RedisServer {
    // Need to make thread safe for concurrent access
    pub db: Mutex<HashMap<String, (RedisValue, Option<Instant>)>>,
    pub config: RedisConfig,
}

impl RedisServer {
    pub fn new(args: Vec<String>) -> RedisServer {
        let mut rs = RedisServer {
            db: Mutex::new(HashMap::new()),
            config: RedisConfig {
                dir: String::from("."),
                dbfilename: String::from("dump.rdb"),
                port: 6379,
                master_host_port: None,
                master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
                master_reploffset: 0,
            },
        };
        rs.parse_command_line(&args);

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

    pub fn info(&self, section: &str) -> RedisValue {
        match section {
            "replication" => {
                let role = if self.config.master_host_port.is_some() {
                    "slave"
                } else {
                    "master"
                };
                RedisValue::String(format!(
                    "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                    role, self.config.master_replid, self.config.master_reploffset
                ))
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
                        self.config.dir = dir.clone();
                    } else {
                        eprintln!("Expected a directory after --dir");
                        return;
                    }
                }
                "--dbfilename" => {
                    if let Some(filename) = args_iter.next() {
                        self.config.dbfilename = filename.clone();
                    } else {
                        eprintln!("Expected a filename after --dbfilename");
                        return;
                    }
                }
                "--port" => {
                    if let Some(port) = args_iter.next() {
                        self.config.port = port.parse().expect("Invalid port number");
                    } else {
                        eprintln!("Expected a port number after --port");
                        return;
                    }
                }
                "--replicaof" => {
                    // --replicaof "<MASTER_HOST> <MASTER_PORT>"
                    if let Some(host_and_port) = args_iter.next() {
                        let (host, port) = host_and_port
                            .split_once(" ")
                            .expect("Invalid replicaof format");
                        self.config.master_host_port =
                            Some((host.to_string(), port.parse().expect("Invalid port number")));
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
