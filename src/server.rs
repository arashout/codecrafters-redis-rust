use bytes::BytesMut;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::vec;
use std::{fmt::Write, num::ParseIntError};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;

use crate::cast;
use crate::log::Logger;
use crate::parser;

// Empty RDB file
const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

const PONG_RESP: &[u8; 7] = b"+PONG\r\n";
const OK_RESP: &[u8; 5] = b"+OK\r\n";
const NULL_RESP: &[u8; 5] = b"$-1\r\n";

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
                for (_, v) in a.iter().enumerate() {
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

    pub fn set(&self, key: &str, value: RedisValue, duration: Option<Duration>) {
        let mut db = self.db.lock().unwrap();
        let mut ttl = None;
        if duration.is_some() {
            ttl = Some(Instant::now() + duration.unwrap());
        }
        db.insert(key.to_string(), (value, ttl));
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

    async fn reply(&self, logger: &Logger, stream: &mut tokio::net::TcpStream, resp: &[u8], no_response: bool) {
        if no_response {
            return;
        }
        logger.log(&format!("Sending Reply: {}", String::from_utf8_lossy(resp)));
        stream
            .write_all(resp)
            .await
            .expect("failed to write to stream");
    }

    pub async fn evaluate(
        &self,
        logger: &Logger,
        bm: BytesMut,
        stream: &mut tokio::net::TcpStream,
        tx: Option<Arc<broadcast::Sender<String>>>,
    ) {
        let is_replica = self.config.master_host_port.is_some();
        let mut pos = 0;
        while pos < bm.len() {
            match bm[pos] {
                b'*' => {
                    let (i, res) = parser::Parser::parse_array(&bm, pos)
                        .expect("failed to parse array")
                        .expect("Expected some result");
                    pos = i;
                    let a = cast!(res, parser::RedisBufSplit::Array);
                    let command = a[0].to_string(&bm);
                    match command.to_lowercase().as_str() {
                        "echo" => {
                            let echo_str = a[1].to_string(&bm);
                            let echo_resp = format!("${}\r\n{}\r\n", echo_str.len(), echo_str);
                            self.reply(&logger, stream, echo_resp.as_bytes(), false).await;
                        }
                        "ping" => {
                            self.reply(&logger, stream, PONG_RESP, false).await;
                        }
                        "set" => {
                            if self.config.master_host_port.is_none() {
                                let s = String::from_utf8_lossy(&bm);
                                logger.log(&format!("Received SET command: {}", s));
                                tx.as_ref().unwrap().send(s.into_owned())
                                    .expect("failed to send to broadcast");
                            }
                            // Determine if there is an expiry by checking the number of arguments
                            if a.len() == 5 {
                                // Set the expiry
                                let key = a[1].to_string(&bm);
                                let value = a[2].to_string(&bm);
                                let expiry = a[4].to_string(&bm);
                                let expiry_millisecond =
                                    expiry.parse::<u64>().expect("failed to parse expiry");
                                let expiry_duration = Duration::from_millis(expiry_millisecond);
                                self.set(&key, RedisValue::String(value), Some(expiry_duration));
                            } else {
                                // No expiry
                                let key = a[1].to_string(&bm);
                                let value = a[2].to_string(&bm);
                                self.set(&key, RedisValue::String(value), None);
                            }
                            self.reply(&logger, stream, OK_RESP, is_replica).await;
                        }
                        "get" => {
                            let key = a[1].to_string(&bm);
                            let value = self.get(&key);
                            match value {
                                Some(value) => {
                                    self.reply(&logger, stream, value.to_response().as_bytes(), false)
                                        .await;
                                }
                                None => {
                                    self.reply(&logger, stream, NULL_RESP, false).await;
                                }
                            };
                        }
                        "docs" => {
                            let docs_str = "https://github.com/redis/redis-doc/blob/master/commands.md";
                            let docs_resp = format!("${}\r\n{}\r\n", docs_str.len(), docs_str);
    
                            self.reply(&logger, stream, docs_resp.as_bytes(), false).await;
                        }
                        "info" => {
                            let arg = a[1].to_string(&bm);
                            let info_resp = self.info(&arg).to_response();
                            self.reply(&logger, stream, info_resp.as_bytes(), false).await;
                        }
                        "replconf" => {
                            stream.write_all(OK_RESP).await.unwrap();
                        }
                        "psync" => {
                            let command = RedisValue::String(format!(
                                "FULLRESYNC {} 0",
                                self.config.master_replid
                            ));
                            self.reply(&logger, stream, command.to_response().as_bytes(), false)
                                .await;
                            let rdb_content = self.rdb_dump();
                            self.reply(
                                &logger,
                                stream,
                                format!("${}\r\n", rdb_content.len()).as_bytes(),
                                false,
                            )
                            .await;
                            self.reply(&logger, stream, &rdb_content, false).await;
    
                            // At this point we know this connection is from master -> replica 
                            let mut rx = tx.as_ref().unwrap().subscribe();
                            loop {
                                let msg = rx.recv().await.unwrap();
                                logger.log(&format!("Received message: {}", msg));
                                stream
                                    .write_all(msg.as_bytes())
                                    .await
                                    .expect("failed to write to stream");
                            }
                        }
                        _ => {
                            logger.log(&format!("Unknown command: {}", command));
                            unimplemented!("No other commands implemented yet for array");
                        }
                    }
                }
                c => {
                    logger.log(&format!("Unknown command: {}", c));
                    unimplemented!("No other commands implemented yet");
                }
            }
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

    // For the purposes of this exercise, we'll just use the empty RDB const
    pub fn rdb_dump(&self) -> Vec<u8> {
        decode_hex(EMPTY_RDB_HEX).expect("Failed to decode empty RDB hex")
    }
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}
