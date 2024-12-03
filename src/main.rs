#![allow(unused_imports)]
use std::{
    fs,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use redis_starter_rust::ThreadPool;
use bytes::BytesMut;
mod parser;
use parser::{RedisBufSplit};

mod server;
use server::RedisServer;

use std::sync::Arc;

const PONG_RESP: &[u8; 7]= b"+PONG\r\n";
const OK_RESP: &[u8; 5]= b"+OK\r\n";
const NULL_RESP: &[u8; 5]= b"$-1\r\n";

macro_rules! cast {
        ($target: expr, $pat: path) => {
            {
                if let $pat(a) = $target { // #1
                    a
                } else {
                    panic!(
                        "mismatch variant when cast to {}", 
                        stringify!($pat)); // #2
                }
            }
        };
    }


fn main() {
    
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let pool = ThreadPool::build(4).expect("Failed to build thread pool");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let server = RedisServer::new();
    let server = Arc::new(server);
    for stream in listener.incoming() {
        let server = server.clone();
        match stream {
            Ok(stream) => {
                pool.execute(|| {
                    handle_connection(server, stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(server: Arc<RedisServer>, mut stream: TcpStream) {
    // TODO: Not quite sure how to handle parsing, since commands come piece by piece
    // Loop over the stream's contents
    let mut buffer = [0; 1024];
    loop {
        // Read up to 1024 bytes from the stream
        let n = stream.read(&mut buffer).expect("failed to read from stream");
        // Print the contents to stdout
        println!("Received: {}", String::from_utf8_lossy(&buffer));
        // If we didn't get any bytes then break the loop
        if n == 0 {
            break;
        }
        let bm = BytesMut::from(&buffer[0..n]);
        if bm.len() == 0 {
            continue;
        }
        match buffer[0] {
            b'*' => {
                let res = parser::Parser::parse_array(&bm, 0).expect("failed to parse array").expect("Expected some result");
                let a = cast!(res.1, RedisBufSplit::Array);
                let command = a[0].to_string(&bm);
                match command.to_lowercase().as_str() {
                    "echo" => {
                        let echo_str = a[1].to_string(&bm);
                        let echo_resp = format!("${}\r\n{}\r\n", echo_str.len(), echo_str);
                        stream.write(echo_resp.as_bytes()).expect("failed to write to stream");
                    }
                    "ping" => {
                        stream.write(PONG_RESP).expect("failed to write to stream");
                    }
                    "set" => {
                        let key = a[1].to_string(&bm);
                        let value = a[2].to_string(&bm);
                        server.set(&key, &value);
                        stream.write(OK_RESP).expect("failed to write to stream");
                    }
                    "get" => {
                        let key = a[1].to_string(&bm);
                        let value = server.get(&key);
                        match value {
                            Some(value) => {
                                let resp = format!("${}\r\n{}\r\n", value.len(), value);
                                stream.write(resp.as_bytes()).expect("failed to write to stream");
                            }
                            None => {
                                stream.write(NULL_RESP).expect("failed to write to stream");
                            }
                        }
                    }
                    "docs" => {
                        let docs_str = "https://github.com/redis/redis-doc/blob/master/commands.md";
                        let docs_resp = format!("${}\r\n{}\r\n", docs_str.len(), docs_str);
                        stream.write(docs_resp.as_bytes()).expect("failed to write to stream");
                    }
                    _ => unimplemented!("No other commands implemented yet"),
                }


            }
            _ => unimplemented!("No other commands implemented yet"),
        }
    }
}