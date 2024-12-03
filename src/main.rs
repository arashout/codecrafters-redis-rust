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

const PONG_RESP: &[u8; 7]= b"+PONG\r\n";

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
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                pool.execute(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
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
                println!("{}", command);
                match command.as_str() {
                    "ECHO" => {
                        let echo_str = a[1].to_string(&bm);
                        let echo_resp = format!("${}\r\n{}\r\n", echo_str.len(), echo_str);
                        stream.write(echo_resp.as_bytes()).expect("failed to write to stream");
                    }
                    "PING" => {
                        stream.write(PONG_RESP).expect("failed to write to stream");
                    }
                    "SET" => {
                        let key = a[1].to_string(&bm);
                        let value = a[2].to_string(&bm);
                        println!("{} {}", key, value);
                    }
                    "GET" => {
                        let key = a[1].to_string(&bm);
                        println!("{}", key);
                    }
                    _ => unimplemented!("No other commands implemented yet"),
                }


            }
            _ => unimplemented!("No other commands implemented yet"),
        }
        // TODO: Use parser to handle Array commands , print results, do something if echo
        // Write PONG_RESP to the stream
        stream.write(PONG_RESP).expect("failed to write to stream");
    }
}