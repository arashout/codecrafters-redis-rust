#![allow(unused_imports)]
use std::{
    fs,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use redis_starter_rust::ThreadPool;

mod parser;

const PONG_RESP: &[u8; 7]= b"+PONG\r\n";
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
        // If we didn't get any bytes then break the loop
        if n == 0 {
            break;
        }
        if b"PING" == &buffer[0..4] {
            stream.write(PONG_RESP).expect("failed to write to stream");
            continue;
        }
        if b"ECHO" == &buffer[0..4] {
        }
        // Print the contents to stdout
        println!("Received: {}", String::from_utf8_lossy(&buffer));
        // TODO: Use parser to handle Array commands , print results, do something if echo
        // Write PONG_RESP to the stream
        stream.write(PONG_RESP).expect("failed to write to stream");
    }


}