#![allow(unused_imports)]
use std::{
    fs,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use redis_starter_rust::ThreadPool;

const PONG_RESP: &[u8; 7]= b"+PONG\r\n";
fn main() {
    
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let pool = ThreadPool::new(4);
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
    // Loop over the stream's contents
    let mut buffer = [0; 1024];
    loop {
        // Read up to 1024 bytes from the stream
        let n = stream.read(&mut buffer).expect("failed to read from stream");
        // If we didn't get any bytes then break the loop
        if n == 0 {
            break;
        }
        // Print the contents to stdout
        println!("Received: {}", String::from_utf8_lossy(&buffer));
        // Write PONG_RESP to the stream
        stream.write(PONG_RESP).expect("failed to write to stream");
    }


}