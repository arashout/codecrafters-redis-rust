#![allow(unused_imports)]
use std::{io::Read, net::TcpListener, io::Write};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                stream.write_all(b"+PONG\r\n").expect("failed to write to stream");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
