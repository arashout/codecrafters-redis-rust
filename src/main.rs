#![allow(unused_imports)]
use std::{io::Read, net::TcpListener, io::Write};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // Loop over the stream's contents
                let mut buffer = [0; 1024];
                stream.read(&mut buffer).expect("failed to read from stream");
                // Split by \n and iterate over the lines
                let lines: Vec<String> = String::from_utf8_lossy(&buffer).split("\n").map(|l| l.to_owned()).collect();
                for line in lines {
                    let mut response = "".to_owned();
                    if line.starts_with("PING") {
                        response = "PONG".to_owned();
                    }
                    if response != "" {
                        stream.write(response.as_bytes()).expect("failed to write to stream");
                    }
                }
                
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
