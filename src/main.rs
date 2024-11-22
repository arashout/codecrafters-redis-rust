#![allow(unused_imports)]
use std::{io::Read, net::TcpListener, io::Write};


const PONG_RESP: &[u8; 7]= b"+PONG\r\n";
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
                    let mut response: Vec<u8> = vec![];
                    if line.starts_with("PING") {
                        response = PONG_RESP.to_vec();
                    }
                    if !response.is_empty() {
                        stream.write(&response).expect("failed to write to stream");
                    }
                }
                
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
