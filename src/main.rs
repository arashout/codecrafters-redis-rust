#![allow(unused_imports)]
use bytes::BytesMut;
use redis_starter_rust::{cast, ThreadPool};
use std::{
    env, fs,
    io::{prelude::*, BufReader, ErrorKind},
    thread,
    time::Duration,
};
mod parser;
use parser::RedisBufSplit;

mod server;
use server::{RedisServer, RedisValue};
use std::sync::Arc;

const PONG_RESP: &[u8; 7] = b"+PONG\r\n";
const OK_RESP: &[u8; 5] = b"+OK\r\n";
const NULL_RESP: &[u8; 5] = b"$-1\r\n";

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn handshake_master_ping(stream: &mut TcpStream, port: u16) -> Result<(), Box<dyn Error>> {
    let command = RedisValue::Array(vec![RedisValue::String("PING".to_string())]);
    stream
        .write_all(command.to_response().as_bytes())
        .await
        .unwrap();
    stream.flush().await.unwrap();
    println!("Sent PING command to master");

    let buf = &mut [0; 1024];
    stream.readable().await?;
    let n = stream.read(buf).await?;
    let response = &buf[..n];
    println!("Received response: {}", String::from_utf8_lossy(response));

    // REPLCONF commands
    let command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("listening-port".to_string()),
        RedisValue::String(port.to_string()),
    ]);
    stream
        .write_all(command.to_response().as_bytes())
        .await
        .unwrap();
    stream.flush().await.unwrap();
    stream.readable().await?;
    let n = stream.read(buf).await?;
    let response = &buf[..n];
    println!("Received response: {}", String::from_utf8_lossy(response));

    let command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("capa".to_string()),
        RedisValue::String("psync2".to_string()),
    ]);
    stream
        .write_all(command.to_response().as_bytes())
        .await
        .unwrap();
    stream.flush().await.unwrap();
    println!("Sent REPLCONF commands to master");
    stream.readable().await?;
    let n = stream.read(buf).await?;
    let response = &buf[..n];
    println!("Received response: {}", String::from_utf8_lossy(response));
    Ok(())
}

#[tokio::main]
async fn main() {
    let server = RedisServer::new(env::args().collect());
    let port = server.config.port;

    if let Some((master_host, master_port)) = server.config.master_host_port.clone() {
        // Do handshake with master if this is a slave
        let master_handle = tokio::spawn(async move {
            // Use basic TCP connection to send PING command to master
            let mut stream = TcpStream::connect((master_host.clone(), master_port.clone()))
                .await
                .expect("Failed to connect to master");
            handshake_master_ping(&mut stream, server.config.port).await;
        });
        master_handle.await.expect("Failed to handshake with master");
    }

    let handle = tokio::spawn(async move {
        let server = Arc::new(server);
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        println!("Listening on port {}", port);
        
        loop {
            while let Ok((stream, _)) = listener.accept().await {
                let server = server.clone();
                tokio::spawn(async move {
                    handle_connection(server, stream).await;
                });
            }
        }
    });
    handle.await.expect("Failed to start server");
}

async fn handle_connection(server: Arc<RedisServer>, mut stream: TcpStream) {
    // Loop over the stream's contents
    let mut buffer = [0; 1024];
    loop {
        // Read up to 1024 bytes from the stream
        let n = stream
            .read(&mut buffer)
            .await
            .expect("failed to read from stream");
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
                let res = parser::Parser::parse_array(&bm, 0)
                    .expect("failed to parse array")
                    .expect("Expected some result");
                let a = cast!(res.1, RedisBufSplit::Array);
                let command = a[0].to_string(&bm);
                match command.to_lowercase().as_str() {
                    "echo" => {
                        let echo_str = a[1].to_string(&bm);
                        let echo_resp = format!("${}\r\n{}\r\n", echo_str.len(), echo_str);
                        stream
                            .write(echo_resp.as_bytes())
                            .await
                            .expect("failed to write to stream");
                    }
                    "ping" => {
                        stream
                            .write(PONG_RESP)
                            .await
                            .expect("failed to write to stream");
                    }
                    "set" => {
                        // Determine if there is an expiry by checking the number of arguments
                        if a.len() == 5 {
                            // Set the expiry
                            let key = a[1].to_string(&bm);
                            let value = a[2].to_string(&bm);
                            let expiry = a[4].to_string(&bm);
                            let expiry_millisecond =
                                expiry.parse::<u64>().expect("failed to parse expiry");
                            let expiry_duration = Duration::from_millis(expiry_millisecond);
                            server.set(&key, RedisValue::String(value), Some(expiry_duration));
                        } else {
                            // No expiry
                            let key = a[1].to_string(&bm);
                            let value = a[2].to_string(&bm);
                            server.set(&key, RedisValue::String(value), None);
                        }
                        stream
                            .write(OK_RESP)
                            .await
                            .expect("failed to write to stream");
                    }
                    "get" => {
                        let key = a[1].to_string(&bm);
                        let value = server.get(&key);
                        match value {
                            Some(value) => {
                                stream
                                    .write(value.to_response().as_bytes())
                                    .await
                                    .expect("failed to write to stream");
                            }
                            None => {
                                stream
                                    .write(NULL_RESP)
                                    .await
                                    .expect("failed to write to stream");
                            }
                        }
                    }
                    "docs" => {
                        let docs_str = "https://github.com/redis/redis-doc/blob/master/commands.md";
                        let docs_resp = format!("${}\r\n{}\r\n", docs_str.len(), docs_str);
                        stream
                            .write(docs_resp.as_bytes())
                            .await
                            .expect("failed to write to stream");
                    }
                    "info" => {
                        let arg = a[1].to_string(&bm);
                        let info_resp = server.info(&arg).to_response();
                        stream
                            .write(info_resp.as_bytes())
                            .await
                            .expect("failed to write to stream");
                    }
                    _ => unimplemented!("No other commands implemented yet"),
                }
            }
            _ => unimplemented!("No other commands implemented yet"),
        }
    }
}
