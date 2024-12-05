#![allow(unused_imports)]
use bytes::BytesMut;
use redis_starter_rust::{cast, ThreadPool};
use std::{
    env, fs,
    io::{prelude::*, BufReader, ErrorKind},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
    vec,
};
mod parser;
use parser::RedisBufSplit;

mod server;
use server::{RedisServer, RedisValue};

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

const PROPAGATE_COMMANDS_CAPACITY: usize = 10;

async fn send_command_and_read_response(
    stream: &mut TcpStream,
    command: RedisValue,
) -> Result<String, Box<dyn Error>> {
    // Send the command
    stream.write_all(command.to_response().as_bytes()).await?;
    stream.flush().await?;
    println!("Sent command: {:?}", command);

    // Read the response
    let mut buf = [0; 1024];
    stream.readable().await?;
    let n = stream.read(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf[..n]).to_string();

    println!("Received response: {}", response);

    Ok(response)
}

async fn handshake_master(stream: &mut TcpStream, port: u16) -> Result<(), Box<dyn Error>> {
    // PING command
    let ping_command = RedisValue::Array(vec![RedisValue::String("PING".to_string())]);
    send_command_and_read_response(stream, ping_command).await?;

    // REPLCONF listening-port command
    let replconf_listen_command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("listening-port".to_string()),
        RedisValue::String(port.to_string()),
    ]);
    send_command_and_read_response(stream, replconf_listen_command).await?;

    // REPLCONF capa command
    let replconf_capa_command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("capa".to_string()),
        RedisValue::String("psync2".to_string()),
    ]);
    send_command_and_read_response(stream, replconf_capa_command).await?;

    // PSYNC command
    let psync_command = RedisValue::Array(vec![
        RedisValue::String("PSYNC".to_string()),
        RedisValue::String("?".to_string()),
        RedisValue::String("-1".to_string()),
    ]);
    send_command_and_read_response(stream, psync_command).await?;

    println!("Handshake with master completed successfully.");
    Ok(())
}

#[tokio::main]
async fn main() {
    let server = RedisServer::new(env::args().collect());
    let port = server.config.port;
    let (tx, _rx) = broadcast::channel::<String>(PROPAGATE_COMMANDS_CAPACITY);
    let sender = Arc::new(tx);
    let server = Arc::new(server);

    if let Some((master_host, master_port)) = server.config.master_host_port.clone() {
        let server = Arc::clone(&server);
        // Do handshake with master if this is a slave
        let master_handle = tokio::spawn(async move {
            // Use basic TCP connection to send PING command to master
            let mut stream = TcpStream::connect((master_host.clone(), master_port.clone()))
                .await
                .expect("Failed to connect to master");
            handshake_master(&mut stream, server.config.port)
                .await
                .expect("Failed to handshake with master");
        });
        master_handle
            .await
            .expect("Failed to handshake with master");
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    println!("Listening on port {}", port);

    loop {
        while let Ok((stream, _)) = listener.accept().await {
            let server = Arc::clone(&server);
            let sender = Arc::clone(&sender);
            tokio::spawn(async move {
                handle_connection(server, stream, sender).await;
            });
        }
    }
}

async fn handle_connection(server: Arc<RedisServer>, mut stream: TcpStream, tx: Arc<broadcast::Sender<String>>) {
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
        server.evaluate(bm, &mut stream, tx.clone()).await;
    }
}
