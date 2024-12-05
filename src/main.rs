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
use parser::{Parser, RedisBufSplit};

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

async fn handshake_master(stream: &mut TcpStream, server: Arc<RedisServer>) -> Result<(), Box<dyn Error>> {
    // PING command
    let ping_command = RedisValue::Array(vec![RedisValue::String("PING".to_string())]);
    send_command_and_read_response(stream, ping_command).await?;

    // REPLCONF listening-port command
    let replconf_listen_command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("listening-port".to_string()),
        RedisValue::String(server.config.port.to_string()),
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
    let resp = send_command_and_read_response(stream, psync_command).await?;
    println!("Handshake with master completed successfully.");
    // Propagation of SET commands come through this stream
    // Also the previous response might contain a bunch of SET commands
    // so we need to parse them and propagate them to the slave
    let buf = BytesMut::from(resp.as_bytes());
    let mut i = 0;
    loop {
        if let Some( (pos, word)) = Parser::token(&buf, i) {
            let word = word.to_string(&buf);
            if word.starts_with("*") {
                println!("Found SET command: {}", word);
                println!("{}", String::from_utf8_lossy(buf[i..].as_ref()));
                let bm = BytesMut::from(buf[i..].as_ref());
                server.evaluate(bm, stream, None).await;
                break;
            }
            i = pos;
        } else {
            break;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let server = RedisServer::new(env::args().collect());
    let port = server.config.port;
    let (tx, _rx) = broadcast::channel::<String>(PROPAGATE_COMMANDS_CAPACITY);
    let arc_sender = Arc::new(tx);
    let arc_server = Arc::new(server);

    // Start serving connections in a separate thread
    let server_clone = Arc::clone(&arc_server);
    let server_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        println!("Listening on port {}", port);

        loop {
            while let Ok((stream, _)) = listener.accept().await {
                println!("peer_address {:?}", stream.peer_addr().unwrap());
                let sender = Arc::clone(&arc_sender);
                let server_clone = Arc::clone(&server_clone);
                tokio::spawn(async move {
                    handle_connection(server_clone, stream, sender).await;
                });
            }
        }
    });

    let server_clone = Arc::clone(&arc_server);
    if let Some((master_host, master_port)) = server_clone.config.master_host_port.clone() {
        // Do handshake with master if this is a slave
        let master_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect((master_host.clone(), master_port.clone()))
                .await
                .expect("Failed to connect to master");
            handshake_master(&mut stream, server_clone)
                .await
                .expect("Failed to handshake with master");
            println!(
                "peer_address master_handshake {:?}",
                stream.peer_addr().unwrap()
            );
        });
        master_handle
            .await
            .expect("Failed to handshake with master");
    }
    server_handle.await.unwrap();
}
async fn handle_connection(
    server: Arc<RedisServer>,
    mut stream: TcpStream,
    tx: Arc<broadcast::Sender<String>>,
) {
    loop {
        let mut buffer = [0; 1024];
        // Read up to 1024 bytes from the stream
        let n = stream
            .read(&mut buffer)
            .await
            .expect("failed to read from stream");
        if n == 0 {
            println!("No data received");
            continue;
        }
        // Print the contents to stdout
        println!("Received: {}", String::from_utf8_lossy(&buffer));
        let bm = BytesMut::from(&buffer[0..n]);
        assert!(bm.len() > 0);
        server.evaluate(bm, &mut stream, Some(tx.clone())).await;
    }
}
