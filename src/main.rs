#![allow(unused_imports)]
use bytes::BytesMut;
use redis_starter_rust::{cast, ThreadPool};
use std::{
    collections::hash_map, env, fs, io::{prelude::*, BufReader, ErrorKind}, sync::{mpsc, Arc, Mutex}, thread, time::Duration, vec
};
mod parser;
use parser::{Parser, RedisBufSplit};

mod server;
use server::{RedisServer, RedisValue};

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

mod log;
use log::Logger;

const PROPAGATE_COMMANDS_CAPACITY: usize = 10;

async fn send_command_and_read_response(
    logger: &Logger,
    stream: &mut TcpStream,
    command: RedisValue,
) -> Result<String, Box<dyn Error>> {
    // Send the command
    stream.write_all(command.to_response().as_bytes()).await?;
    stream.flush().await?;
    logger.log(&format!("Sent command: {:?}", command));

    // Read the response
    let mut buf = [0; 1024];
    stream.readable().await?;
    let n = stream.read(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf[..n]).to_string();
    logger.log(&format!("Received response: {}", response));

    Ok(response)
}

async fn handshake_master(logger: &Logger, stream: &mut TcpStream, server: Arc<RedisServer>) -> Result<(), Box<dyn Error>> {
    // PING command
    let ping_command = RedisValue::Array(vec![RedisValue::String("PING".to_string())]);
    send_command_and_read_response(&logger, stream, ping_command).await?;

    // REPLCONF listening-port command
    let replconf_listen_command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("listening-port".to_string()),
        RedisValue::String(server.config.port.to_string()),
    ]);
    send_command_and_read_response(&logger, stream, replconf_listen_command).await?;

    // REPLCONF capa command
    let replconf_capa_command = RedisValue::Array(vec![
        RedisValue::String("REPLCONF".to_string()),
        RedisValue::String("capa".to_string()),
        RedisValue::String("psync2".to_string()),
    ]);
    send_command_and_read_response(&logger, stream, replconf_capa_command).await?;

    // PSYNC command
    let psync_command = RedisValue::Array(vec![
        RedisValue::String("PSYNC".to_string()),
        RedisValue::String("?".to_string()),
        RedisValue::String("-1".to_string()),
    ]);
    let resp = send_command_and_read_response(&logger, stream, psync_command).await?;
    logger.log("Handshake with master completed successfully.");
    // Propagation of SET commands come through this stream
    // Also the previous response might contain a bunch of SET commands
    // so we need to parse them and propagate them to the slave
    let buf = BytesMut::from(resp.as_bytes());
    let mut i = 0;
    loop {
        if let Some( (pos, word)) = Parser::token(&buf, i) {
            let word = word.to_string(&buf);
            if word.starts_with("*") {
                logger.log(&format!("Found SET command: {}", word));
                println!("{}", String::from_utf8_lossy(buf[i..].as_ref()));
                let bm = BytesMut::from(buf[i..].as_ref());
                server.evaluate(&logger, bm, stream, None).await;
                break;
            }
            i = pos;
        } else {
            break;
        }
    }
    // Try and poll the stream for new commands
    loop {
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let buf = BytesMut::from(&buf[..n]);
        server.evaluate(&logger, buf, stream, None).await;
    }
    logger.log("Closing handshake connection with master.");

    Ok(())
}

#[tokio::main]
async fn main() {
    let server = RedisServer::new(env::args().collect());
    let port = server.config.port;
    let (tx, _rx) = broadcast::channel::<String>(PROPAGATE_COMMANDS_CAPACITY);
    let arc_sender = Arc::new(tx);
    let arc_server = Arc::new(server);

    let mut logger = Logger::new();
    logger = logger.with("replica", &arc_server.clone().config.master_host_port.is_some().to_string());

    // Start serving connections in a separate thread
    let server_clone = Arc::clone(&arc_server);
    let server_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        logger.log(&format!("Listening on port {}", port));

        loop {
            while let Ok((stream, _)) = listener.accept().await {
                logger.log(&format!("Accepted connection from {}", stream.peer_addr().unwrap()));
                let sender = Arc::clone(&arc_sender);
                let server_clone = Arc::clone(&server_clone);
                tokio::spawn(async move {
                    let logger = Logger{
                        kv: hash_map::HashMap::new(),
                    };
                    let logger = logger.with("replica", &server_clone.config.master_host_port.is_some().to_string());
                    handle_connection(&logger, server_clone, stream, sender).await;
                });
            }
        }
    });

    let server_clone = Arc::clone(&arc_server);
    if let Some((master_host, master_port)) = server_clone.config.master_host_port.clone() {
        // Do handshake with master if this is a slave
        let logger = Logger::new();
        let logger = logger.with("replica", "true");
        let master_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect((master_host.clone(), master_port.clone()))
                .await
                .expect("Failed to connect to master");
            handshake_master(&logger, &mut stream, server_clone)
                .await
                .expect("Failed to handshake with master");
            logger.log(&format!("peer_address master_handshake {:?}", stream.peer_addr().unwrap()));

        });
        master_handle
            .await
            .expect("Failed to handshake with master");
    }
    server_handle.await.unwrap();
}
async fn handle_connection(
    logger: &Logger,
    server: Arc<RedisServer>,
    mut stream: TcpStream,
    tx: Arc<broadcast::Sender<String>>,
) {
    loop {
        let mut buffer = [0; 1024];
        if stream.readable().await.is_err(){
            logger.log("Stream not readable");
            break;
        }
        // Read up to 1024 bytes from the stream
        let n = stream
            .read(&mut buffer)
            .await
            .expect("failed to read from stream");
        if n == 0 {
            continue;
        }
        // Print the contents to stdout
        logger.log(&format!("Received: {}", String::from_utf8_lossy(&buffer)));
        let bm = BytesMut::from(&buffer[0..n]);
        assert!(bm.len() > 0);
        server.evaluate(&logger, bm, &mut stream, Some(tx.clone())).await;
    }
}
