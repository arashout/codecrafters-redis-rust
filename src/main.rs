#![allow(unused_imports)]
use bytes::BytesMut;
use redis_starter_rust::{cast, ThreadPool};
use std::{
    collections::hash_map,
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

async fn replication_connection(
    logger: &Logger,
    stream: &mut TcpStream,
    server: Arc<RedisServer>,
) -> Result<(), Box<dyn Error>> {
    /// This is the connection that handles the handshake between master and slace
    /// The established connection is used to send the replication data to the slave
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

    // The FULLLRESYNC response might also contain RESP ARRAY commands that need to be evaluated like "SET" or "REPLCONF GETACK"
    let mut replicated_bytes_count = 0;

    let buf = BytesMut::from(resp.as_bytes());
    loop {
        if let Some(pos) = Parser::find_start_resp_data_type(&buf, 0, &parser::RESPDataType::Array)
        {
            logger.log(&format!(
                "Found Redis Array start in FULLRESYNC response: {}",
                String::from_utf8_lossy(&buf[pos..])
            ));
            let bm = BytesMut::from(buf[pos..].as_ref());
            replicated_bytes_count = server.evaluate(&logger, bm, stream, None, replicated_bytes_count).await;
            break;
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
        // TODO: This is probably fairly inefficient
        let buf = BytesMut::from(&buf[..n]);
        replicated_bytes_count += server.evaluate(&logger, buf, stream, None, replicated_bytes_count).await;
    }
    logger.log("Closing handshake connection with master.");

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = env::args().collect::<Vec<String>>();
    let server = RedisServer::new(&args);
    let port = server.config.port;
    let config = server.config.clone();
    let (tx, _rx) = broadcast::channel::<String>(PROPAGATE_COMMANDS_CAPACITY);
    let arc_sender = Arc::new(tx);
    let arc_server = Arc::new(server);

    let mut logger = Logger::new();
    logger = logger.with("replica", &config.master_host_port.is_some().to_string());

    // Start serving connections in a separate thread
    let server_clone = Arc::clone(&arc_server);
    let server_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        logger.log(&format!("Listening on port {}", port));

        loop {
            while let Ok((stream, _)) = listener.accept().await {
                logger.log(&format!(
                    "Accepted connection from {}",
                    stream.peer_addr().unwrap()
                ));
                let sender = Arc::clone(&arc_sender);
                let server_clone = Arc::clone(&server_clone);
                tokio::spawn(async move {
                    let logger = Logger {
                        kv: hash_map::HashMap::new(),
                    };
                    let logger = logger.with("replica", &config.is_replica.to_string());
                    handle_connection(&logger, &server_clone, stream, sender).await;
                });
            }
        }
    });

    // If this is a replica, do the handshake with the master and create a separate thread to handle replication
    let server_clone = Arc::clone(&arc_server);
    if config.is_replica {
        // Do handshake with master if this is a slave
        let logger = Logger::new();
        let logger = logger.with("replica", "true");
        let (master_host, master_port) = config.master_host_port.unwrap();

        let master_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect((master_host, master_port))
                .await
                .expect("Failed to connect to master");
            replication_connection(&logger, &mut stream, server_clone)
                .await
                .expect("Failed to handshake with master");
            logger.log(&format!(
                "peer_address master_handshake {:?}",
                stream.peer_addr().unwrap()
            ));
        });
        master_handle
            .await
            .expect("Failed to handshake with master");
    }
    server_handle.await.unwrap();
}

async fn handle_connection<'a>(
    logger: &Logger,
    server: &Arc<RedisServer>,
    mut stream: TcpStream,
    tx: Arc<broadcast::Sender<String>>,
) {
    loop {
        let mut buffer = [0; 1024];
        if stream.readable().await.is_err() {
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
        server
            .evaluate(&logger, bm, &mut stream, Some(tx.clone()), 0)
            .await;
    }
}
