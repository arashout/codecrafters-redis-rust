use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
};

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod server;
use server::{RedisServer, RedisValue};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, Clone)]
pub enum PoolCreationError {
    ZeroSize,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }
    pub fn build(size: usize) -> Result<ThreadPool, PoolCreationError> {
        match size {
            0 => Err(PoolCreationError::ZeroSize),
            _ => Ok(ThreadPool::new(size)),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender
            .send(job)
            .expect("failed to send job to worker pool");
    }
}

pub struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let job = receiver.lock().unwrap().recv().unwrap();

            println!("Worker {id} got a job; executing.");

            job();
        });

        Worker { id, thread }
    }
}

#[macro_export]
macro_rules! cast {
        ($target: expr, $pat: path) => {
            {
                if let $pat(a) = $target { // #1
                    a
                } else {
                    panic!(
                        "mismatch variant when cast to {}", 
                        stringify!($pat)); // #2
                }
            }
        };
    }


pub async fn send_command_and_read_response(
    stream: &mut TcpStream, 
    command: RedisValue
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


