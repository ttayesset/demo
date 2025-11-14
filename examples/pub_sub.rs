use std::env;
use std::error::Error;
use std::thread;
use std::time::Duration;

use shared_shmem_queue::{PopError, QueueError, SharedMpmcQueue};

const SHARED_NAME: &str = "/shared-mpmc-example";
const DEFAULT_CAPACITY: usize = 256;
const DEFAULT_MESSAGES: u32 = 32;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let role = args.next().unwrap_or_else(|| {
        eprintln!("Usage: cargo run --example pub_sub -- <pub|sub> [messages]");
        std::process::exit(1);
    });
    let messages: u32 = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_MESSAGES);

    match role.as_str() {
        "pub" | "publisher" => run_publisher(messages)?,
        "sub" | "subscriber" => run_subscriber(messages)?,
        _ => {
            eprintln!("Unknown role '{role}'. Use 'pub' or 'sub'.");
            std::process::exit(1);
        }
    }

    Ok(())
}

fn open_or_create_queue() -> Result<SharedMpmcQueue<u32>, QueueError> {
    match SharedMpmcQueue::create(SHARED_NAME, DEFAULT_CAPACITY) {
        Ok(queue) => Ok(queue),
        Err(QueueError::Shmem(_)) => SharedMpmcQueue::open(SHARED_NAME),
        Err(err) => Err(err),
    }
}

fn run_publisher(count: u32) -> Result<(), Box<dyn Error>> {
    let queue = open_or_create_queue()?;

    for value in 0..count {
        loop {
            match queue.try_push(value) {
                Ok(()) => {
                    println!("published {value}");
                    break;
                }
                Err(_) => thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    Ok(())
}

fn run_subscriber(count: u32) -> Result<(), Box<dyn Error>> {
    let queue = SharedMpmcQueue::open(SHARED_NAME)?;
    let mut received = 0;

    while received < count {
        match queue.try_pop() {
            Ok(value) => {
                println!("received {value}");
                received += 1;
            }
            Err(PopError::Empty) => thread::sleep(Duration::from_millis(5)),
        }
    }

    Ok(())
}
