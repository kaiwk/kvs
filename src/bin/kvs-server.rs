use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;

use anyhow::{bail, Result};
use clap::{ArgEnum, Parser};
use kvs::engine::KvsEngine;
use kvs::kvs::EngineError;
use kvs::thread_pool::*;
use kvs::{KvStore, SledEngine};
use log::debug;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum, Debug)]
enum Engine {
    Kvs,
    Sled,
}

#[derive(Parser)]
#[clap(name = "kvs-server", author, version)]
#[clap(about = "A KvStore CLI Server", long_about = None)]
struct KvsServer {
    #[clap(long)]
    addr: Option<String>,

    #[clap(long, arg_enum)]
    engine: Option<Engine>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = KvsServer::parse();
    let addr = args.addr.unwrap_or("127.0.0.1:4000".to_owned());
    let engine = args.engine.unwrap_or(Engine::Kvs);

    debug!("kvs-server version: {:?}", env!("CARGO_PKG_VERSION"));
    debug!("listening {:?} with storage engine {:?}", addr, engine);

    let tcp_listener = TcpListener::bind(addr)?;
    let socket = tcp_listener.as_raw_fd();

    // Keep trying to set socket non-blocking until we succeed
    loop {
        if tcp_listener.set_nonblocking(true).is_ok() {
            break;
        }
    }

    let thread_pool = NaiveThreadPool::new(4)?;

    for stream in tcp_listener.incoming() {
        match stream {
            Ok(stream) => match engine {
                Engine::Kvs => thread_pool.spawn(|| {
                    debug!("spawn job in thread: {:?}", std::thread::current().id());
                    handle_client(
                        KvStore::open(std::env::current_dir().unwrap()).unwrap(),
                        stream,
                    )
                    .unwrap()
                }),
                Engine::Sled => thread_pool.spawn(|| {
                    handle_client(
                        SledEngine::open(std::env::current_dir().unwrap()).unwrap(),
                        stream,
                    )
                    .unwrap()
                }),
            },
            Err(err) => {
                if err.kind() != std::io::ErrorKind::WouldBlock {
                    break;
                }
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    unsafe {
        libc::shutdown(socket, libc::SHUT_RD);
    }

    Ok(())
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ArgEnum, Debug)]
enum Method {
    Set,
    Get,
    Remove,
}

/// The protocol is simple:
/// we use first byte to indicate method:
/// 's' 0x73 -> `Set`
/// 'g' 0x67 -> `Get`
/// 'r' 0x72 -> `Remove`
/// and 4 bytes to indicate key size, followed by key,
/// and 4 bytes to indicate value size(if value exist), followed by value
/// return status code:
/// 0x00 -> success
/// 0x01 -> failed,
/// followed by the returned value size and value it self
fn handle_client<T: KvsEngine>(engine: T, mut stream: TcpStream) -> Result<()> {
    let mut bytes = [0; 1];
    stream.read_exact(&mut bytes)?;

    let method = bytes[0] as char;

    let method = match method {
        's' => Method::Set,
        'g' => Method::Get,
        'r' => Method::Remove,
        _ => bail!("Invalid method"),
    };

    debug!("method: {:?}", method);

    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes)?;
    let key_size = u32::from_be_bytes(bytes) as usize;

    let mut key = vec![0; key_size];
    stream.read_exact(&mut key)?;
    let key = String::from_utf8_lossy(&key).to_string();

    debug!("key_size: {:?}, key: {:?}", key_size, key);

    if method == Method::Get {
        let value = match engine.get(key) {
            Ok(value) => match value {
                Some(value) => {
                    stream.write(&0_u8.to_be_bytes())?;
                    value.to_owned()
                }
                None => {
                    stream.write(&1_u8.to_be_bytes())?;
                    "Key not found".to_owned()
                }
            },
            Err(e) => {
                stream.write(&1_u8.to_be_bytes())?;
                bail!("Command get failed: {:?}", e);
            }
        };

        stream.write(&(value.len() as u32).to_be_bytes())?;
        stream.write(value.as_bytes())?;

        return Ok(());
    }

    if matches!(method, Method::Remove) {
        if let Err(e) = engine.remove(key) {
            if matches!(e, EngineError::NotFound(_)) {
                let value = "Key not found".to_owned();

                stream.write(&1_u8.to_be_bytes())?;
                stream.write(&(value.len() as u32).to_be_bytes())?;
                stream.write(value.as_bytes())?;
            }
        }

        stream.write(&0_u8.to_be_bytes())?;
        return Ok(());
    }

    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes)?;
    let value_size = u32::from_be_bytes(bytes) as usize;

    let mut value = vec![0; value_size];
    stream.read_exact(&mut value)?;
    let value = String::from_utf8_lossy(&value).to_string();

    debug!("value_size: {:?}, value: {:?}", value_size, value);

    if matches!(method, Method::Set) {
        engine.set(key, value)?;
        stream.write(&0_u8.to_be_bytes())?;
        return Ok(());
    }

    bail!("handle client request failed");
}
