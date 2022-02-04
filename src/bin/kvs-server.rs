use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use anyhow::{anyhow, bail, Result};
use clap::{AppSettings, ArgEnum, Parser, Subcommand};
use kvs::engine::KvsEngine;
use kvs::KvStore;
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

    debug!("listening {:?} with storage engine {:?}", addr, engine);

    let tcp_listener = TcpListener::bind(addr)?;

    for stream in tcp_listener.incoming() {
        handle_client(stream?)?;
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
fn handle_client(mut stream: TcpStream) -> Result<()> {
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
    stream.read_exact(&mut bytes);
    let key_size = u32::from_be_bytes(bytes) as usize;

    let mut key = vec![0; key_size];
    stream.read_exact(&mut key);
    let key = String::from_utf8_lossy(&key).to_string();

    debug!("key_size: {:?}, key: {:?}", key_size, key);

    let mut kv_store = KvStore::open(std::env::current_dir()?)?;

    if method == Method::Get {
        let value = match kv_store.get(key) {
            Ok(value) => match value {
                Some(value) => value.to_owned(),
                None => "Key not found".to_owned(),
            },
            Err(e) => {
                println!("Command get failed: {:?}", e);
                bail!("Command get failed: {:?}", e);
            }
        };
        stream.write(&(value.len() as u32).to_be_bytes())?;
        stream.write(value.as_bytes())?;

        return Ok(());
    }

    if matches!(method, Method::Remove) {
        kv_store.remove(key)?;
        return Ok(());
    }

    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes);
    let value_size = u32::from_be_bytes(bytes) as usize;

    let mut value = vec![0; value_size];
    stream.read_exact(&mut value);
    let value = String::from_utf8_lossy(&value).to_string();

    debug!("value_size: {:?}, value: {:?}", value_size, value);

    if matches!(method, Method::Set) {
        kv_store.set(key, value);
        return Ok(());
    }

    bail!("handle client request failed");
}
