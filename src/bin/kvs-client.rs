use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::Result;
use clap::{AppSettings, Parser, Subcommand};
use kvs::engine::KvsEngine;
use kvs::KvStore;

#[derive(Parser)]
#[clap(name = "kvs-client", author, version)]
#[clap(about = "A KvStore CLI Client", long_about = None)]
struct KvsClient {
    #[clap(subcommand)]
    command: Commands,

    #[clap(long)]
    addr: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Clones repos
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    Set {
        #[clap(required = true)]
        key: String,
        #[clap(required = true)]
        value: String,
    },
    /// pushes things
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    Get {
        #[clap(required = true)]
        key: String,
    },
    /// adds things
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    Rm {
        /// Stuff to add
        #[clap(required = true)]
        key: String,
    },
}
fn main() -> Result<()> {
    let args = KvsClient::parse();
    let ip_port = args.addr.unwrap_or("127.0.0.1:4000".to_owned());
    let mut stream = TcpStream::connect(ip_port)?;

    match &args.command {
        Commands::Set { key, value } => {
            stream.write(&['s' as u8])?;
            stream.write(&(key.len() as u32).to_be_bytes())?;
            stream.write(key.as_bytes())?;
            stream.write(&(value.len() as u32).to_be_bytes())?;
            stream.write(value.as_bytes())?;
        }
        Commands::Get { key } => {
            stream.write(&['g' as u8])?;
            stream.write(&(key.len() as u32).to_be_bytes())?;
            stream.write(key.as_bytes())?;

            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes);
            let value_size = u32::from_be_bytes(bytes) as usize;

            let mut value = vec![0; value_size];
            stream.read_exact(&mut value);
            let value = String::from_utf8_lossy(&value).to_string();
            println!("{}", value);
        }
        Commands::Rm { key } => {
            stream.write(&['r' as u8])?;
            stream.write(&(key.len() as u32).to_be_bytes())?;
            stream.write(key.as_bytes())?;
        }
    }

    Ok(())
}
