use anyhow::Result;
use clap::{AppSettings, Parser, Subcommand};
use kvs::KvStore;

#[derive(Parser)]
#[clap(name = "kvs", author, version)]
#[clap(about = "A KvStore CLI", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
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
    let args = Cli::parse();

    let mut kv_store = KvStore::open(std::env::current_dir()?)?;

    match &args.command {
        Commands::Set { key, value } => {
            kv_store.set(key.to_owned(), value.to_owned());
        }
        Commands::Get { key } => match kv_store.get(key.to_owned()) {
            Ok(val) => match val {
                Some(val) => println!("{}", val),
                None => println!("Key not found"),
            },
            Err(e) => {
                println!("Command get failed: {:?}", e);
            }
        },
        Commands::Rm { key } => {
            kv_store.remove(key.to_owned())?;
        }
    }

    Ok(())
}
