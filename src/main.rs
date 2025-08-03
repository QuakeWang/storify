use clap::Parser;

mod cli;
mod config;
mod error;
mod storage;
mod utils;

use cli::Args;
use config::load_storage_config;
use error::Result;
use storage::StorageClient;

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Err(e) = run_app(args).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run_app(args: Args) -> Result<()> {
    let config = load_storage_config()?;
    let client = StorageClient::new(config).await?;
    cli::run(args, client).await?;
    Ok(())
}
