use anyhow::Result;
use clap::Parser;

mod cli;
mod config;
mod storage;
mod utils;

use cli::Args;
use config::load_storage_config;
use storage::StorageClient;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = load_storage_config()?;

    let client = StorageClient::new(config).await?;

    cli::run(args, client).await?;

    Ok(())
}
