use clap::Parser;

use ossify::cli;
use ossify::error::Result;
use ossify::storage::StorageClient;

use ossify::cli::Args;
use ossify::config::load_storage_config;

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
