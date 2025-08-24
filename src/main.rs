use clap::Parser;

use storify::cli;
use storify::error::Result;
use storify::storage::StorageClient;

use storify::cli::Args;
use storify::config::load_storage_config;

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
