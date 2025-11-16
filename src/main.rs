mod cli;
mod config;
mod error;
mod storage;
mod utils;

#[cfg(test)]
mod tests;

use clap::Parser;

use crate::cli::{Args, run};

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Err(e) = run(args).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
