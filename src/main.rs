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
    init_logging();

    let args = Args::parse();

    if let Err(e) = run(args).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn init_logging() {
    let _ = tracing_log::LogTracer::init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}
