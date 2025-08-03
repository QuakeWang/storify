/// This module handles Command Line Interface (CLI) related logic.
use crate::error::Result;
use crate::storage::StorageClient;
use crate::utils::confirm_deletion;
use clap::{Parser, Subcommand};

/// Custom parser to validate that a path is not empty.
fn parse_validated_path(path_str: &str) -> std::result::Result<String, String> {
    if path_str.trim().is_empty() {
        Err("Path cannot be empty or just whitespace".to_string())
    } else {
        Ok(path_str.to_string())
    }
}

/// Ossify - A unified tool for managing object storage with HDFS-like interface
#[derive(Parser, Debug)]
#[command(
    version = "0.1.0",
    author = "WangErxi",
    about = "A unified tool for managing object storage (OSS, S3, etc.)",
    after_help = "Enjoy the unified experience!"
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List directory contents (equivalent to hdfs dfs -ls)
    Ls(LsArgs),
    /// Download files from remote to local (equivalent to hdfs dfs -get)
    Get(GetArgs),
    /// Show disk usage statistics (equivalent to hdfs dfs -du)
    Du(DuArgs),
    /// Upload files from local to remote (equivalent to hdfs dfs -put)
    Put(PutArgs),
    /// Remove files/directories from remote storage (equivalent to hdfs dfs -rm)
    Rm(RmArgs),
}

#[derive(Parser, Debug)]
pub struct LsArgs {
    /// The path to list
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Show detailed information (long format)
    #[arg(short = 'L', long)]
    pub long: bool,

    /// Process directories recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,
}

#[derive(Parser, Debug)]
pub struct GetArgs {
    /// The remote path to download from
    #[arg(value_name = "REMOTE", value_parser = parse_validated_path)]
    pub remote: String,

    /// The local path to download to
    #[arg(value_name = "LOCAL", value_parser = parse_validated_path)]
    pub local: String,
}

#[derive(Parser, Debug)]
pub struct DuArgs {
    /// The path to check usage for
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Show summary only
    #[arg(short = 's', long)]
    pub summary: bool,
}

#[derive(Parser, Debug)]
pub struct PutArgs {
    /// The local path to upload from
    #[arg(value_name = "LOCAL", value_parser = parse_validated_path)]
    pub local: String,

    /// The remote path to upload to
    #[arg(value_name = "REMOTE", value_parser = parse_validated_path)]
    pub remote: String,

    /// Process directories recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,
}

#[derive(Parser, Debug)]
pub struct RmArgs {
    /// Remote path(s) to delete
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Remove directories and their contents recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,

    /// Force deletion without confirmation
    #[arg(short = 'f', long)]
    pub force: bool,
}

pub async fn run(args: Args, client: StorageClient) -> Result<()> {
    match args.command {
        Commands::Ls(ls_args) => {
            client
                .list_directory(&ls_args.path, ls_args.long, ls_args.recursive)
                .await?;
        }
        Commands::Get(get_args) => {
            client
                .download_files(&get_args.remote, &get_args.local)
                .await?;
        }
        Commands::Du(du_args) => {
            client.disk_usage(&du_args.path, du_args.summary).await?;
        }
        Commands::Put(put_args) => {
            client
                .upload_files(&put_args.local, &put_args.remote, put_args.recursive)
                .await?;
        }
        Commands::Rm(rm_args) => {
            if !confirm_deletion(&rm_args.paths, rm_args.force)? {
                println!("Operation cancelled.");
                return Ok(());
            }
            client
                .delete_files(&rm_args.paths, rm_args.recursive)
                .await?;
        }
    }
    Ok(())
}
