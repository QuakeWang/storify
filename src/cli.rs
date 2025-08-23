/// This module handles Command Line Interface (CLI) related logic.
use crate::error::{Error, Result};
use crate::storage::StorageClient;
use crate::utils::confirm_deletion;
use clap::{Parser, Subcommand};

/// Custom parser to validate that a path is not empty.
fn parse_validated_path(path_str: &str) -> Result<String> {
    if path_str.trim().is_empty() {
        Err(Error::InvalidPath {
            path: path_str.to_string(),
        })
    } else {
        Ok(path_str.to_string())
    }
}

/// Storify - A unified tool for managing object storage with HDFS-like interface
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
    /// List directory contents
    Ls(LsArgs),
    /// Download files from remote to local
    Get(GetArgs),
    /// Show disk usage statistics
    Du(DuArgs),
    /// Upload files from local to remote
    Put(PutArgs),
    /// Remove files/directories from remote storage
    Rm(RmArgs),
    /// Copy files/directories from remote to remote
    Cp(CpArgs),
    /// Create directories in remote storage
    Mkdir(MkdirArgs),
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

#[derive(Parser, Debug)]
pub struct CpArgs {
    /// The remote path to copy from
    #[arg(value_name = "SRC", value_parser = parse_validated_path)]
    pub src_path: String,

    /// The remote path to copy to
    #[arg(value_name = "DEST", value_parser = parse_validated_path)]
    pub dest_path: String,
}

#[derive(Parser, Debug)]
pub struct MkdirArgs {
    /// The directory path to create
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Create parent directories as needed
    #[arg(short, long)]
    pub parents: bool,
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
        Commands::Cp(cp_args) => {
            client
                .copy_files(&cp_args.src_path, &cp_args.dest_path)
                .await?;
        }
        Commands::Mkdir(mkdir_args) => {
            client
                .create_directory(&mkdir_args.path, mkdir_args.parents)
                .await?;
        }
    }
    Ok(())
}
