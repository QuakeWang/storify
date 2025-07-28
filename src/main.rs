use anyhow::Result;
use clap::Parser;
use std::env;
use std::str::FromStr;

mod storage;
use storage::{StorageClient, StorageConfig, StorageProvider};

/// Ossify - A unified tool for managing object storage with HDFS-like interface
#[derive(Parser, Debug)]
#[command(
    version = "0.1.0",
    author = "WangErxi",
    about = "ossify is a unified tool for managing object storage (OSS, S3, etc.)"
)]
struct Args {
    /// List directory contents (equivalent to hdfs dfs -ls)
    #[arg(short = 'l', long = "ls", value_name = "PATH")]
    ls: Option<String>,

    /// Download files from remote to local (equivalent to hdfs dfs -get)
    #[arg(short = 'g', long = "get", num_args = 2, value_names = ["REMOTE", "LOCAL"])]
    get: Option<Vec<String>>,

    /// Show disk usage statistics (equivalent to hdfs dfs -du)
    #[arg(short = 'd', long = "du", value_name = "PATH")]
    du: Option<String>,

    /// Show detailed information (long format)
    #[arg(short = 'L', long = "long")]
    long: bool,

    /// Process directories recursively
    #[arg(short = 'R', long = "recursive")]
    recursive: bool,

    /// Show summary only (for du command)
    #[arg(short = 's', long = "summary")]
    summary: bool,

    /// Upload files from local to remote
    #[arg(short = 'p', long = "put", num_args = 2, value_names = ["LOCAL", "REMOTE"])]
    put: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load storage configuration from environment
    let config = load_storage_config()?;
    let client = StorageClient::new(config).await?;

    // Execute command based on provided flags
    match () {
        _ if args.ls.is_some() => {
            let path = args.ls.unwrap();
            validate_path(&path)?;
            client
                .list_directory(&path, args.long, args.recursive)
                .await?;
        }
        _ if args.get.is_some() => {
            let paths = args.get.unwrap();
            validate_path(&paths[0])?;
            validate_path(&paths[1])?;
            client.download_files(&paths[0], &paths[1]).await?;
        }
        _ if args.du.is_some() => {
            let path = args.du.unwrap();
            validate_path(&path)?;
            client.disk_usage(&path, args.summary).await?;
        }
        _ if args.put.is_some() => {
            let paths = args.put.unwrap();
            validate_path(&paths[0])?;
            validate_path(&paths[1])?;
            client
                .upload_files(&paths[0], &paths[1], args.recursive)
                .await?;
        }
        _ => {
            eprintln!("Error: No command specified. Use --help for usage information.");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Load storage configuration from environment variables
fn load_storage_config() -> Result<StorageConfig> {
    let provider_str = env::var("STORAGE_PROVIDER").unwrap_or_else(|_| "oss".to_string());
    let provider = StorageProvider::from_str(&provider_str)?;

    match provider {
        StorageProvider::Oss => load_oss_config(),
        StorageProvider::S3 => load_s3_config(&provider_str),
        StorageProvider::Fs => load_fs_config(),
    }
}

/// Load OSS (Alibaba Cloud) configuration
fn load_oss_config() -> Result<StorageConfig> {
    let bucket = env::var("STORAGE_BUCKET")
        .or_else(|_| env::var("OSS_BUCKET"))
        .map_err(|_| {
            anyhow::anyhow!("STORAGE_BUCKET or OSS_BUCKET environment variable is required")
        })?;

    let access_key_id = env::var("STORAGE_ACCESS_KEY_ID")
        .or_else(|_| env::var("OSS_ACCESS_KEY_ID"))
        .map_err(|_| {
            anyhow::anyhow!(
                "STORAGE_ACCESS_KEY_ID or OSS_ACCESS_KEY_ID environment variable is required"
            )
        })?;

    let access_key_secret = env::var("STORAGE_ACCESS_KEY_SECRET")
        .or_else(|_| env::var("OSS_ACCESS_KEY_SECRET"))
        .map_err(|_| anyhow::anyhow!("STORAGE_ACCESS_KEY_SECRET or OSS_ACCESS_KEY_SECRET environment variable is required"))?;

    let region = env::var("STORAGE_REGION")
        .or_else(|_| env::var("OSS_REGION"))
        .ok();

    let endpoint = env::var("STORAGE_ENDPOINT")
        .or_else(|_| env::var("OSS_ENDPOINT"))
        .unwrap_or_else(|_| "https://oss-cn-hangzhou.aliyuncs.com".to_string());

    let mut config = StorageConfig::oss(bucket, access_key_id, access_key_secret, region);
    config.endpoint = Some(endpoint);
    Ok(config)
}

/// Load S3 (AWS) configuration
fn load_s3_config(provider_str: &str) -> Result<StorageConfig> {
    let is_minio = provider_str.to_lowercase() == "minio";

    let bucket = if is_minio {
        env::var("STORAGE_BUCKET")
            .or_else(|_| env::var("MINIO_BUCKET"))
            .map_err(|_| {
                anyhow::anyhow!("STORAGE_BUCKET or MINIO_BUCKET environment variable is required")
            })?
    } else {
        env::var("STORAGE_BUCKET")
            .or_else(|_| env::var("AWS_S3_BUCKET"))
            .map_err(|_| {
                anyhow::anyhow!("STORAGE_BUCKET or AWS_S3_BUCKET environment variable is required")
            })?
    };

    let access_key_id = if is_minio {
        env::var("STORAGE_ACCESS_KEY_ID")
            .or_else(|_| env::var("MINIO_ACCESS_KEY"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "STORAGE_ACCESS_KEY_ID or MINIO_ACCESS_KEY environment variable is required"
                )
            })?
    } else {
        env::var("STORAGE_ACCESS_KEY_ID")
            .or_else(|_| env::var("AWS_ACCESS_KEY_ID"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "STORAGE_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID environment variable is required"
                )
            })?
    };

    let secret_access_key = if is_minio {
        env::var("STORAGE_ACCESS_KEY_SECRET")
            .or_else(|_| env::var("MINIO_SECRET_KEY"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "STORAGE_ACCESS_KEY_SECRET or MINIO_SECRET_KEY environment variable is required"
                )
            })?
    } else {
        env::var("STORAGE_ACCESS_KEY_SECRET")
            .or_else(|_| env::var("AWS_SECRET_ACCESS_KEY"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "STORAGE_ACCESS_KEY_SECRET or AWS_SECRET_ACCESS_KEY environment variable is required"
                )
        })?
    };

    let region = if is_minio {
        env::var("STORAGE_REGION")
            .or_else(|_| env::var("MINIO_DEFAULT_REGION"))
            .ok()
    } else {
        env::var("STORAGE_REGION")
            .or_else(|_| env::var("AWS_DEFAULT_REGION"))
            .ok()
    };

    let endpoint = if is_minio {
        Some(
            env::var("STORAGE_ENDPOINT")
                .or_else(|_| env::var("MINIO_ENDPOINT"))
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
        )
    } else {
        env::var("STORAGE_ENDPOINT").ok()
    };

    if is_minio {
        let mut config = StorageConfig::s3(bucket, access_key_id, secret_access_key, region);
        config.endpoint = endpoint;
        Ok(config)
    } else {
        Ok(StorageConfig::s3(
            bucket,
            access_key_id,
            secret_access_key,
            region,
        ))
    }
}

/// Load filesystem configuration (for testing)
fn load_fs_config() -> Result<StorageConfig> {
    let root_path = env::var("STORAGE_ROOT_PATH").unwrap_or_else(|_| "./storage".to_string());
    Ok(StorageConfig::fs(root_path))
}

/// Validate that the path is not empty or whitespace-only
fn validate_path(path: &str) -> Result<()> {
    if path.trim().is_empty() {
        return Err(anyhow::anyhow!("Path cannot be empty"));
    }
    Ok(())
}
