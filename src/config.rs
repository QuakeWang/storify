use crate::error::{Error, Result};
use crate::storage::constants::DEFAULT_FS_ROOT;
use crate::storage::{StorageConfig, StorageProvider};
use log::warn;
use std::env;
use std::str::FromStr;

/// Read the first available environment variable from a list of keys
fn env_any(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(val) = env::var(key) {
            return Some(val);
        }
    }
    None
}

/// Read a required environment variable from a list of keys
fn env_any_required(keys: &[&str]) -> Result<String> {
    env_any(keys).ok_or_else(|| Error::MissingEnvVar {
        key: keys.join(" or "),
    })
}

/// Provider-specific environment variable keys
struct ProviderKeys {
    bucket: Vec<&'static str>,
    access_key_id: Vec<&'static str>,
    secret_key: Vec<&'static str>,
    region: Vec<&'static str>,
    endpoint: Vec<&'static str>,
}

impl ProviderKeys {
    fn for_oss() -> Self {
        Self {
            bucket: vec!["STORAGE_BUCKET", "OSS_BUCKET"],
            access_key_id: vec!["STORAGE_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID"],
            secret_key: vec!["STORAGE_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET"],
            region: vec!["STORAGE_REGION", "OSS_REGION"],
            endpoint: vec!["STORAGE_ENDPOINT", "OSS_ENDPOINT"],
        }
    }

    fn for_aws() -> Self {
        Self {
            bucket: vec!["STORAGE_BUCKET", "AWS_S3_BUCKET"],
            access_key_id: vec!["STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"],
            secret_key: vec!["STORAGE_ACCESS_KEY_SECRET", "AWS_SECRET_ACCESS_KEY"],
            region: vec!["STORAGE_REGION", "AWS_DEFAULT_REGION"],
            endpoint: vec!["STORAGE_ENDPOINT"],
        }
    }

    fn for_minio() -> Self {
        Self {
            bucket: vec!["STORAGE_BUCKET", "MINIO_BUCKET"],
            access_key_id: vec!["STORAGE_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"],
            secret_key: vec!["STORAGE_ACCESS_KEY_SECRET", "MINIO_SECRET_KEY"],
            region: vec!["STORAGE_REGION", "MINIO_DEFAULT_REGION"],
            endpoint: vec!["STORAGE_ENDPOINT", "MINIO_ENDPOINT"],
        }
    }
}

/// Select appropriate ProviderKeys for S3-like providers (AWS/MinIO)
fn s3_like_keys(provider_str: &str) -> ProviderKeys {
    if provider_str.eq_ignore_ascii_case("minio") {
        ProviderKeys::for_minio()
    } else {
        ProviderKeys::for_aws()
    }
}

/// Load storage configuration from environment variables
pub fn load_storage_config() -> Result<StorageConfig> {
    let provider_str = env::var("STORAGE_PROVIDER").unwrap_or_else(|_| {
        warn!("STORAGE_PROVIDER not set, using default: oss");
        "oss".to_string()
    });
    let provider = StorageProvider::from_str(&provider_str)?;

    match provider {
        StorageProvider::Oss => load_cloud_config(ProviderKeys::for_oss(), StorageConfig::oss),
        StorageProvider::S3 => load_cloud_config(s3_like_keys(&provider_str), StorageConfig::s3),
        StorageProvider::Fs => load_fs_config(),
        StorageProvider::Hdfs => load_hdfs_config(),
    }
}

/// Load configuration for any cloud storage provider
fn load_cloud_config<F>(keys: ProviderKeys, config_constructor: F) -> Result<StorageConfig>
where
    F: FnOnce(String, String, String, Option<String>) -> StorageConfig,
{
    let bucket = env_any_required(&keys.bucket)?;
    let access_key_id = env_any_required(&keys.access_key_id)?;
    let secret_key = env_any_required(&keys.secret_key)?;

    let region = env_any(&keys.region);
    let endpoint = env_any(&keys.endpoint);

    let mut config = config_constructor(bucket, access_key_id, secret_key, region);
    config.endpoint = endpoint;
    Ok(config)
}

/// Load HDFS configuration
fn load_hdfs_config() -> Result<StorageConfig> {
    let name_node = env::var("HDFS_NAME_NODE").map_err(|_| Error::MissingEnvVar {
        key: "HDFS_NAME_NODE".to_string(),
    })?;
    let root_path = env::var("HDFS_ROOT_PATH").unwrap_or_else(|_| "/".to_string());
    Ok(StorageConfig::hdfs(name_node, root_path))
}

/// Load filesystem configuration (for testing)
fn load_fs_config() -> Result<StorageConfig> {
    let root_path = env::var("STORAGE_ROOT_PATH").unwrap_or_else(|_| DEFAULT_FS_ROOT.to_string());
    Ok(StorageConfig::fs(root_path))
}
