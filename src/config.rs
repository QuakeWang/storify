use crate::error::{Error, Result};
use crate::storage::{StorageConfig, StorageProvider};
use std::env;
use std::str::FromStr;

fn get_env_var(primary_key: &str, secondary_key: &str) -> Result<String> {
    env::var(primary_key)
        .or_else(|_| env::var(secondary_key))
        .map_err(|_| Error::MissingEnvVar {
            key: format!("{primary_key} or {secondary_key}"),
        })
}

/// Load storage configuration from environment variables
pub fn load_storage_config() -> Result<StorageConfig> {
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
    let bucket = get_env_var("STORAGE_BUCKET", "OSS_BUCKET")?;
    let access_key_id = get_env_var("STORAGE_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID")?;
    let access_key_secret = get_env_var("STORAGE_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET")?;

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
        get_env_var("STORAGE_BUCKET", "MINIO_BUCKET")?
    } else {
        get_env_var("STORAGE_BUCKET", "AWS_S3_BUCKET")?
    };

    let access_key_id = if is_minio {
        get_env_var("STORAGE_ACCESS_KEY_ID", "MINIO_ACCESS_KEY")?
    } else {
        get_env_var("STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")?
    };

    let secret_access_key = if is_minio {
        get_env_var("STORAGE_ACCESS_KEY_SECRET", "MINIO_SECRET_KEY")?
    } else {
        get_env_var("STORAGE_ACCESS_KEY_SECRET", "AWS_SECRET_ACCESS_KEY")?
    };

    let region = env::var("STORAGE_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .or_else(|_| env::var("MINIO_DEFAULT_REGION"))
        .ok();

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
