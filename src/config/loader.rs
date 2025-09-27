use crate::config::{StorageProvider, prepare_storage_config, storage_config::StorageConfig};
use crate::error::{Error, Result};
use log::warn;
use std::env;
use std::str::FromStr;

fn env_value(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|val| val.trim().to_string())
        .filter(|val| !val.is_empty())
}

/// Read the first available environment variable from a list of keys.
/// Empty strings are treated as missing values.
fn env_any_from(keys: &[&str], get: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    keys.iter().find_map(|key| get(key))
}

fn env_any_required_from(keys: &[&str], get: &dyn Fn(&str) -> Option<String>) -> Result<String> {
    env_any_from(keys, get).ok_or_else(|| Error::MissingEnvVar {
        key: keys.join(" or "),
    })
}

/// Provider-specific environment variable keys
#[derive(Clone, Copy)]
struct ProviderKeys {
    bucket: &'static [&'static str],
    access_key_id: &'static [&'static str],
    secret_key: &'static [&'static str],
    region: &'static [&'static str],
    endpoint: &'static [&'static str],
}

impl ProviderKeys {
    const fn new(
        bucket: &'static [&'static str],
        access_key_id: &'static [&'static str],
        secret_key: &'static [&'static str],
        region: &'static [&'static str],
        endpoint: &'static [&'static str],
    ) -> Self {
        Self {
            bucket,
            access_key_id,
            secret_key,
            region,
            endpoint,
        }
    }
}

fn provider_keys(provider: StorageProvider, raw_provider: &str) -> ProviderKeys {
    match provider {
        StorageProvider::Oss => ProviderKeys::new(
            &["STORAGE_BUCKET", "OSS_BUCKET"],
            &["STORAGE_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID"],
            &["STORAGE_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET"],
            &["STORAGE_REGION", "OSS_REGION"],
            &["STORAGE_ENDPOINT", "OSS_ENDPOINT"],
        ),
        StorageProvider::S3 => {
            if raw_provider.eq_ignore_ascii_case("minio") {
                ProviderKeys::new(
                    &["STORAGE_BUCKET", "MINIO_BUCKET"],
                    &["STORAGE_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"],
                    &["STORAGE_ACCESS_KEY_SECRET", "MINIO_SECRET_KEY"],
                    &["STORAGE_REGION", "MINIO_DEFAULT_REGION"],
                    &["STORAGE_ENDPOINT", "MINIO_ENDPOINT"],
                )
            } else {
                ProviderKeys::new(
                    &["STORAGE_BUCKET", "AWS_S3_BUCKET"],
                    &["STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"],
                    &["STORAGE_ACCESS_KEY_SECRET", "AWS_SECRET_ACCESS_KEY"],
                    &["STORAGE_REGION", "AWS_DEFAULT_REGION"],
                    &["STORAGE_ENDPOINT"],
                )
            }
        }
        StorageProvider::Cos => ProviderKeys::new(
            &["STORAGE_BUCKET", "COS_BUCKET"],
            &["STORAGE_ACCESS_KEY_ID", "COS_SECRET_ID"],
            &["STORAGE_ACCESS_KEY_SECRET", "COS_SECRET_KEY"],
            &["STORAGE_REGION", "COS_REGION"],
            &["STORAGE_ENDPOINT", "COS_ENDPOINT"],
        ),
        StorageProvider::Fs | StorageProvider::Hdfs => unreachable!(
            "provider '{}' does not use cloud environment keys",
            provider.as_str()
        ),
    }
}

/// Load storage configuration from environment variables
pub fn load_storage_config() -> Result<StorageConfig> {
    load_storage_config_from_source(&env_value, None)
}

fn load_storage_config_from_source(
    get: &dyn Fn(&str) -> Option<String>,
    provider_hint: Option<String>,
) -> Result<StorageConfig> {
    let provider_str = if let Some(explicit) = provider_hint {
        explicit
    } else if let Some(raw) = get("STORAGE_PROVIDER") {
        raw
    } else {
        warn!("STORAGE_PROVIDER not set, using default: oss");
        "oss".to_string()
    };

    let provider = StorageProvider::from_str(&provider_str)?;

    let mut config = match provider {
        StorageProvider::Oss => {
            load_cloud_config(provider, &provider_str, get, StorageConfig::oss)?
        }
        StorageProvider::S3 => load_cloud_config(provider, &provider_str, get, StorageConfig::s3)?,
        StorageProvider::Cos => {
            load_cloud_config(provider, &provider_str, get, StorageConfig::cos)?
        }
        StorageProvider::Fs => load_fs_config(get)?,
        StorageProvider::Hdfs => load_hdfs_config(get)?,
    };

    prepare_storage_config(&mut config)?;
    Ok(config)
}

/// Load configuration for any cloud storage provider
fn load_cloud_config(
    provider: StorageProvider,
    raw_provider: &str,
    get: &dyn Fn(&str) -> Option<String>,
    config_constructor: impl FnOnce(String) -> StorageConfig,
) -> Result<StorageConfig> {
    let keys = provider_keys(provider, raw_provider);
    let bucket = env_any_required_from(keys.bucket, get)?;
    let mut config = config_constructor(bucket);

    config.access_key_id = env_any_from(keys.access_key_id, get);
    config.access_key_secret = env_any_from(keys.secret_key, get);
    config.region = env_any_from(keys.region, get);
    config.endpoint = env_any_from(keys.endpoint, get);
    Ok(config)
}

/// Load HDFS configuration
fn load_hdfs_config(get: &dyn Fn(&str) -> Option<String>) -> Result<StorageConfig> {
    let name_node = get("HDFS_NAME_NODE").ok_or_else(|| Error::MissingEnvVar {
        key: "HDFS_NAME_NODE".to_string(),
    })?;
    let root_path = get("HDFS_ROOT_PATH");
    Ok(StorageConfig::hdfs(Some(name_node), root_path))
}

/// Load filesystem configuration (for testing)
fn load_fs_config(get: &dyn Fn(&str) -> Option<String>) -> Result<StorageConfig> {
    Ok(StorageConfig::fs(get("STORAGE_ROOT_PATH")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::constants::DEFAULT_FS_ROOT;
    use std::collections::HashMap;
    struct TestEnv<'a> {
        map: HashMap<&'a str, Option<&'a str>>,
    }

    impl<'a> TestEnv<'a> {
        fn new(vars: &'a [(&'a str, Option<&'a str>)]) -> Self {
            Self {
                map: vars.iter().copied().collect(),
            }
        }

        fn getter(&'a self) -> impl Fn(&str) -> Option<String> + 'a {
            move |key| {
                self.map.get(key).and_then(|opt| {
                    opt.and_then(|value| {
                        let trimmed = value.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    })
                })
            }
        }
    }

    #[test]
    fn load_fs_config_uses_default_root() {
        let env = TestEnv::new(&[]);
        let getter = env.getter();
        let mut cfg = load_fs_config(&getter).unwrap();
        prepare_storage_config(&mut cfg).unwrap();
        assert_eq!(cfg.root_path.as_deref(), Some(DEFAULT_FS_ROOT));
    }

    #[test]
    fn s3_env_allows_anonymous_when_credentials_missing() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("s3")),
            ("STORAGE_BUCKET", Some("bucket")),
        ]);
        let getter = env.getter();

        let cfg = load_storage_config_from_source(&getter, None).expect("anonymous s3 config");
        assert_eq!(cfg.bucket, "bucket");
        assert!(cfg.anonymous);
    }

    #[test]
    fn cos_env_empty_credentials_fails() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("cos")),
            ("STORAGE_BUCKET", Some("bucket")),
            ("STORAGE_ACCESS_KEY_ID", Some("")),
            ("STORAGE_ACCESS_KEY_SECRET", Some("")),
        ]);
        let getter = env.getter();

        let err =
            load_storage_config_from_source(&getter, None).expect_err("cos requires credentials");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }
}
