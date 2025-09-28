use crate::config::{
    ProfileStore, StorageProvider, prepare_storage_config, storage_config::StorageConfig,
};
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

const OSS_BUCKET_KEYS: &[&str] = &["STORAGE_BUCKET", "OSS_BUCKET"];
const OSS_ACCESS_KEY_ID_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_ID", "OSS_ACCESS_KEY_ID"];
const OSS_SECRET_KEY_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_SECRET", "OSS_ACCESS_KEY_SECRET"];
const OSS_REGION_KEYS: &[&str] = &["STORAGE_REGION", "OSS_REGION"];
const OSS_ENDPOINT_KEYS: &[&str] = &["STORAGE_ENDPOINT", "OSS_ENDPOINT"];

const S3_BUCKET_KEYS: &[&str] = &["STORAGE_BUCKET", "AWS_S3_BUCKET", "MINIO_BUCKET"];
const S3_ACCESS_KEY_ID_KEYS: &[&str] = &[
    "STORAGE_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY_ID",
    "MINIO_ACCESS_KEY",
];
const S3_SECRET_KEY_KEYS: &[&str] = &[
    "STORAGE_ACCESS_KEY_SECRET",
    "AWS_SECRET_ACCESS_KEY",
    "MINIO_SECRET_KEY",
];
const S3_REGION_KEYS: &[&str] = &[
    "STORAGE_REGION",
    "AWS_DEFAULT_REGION",
    "MINIO_DEFAULT_REGION",
];
const S3_ENDPOINT_KEYS: &[&str] = &["STORAGE_ENDPOINT", "MINIO_ENDPOINT"];

const MINIO_BUCKET_KEYS: &[&str] = &["STORAGE_BUCKET", "MINIO_BUCKET"];
const MINIO_ACCESS_KEY_ID_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"];
const MINIO_SECRET_KEY_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_SECRET", "MINIO_SECRET_KEY"];
const MINIO_REGION_KEYS: &[&str] = &["STORAGE_REGION", "MINIO_DEFAULT_REGION"];
const MINIO_ENDPOINT_KEYS: &[&str] = &["STORAGE_ENDPOINT", "MINIO_ENDPOINT"];

const COS_BUCKET_KEYS: &[&str] = &["STORAGE_BUCKET", "COS_BUCKET"];
const COS_ACCESS_KEY_ID_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_ID", "COS_SECRET_ID"];
const COS_SECRET_KEY_KEYS: &[&str] = &["STORAGE_ACCESS_KEY_SECRET", "COS_SECRET_KEY"];
const COS_REGION_KEYS: &[&str] = &["STORAGE_REGION", "COS_REGION"];
const COS_ENDPOINT_KEYS: &[&str] = &["STORAGE_ENDPOINT", "COS_ENDPOINT"];

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

/// Provider-specific key priority order.
///
/// - OSS: `STORAGE_*` > `OSS_*`
/// - S3: `STORAGE_*` > `AWS_*` > `MINIO_*`
/// - MinIO alias (`STORAGE_PROVIDER=minio`): `STORAGE_*` > `MINIO_*`
/// - COS: `STORAGE_*` > `COS_*`
fn provider_keys(provider: StorageProvider, raw_provider: &str) -> ProviderKeys {
    match provider {
        StorageProvider::Oss => ProviderKeys::new(
            OSS_BUCKET_KEYS,
            OSS_ACCESS_KEY_ID_KEYS,
            OSS_SECRET_KEY_KEYS,
            OSS_REGION_KEYS,
            OSS_ENDPOINT_KEYS,
        ),
        StorageProvider::S3 => {
            if raw_provider.eq_ignore_ascii_case("minio") {
                ProviderKeys::new(
                    MINIO_BUCKET_KEYS,
                    MINIO_ACCESS_KEY_ID_KEYS,
                    MINIO_SECRET_KEY_KEYS,
                    MINIO_REGION_KEYS,
                    MINIO_ENDPOINT_KEYS,
                )
            } else {
                ProviderKeys::new(
                    S3_BUCKET_KEYS,
                    S3_ACCESS_KEY_ID_KEYS,
                    S3_SECRET_KEY_KEYS,
                    S3_REGION_KEYS,
                    S3_ENDPOINT_KEYS,
                )
            }
        }
        StorageProvider::Cos => ProviderKeys::new(
            COS_BUCKET_KEYS,
            COS_ACCESS_KEY_ID_KEYS,
            COS_SECRET_KEY_KEYS,
            COS_REGION_KEYS,
            COS_ENDPOINT_KEYS,
        ),
        StorageProvider::Fs | StorageProvider::Hdfs => unreachable!(
            "provider '{}' does not use cloud environment keys",
            provider.as_str()
        ),
    }
}

/// Raw values resolved from environment variables before spec validation.
#[derive(Debug, PartialEq, Eq)]
struct EnvConfig {
    provider: StorageProvider,
    bucket: Option<String>,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    root_path: Option<String>,
    name_node: Option<String>,
}

impl EnvConfig {
    fn new(provider: StorageProvider) -> Self {
        Self {
            provider,
            bucket: None,
            access_key_id: None,
            access_key_secret: None,
            region: None,
            endpoint: None,
            root_path: None,
            name_node: None,
        }
    }
}

/// Alias used to align terminology with design documents.
type RawConfigValues = EnvConfig;

/// Load storage configuration using environment variables and profile store fallback.
pub fn load_storage_config() -> Result<StorageConfig> {
    load_storage_config_with_profile(None, None)
}

/// Load storage configuration with explicit profile selection.
pub fn load_storage_config_with_profile(
    profile_hint: Option<&str>,
    master_password: Option<String>,
) -> Result<StorageConfig> {
    match ProfileStore::open_default(master_password) {
        Ok(store) => load_with_store(&env_value, None, profile_hint, &store),
        Err(Error::ProfileStoreUnavailable) if profile_hint.is_none() => {
            load_storage_config_from_source(&env_value, None)
        }
        Err(err) => Err(err),
    }
}

fn load_storage_config_from_source(
    get: &dyn Fn(&str) -> Option<String>,
    provider_hint: Option<String>,
) -> Result<StorageConfig> {
    load_env_config(get, provider_hint)
        .and_then(build_config)
        .map_err(with_config_hint)
}

fn load_with_store(
    get: &dyn Fn(&str) -> Option<String>,
    provider_hint: Option<String>,
    profile_hint: Option<&str>,
    store: &ProfileStore,
) -> Result<StorageConfig> {
    if let Some(name) = profile_hint {
        return load_profile_from_store(store, name);
    }

    match load_storage_config_from_source(get, provider_hint.clone()) {
        Ok(cfg) => Ok(cfg),
        Err(err) => match err {
            Error::MissingEnvVar { .. } | Error::MissingConfigField { .. } => {
                if let Some(default_name) = store.default_profile()? {
                    return load_profile_from_store(store, &default_name);
                }
                let profiles = store.list_profiles().unwrap_or_default();
                let display = if profiles.is_empty() {
                    "none".to_string()
                } else {
                    profiles.join(", ")
                };
                Err(Error::NoConfiguration { profiles: display })
            }
            other => Err(other),
        },
    }
}

fn load_profile_from_store(store: &ProfileStore, name: &str) -> Result<StorageConfig> {
    match store.load(name)? {
        Some(profile) => profile.into_config(),
        None => Err(Error::ProfileNotFound {
            name: name.to_string(),
        }),
    }
}

fn load_env_config(
    get: &dyn Fn(&str) -> Option<String>,
    provider_hint: Option<String>,
) -> Result<RawConfigValues> {
    let provider_str = if let Some(explicit) = provider_hint {
        explicit
    } else if let Some(raw) = get("STORAGE_PROVIDER") {
        raw
    } else {
        warn!("STORAGE_PROVIDER not set, using default: oss");
        "oss".to_string()
    };

    let provider = StorageProvider::from_str(&provider_str)?;

    let env = match provider {
        StorageProvider::Oss | StorageProvider::S3 | StorageProvider::Cos => {
            load_cloud_env(provider, &provider_str, get)
        }
        StorageProvider::Fs => load_fs_env(get),
        StorageProvider::Hdfs => load_hdfs_env(get),
    }?;
    Ok(env)
}

fn load_cloud_env(
    provider: StorageProvider,
    raw_provider: &str,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<RawConfigValues> {
    let keys = provider_keys(provider, raw_provider);
    let mut env = EnvConfig::new(provider);
    env.bucket = Some(env_any_required_from(keys.bucket, get)?);
    env.access_key_id = env_any_from(keys.access_key_id, get);
    env.access_key_secret = env_any_from(keys.secret_key, get);
    env.region = env_any_from(keys.region, get);
    env.endpoint = env_any_from(keys.endpoint, get);
    Ok(env)
}

fn load_fs_env(get: &dyn Fn(&str) -> Option<String>) -> Result<RawConfigValues> {
    let mut env = EnvConfig::new(StorageProvider::Fs);
    env.root_path = get("STORAGE_ROOT_PATH");
    Ok(env)
}

fn load_hdfs_env(get: &dyn Fn(&str) -> Option<String>) -> Result<RawConfigValues> {
    let mut env = EnvConfig::new(StorageProvider::Hdfs);
    env.name_node = Some(env_any_required_from(&["HDFS_NAME_NODE"], get)?);
    env.root_path = get("HDFS_ROOT_PATH");
    Ok(env)
}

fn require_bucket(bucket: &mut Option<String>, provider: StorageProvider) -> Result<String> {
    bucket.take().ok_or_else(|| Error::MissingConfigField {
        provider: provider.as_str().to_string(),
        field: "bucket".to_string(),
    })
}

fn build_config(env: RawConfigValues) -> Result<StorageConfig> {
    let EnvConfig {
        provider,
        mut bucket,
        access_key_id,
        access_key_secret,
        region,
        endpoint,
        mut root_path,
        mut name_node,
    } = env;

    let mut config = match provider {
        StorageProvider::Oss => StorageConfig::oss(require_bucket(&mut bucket, provider)?),
        StorageProvider::S3 => StorageConfig::s3(require_bucket(&mut bucket, provider)?),
        StorageProvider::Cos => StorageConfig::cos(require_bucket(&mut bucket, provider)?),
        StorageProvider::Fs => StorageConfig::fs(root_path.take()),
        StorageProvider::Hdfs => StorageConfig::hdfs(name_node.take(), root_path.take()),
    };

    config.access_key_id = access_key_id;
    config.access_key_secret = access_key_secret;
    config.region = region;
    config.endpoint = endpoint;

    prepare_storage_config(&mut config)?;
    Ok(config)
}

const CONFIG_HINT: &str = "hint: run `storify config` or supply --profile";

fn append_hint(text: String) -> String {
    if text.contains("storify config") {
        text
    } else {
        format!("{text} ({CONFIG_HINT})")
    }
}

fn with_config_hint(err: Error) -> Error {
    match err {
        Error::MissingEnvVar { key } => Error::MissingEnvVar {
            key: append_hint(key),
        },
        Error::MissingConfigField { provider, field } => Error::MissingConfigField {
            provider,
            field: append_hint(field),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoredProfile;
    use std::collections::HashMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
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
        let msg = format!("{err}");
        assert!(matches!(err, Error::MissingConfigField { .. }));
        assert!(msg.contains("storify config"));
    }

    #[test]
    fn minio_alias_uses_minio_specific_keys() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("minio")),
            ("MINIO_BUCKET", Some("bucket")),
            ("MINIO_ACCESS_KEY", Some("id")),
            ("MINIO_SECRET_KEY", Some("secret")),
            ("MINIO_DEFAULT_REGION", Some("us-east-1")),
            ("MINIO_ENDPOINT", Some("https://minio.example")),
        ]);
        let getter = env.getter();

        let raw = load_env_config(&getter, None).expect("minio should resolve as s3");
        assert_eq!(raw.provider, StorageProvider::S3);
        assert_eq!(raw.bucket.as_deref(), Some("bucket"));
        assert_eq!(raw.access_key_id.as_deref(), Some("id"));
        assert_eq!(raw.access_key_secret.as_deref(), Some("secret"));
        assert_eq!(raw.region.as_deref(), Some("us-east-1"));
        assert_eq!(raw.endpoint.as_deref(), Some("https://minio.example"));
    }

    #[test]
    fn s3_env_falls_back_to_minio_aliases() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("s3")),
            ("MINIO_BUCKET", Some("bucket")),
            ("MINIO_ACCESS_KEY", Some("id")),
            ("MINIO_SECRET_KEY", Some("secret")),
            ("MINIO_DEFAULT_REGION", Some("us-east-1")),
            ("MINIO_ENDPOINT", Some("https://minio.example")),
        ]);
        let getter = env.getter();

        let raw = load_env_config(&getter, None).expect("minio aliases should be accepted by s3");
        assert_eq!(raw.provider, StorageProvider::S3);
        assert_eq!(raw.bucket.as_deref(), Some("bucket"));
        assert_eq!(raw.access_key_id.as_deref(), Some("id"));
        assert_eq!(raw.access_key_secret.as_deref(), Some("secret"));
        assert_eq!(raw.region.as_deref(), Some("us-east-1"));
        assert_eq!(raw.endpoint.as_deref(), Some("https://minio.example"));
    }

    #[test]
    fn build_config_missing_bucket_returns_error() {
        let env = EnvConfig::new(StorageProvider::S3);
        let err = build_config(env).expect_err("missing bucket should error");
        assert!(matches!(err, Error::MissingConfigField { field, .. } if field.contains("bucket")));
    }

    #[test]
    fn hdfs_env_missing_name_node_errors() {
        let env = TestEnv::new(&[("STORAGE_PROVIDER", Some("hdfs"))]);
        let getter = env.getter();

        let err = load_env_config(&getter, Some("hdfs".to_string()))
            .expect_err("hdfs should require name node");
        assert!(matches!(err, Error::MissingEnvVar { key } if key.contains("HDFS_NAME_NODE")));
    }

    #[test]
    fn profile_hint_overrides_environment() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("s3")),
            ("STORAGE_BUCKET", Some("env-bucket")),
        ]);
        let getter = env.getter();
        let store = temp_profile_store("hint", None);
        store
            .save(
                "prod",
                stored_profile(StorageProvider::Oss, "profile-bucket"),
                true,
            )
            .expect("save profile");

        let cfg = load_with_store(&getter, None, Some("prod"), &store).expect("profile hint");
        assert_eq!(cfg.provider, StorageProvider::Oss);
        assert_eq!(cfg.bucket, "profile-bucket");
        cleanup_store(&store);
    }

    #[test]
    fn default_profile_used_when_env_missing() {
        let env = TestEnv::new(&[]);
        let getter = env.getter();
        let store = temp_profile_store("default", None);
        store
            .save(
                "dev",
                stored_profile(StorageProvider::Cos, "profile-bucket"),
                true,
            )
            .expect("save profile");

        let cfg = load_with_store(&getter, None, None, &store).expect("fallback profile");
        assert_eq!(cfg.provider, StorageProvider::Cos);
        assert_eq!(cfg.bucket, "profile-bucket");
        cleanup_store(&store);
    }

    #[test]
    fn no_configuration_lists_profiles() {
        let env = TestEnv::new(&[]);
        let getter = env.getter();
        let store = temp_profile_store("none", None);

        let err = load_with_store(&getter, None, None, &store).expect_err("no configuration");
        match err {
            Error::NoConfiguration { profiles } => assert_eq!(profiles, "none"),
            other => panic!("unexpected error: {other:?}"),
        }
        cleanup_store(&store);
    }

    fn temp_profile_store(name: &str, password: Option<&str>) -> ProfileStore {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "storify-loader-profile-{}-{}.toml",
            name,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        ProfileStore::with_path(path, password.map(|s| s.to_string()))
    }

    fn stored_profile(provider: StorageProvider, bucket: &str) -> StoredProfile {
        StoredProfile {
            provider: provider.as_str().to_string(),
            bucket: bucket.to_string(),
            access_key_id: Some("id".into()),
            access_key_secret: Some("secret".into()),
            endpoint: None,
            region: None,
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }

    fn cleanup_store(store: &ProfileStore) {
        let path = store.path().to_path_buf();
        fs::remove_file(&path).ok();
        fs::remove_file(path.with_extension("bak")).ok();
    }
}
