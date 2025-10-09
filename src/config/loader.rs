use crate::config::{
    ProfileStore, ProfileStoreOpenOptions, StorageProvider, prepare_storage_config,
    storage_config::StorageConfig,
};
use crate::error::{Error, Result};
use secrecy::SecretString;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Clone, Default)]
pub struct ConfigRequest {
    pub profile: Option<String>,
    pub profile_store_path: Option<PathBuf>,
    pub non_interactive: bool,
    pub require_storage: bool,
    pub master_password: Option<SecretString>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    ExplicitProfile,
    DefaultProfile,
    Environment,
}

#[derive(Debug, Clone, Default)]
pub struct ResolvedConfig {
    pub storage: Option<StorageConfig>,
    pub profile: Option<String>,
    pub profile_store_path: Option<PathBuf>,
    pub available_profiles: Vec<String>,
    pub default_profile: Option<String>,
    pub source: Option<ConfigSource>,
}

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

fn ensure_interactive(request: &ConfigRequest, action: &str) -> Result<()> {
    if request.non_interactive {
        Err(Error::non_interactive(action))
    } else {
        Ok(())
    }
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

/// Open profile store and populate resolved metadata
fn open_and_populate_store(
    request: &ConfigRequest,
    resolved: &mut ResolvedConfig,
) -> Result<Option<ProfileStore>> {
    match ProfileStore::open_with_options(ProfileStoreOpenOptions {
        path: request.profile_store_path.clone(),
        master_password: request.master_password.clone(),
    }) {
        Ok(store) => {
            resolved.profile_store_path = Some(store.path().to_path_buf());
            resolved.available_profiles = store.available_profiles();
            resolved.default_profile = store.default_profile().map(str::to_string);
            Ok(Some(store))
        }
        Err(Error::ProfileStoreLocked { path }) => {
            resolved.profile_store_path = Some(path.clone());
            if request.require_storage {
                Err(Error::ProfileStoreLocked { path })
            } else {
                Ok(None)
            }
        }
        Err(err) => Err(err),
    }
}

/// Load profile config and populate resolved
fn load_profile(
    store: &ProfileStore,
    profile_name: &str,
    source: ConfigSource,
    resolved: &mut ResolvedConfig,
) -> Result<()> {
    let config = store.get_profile(profile_name)?.into_config()?;
    resolved.storage = Some(config);
    resolved.profile = Some(profile_name.to_string());
    resolved.source = Some(source);
    Ok(())
}

/// Try to load config from environment variables
fn try_load_env(resolved: &mut ResolvedConfig) -> bool {
    if env_value("STORAGE_PROVIDER").is_none() {
        return false;
    }

    if let Ok(config) = load_env_config(&env_value, None)
        .and_then(build_config)
        .map_err(with_config_hint)
    {
        resolved.storage = Some(config);
        resolved.source = Some(ConfigSource::Environment);
        true
    } else {
        false
    }
}

pub fn resolve(request: ConfigRequest) -> Result<ResolvedConfig> {
    let mut resolved = ResolvedConfig::default();
    let store = open_and_populate_store(&request, &mut resolved)?;

    if let Some(profile_name) = request.profile.as_deref() {
        let store = store.as_ref().ok_or_else(|| Error::ProfileStoreLocked {
            path: resolved
                .profile_store_path
                .clone()
                .unwrap_or_else(ProfileStore::default_path),
        })?;
        load_profile(
            store,
            profile_name,
            ConfigSource::ExplicitProfile,
            &mut resolved,
        )?;
        return Ok(resolved);
    }

    if let Some(store) = store.as_ref()
        && let Some(default_name) = store.default_profile()
    {
        load_profile(
            store,
            default_name,
            ConfigSource::DefaultProfile,
            &mut resolved,
        )?;
        return Ok(resolved);
    }

    if try_load_env(&mut resolved) {
        return Ok(resolved);
    }

    if request.require_storage {
        ensure_interactive(&request, "Resolving configuration from environment")?;
        return Err(Error::NoConfiguration {
            profiles: resolved.available_profiles.join(", "),
        });
    }

    Ok(resolved)
}

/// Load storage configuration using environment variables only.
pub fn load_storage_config() -> Result<StorageConfig> {
    let resolved = resolve(ConfigRequest {
        profile: None,
        profile_store_path: None,
        non_interactive: false,
        require_storage: true,
        master_password: None,
    })?;

    let storage = resolved.storage.ok_or_else(|| Error::NoConfiguration {
        profiles: "none".to_string(),
    })?;
    Ok(storage)
}

pub fn load_env_storage_config(provider_hint: Option<String>) -> Result<StorageConfig> {
    load_env_config(&env_value, provider_hint)
        .and_then(build_config)
        .map_err(with_config_hint)
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
        return Err(Error::MissingEnvVar {
            key: "STORAGE_PROVIDER".to_string(),
        });
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

fn append_hint(text: &str) -> String {
    if text.contains("storify config") {
        text.to_string()
    } else {
        format!("{text} ({CONFIG_HINT})")
    }
}

fn with_config_hint(err: Error) -> Error {
    match err {
        Error::MissingEnvVar { key } => {
            let hinted = append_hint(&key);
            Error::MissingEnvVar { key: hinted }
        }
        Error::MissingConfigField { provider, field } => {
            let hinted = append_hint(&field);
            Error::MissingConfigField {
                provider,
                field: hinted,
            }
        }
        Error::ProfileNotFound { name } => {
            let hinted = append_hint(&name);
            Error::ProfileNotFound { name: hinted }
        }
        Error::ProfileStoreLocked { path } => Error::ProfileStoreLocked { path },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn build_from_env(
        getter: &dyn Fn(&str) -> Option<String>,
        provider_hint: Option<String>,
    ) -> Result<StorageConfig> {
        load_env_config(getter, provider_hint)
            .and_then(build_config)
            .map_err(with_config_hint)
    }
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
    fn cos_env_empty_credentials_fails() {
        let env = TestEnv::new(&[
            ("STORAGE_PROVIDER", Some("cos")),
            ("STORAGE_BUCKET", Some("bucket")),
            ("STORAGE_ACCESS_KEY_ID", Some("")),
            ("STORAGE_ACCESS_KEY_SECRET", Some("")),
        ]);
        let getter = env.getter();

        let err = build_from_env(&getter, None).expect_err("cos requires credentials");
        let msg = format!("{err}");
        assert!(matches!(err, Error::MissingConfigField { .. }));
        assert!(msg.contains("storify config"));
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
}
