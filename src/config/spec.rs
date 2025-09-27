use crate::config::{StorageProvider, storage_config::StorageConfig};
use crate::error::{Error, Result};
use crate::storage::constants::{DEFAULT_COS_ENDPOINT, DEFAULT_FS_ROOT, DEFAULT_HDFS_ROOT};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requirement {
    Required,
    Optional,
    Unsupported,
}

#[derive(Clone, Copy, Debug)]
pub struct FieldRule {
    requirement: Requirement,
    default: Option<&'static str>,
}

impl FieldRule {
    pub const fn required() -> Self {
        Self {
            requirement: Requirement::Required,
            default: None,
        }
    }

    pub const fn optional() -> Self {
        Self {
            requirement: Requirement::Optional,
            default: None,
        }
    }

    pub const fn optional_with_default(default: &'static str) -> Self {
        Self {
            requirement: Requirement::Optional,
            default: Some(default),
        }
    }

    pub const fn unsupported() -> Self {
        Self {
            requirement: Requirement::Unsupported,
            default: None,
        }
    }

    pub fn apply(
        &self,
        provider: StorageProvider,
        field: &'static str,
        value: &mut Option<String>,
    ) -> Result<()> {
        match self.requirement {
            Requirement::Required => {
                if value.is_none() {
                    return Err(Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: field.to_string(),
                    });
                }
            }
            Requirement::Optional => {}
            Requirement::Unsupported => {
                if value.is_some() {
                    *value = None;
                }
            }
        }

        if self.requirement != Requirement::Unsupported
            && value.is_none()
            && let Some(default) = self.default
        {
            *value = Some(default.to_string());
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProviderSpec {
    allow_anonymous: bool,
    access_key: FieldRule,
    secret_key: FieldRule,
    region: FieldRule,
    endpoint: FieldRule,
    root_path: FieldRule,
    name_node: FieldRule,
}

impl ProviderSpec {
    const fn cloud(region: FieldRule, endpoint: FieldRule, allow_anonymous: bool) -> Self {
        let cred_rule = if allow_anonymous {
            FieldRule::optional()
        } else {
            FieldRule::required()
        };
        Self {
            allow_anonymous,
            access_key: cred_rule,
            secret_key: cred_rule,
            region,
            endpoint,
            root_path: FieldRule::unsupported(),
            name_node: FieldRule::unsupported(),
        }
    }

    const fn filesystem(root_rule: FieldRule) -> Self {
        Self {
            allow_anonymous: false,
            access_key: FieldRule::unsupported(),
            secret_key: FieldRule::unsupported(),
            region: FieldRule::unsupported(),
            endpoint: FieldRule::unsupported(),
            root_path: root_rule,
            name_node: FieldRule::unsupported(),
        }
    }

    const fn hdfs(root_rule: FieldRule, name_node_rule: FieldRule) -> Self {
        Self {
            allow_anonymous: false,
            access_key: FieldRule::unsupported(),
            secret_key: FieldRule::unsupported(),
            region: FieldRule::unsupported(),
            endpoint: FieldRule::unsupported(),
            root_path: root_rule,
            name_node: name_node_rule,
        }
    }

    pub fn prepare(&self, provider: StorageProvider, config: &mut StorageConfig) -> Result<()> {
        self.access_key
            .apply(provider, "access_key_id", &mut config.access_key_id)?;
        self.secret_key
            .apply(provider, "access_key_secret", &mut config.access_key_secret)?;
        self.region.apply(provider, "region", &mut config.region)?;
        self.endpoint
            .apply(provider, "endpoint", &mut config.endpoint)?;
        self.root_path
            .apply(provider, "root_path", &mut config.root_path)?;
        self.name_node
            .apply(provider, "name_node", &mut config.name_node)?;

        if self.access_key.requirement != Requirement::Unsupported
            || self.secret_key.requirement != Requirement::Unsupported
        {
            enforce_credentials(self.allow_anonymous, provider, config)?;
        } else {
            config.anonymous = false;
        }
        Ok(())
    }
}

pub fn provider_spec(provider: StorageProvider) -> ProviderSpec {
    match provider {
        StorageProvider::Oss => {
            ProviderSpec::cloud(FieldRule::unsupported(), FieldRule::optional(), true)
        }
        StorageProvider::S3 => {
            ProviderSpec::cloud(FieldRule::optional(), FieldRule::optional(), true)
        }
        StorageProvider::Cos => ProviderSpec::cloud(
            FieldRule::optional(),
            FieldRule::optional_with_default(DEFAULT_COS_ENDPOINT),
            false,
        ),
        StorageProvider::Fs => {
            ProviderSpec::filesystem(FieldRule::optional_with_default(DEFAULT_FS_ROOT))
        }
        StorageProvider::Hdfs => ProviderSpec::hdfs(
            FieldRule::optional_with_default(DEFAULT_HDFS_ROOT),
            FieldRule::required(),
        ),
    }
}

pub fn prepare_storage_config(config: &mut StorageConfig) -> Result<()> {
    provider_spec(config.provider).prepare(config.provider, config)
}

fn enforce_credentials(
    allow_anonymous: bool,
    provider: StorageProvider,
    config: &mut StorageConfig,
) -> Result<()> {
    let access = config.access_key_id.as_ref();
    let secret = config.access_key_secret.as_ref();

    match (access, secret) {
        (Some(_), Some(_)) => {
            config.anonymous = false;
            Ok(())
        }
        (None, None) => {
            if allow_anonymous {
                config.anonymous = true;
                Ok(())
            } else {
                Err(Error::MissingConfigField {
                    provider: provider.as_str().to_string(),
                    field: "access_key_id".to_string(),
                })
            }
        }
        (None, Some(_)) => Err(Error::MissingConfigField {
            provider: provider.as_str().to_string(),
            field: "access_key_id".to_string(),
        }),
        (Some(_), None) => Err(Error::MissingConfigField {
            provider: provider.as_str().to_string(),
            field: "access_key_secret".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn cos_injects_default_endpoint_when_missing() {
        let mut config = StorageConfig::cos("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.endpoint = None;
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert_eq!(config.endpoint.as_deref(), Some(DEFAULT_COS_ENDPOINT));
    }

    #[test]
    fn cos_respects_explicit_endpoint() {
        let mut config = StorageConfig::cos("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.endpoint = Some("https://custom.example".into());
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert_eq!(config.endpoint.as_deref(), Some("https://custom.example"));
    }

    #[test]
    fn s3_missing_access_key_id_errors() {
        let mut config = StorageConfig::s3("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.access_key_id = None;
        let err =
            prepare_storage_config(&mut config).expect_err("missing access key id should error");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }

    #[test]
    fn fs_strips_credentials() {
        let mut config = StorageConfig::fs(Some("/tmp".into()));
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert!(config.access_key_id.is_none());
        assert!(config.access_key_secret.is_none());
        assert!(!config.anonymous);
    }

    #[test]
    fn hdfs_requires_name_node() {
        let mut config =
            StorageConfig::hdfs(Some("namenode".into()), Some(DEFAULT_HDFS_ROOT.into()));
        config.name_node = None;
        let err = prepare_storage_config(&mut config).expect_err("missing name node should error");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }

    #[test]
    fn oss_region_is_cleared_when_unsupported() {
        let mut config = StorageConfig::oss("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.region = Some("cn-hangzhou".into());
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert!(config.region.is_none());
    }

    #[test]
    fn fs_injects_default_root_when_missing() {
        let mut config = StorageConfig::fs(None);
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert_eq!(config.root_path.as_deref(), Some(DEFAULT_FS_ROOT));
        assert!(!config.anonymous);
    }

    #[test]
    fn s3_anonymous_allowed_when_credentials_missing() {
        let mut config = StorageConfig::s3("bucket".into());
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert!(config.anonymous);
    }

    #[test]
    fn s3_missing_secret_key_errors() {
        let mut config = StorageConfig::s3("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.access_key_secret = None;
        let err = prepare_storage_config(&mut config).expect_err("missing secret key should error");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }

    #[test]
    fn oss_anonymous_allowed_when_credentials_missing() {
        let mut config = StorageConfig::oss("bucket".into());
        prepare_storage_config(&mut config).expect("prepare_storage_config should succeed");
        assert!(config.anonymous);
    }

    #[test]
    fn oss_missing_secret_key_errors() {
        let mut config = StorageConfig::oss("bucket".into());
        config.access_key_id = Some("id".into());
        config.access_key_secret = Some("secret".into());
        config.access_key_secret = None;
        let err = prepare_storage_config(&mut config).expect_err("missing secret key should error");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }

    #[test]
    fn cos_requires_credentials() {
        let mut config = StorageConfig::cos("bucket".into());
        let err = prepare_storage_config(&mut config).expect_err("cos requires credentials");
        assert!(matches!(err, Error::MissingConfigField { .. }));
    }
}
