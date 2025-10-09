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

    pub const fn requirement(&self) -> Requirement {
        self.requirement
    }

    pub const fn default(&self) -> Option<&'static str> {
        self.default
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

#[derive(Clone, Copy, Debug)]
pub struct FieldInfo {
    pub name: &'static str,
    pub rule: FieldRule,
}

impl FieldInfo {
    pub const fn new(name: &'static str, rule: FieldRule) -> Self {
        Self { name, rule }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderBackend {
    Oss {
        bucket: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        endpoint: Option<String>,
        anonymous: bool,
    },
    S3 {
        bucket: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        anonymous: bool,
    },
    Cos {
        bucket: String,
        secret_id: String,
        secret_key: String,
        endpoint: Option<String>,
    },
    Fs {
        root: String,
    },
    Hdfs {
        root: String,
        name_node: String,
    },
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

    pub const fn allows_anonymous(&self) -> bool {
        self.allow_anonymous
    }

    pub const fn field_matrix(&self) -> [FieldInfo; 6] {
        [
            FieldInfo::new("access_key_id", self.access_key),
            FieldInfo::new("access_key_secret", self.secret_key),
            FieldInfo::new("region", self.region),
            FieldInfo::new("endpoint", self.endpoint),
            FieldInfo::new("root_path", self.root_path),
            FieldInfo::new("name_node", self.name_node),
        ]
    }

    pub fn prepare(
        &self,
        provider: StorageProvider,
        config: &mut StorageConfig,
    ) -> Result<ProviderBackend> {
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

        let backend = match provider {
            StorageProvider::Oss => ProviderBackend::Oss {
                bucket: config.bucket.clone(),
                access_key: config.access_key_id.clone(),
                secret_key: config.access_key_secret.clone(),
                endpoint: config.endpoint.clone(),
                anonymous: config.anonymous,
            },
            StorageProvider::S3 => ProviderBackend::S3 {
                bucket: config.bucket.clone(),
                access_key: config.access_key_id.clone(),
                secret_key: config.access_key_secret.clone(),
                region: config.region.clone(),
                endpoint: config.endpoint.clone(),
                anonymous: config.anonymous,
            },
            StorageProvider::Cos => ProviderBackend::Cos {
                bucket: config.bucket.clone(),
                secret_id: config.access_key_id.clone().ok_or_else(|| {
                    Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: "access_key_id".to_string(),
                    }
                })?,
                secret_key: config.access_key_secret.clone().ok_or_else(|| {
                    Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: "access_key_secret".to_string(),
                    }
                })?,
                endpoint: config.endpoint.clone(),
            },
            StorageProvider::Fs => ProviderBackend::Fs {
                root: config
                    .root_path
                    .clone()
                    .ok_or_else(|| Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: "root_path".to_string(),
                    })?,
            },
            StorageProvider::Hdfs => ProviderBackend::Hdfs {
                root: config
                    .root_path
                    .clone()
                    .ok_or_else(|| Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: "root_path".to_string(),
                    })?,
                name_node: config
                    .name_node
                    .clone()
                    .ok_or_else(|| Error::MissingConfigField {
                        provider: provider.as_str().to_string(),
                        field: "name_node".to_string(),
                    })?,
            },
        };

        Ok(backend)
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

pub fn prepare_storage_backend(config: &mut StorageConfig) -> Result<ProviderBackend> {
    provider_spec(config.provider).prepare(config.provider, config)
}

pub fn prepare_storage_config(config: &mut StorageConfig) -> Result<()> {
    prepare_storage_backend(config).map(|_| ())
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
