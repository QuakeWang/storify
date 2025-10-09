use crate::config::StorageProvider;

/// Unified storage configuration for different providers
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub provider: StorageProvider,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub access_key_secret: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub root_path: Option<String>,
    pub name_node: Option<String>,
    pub anonymous: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            provider: StorageProvider::Oss, // Placeholder, will be overridden
            bucket: String::new(),
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }
}

impl StorageConfig {
    /// Create a new storage configuration with the given provider and bucket
    fn new(provider: StorageProvider, bucket: impl Into<String>) -> Self {
        Self {
            provider,
            bucket: bucket.into(),
            ..Default::default()
        }
    }

    pub fn oss(bucket: impl Into<String>) -> Self {
        Self::new(StorageProvider::Oss, bucket)
    }

    pub fn s3(bucket: impl Into<String>) -> Self {
        Self::new(StorageProvider::S3, bucket)
    }

    pub fn cos(bucket: impl Into<String>) -> Self {
        Self::new(StorageProvider::Cos, bucket)
    }

    pub fn fs(root_path: Option<String>) -> Self {
        Self {
            provider: StorageProvider::Fs,
            bucket: "local".to_string(),
            root_path,
            ..Default::default()
        }
    }

    pub fn hdfs(name_node: Option<String>, root_path: Option<String>) -> Self {
        Self {
            provider: StorageProvider::Hdfs,
            bucket: "hdfs".to_string(),
            name_node,
            root_path,
            ..Default::default()
        }
    }
}
