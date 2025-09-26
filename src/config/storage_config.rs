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

impl StorageConfig {
    pub fn oss(bucket: String) -> Self {
        Self {
            provider: StorageProvider::Oss,
            bucket,
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }

    pub fn s3(bucket: String) -> Self {
        Self {
            provider: StorageProvider::S3,
            bucket,
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }

    pub fn cos(bucket: String) -> Self {
        Self {
            provider: StorageProvider::Cos,
            bucket,
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }

    pub fn fs(root_path: Option<String>) -> Self {
        Self {
            provider: StorageProvider::Fs,
            bucket: "local".to_string(),
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path,
            name_node: None,
            anonymous: false,
        }
    }

    pub fn hdfs(name_node: Option<String>, root_path: Option<String>) -> Self {
        Self {
            provider: StorageProvider::Hdfs,
            bucket: "hdfs".to_string(),
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path,
            name_node,
            anonymous: false,
        }
    }
}
