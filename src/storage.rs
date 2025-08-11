use crate::error::{Error, Result};
use opendal::Operator;
use std::str::FromStr;

pub mod constants;
mod operations;
mod utils;

use self::operations::copy::OpenDalCopier;
use self::operations::delete::OpenDalDeleter;
use self::operations::download::OpenDalDownloader;
use self::operations::list::OpenDalLister;
use self::operations::upload::OpenDalUploader;
use self::operations::usage::OpenDalUsageCalculator;
use self::operations::{Copier, Deleter, Downloader, Lister, Uploader, UsageCalculator};
use crate::wrap_err;

/// Storage provider types
#[derive(Debug, Clone, Copy)]
pub enum StorageProvider {
    Oss,
    S3,
    Fs,
    Hdfs,
}

impl FromStr for StorageProvider {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "oss" => Ok(Self::Oss),
            "s3" | "minio" => Ok(Self::S3),
            "fs" => Ok(Self::Fs),
            "hdfs" => Ok(Self::Hdfs),
            _ => Err(Error::UnsupportedProvider {
                provider: s.to_string(),
            }),
        }
    }
}

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
}

impl StorageConfig {
    pub fn oss(
        bucket: String,
        access_key_id: String,
        access_key_secret: String,
        region: Option<String>,
    ) -> Self {
        Self {
            provider: StorageProvider::Oss,
            bucket,
            access_key_id: Some(access_key_id),
            access_key_secret: Some(access_key_secret),
            endpoint: None,
            region,
            root_path: None,
            name_node: None,
        }
    }

    pub fn s3(
        bucket: String,
        access_key_id: String,
        secret_access_key: String,
        region: Option<String>,
    ) -> Self {
        Self {
            provider: StorageProvider::S3,
            bucket,
            access_key_id: Some(access_key_id),
            access_key_secret: Some(secret_access_key),
            endpoint: None,
            region,
            root_path: None,
            name_node: None,
        }
    }

    pub fn fs(root_path: String) -> Self {
        Self {
            provider: StorageProvider::Fs,
            bucket: "local".to_string(),
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: Some(root_path),
            name_node: None,
        }
    }

    pub fn hdfs(name_node: String, root_path: String) -> Self {
        Self {
            provider: StorageProvider::Hdfs,
            bucket: "hdfs".to_string(), // Bucket is not really used for HDFS
            access_key_id: None,
            access_key_secret: None,
            endpoint: None,
            region: None,
            root_path: Some(root_path),
            name_node: Some(name_node),
        }
    }
}

/// Unified storage client using OpenDAL
#[derive(Clone)]
pub struct StorageClient {
    operator: Operator,
    provider: StorageProvider,
}

impl StorageClient {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let operator = Self::build_operator(&config)?;
        Ok(Self {
            operator,
            provider: config.provider,
        })
    }

    pub fn provider(&self) -> StorageProvider {
        self.provider
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    fn build_operator(config: &StorageConfig) -> Result<Operator> {
        match &config.provider {
            StorageProvider::Oss => {
                let mut builder = opendal::services::Oss::default().bucket(&config.bucket);
                if let Some(access_key_id) = &config.access_key_id {
                    builder = builder.access_key_id(access_key_id);
                }
                if let Some(access_key_secret) = &config.access_key_secret {
                    builder = builder.access_key_secret(access_key_secret);
                }
                if let Some(endpoint) = &config.endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)?.finish())
            }
            StorageProvider::S3 => {
                let mut builder = opendal::services::S3::default().bucket(&config.bucket);
                if let Some(access_key_id) = &config.access_key_id {
                    builder = builder.access_key_id(access_key_id);
                }
                if let Some(secret_access_key) = &config.access_key_secret {
                    builder = builder.secret_access_key(secret_access_key);
                }
                if let Some(region) = &config.region {
                    builder = builder.region(region);
                }
                if let Some(endpoint) = &config.endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)?.finish())
            }
            StorageProvider::Fs => {
                let root = config.root_path.as_deref().unwrap_or("./");
                let builder = opendal::services::Fs::default().root(root);
                Ok(Operator::new(builder)?.finish())
            }
            StorageProvider::Hdfs => {
                #[cfg(feature = "hdfs")]
                {
                    let root = config.root_path.as_deref().unwrap_or("/");
                    let name_node = config.name_node.as_deref().unwrap_or_default();
                    let builder = opendal::services::Hdfs::default()
                        .root(root)
                        .name_node(name_node);
                    Ok(Operator::new(builder)?.finish())
                }

                #[cfg(not(feature = "hdfs"))]
                {
                    Err(Error::UnsupportedProvider {
                        provider: "hdfs (feature disabled)".to_string(),
                    })
                }
            }
        }
    }

    pub async fn list_directory(&self, path: &str, long: bool, recursive: bool) -> Result<()> {
        log::debug!(
            "list_directory provider={:?} path={} long={} recursive={}",
            self.provider,
            path,
            long,
            recursive
        );
        let lister = OpenDalLister::new(self.operator.clone());
        wrap_err!(
            lister.list(path, long, recursive).await,
            ListDirectoryFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn download_files(&self, remote_path: &str, local_path: &str) -> Result<()> {
        log::debug!(
            "download_files provider={:?} remote_path={} local_path={}",
            self.provider,
            remote_path,
            local_path
        );
        let downloader = OpenDalDownloader::new(self.operator.clone());
        wrap_err!(
            downloader.download(remote_path, local_path).await,
            DownloadFailed {
                remote_path: remote_path.to_string(),
                local_path: local_path.to_string()
            }
        )
    }

    pub async fn disk_usage(&self, path: &str, summary: bool) -> Result<()> {
        log::debug!(
            "disk_usage provider={:?} path={} summary={}",
            self.provider,
            path,
            summary
        );
        let calculator = OpenDalUsageCalculator::new(self.operator.clone());
        wrap_err!(
            calculator.calculate_usage(path, summary).await,
            DiskUsageFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn upload_files(
        &self,
        local_path: &str,
        remote_path: &str,
        is_recursive: bool,
    ) -> Result<()> {
        log::debug!(
            "upload_files provider={:?} local_path={} remote_path={} recursive={}",
            self.provider,
            local_path,
            remote_path,
            is_recursive
        );
        let uploader = OpenDalUploader::new(self.operator.clone());
        wrap_err!(
            uploader.upload(local_path, remote_path, is_recursive).await,
            UploadFailed {
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string()
            }
        )
    }

    pub async fn delete_files(&self, paths: &[String], recursive: bool) -> Result<()> {
        log::debug!(
            "delete_files provider={:?} paths_count={} recursive={}",
            self.provider,
            paths.len(),
            recursive
        );
        let deleter = OpenDalDeleter::new(self.operator.clone());
        wrap_err!(
            deleter.delete(paths, recursive).await,
            DeleteFailed {
                // summarize inputs to avoid huge error strings
                paths: paths.iter().take(5).cloned().collect::<Vec<_>>().join(","),
                recursive: recursive
            }
        )
    }

    pub async fn copy_files(&self, src_path: &str, dest_path: &str) -> Result<()> {
        log::debug!(
            "copy_files provider={:?} src_path={} dest_path={}",
            self.provider,
            src_path,
            dest_path
        );
        let copier = OpenDalCopier::new(self.operator.clone());
        wrap_err!(
            copier.copy(src_path, dest_path).await,
            CopyFailed {
                src_path: src_path.to_string(),
                dest_path: dest_path.to_string()
            }
        )
    }
}
