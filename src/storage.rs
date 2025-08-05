use crate::error::{
    DirectoryDeletionNotRecursiveSnafu, DirectoryUploadNotRecursiveSnafu, Error,
    PartialDeletionSnafu, PathNotFoundSnafu, Result,
};
use async_recursion::async_recursion;
use futures::stream::TryStreamExt;
use opendal::{EntryMode, Operator};
use snafu::ensure;
use std::path::Path;
use std::str::FromStr;
use std::{ffi::OsStr, fmt};
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

/// Storage provider types
#[derive(Debug, Clone)]
pub enum StorageProvider {
    Oss,
    S3,
    Fs,
}

impl FromStr for StorageProvider {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "oss" => Ok(Self::Oss),
            "s3" | "minio" => Ok(Self::S3),
            "fs" => Ok(Self::Fs),
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
        }
    }
}

/// Unified storage client using OpenDAL
pub struct StorageClient {
    operator: Operator,
    #[allow(dead_code)]
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
        }
    }

    pub async fn list_directory(&self, path: &str, long: bool, recursive: bool) -> Result<()> {
        let lister = self
            .operator
            .lister_with(path)
            .recursive(recursive)
            .await
            .map_err(|e| Error::ListDirectoryFailed {
                path: path.to_string(),
                source: Box::new(Error::from(e)),
            })?;

        lister
            .map_err(|e| Error::ListDirectoryFailed {
                path: path.to_string(),
                source: Box::new(Error::from(e)),
            })
            .try_for_each(|entry| async move {
                self.print_entry(&entry, long);
                Ok(())
            })
            .await
    }

    pub async fn download_files(&self, remote_path: &str, local_path: &str) -> Result<()> {
        self.download_files_impl(remote_path, local_path)
            .await
            .map_err(|e| Error::DownloadFailed {
                remote_path: remote_path.to_string(),
                local_path: local_path.to_string(),
                source: Box::new(e),
            })
    }

    async fn download_files_impl(&self, remote_path: &str, local_path: &str) -> Result<()> {
        let lister = self
            .operator
            .lister_with(remote_path)
            .recursive(true)
            .await?;

        let mut stream = lister;
        while let Some(entry) = stream.try_next().await? {
            let meta = entry.metadata();
            let remote_file_path = entry.path();
            let relative_path = remote_file_path
                .strip_prefix(remote_path)
                .unwrap_or(remote_file_path);
            let local_file_path = Path::new(local_path).join(relative_path);

            if meta.mode() == EntryMode::DIR {
                fs::create_dir_all(&local_file_path).await?;
            } else {
                if let Some(parent) = local_file_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                let data = self.operator.read(remote_file_path).await?;
                fs::write(&local_file_path, data.to_vec()).await?;
                println!(
                    "Downloaded: {remote_file_path} → {}",
                    local_file_path.display()
                );
            }
        }

        Ok(())
    }

    pub async fn disk_usage(&self, path: &str, summary: bool) -> Result<()> {
        self.disk_usage_impl(path, summary)
            .await
            .map_err(|e| Error::DiskUsageFailed {
                path: path.to_string(),
                source: Box::new(e),
            })
    }

    async fn disk_usage_impl(&self, path: &str, summary: bool) -> Result<()> {
        let lister = self.operator.lister_with(path).recursive(true).await?;
        let (total_size, total_files) = lister
            .try_fold((0, 0), |(size, count), entry| async move {
                let meta = entry.metadata();
                if !summary {
                    println!("{} {}", format_size(meta.content_length()), entry.path());
                }
                Ok((size + meta.content_length(), count + 1))
            })
            .await?;

        if summary {
            println!("{} {path}", format_size(total_size));
            println!("Total files: {total_files}");
        }
        Ok(())
    }

    pub async fn upload_files(
        &self,
        local_path: &str,
        remote_path: &str,
        is_recursive: bool,
    ) -> Result<()> {
        self.upload_files_impl(local_path, remote_path, is_recursive)
            .await
            .map_err(|e| Error::UploadFailed {
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string(),
                source: Box::new(e),
            })
    }

    async fn upload_files_impl(
        &self,
        local_path: &str,
        remote_path: &str,
        is_recursive: bool,
    ) -> Result<()> {
        let path = Path::new(local_path);
        ensure!(
            path.exists(),
            PathNotFoundSnafu {
                path: path.to_path_buf()
            }
        );

        if path.is_file() {
            let file_name = path.file_name().unwrap_or(OsStr::new(local_path));
            let remote_file_path = Path::new(remote_path)
                .join(file_name)
                .to_string_lossy()
                .to_string();
            self.upload_file_streaming(Path::new(local_path), &remote_file_path)
                .await?;
        } else if path.is_dir() {
            if is_recursive {
                self.upload_recursive(local_path, remote_path).await?;
            } else {
                return DirectoryUploadNotRecursiveSnafu.fail();
            }
        }

        Ok(())
    }

    #[async_recursion]
    async fn upload_recursive(&self, local_path: &str, remote_path: &str) -> Result<()> {
        let mut entries = fs::read_dir(local_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let local_file_path = entry.path();
            let file_name = local_file_path.file_name().unwrap_or_default();
            let new_remote_path = Path::new(remote_path)
                .join(file_name)
                .to_string_lossy()
                .to_string();

            if local_file_path.is_dir() {
                self.upload_recursive(&local_file_path.to_string_lossy(), &new_remote_path)
                    .await?;
            } else {
                self.upload_file_streaming(&local_file_path, &new_remote_path)
                    .await?;
            }
        }
        Ok(())
    }

    async fn upload_file_streaming(&self, local_path: &Path, remote_path: &str) -> Result<()> {
        const BUFFER_SIZE: usize = 8192;
        let file = fs::File::open(local_path).await?;
        let file_size = file.metadata().await?.len();
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        let mut writer = self.operator.writer(remote_path).await?;

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            writer.write(buffer[..bytes_read].to_vec()).await?;
            total_bytes += bytes_read as u64;
            if file_size > 0 {
                let progress = (total_bytes as f64 / file_size as f64 * 100.0) as u32;
                if total_bytes.is_multiple_of(BUFFER_SIZE as u64 * 100) {
                    print!("\r📤 Uploading {}: {progress}%", local_path.display());
                    use std::io::{self, Write};
                    let _ = io::stdout().flush();
                }
            }
        }
        writer.close().await?;
        println!(
            "\n✅ Upload: {} → {remote_path} ({total_bytes} bytes)",
            local_path.display(),
        );
        Ok(())
    }

    fn print_entry(&self, entry: &opendal::Entry, long: bool) {
        if long {
            let file_info = FileInfo::from_entry(entry);
            println!("{file_info}");
        } else {
            println!("{}", entry.path());
        }
    }

    pub async fn delete_files(&self, paths: &[String], recursive: bool) -> Result<()> {
        let mut failed_paths = Vec::new();

        for path in paths {
            if !self.path_exists(path).await? {
                eprintln!("Path not found: {path}");
                failed_paths.push(path.clone());
                continue;
            }

            if self.is_directory(path).await? && !recursive {
                return DirectoryDeletionNotRecursiveSnafu { path: path.clone() }.fail();
            }

            match self.operator.remove_all(path).await {
                Ok(_) => println!("Deleted: {path}"),
                Err(e) => {
                    eprintln!("Failed to delete {path}: {e}");
                    failed_paths.push(path.clone());
                }
            }
        }

        if !failed_paths.is_empty() {
            return PartialDeletionSnafu { failed_paths }.fail();
        }

        Ok(())
    }

    async fn path_exists(&self, path: &str) -> Result<bool> {
        match self.operator.stat(path).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn is_directory(&self, path: &str) -> Result<bool> {
        match self.operator.stat(path).await {
            Ok(metadata) => Ok(metadata.mode().is_dir()),
            Err(_) => Ok(false),
        }
    }
}

struct FileInfo {
    path: String,
    size: u64,
    modified: Option<String>,
    is_dir: bool,
}

impl FileInfo {
    fn from_entry(entry: &opendal::Entry) -> Self {
        let meta = entry.metadata();
        Self {
            path: entry.path().to_string(),
            size: meta.content_length(),
            modified: meta.last_modified().map(|t| t.to_rfc3339()),
            is_dir: meta.mode().is_dir(),
        }
    }
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_type = if self.is_dir { "DIR" } else { "FILE" };
        let size_str = if self.is_dir {
            "-".to_string()
        } else {
            format_size(self.size)
        };
        let modified = self.modified.as_deref().unwrap_or("Unknown");
        write!(f, "{file_type:<6} {size_str:>10} {modified} {}", self.path)
    }
}

fn format_size(size: u64) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    const THRESHOLD: u64 = 1024;
    if size < THRESHOLD {
        return format!("{size}B");
    }
    let mut size_f = size as f64;
    let mut unit_index = 0;
    while size_f >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size_f /= THRESHOLD as f64;
        unit_index += 1;
    }
    format!("{size_f:.1}{}", UNITS[unit_index])
}
