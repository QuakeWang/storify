use crate::error::{Error, Result};
use crate::storage::utils::path::get_root_relative_path;
use futures::stream::TryStreamExt;
use opendal::{EntryMode, Operator};
use std::path::{Path, PathBuf};
use tokio::fs;

/// Trait for downloading files and directories from storage.
pub trait Downloader {
    /// Download a single file or entire directory from remote to local.
    ///
    /// # Arguments
    /// * `remote_path` - Source path in storage (file or directory)
    /// * `local_path` - Destination path on local filesystem
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn download(&self, remote_path: &str, local_path: &str) -> Result<()>;
}

/// Implementation of Downloader for OpenDAL Operator.
pub struct OpenDalDownloader {
    operator: Operator,
}

impl OpenDalDownloader {
    /// Create a new downloader with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

impl Downloader for OpenDalDownloader {
    async fn download(&self, remote_path: &str, local_path: &str) -> Result<()> {
        if !self.operator.exists(remote_path).await? {
            return Err(Error::PathNotFound {
                path: PathBuf::from(remote_path),
            });
        }

        let lister = self
            .operator
            .lister_with(remote_path)
            .recursive(true)
            .await?;

        let mut stream = lister;
        while let Some(entry) = stream.try_next().await? {
            let meta = entry.metadata();
            let remote_file_path = entry.path();
            // Skip malformed keys that contain double slashes which may be normalized differently at read time
            if remote_file_path.contains("//") {
                log::warn!(
                    "Skip malformed remote key containing double slashes: {}",
                    remote_file_path
                );
                continue;
            }
            let mut relative_path = get_root_relative_path(remote_file_path, remote_path);
            if relative_path.is_empty() {
                // Fallback: use base name
                relative_path = Path::new(remote_file_path)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
            }
            let local_file_path = Path::new(local_path).join(relative_path);

            if meta.mode() == EntryMode::DIR {
                fs::create_dir_all(&local_file_path).await?;
            } else {
                if let Some(parent) = local_file_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                match self.operator.read(remote_file_path).await {
                    Ok(data) => {
                        fs::write(&local_file_path, data.to_vec()).await?;
                        println!(
                            "Downloaded: {remote_file_path} → {}",
                            local_file_path.display()
                        );
                    }
                    Err(e) => {
                        // Gracefully skip objects that cannot be found due to key normalization issues
                        if e.kind() == opendal::ErrorKind::NotFound {
                            log::warn!(
                                "Skip not found at read (likely normalized key): {}",
                                remote_file_path
                            );
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            }
        }

        Ok(())
    }
}
