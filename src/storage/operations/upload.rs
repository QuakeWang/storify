use crate::error::{DirectoryUploadNotRecursiveSnafu, PathNotFoundSnafu, Result};
use crate::storage::constants::{DEFAULT_BUFFER_SIZE, PROGRESS_UPDATE_INTERVAL};
use crate::storage::utils::path::build_remote_path;
use crate::storage::utils::progress::ConsoleProgressReporter;
use async_recursion::async_recursion;
use opendal::Operator;
use snafu::ensure;
use std::ffi::OsStr;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

/// Trait for uploading files and directories to storage.
pub trait Uploader {
    /// Upload a single file or directory from local to remote storage.
    ///
    /// # Arguments
    /// * `local_path` - Source path on local filesystem (file or directory)
    /// * `remote_path` - Destination path in storage
    /// * `recursive` - Whether to upload directories recursively
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn upload(&self, local_path: &str, remote_path: &str, recursive: bool) -> Result<()>;
}

/// Implementation of Uploader for OpenDAL Operator.
pub struct OpenDalUploader {
    operator: Operator,
}

impl OpenDalUploader {
    /// Create a new uploader with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Upload a single file with streaming progress.
    async fn upload_file_streaming(&self, local_path: &Path, remote_path: &str) -> Result<()> {
        let file = fs::File::open(local_path).await?;
        let file_size = file.metadata().await?.len();
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; DEFAULT_BUFFER_SIZE];
        let mut total_bytes = 0u64;
        let mut writer = self.operator.writer(remote_path).await?;

        let step_bytes = DEFAULT_BUFFER_SIZE as u64 * PROGRESS_UPDATE_INTERVAL;
        let reporter = ConsoleProgressReporter::new(
            format!("Uploading {}", local_path.display()),
            Some(file_size),
            step_bytes,
        );

        loop {
            let bytes_read = reader.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            writer.write(buffer[..bytes_read].to_vec()).await?;
            total_bytes += bytes_read as u64;
            reporter.maybe_report(total_bytes);
        }
        writer.close().await?;
        println!(
            "\n✅ Upload: {} → {remote_path} ({total_bytes} bytes)",
            local_path.display(),
        );
        Ok(())
    }

    /// Upload a directory recursively.
    #[async_recursion]
    async fn upload_recursive(&self, local_path: &str, remote_path: &str) -> Result<()> {
        let mut entries = fs::read_dir(local_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let local_file_path = entry.path();
            let file_name = local_file_path.file_name().unwrap_or_default();
            let file_name_str = file_name.to_string_lossy();
            let new_remote_path = build_remote_path(remote_path, &file_name_str);

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
}

impl Uploader for OpenDalUploader {
    async fn upload(&self, local_path: &str, remote_path: &str, recursive: bool) -> Result<()> {
        let path = Path::new(local_path);
        ensure!(
            path.exists(),
            PathNotFoundSnafu {
                path: path.to_path_buf()
            }
        );

        if path.is_file() {
            let file_name = path.file_name().unwrap_or(OsStr::new(local_path));
            let file_name_str = file_name.to_string_lossy();
            let remote_file_path = build_remote_path(remote_path, &file_name_str);
            self.upload_file_streaming(Path::new(local_path), &remote_file_path)
                .await?;
        } else if path.is_dir() {
            if recursive {
                self.upload_recursive(local_path, remote_path).await?;
            } else {
                return DirectoryUploadNotRecursiveSnafu.fail();
            }
        }

        Ok(())
    }
}
