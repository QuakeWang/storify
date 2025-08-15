use crate::error::{InvalidPathSnafu, Result};
use crate::storage::constants::DEFAULT_CHUNK_SIZE;
use crate::storage::utils::path::{build_remote_path, get_root_relative_path};
use crate::storage::utils::progress::ConsoleProgressReporter;
use async_recursion::async_recursion;
use futures::stream::TryStreamExt;
use opendal::{EntryMode, Operator};
use snafu::ensure;

/// Trait for copying files and directories within storage.
pub trait Copier {
    /// Copy a single file or entire directory from one location to another in object storage.
    ///
    /// # Arguments
    /// * `src_path` - Source path in object storage (file or directory)
    /// * `dest_path` - Destination path in object storage
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn copy(&self, src_path: &str, dest_path: &str) -> Result<()>;
}

/// Implementation of Copier for OpenDAL Operator.
pub struct OpenDalCopier {
    operator: Operator,
}

impl OpenDalCopier {
    /// Create a new copier with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Copy files recursively with directory structure preservation.
    #[async_recursion]
    async fn copy_file_recursive(&self, src_path: &str, dest_path: &str) -> Result<()> {
        let lister = self.operator.lister_with(src_path).recursive(true).await?;

        let mut stream = lister;
        while let Some(entry) = stream.try_next().await? {
            let meta = entry.metadata();
            let entry_path = entry.path();

            let relative_path = get_root_relative_path(entry_path, src_path);
            let new_dest_path = build_remote_path(dest_path, &relative_path);

            if meta.mode() == EntryMode::DIR {
                self.operator.create_dir(&new_dest_path).await?;
            } else {
                self.stream_copy(entry_path, &new_dest_path).await?;
            }
        }

        Ok(())
    }

    /// Stream copy a single file with progress reporting.
    async fn stream_copy(&self, src_path: &str, dest_path: &str) -> opendal::Result<()> {
        let metadata = self.operator.stat(src_path).await?;
        let file_size = metadata.content_length();

        let mut writer = self.operator.writer(dest_path).await?;
        let mut total_bytes = 0u64;
        let mut offset = 0u64;

        let reporter = ConsoleProgressReporter::new(
            format!("Copying {src_path}"),
            Some(file_size),
            DEFAULT_CHUNK_SIZE as u64,
        );

        loop {
            let chunk_size = std::cmp::min(DEFAULT_CHUNK_SIZE as u64, file_size - offset);

            let data = self
                .operator
                .read_with(src_path)
                .range(offset..offset + chunk_size)
                .await?;
            let data_len = data.len();
            if data_len == 0 {
                break;
            }

            writer.write(data).await?;
            total_bytes += data_len as u64;
            offset += chunk_size;

            reporter.maybe_report(total_bytes);
        }

        writer.close().await?;
        println!("\n✅ Copied: {src_path} → {dest_path} ({total_bytes} bytes)");

        Ok(())
    }
}

impl Copier for OpenDalCopier {
    async fn copy(&self, src_path: &str, dest_path: &str) -> Result<()> {
        match self.operator.list_with(src_path).limit(1).await {
            Ok(entries) if !entries.is_empty() => {
                self.copy_file_recursive(src_path, dest_path).await?;
                Ok(())
            }
            Ok(_) => {
                ensure!(
                    self.operator.exists(src_path).await?,
                    InvalidPathSnafu {
                        path: src_path.to_string(),
                    }
                );
                Ok(())
            }
            Err(_) => Err(crate::error::Error::InvalidPath {
                path: src_path.to_string(),
            }),
        }
    }
}
