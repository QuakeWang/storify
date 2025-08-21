use crate::error::{InvalidPathSnafu, Result};
use crate::storage::constants::DEFAULT_CHUNK_SIZE;
use crate::storage::utils::path::{
    basename, build_remote_path, ensure_trailing_slash, get_root_relative_path,
};
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

    /// Hybrid directory detection for object storage: stat first; if not available, probe prefix.
    async fn is_directory(&self, path: &str) -> bool {
        match self.operator.stat(path).await.ok().map(|m| m.mode()) {
            Some(EntryMode::DIR) => true,
            Some(_) => false,
            None => {
                let probe = ensure_trailing_slash(path);
                self.operator
                    .list_with(&probe)
                    .limit(1)
                    .await
                    .map(|entries| !entries.is_empty())
                    .unwrap_or(false)
            }
        }
    }

    /// Ensure a remote directory exists (appends trailing '/').
    async fn ensure_directory(&self, dir_path: &str) -> Result<()> {
        let to_create = ensure_trailing_slash(dir_path);
        self.operator.create_dir(&to_create).await?;
        Ok(())
    }

    /// Copy files recursively with directory structure preservation.
    #[async_recursion]
    async fn copy_file_recursive(&self, src_path: &str, dest_path: &str) -> Result<()> {
        let lister = self.operator.lister_with(src_path).recursive(true).await?;

        let mut stream = lister;
        while let Some(entry) = stream.try_next().await? {
            let meta = entry.metadata();
            let entry_path = entry.path();

            // Skip creating the root directory itself; caller ensured destination root exists
            let src_norm = src_path.trim_start_matches('/');
            let entry_norm = entry_path.trim_start_matches('/');
            if meta.mode() == EntryMode::DIR && entry_norm == src_norm {
                continue;
            }

            let relative_path = get_root_relative_path(entry_path, src_path);
            let new_dest_path = build_remote_path(dest_path, &relative_path);

            if meta.mode() == EntryMode::DIR {
                self.ensure_directory(&new_dest_path).await?;
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
        let src_stat = self.operator.stat(src_path).await.ok();
        let src_is_dir = self.is_directory(src_path).await;
        ensure!(
            src_stat.is_some() || src_is_dir,
            InvalidPathSnafu {
                path: src_path.to_string()
            }
        );

        if src_is_dir {
            let target_root = if self.is_directory(dest_path).await {
                let base_name = basename(src_path);
                let target_root = build_remote_path(dest_path, &base_name);
                self.ensure_directory(&target_root).await?;
                target_root
            } else {
                self.ensure_directory(dest_path).await?;
                dest_path.to_string()
            };

            self.copy_file_recursive(src_path, &target_root).await?;
            Ok(())
        } else {
            let dest_is_dir_hint = dest_path.ends_with('/');
            let dest_is_dir = self.is_directory(dest_path).await;

            if dest_is_dir_hint && !dest_is_dir {
                return Err(crate::error::Error::InvalidPath {
                    path: dest_path.to_string(),
                });
            }

            let final_dest = if dest_is_dir_hint || dest_is_dir {
                let file_name = basename(src_path);
                build_remote_path(dest_path, &file_name)
            } else {
                dest_path.to_string()
            };

            self.stream_copy(src_path, &final_dest).await?;
            Ok(())
        }
    }
}
