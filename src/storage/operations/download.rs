use crate::error::{Error, Result};
use crate::storage::constants::DEFAULT_CHUNK_SIZE;
use crate::storage::utils::path::get_root_relative_path;
use crate::storage::utils::progress::ConsoleProgressReporter;
use crate::storage::utils::retry::read_range_with_retry;
use futures::stream::TryStreamExt;
use opendal::{EntryMode, Operator};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

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

    async fn is_directory(&self, path: &str) -> bool {
        match self.operator.stat(path).await.ok().map(|m| m.mode()) {
            Some(EntryMode::DIR) => true,
            Some(_) => false,
            None => {
                let probe = if path.ends_with('/') {
                    path.to_string()
                } else {
                    format!("{path}/")
                };
                self.operator
                    .list_with(&probe)
                    .limit(1)
                    .await
                    .map(|entries| !entries.is_empty())
                    .unwrap_or(false)
            }
        }
    }

    async fn resolve_local_file_path(
        &self,
        remote_file_path: &str,
        local_path: &str,
    ) -> Result<PathBuf> {
        let local = Path::new(local_path);

        let local_is_dir = if local_path.ends_with('/') {
            true
        } else if local.extension().is_some() {
            false
        } else {
            match fs::metadata(local).await {
                Ok(meta) => meta.is_dir(),
                Err(_) => true,
            }
        };

        if local_is_dir {
            let file_name = Path::new(remote_file_path)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            Ok(local.join(file_name))
        } else {
            Ok(local.to_path_buf())
        }
    }

    async fn download_single_file(
        &self,
        remote_file_path: &str,
        local_file_path: &Path,
    ) -> Result<()> {
        if let Some(parent) = local_file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let metadata = self.operator.stat(remote_file_path).await?;
        let file_size = metadata.content_length();
        let remote_etag = metadata.etag().map(str::to_string);

        let mut offset = match fs::metadata(local_file_path).await {
            Ok(meta) if meta.is_file() => {
                let candidate = meta.len().min(file_size);
                // Validate resume: check that the remote object hasn't changed
                // since we started downloading by comparing ETags.
                if candidate > 0 {
                    let etag_path = local_file_path.with_extension("etag");
                    let saved_etag = fs::read_to_string(&etag_path).await.ok();
                    match (&saved_etag, &remote_etag) {
                        (Some(saved), Some(remote)) if saved.trim() == remote.trim() => candidate,
                        (Some(_), Some(_)) => {
                            // ETag mismatch — remote object changed, restart
                            log::warn!(
                                "Remote object changed since partial download, restarting: {}",
                                remote_file_path
                            );
                            let _ = fs::remove_file(&etag_path).await;
                            0u64
                        }
                        _ => candidate, // No ETag available — best effort resume
                    }
                } else {
                    0u64
                }
            }
            _ => 0u64,
        };

        if offset == file_size && file_size > 0 {
            println!(
                "Skipped (already downloaded): {remote_file_path} → {} ({file_size} bytes)",
                local_file_path.display()
            );
            return Ok(());
        }

        // Persist remote ETag for resume validation on future runs
        if let Some(ref etag) = remote_etag {
            let etag_path = local_file_path.with_extension("etag");
            let _ = fs::write(&etag_path, etag.as_bytes()).await;
        }

        let mut file = if offset > 0 {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(local_file_path)
                .await?
        } else {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(local_file_path)
                .await?
        };

        let mut total_bytes = offset;

        let mut reporter = ConsoleProgressReporter::new(
            format!("Downloading {remote_file_path}"),
            Some(file_size),
            DEFAULT_CHUNK_SIZE as u64,
        );

        loop {
            if offset >= file_size {
                break;
            }

            let chunk_size = std::cmp::min(DEFAULT_CHUNK_SIZE as u64, file_size - offset);
            let data = read_range_with_retry(
                &self.operator,
                remote_file_path,
                offset..offset + chunk_size,
            )
            .await?;
            let data_len = data.len();
            if data_len == 0 {
                break;
            }

            let bytes = data.to_bytes();
            file.write_all(&bytes).await?;
            total_bytes += data_len as u64;
            offset += data_len as u64;

            reporter.maybe_report(total_bytes);
        }

        file.flush().await?;

        // Clean up ETag sidecar after successful download
        let etag_path = local_file_path.with_extension("etag");
        let _ = fs::remove_file(&etag_path).await;

        println!(
            "\nDownloaded: {remote_file_path} → {} ({total_bytes} bytes)",
            local_file_path.display()
        );
        Ok(())
    }
}

impl Downloader for OpenDalDownloader {
    async fn download(&self, remote_path: &str, local_path: &str) -> Result<()> {
        let remote_path = if remote_path == "/" {
            ""
        } else {
            remote_path.trim_start_matches('/')
        };

        if !self.is_directory(remote_path).await {
            if let Err(e) = self.operator.stat(remote_path).await {
                if e.kind() == opendal::ErrorKind::NotFound {
                    return Err(Error::PathNotFound {
                        path: PathBuf::from(remote_path),
                    });
                }
                return Err(e.into());
            }

            let local_file_path = self
                .resolve_local_file_path(remote_path, local_path)
                .await?;
            return self
                .download_single_file(remote_path, &local_file_path)
                .await;
        }

        let lister = self
            .operator
            .lister_with(remote_path)
            .recursive(true)
            .await?;

        let mut stream = lister;
        let mut saw_any = false;
        while let Some(entry) = stream.try_next().await? {
            saw_any = true;
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
                match self
                    .download_single_file(remote_file_path, &local_file_path)
                    .await
                {
                    Ok(()) => {}
                    Err(Error::OpenDal { source })
                        if source.kind() == opendal::ErrorKind::NotFound =>
                    {
                        log::warn!(
                            "Skip not found at read (likely normalized key): {}",
                            remote_file_path
                        );
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        if !saw_any {
            return Err(Error::PathNotFound {
                path: PathBuf::from(remote_path),
            });
        }

        Ok(())
    }
}
