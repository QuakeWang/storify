use crate::error::{Error, Result};
use crate::storage::constants::DEFAULT_CHUNK_SIZE;
use opendal::Operator;
use std::io::IsTerminal;
use std::io::{self, Write};
use std::path::PathBuf;

/// Trait for displaying file contents in object storage.
pub trait Cater {
    /// Display file contents with optional large-file protection.
    ///
    /// # Arguments
    /// * `path` - File path to display
    /// * `force` - Whether to bypass size-limit confirmation
    /// * `size_limit_mb` - Maximum file size (in MB) before asking for confirmation; `0` disables the check
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn cat(&self, path: &str, force: bool, size_limit_mb: u64) -> Result<()>;
}

/// Implementation of Cater for OpenDAL Operator.
pub struct OpenDalFileReader {
    operator: Operator,
}

impl OpenDalFileReader {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Read and display file content.
    ///
    /// # Arguments
    /// * `path` - File path to display
    /// * `force` - Whether to bypass size-limit confirmation
    /// * `size_limit_mb` - Maximum file size (in MB) before asking for confirmation; `0` disables the check
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    pub async fn read_and_display(
        &self,
        path: &str,
        force: bool,
        size_limit_mb: u64,
    ) -> Result<()> {
        // Get file metadata
        let metadata = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: PathBuf::from(path),
                }
            } else {
                self.map_to_cat_failed(path, e)
            }
        })?;

        // Check size limit
        if size_limit_mb > 0 {
            let size = metadata.content_length();
            let file_size_mb = size.div_ceil(1024 * 1024);
            if file_size_mb > size_limit_mb
                && !force
                && !self.confirm_large_file(file_size_mb, size_limit_mb).await?
            {
                return Ok(());
            }
        }

        // Stream read and display
        let file_size = metadata.content_length();
        let stdout = io::stdout();
        let mut handle = stdout.lock();

        let mut offset: u64 = 0;
        while offset < file_size {
            let chunk_size = std::cmp::min(DEFAULT_CHUNK_SIZE as u64, file_size - offset);
            let data = self
                .operator
                .read_with(path)
                .range(offset..offset + chunk_size)
                .await
                .map_err(|e| self.map_to_cat_failed(path, e))?;

            if data.is_empty() {
                break;
            }

            let bytes = data.to_vec();
            handle.write_all(&bytes).map_err(|e| Error::CatFailed {
                path: path.to_string(),
                source: Box::new(e.into()),
            })?;
            offset += bytes.len() as u64;
        }

        handle.flush().map_err(|e| Error::CatFailed {
            path: path.to_string(),
            source: Box::new(e.into()),
        })
    }

    /// Prompt for confirmation when the file exceeds the size limit.
    ///
    /// # Arguments
    /// * `file_size_mb` - The file's size in MB
    /// * `limit_mb` - The size limit in MB that triggers confirmation
    ///
    /// # Returns
    /// * `Result<bool>` - `Ok(true)` to continue, `Ok(false)` to abort; error on I/O failures
    async fn confirm_large_file(&self, file_size_mb: u64, limit_mb: u64) -> Result<bool> {
        if !io::stdin().is_terminal() {
            eprintln!("File too large ({file_size_mb}MB > {limit_mb}MB). Use force to override.");
            return Ok(false);
        }

        eprint!("File too large ({file_size_mb}MB > {limit_mb}MB). Continue? [y/N]: ");
        io::stderr().flush().map_err(|e| Error::CatFailed {
            path: "stderr".to_string(),
            source: Box::new(e.into()),
        })?;

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| Error::CatFailed {
                path: "stdin".to_string(),
                source: Box::new(e.into()),
            })?;

        let ans = input.trim();
        Ok(ans.eq_ignore_ascii_case("y") || ans.eq_ignore_ascii_case("yes"))
    }

    /// Map OpenDAL error to CatFailed error.
    fn map_to_cat_failed(&self, path: &str, err: opendal::Error) -> Error {
        Error::CatFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }
}

impl Cater for OpenDalFileReader {
    async fn cat(&self, path: &str, force: bool, size_limit_mb: u64) -> Result<()> {
        self.read_and_display(path, force, size_limit_mb).await
    }
}
