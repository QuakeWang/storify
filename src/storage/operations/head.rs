use crate::error::{Error, Result};
use opendal::Operator;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;

/// Trait for displaying the beginning of file contents in object storage.
pub trait Header {
    /// Display the beginning of file contents with optional size limits.
    ///
    /// # Arguments
    /// * `path` - File path to display
    /// * `lines` - Number of lines to display (None for byte-based reading)
    /// * `bytes` - Number of bytes to display (None for line-based reading)
    /// * `force` - Whether to bypass size-limit confirmation
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn head(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
        force: bool,
    ) -> Result<()>;
}

/// Implementation of Header for OpenDAL Operator.
pub struct OpenDalHeadReader {
    operator: Operator,
}

impl OpenDalHeadReader {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Read and display the beginning of file content.
    ///
    /// # Arguments
    /// * `path` - File path to display
    /// * `lines` - Number of lines to display (None for byte-based reading)
    /// * `bytes` - Number of bytes to display (None for line-based reading)
    /// * `force` - Whether to bypass size-limit confirmation
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    pub async fn read_and_display_head(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
        force: bool,
    ) -> Result<()> {
        // Get file metadata
        let metadata = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: PathBuf::from(path),
                }
            } else {
                self.map_to_head_failed(path, e)
            }
        })?;

        // Determine reading mode and size limit
        let (mode, size_limit) = match (lines, bytes) {
            (Some(line_count), None) => (HeadMode::Lines(line_count), None),
            (None, Some(byte_count)) => (HeadMode::Bytes(byte_count), Some(byte_count)),
            (None, None) => (HeadMode::Bytes(1024), Some(1024)), // Default 1KB
            (Some(_), Some(_)) => {
                return Err(Error::InvalidArgument {
                    message: "Cannot specify both lines and bytes options".to_string(),
                });
            }
        };

        // Check size limit for large files
        if let Some(limit) = size_limit {
            let file_size = metadata.content_length();
            if file_size > limit as u64
                && !force
                && !self.confirm_large_file(file_size, limit as u64).await?
            {
                return Ok(());
            }
        }

        // Read and display based on mode
        match mode {
            HeadMode::Lines(line_count) => {
                self.head_by_lines(path, line_count).await?;
            }
            HeadMode::Bytes(byte_count) => {
                self.head_by_bytes(path, byte_count).await?;
            }
        }

        Ok(())
    }

    /// Read and display file content by lines.
    async fn head_by_lines(&self, path: &str, lines: usize) -> Result<()> {
        let data = self
            .operator
            .read(path)
            .await
            .map_err(|e| self.map_to_head_failed(path, e))?;
        let data_vec = data.to_vec();
        let content = String::from_utf8_lossy(&data_vec);

        let line_count = content.lines().count();
        let lines_to_show = lines.min(line_count);

        let stdout = io::stdout();
        let mut handle = stdout.lock();

        for (i, line) in content.lines().enumerate() {
            if i >= lines_to_show {
                break;
            }
            writeln!(handle, "{}", line).map_err(|e| Error::HeadFailed {
                path: path.to_string(),
                source: Box::new(e.into()),
            })?;
        }

        handle.flush().map_err(|e| Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(e.into()),
        })
    }

    /// Read and display file content by bytes.
    async fn head_by_bytes(&self, path: &str, bytes: usize) -> Result<()> {
        let file_size = self
            .operator
            .stat(path)
            .await
            .map_err(|e| self.map_to_head_failed(path, e))?
            .content_length();

        let bytes_to_read = std::cmp::min(bytes, file_size as usize);

        if bytes_to_read == 0 {
            return Ok(());
        }

        let data = self
            .operator
            .read_with(path)
            .range(0..bytes_to_read as u64)
            .await
            .map_err(|e| self.map_to_head_failed(path, e))?;

        let stdout = io::stdout();
        let mut handle = stdout.lock();

        let bytes = data.to_vec();
        handle.write_all(&bytes).map_err(|e| Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(e.into()),
        })?;

        handle.flush().map_err(|e| Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(e.into()),
        })
    }

    /// Prompt for confirmation when the file exceeds the size limit.
    ///
    /// # Arguments
    /// * `file_size` - The file's size in bytes
    /// * `limit` - The size limit in bytes that triggers confirmation
    ///
    /// # Returns
    /// * `Result<bool>` - `Ok(true)` to continue, `Ok(false)` to abort; error on I/O failures
    async fn confirm_large_file(&self, file_size: u64, limit: u64) -> Result<bool> {
        if !io::stdin().is_terminal() {
            eprintln!(
                "File too large ({} bytes > {} bytes). Use force to override.",
                file_size, limit
            );
            return Ok(false);
        }

        eprint!(
            "File too large ({} bytes > {} bytes). Continue? [y/N]: ",
            file_size, limit
        );
        io::stderr().flush().map_err(|e| Error::HeadFailed {
            path: "stderr".to_string(),
            source: Box::new(e.into()),
        })?;

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| Error::HeadFailed {
                path: "stdin".to_string(),
                source: Box::new(e.into()),
            })?;

        let ans = input.trim();
        Ok(ans.eq_ignore_ascii_case("y") || ans.eq_ignore_ascii_case("yes"))
    }

    /// Map OpenDAL error to HeadFailed error.
    fn map_to_head_failed(&self, path: &str, err: opendal::Error) -> Error {
        Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }
}

impl Header for OpenDalHeadReader {
    async fn head(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
        force: bool,
    ) -> Result<()> {
        self.read_and_display_head(path, lines, bytes, force).await
    }
}

/// Enum to represent different head reading modes.
#[derive(Debug, Clone)]
enum HeadMode {
    Lines(usize),
    Bytes(usize),
}
