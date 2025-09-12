use crate::error::{Error, Result};
use opendal::Operator;
use std::io::{self, Write};
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
        _force: bool,
    ) -> Result<()> {
        // Ensure path exists and map NotFound to PathNotFound
        let _ = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: PathBuf::from(path),
                }
            } else {
                self.map_to_head_failed(path, e)
            }
        })?;

        // Determine reading mode
        // Default behavior (no flags) reads first 10 lines without confirmation.
        let mode = match (lines, bytes) {
            (Some(line_count), None) => HeadMode::Lines(line_count),
            (None, Some(byte_count)) => HeadMode::Bytes(byte_count),
            (None, None) => HeadMode::Lines(10), // Default 10 lines
            (Some(_), Some(_)) => {
                return Err(Error::InvalidArgument {
                    message: "Cannot specify both lines and bytes options".to_string(),
                });
            }
        };

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

    /// Display the beginning of multiple files with minimal GNU head-like headers.
    pub async fn head_many(
        &self,
        paths: &[String],
        lines: Option<usize>,
        bytes: Option<usize>,
        quiet: bool,
        verbose: bool,
        force: bool,
    ) -> Result<()> {
        if quiet && verbose {
            return Err(Error::InvalidArgument {
                message: "Cannot specify both --quiet and --verbose".to_string(),
            });
        }

        let should_show_header = |total: usize| -> bool {
            if verbose {
                return true;
            }
            if quiet {
                return false;
            }
            total > 1
        };

        let total = paths.len();
        for (idx, p) in paths.iter().enumerate() {
            let show_header = should_show_header(total);
            if show_header {
                if idx > 0 {
                    println!();
                }
                println!("==> {} <==", p);
            }

            if let Err(e) = self.read_and_display_head(p, lines, bytes, force).await {
                eprintln!("{}", e);
            }
        }

        Ok(())
    }

    /// Read and display file content by lines using ranged, chunked reads.
    async fn head_by_lines(&self, path: &str, max_lines: usize) -> Result<()> {
        if max_lines == 0 {
            return Ok(());
        }

        // Determine file size to clamp range end within object bounds.
        let file_size = self
            .operator
            .stat(path)
            .await
            .map_err(|e| self.map_to_head_failed(path, e))?
            .content_length();

        // Use small ranged reads to avoid loading entire file.
        const CHUNK_SIZE: u64 = 8192;
        let stdout = io::stdout();
        let mut handle = stdout.lock();

        let mut next_offset: u64 = 0;
        let mut lines_emitted: usize = 0;
        let mut carry: Vec<u8> = Vec::new();

        loop {
            if next_offset >= file_size {
                // EOF: output any remaining partial line without adding extra newline.
                if !carry.is_empty() {
                    handle.write_all(&carry).map_err(|e| Error::HeadFailed {
                        path: path.to_string(),
                        source: Box::new(e.into()),
                    })?;
                }
                break;
            }

            let end_offset = std::cmp::min(next_offset + CHUNK_SIZE, file_size);
            if end_offset == next_offset {
                break;
            }

            let data = self
                .operator
                .read_with(path)
                .range(next_offset..end_offset)
                .await
                .map_err(|e| self.map_to_head_failed(path, e))?;

            let chunk = data.to_vec();
            let n = chunk.len();

            if n == 0 {
                // EOF: output any remaining partial line without adding extra newline.
                if !carry.is_empty() {
                    handle.write_all(&carry).map_err(|e| Error::HeadFailed {
                        path: path.to_string(),
                        source: Box::new(e.into()),
                    })?;
                }
                break;
            }

            let mut start_idx: usize = 0;
            for i in 0..n {
                if chunk[i] == b'\n' {
                    // Write carry, then the current line including the newline.
                    if !carry.is_empty() {
                        handle.write_all(&carry).map_err(|e| Error::HeadFailed {
                            path: path.to_string(),
                            source: Box::new(e.into()),
                        })?;
                        carry.clear();
                    }
                    handle
                        .write_all(&chunk[start_idx..=i])
                        .map_err(|e| Error::HeadFailed {
                            path: path.to_string(),
                            source: Box::new(e.into()),
                        })?;
                    lines_emitted += 1;
                    start_idx = i + 1;
                    if lines_emitted >= max_lines {
                        handle.flush().map_err(|e| Error::HeadFailed {
                            path: path.to_string(),
                            source: Box::new(e.into()),
                        })?;
                        return Ok(());
                    }
                }
            }

            // Save remainder (partial line) for next chunk.
            if start_idx < n {
                carry.extend_from_slice(&chunk[start_idx..n]);
            }

            next_offset += n as u64;
        }

        handle.flush().map_err(|e| Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(e.into()),
        })
    }

    /// Read and display file content by bytes.
    async fn head_by_bytes(&self, path: &str, bytes: usize) -> Result<()> {
        // Try to stat file to get size; if stat fails or returns 0, still attempt to read requested bytes.
        let file_size = match self.operator.stat(path).await {
            Ok(meta) => meta.content_length(),
            Err(_) => 0,
        } as usize;

        let bytes_to_read = if file_size == 0 {
            // Unknown size; attempt to read the requested range anyway
            bytes
        } else {
            std::cmp::min(bytes, file_size)
        };

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
