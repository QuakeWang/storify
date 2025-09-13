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
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn head(&self, path: &str, lines: Option<usize>, bytes: Option<usize>) -> Result<()>;
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
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    pub async fn read_and_display_head(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
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

            if let Err(e) = self.read_and_display_head(p, lines, bytes).await {
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
        let mut lines_remaining: usize = max_lines;

        loop {
            if next_offset >= file_size {
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
                break;
            }

            let mut last_emit: usize = 0;
            for i in 0..n {
                if chunk[i] == b'\n' && lines_remaining > 0 {
                    self.write_all_handle(path, &mut handle, &chunk[last_emit..=i])?;
                    lines_remaining -= 1;
                    last_emit = i + 1;
                    if lines_remaining == 0 {
                        self.flush_handle(path, &mut handle)?;
                        return Ok(());
                    }
                }
            }

            if lines_remaining > 0 && last_emit < n {
                self.write_all_handle(path, &mut handle, &chunk[last_emit..n])?;
            }

            next_offset += n as u64;
        }

        self.flush_handle(path, &mut handle)
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
        self.write_all_handle(path, &mut handle, &bytes)?;
        self.flush_handle(path, &mut handle)
    }

    /// Map OpenDAL error to HeadFailed error.
    fn map_to_head_failed(&self, path: &str, err: opendal::Error) -> Error {
        Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    /// Map std::io::Error to HeadFailed error.
    fn map_io_to_head_failed(&self, path: &str, err: io::Error) -> Error {
        Error::HeadFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    /// Helper to write all bytes with unified error handling.
    fn write_all_handle<W: Write>(&self, path: &str, handle: &mut W, buf: &[u8]) -> Result<()> {
        handle
            .write_all(buf)
            .map_err(|e| self.map_io_to_head_failed(path, e))
    }

    /// Helper to flush with unified error handling.
    fn flush_handle<W: Write>(&self, path: &str, handle: &mut W) -> Result<()> {
        handle
            .flush()
            .map_err(|e| self.map_io_to_head_failed(path, e))
    }

    // carry 相关逻辑已移除，改为按块流式输出
}

impl Header for OpenDalHeadReader {
    async fn head(&self, path: &str, lines: Option<usize>, bytes: Option<usize>) -> Result<()> {
        self.read_and_display_head(path, lines, bytes).await
    }
}

/// Enum to represent different head reading modes.
#[derive(Debug, Clone)]
enum HeadMode {
    Lines(usize),
    Bytes(usize),
}
