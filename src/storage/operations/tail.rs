use crate::error::{Error, Result};
use opendal::Operator;
use std::io::{self, Write};
use std::path::PathBuf;

/// Trait for displaying the end of file contents in object storage.
pub trait Tailer {
    /// Display the end of file contents with optional size limits.
    ///
    /// # Arguments
    /// * `path` - File path to display
    /// * `lines` - Number of lines to display (None for byte-based reading)
    /// * `bytes` - Number of bytes to display (None for line-based reading)
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn tail(&self, path: &str, lines: Option<usize>, bytes: Option<usize>) -> Result<()>;
}

/// Implementation of Tailer for OpenDAL Operator.
pub struct OpenDalTailReader {
    operator: Operator,
}

impl OpenDalTailReader {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub async fn read_and_display_tail(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
    ) -> Result<()> {
        let meta = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: PathBuf::from(path),
                }
            } else {
                self.map_to_tail_failed(path, e)
            }
        })?;
        let file_size = meta.content_length();

        let mode = match (lines, bytes) {
            (Some(line_count), None) => TailMode::Lines(line_count),
            (None, Some(byte_count)) => TailMode::Bytes(byte_count),
            (None, None) => TailMode::Lines(10),
            (Some(_), Some(_)) => {
                return Err(Error::InvalidArgument {
                    message: "Cannot specify both lines and bytes options".to_string(),
                });
            }
        };

        match mode {
            TailMode::Lines(line_count) => {
                self.tail_by_lines(path, line_count, file_size).await?;
            }
            TailMode::Bytes(byte_count) => {
                self.tail_by_bytes(path, byte_count, file_size).await?;
            }
        }
        Ok(())
    }

    async fn tail_by_bytes(&self, path: &str, bytes: usize, file_size: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let to_read = std::cmp::min(bytes as u64, file_size);
        if to_read == 0 {
            return Ok(());
        }

        let start = file_size.saturating_sub(to_read);
        let data = self
            .operator
            .read_with(path)
            .range(start..file_size)
            .await
            .map_err(|e| self.map_to_tail_failed(path, e))?;

        let stdout = io::stdout();
        let mut handle = stdout.lock();
        self.write_all_handle(path, &mut handle, &data.to_vec())?;
        self.flush_handle(path, &mut handle)
    }

    async fn tail_by_lines(&self, path: &str, max_lines: usize, file_size: u64) -> Result<()> {
        if max_lines == 0 || file_size == 0 {
            return Ok(());
        }

        const CHUNK_SIZE: u64 = 8192;
        let mut remain_end = file_size;
        // Collect absolute newline offsets (in ascending order)
        let mut newline_offsets: Vec<u64> = Vec::new();

        // Check if file ends with a newline
        let ends_with_newline = {
            let last_byte = self
                .operator
                .read_with(path)
                .range(file_size.saturating_sub(1)..file_size)
                .await
                .map(|d| d.to_vec())
                .unwrap_or_default();
            !last_byte.is_empty() && last_byte[0] == b'\n'
        };

        let needed_newlines = if ends_with_newline {
            max_lines + 1
        } else {
            max_lines
        };

        while remain_end > 0 && newline_offsets.len() < needed_newlines {
            let start = remain_end.saturating_sub(CHUNK_SIZE);
            let end = remain_end;
            let chunk = self
                .operator
                .read_with(path)
                .range(start..end)
                .await
                .map_err(|e| self.map_to_tail_failed(path, e))?;

            // scan only the newly read chunk for newlines and merge in front
            if !chunk.is_empty() {
                let data = chunk.to_vec();
                let mut chunk_newlines: Vec<u64> = Vec::new();
                for (i, b) in data.iter().enumerate() {
                    if *b == b'\n' {
                        chunk_newlines.push(start + i as u64);
                    }
                }
                if !chunk_newlines.is_empty() {
                    let mut merged =
                        Vec::with_capacity(chunk_newlines.len() + newline_offsets.len());
                    merged.extend_from_slice(&chunk_newlines);
                    merged.extend_from_slice(&newline_offsets);
                    newline_offsets = merged;
                }
            }

            remain_end = start;
        }

        // Determine start offset based on collected newline offsets
        let start_offset: u64 = if ends_with_newline {
            if newline_offsets.len() > max_lines {
                newline_offsets[newline_offsets.len() - (max_lines + 1)] + 1
            } else {
                0
            }
        } else if newline_offsets.len() >= max_lines {
            newline_offsets[newline_offsets.len() - max_lines] + 1
        } else {
            0
        };

        // Read the tail once from start_offset to EOF and output
        if start_offset >= file_size {
            return Ok(());
        }
        let data = self
            .operator
            .read_with(path)
            .range(start_offset..file_size)
            .await
            .map_err(|e| self.map_to_tail_failed(path, e))?;
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        self.write_all_handle(path, &mut handle, &data.to_vec())?;
        self.flush_handle(path, &mut handle)
    }

    fn map_to_tail_failed(&self, path: &str, err: opendal::Error) -> Error {
        Error::TailFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    fn map_io_to_tail_failed(&self, path: &str, err: io::Error) -> Error {
        Error::TailFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    fn write_all_handle<W: Write>(&self, path: &str, handle: &mut W, buf: &[u8]) -> Result<()> {
        handle
            .write_all(buf)
            .map_err(|e| self.map_io_to_tail_failed(path, e))
    }

    fn flush_handle<W: Write>(&self, path: &str, handle: &mut W) -> Result<()> {
        handle
            .flush()
            .map_err(|e| self.map_io_to_tail_failed(path, e))
    }
}

impl Tailer for OpenDalTailReader {
    async fn tail(&self, path: &str, lines: Option<usize>, bytes: Option<usize>) -> Result<()> {
        self.read_and_display_tail(path, lines, bytes).await
    }
}

#[derive(Debug, Clone, Copy)]
enum TailMode {
    Lines(usize),
    Bytes(usize),
}
