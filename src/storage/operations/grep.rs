use crate::error::{Error, Result};
use memchr::memchr_iter;
use opendal::Operator;
use std::io::BufWriter;
use std::io::{self, Write};

struct GrepOptions<'a, W: Write> {
    path: &'a str,
    needle: &'a str,
    ignore_case: bool,
    line_number: bool,
    with_filename: bool,
    handle: &'a mut W,
    out_buf: String,
}

/// Trait for searching patterns in files.
pub trait Greper {
    /// Search for lines matching pattern in file and print matches.
    ///
    /// - `ignore_case`: case-insensitive when true
    /// - `line_number`: prefix output lines with 1-based line numbers when true
    async fn grep(
        &self,
        path: &str,
        pattern: &str,
        ignore_case: bool,
        line_number: bool,
        with_filename: bool,
    ) -> Result<()>;
}

/// OpenDAL-based grep implementation.
pub struct OpenDalGreper {
    operator: Operator,
}

impl OpenDalGreper {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub async fn search_and_print(
        &self,
        path: &str,
        pattern: &str,
        ignore_case: bool,
        line_number: bool,
        with_filename: bool,
    ) -> Result<()> {
        // Ensure target exists; map NotFound to PathNotFound
        let meta = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: std::path::PathBuf::from(path),
                }
            } else {
                self.map_to_grep_failed(path, e)
            }
        })?;
        let file_size = meta.content_length();

        // Prepare search parameters
        let needle = if ignore_case {
            pattern.to_lowercase()
        } else {
            pattern.to_string()
        };

        // Stream-read the object by ranged reads
        const CHUNK_SIZE: u64 = 64 * 1024;
        let stdout = io::stdout();
        let mut handle = BufWriter::new(stdout.lock());

        let mut next_offset: u64 = 0;
        let mut line_no: usize = 0;
        let mut leftover: Vec<u8> = Vec::new();

        // If file_size == 0, the object may be empty or provider doesn't expose size.
        // We still attempt ranged reads in fixed chunks until EOF.
        let known_size = file_size > 0;
        let mut opts = GrepOptions {
            path,
            needle: &needle,
            ignore_case,
            line_number,
            with_filename,
            handle: &mut handle,
            out_buf: String::with_capacity(256),
        };

        // Tracks EOF for unknown-size reads when we observe a short read
        let mut reached_eof: bool = false;

        loop {
            let data = if known_size {
                if next_offset >= file_size {
                    break;
                }
                let end_offset = std::cmp::min(next_offset + CHUNK_SIZE, file_size);
                if end_offset == next_offset {
                    break;
                }
                let d = self
                    .operator
                    .read_with(path)
                    .range(next_offset..end_offset)
                    .await
                    .map_err(|e| self.map_to_grep_failed(path, e))?;
                next_offset = end_offset;
                d
            } else {
                let start_offset = next_offset;
                let end_offset = start_offset + CHUNK_SIZE;
                match self
                    .operator
                    .read_with(path)
                    .range(start_offset..end_offset)
                    .await
                {
                    Ok(d) => {
                        // If provider doesn't expose size, a short read indicates EOF
                        let requested = end_offset - start_offset;
                        next_offset = end_offset;
                        if (d.len() as u64) < requested {
                            reached_eof = true;
                        }
                        d
                    }
                    Err(e) => {
                        // Treat Range Not Satisfied (HTTP 416) as EOF instead of failure
                        if e.kind() == opendal::ErrorKind::RangeNotSatisfied {
                            reached_eof = true;
                            opendal::Buffer::from(Vec::<u8>::new())
                        } else {
                            return Err(self.map_to_grep_failed(path, e));
                        }
                    }
                }
            };

            let chunk = data.to_vec();
            if chunk.is_empty() {
                break;
            }

            // Concatenate leftover with current chunk
            // Build a combined buffer without extra allocations when possible
            let mut buf = leftover;
            if buf.capacity() < buf.len() + chunk.len() {
                // Ensure we don't reallocate repeatedly in tight loops
                buf.reserve(chunk.len());
            }
            buf.extend_from_slice(&chunk);
            // Reset leftover for reuse
            let combined = buf;

            // Split by '\n'; keep last partial line in leftover (memchr for speed)
            let mut start: usize = 0;
            for i in memchr_iter(b'\n', &combined) {
                let mut line_bytes = &combined[start..i];
                if let Some(&b'\r') = line_bytes.last() {
                    line_bytes = &line_bytes[..line_bytes.len() - 1];
                }

                line_no += 1;
                self.process_line(&mut opts, line_no, line_bytes)?;

                start = i + 1;
            }

            // Save remaining partial line to leftover (reuse allocation)
            leftover = if start < combined.len() {
                let mut combined = combined; // take ownership
                combined.split_off(start)
            } else {
                Vec::new()
            };

            if reached_eof {
                break;
            }
        }

        // Process leftover as the final line (no trailing newline)
        if !leftover.is_empty() {
            let mut line_bytes = leftover.as_slice();
            if let Some(&b'\r') = line_bytes.last() {
                line_bytes = &line_bytes[..line_bytes.len() - 1];
            }
            line_no += 1;
            self.process_line(&mut opts, line_no, line_bytes)?;
        }

        self.flush_handle(path, &mut handle)
    }

    fn process_line<W: Write>(
        &self,
        opts: &mut GrepOptions<W>,
        line_no: usize,
        line_bytes: &[u8],
    ) -> Result<()> {
        let line = String::from_utf8_lossy(line_bytes);
        // Optimize ASCII fast-path for case-insensitive checks without allocations
        let matched = if opts.ignore_case {
            if line.is_ascii() && opts.needle.is_ascii() {
                // opts.needle 已在外层按 ignore_case 预小写，这里仅对 haystack 做无分配按位比较
                Self::ascii_contains_case_insensitive(line.as_bytes(), opts.needle.as_bytes())
            } else {
                line.to_lowercase().contains(opts.needle)
            }
        } else {
            line.contains(opts.needle)
        };

        if matched {
            use std::fmt::Write as _;
            opts.out_buf.clear();
            if opts.with_filename {
                let _ = write!(&mut opts.out_buf, "{}:", opts.path);
            }
            if opts.line_number {
                let _ = write!(&mut opts.out_buf, "{}:", line_no);
            }
            let _ = writeln!(&mut opts.out_buf, "{}", line);
            self.write_all_handle(opts.path, &mut opts.handle, opts.out_buf.as_bytes())?;
        }
        Ok(())
    }

    #[inline]
    fn ascii_contains_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
        if needle.is_empty() {
            return true;
        }
        if needle.len() > haystack.len() {
            return false;
        }
        let nlen = needle.len();
        for i in 0..=haystack.len() - nlen {
            let mut j = 0;
            while j < nlen {
                if haystack[i + j].to_ascii_lowercase() != needle[j] {
                    break;
                }
                j += 1;
            }
            if j == nlen {
                return true;
            }
        }
        false
    }

    fn map_to_grep_failed(&self, path: &str, err: opendal::Error) -> Error {
        Error::GrepFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    fn map_io_to_grep_failed(&self, path: &str, err: io::Error) -> Error {
        Error::GrepFailed {
            path: path.to_string(),
            source: Box::new(err.into()),
        }
    }

    fn write_all_handle<W: Write>(&self, path: &str, handle: &mut W, buf: &[u8]) -> Result<()> {
        handle
            .write_all(buf)
            .map_err(|e| self.map_io_to_grep_failed(path, e))
    }

    fn flush_handle<W: Write>(&self, path: &str, handle: &mut W) -> Result<()> {
        handle
            .flush()
            .map_err(|e| self.map_io_to_grep_failed(path, e))
    }
}

impl Greper for OpenDalGreper {
    async fn grep(
        &self,
        path: &str,
        pattern: &str,
        ignore_case: bool,
        line_number: bool,
        with_filename: bool,
    ) -> Result<()> {
        self.search_and_print(path, pattern, ignore_case, line_number, with_filename)
            .await
    }
}
