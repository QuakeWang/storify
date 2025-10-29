use crate::error::{Error, Result};
use crate::storage::constants::{DEFAULT_BUFFER_SIZE, DEFAULT_CHUNK_SIZE};
use crate::storage::operations::mkdir::OpenDalMkdirer;
use crate::storage::operations::mv::OpenDalMover;
use crate::storage::operations::{Mkdirer, Mover};
use crate::storage::utils::path::parent_dir_of;
use opendal::{ErrorKind, Operator};
use uuid::Uuid;

pub trait Truncater {
    /// Truncate or extend a file to the specified size in bytes.
    ///
    /// - If the file is missing and `no_create` is true, this is a no-op.
    /// - If `parents` is true, attempt to create parent directories when needed.
    async fn truncate(&self, path: &str, size: u64, no_create: bool, parents: bool) -> Result<()>;
}

pub struct OpenDalTruncater {
    operator: Operator,
}

impl OpenDalTruncater {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    fn temp_path_for(path: &str) -> String {
        format!("{}.truncate.tmp-{}", path, Uuid::new_v4().simple())
    }

    async fn write_zeros(
        &self,
        writer: &mut opendal::Writer,
        mut remaining: u64,
    ) -> opendal::Result<()> {
        if remaining == 0 {
            return Ok(());
        }
        while remaining > 0 {
            let to_write = std::cmp::min(remaining, DEFAULT_CHUNK_SIZE as u64) as usize;
            let block = vec![0u8; to_write];
            writer.write(block).await?;
            remaining -= to_write as u64;
        }
        Ok(())
    }
}

impl Truncater for OpenDalTruncater {
    async fn truncate(&self, path: &str, size: u64, no_create: bool, parents: bool) -> Result<()> {
        if path.ends_with('/') {
            return Err(Error::InvalidArgument {
                message: "truncate does not support directories; use mkdir".to_string(),
            });
        }

        match self.operator.stat(path).await {
            Ok(meta) => {
                if meta.mode().is_dir() {
                    return Err(Error::InvalidArgument {
                        message: "Path is a directory; use mkdir".to_string(),
                    });
                }

                let orig_size = meta.content_length();
                // No-op when size is unchanged
                if size == orig_size {
                    return Ok(());
                }

                // Fast path for truncating to zero
                if size == 0 {
                    let mut writer = self.operator.writer(path).await?;
                    writer.close().await?;
                    println!("Truncated: {} -> 0", path);
                    return Ok(());
                }

                // General path: create a temp object with desired content then move over
                let temp_path = Self::temp_path_for(path);
                let mut writer = self.operator.writer(&temp_path).await?;

                let copy_len = std::cmp::min(size, orig_size);

                // Copy prefix from existing file in ranges
                let mut offset: u64 = 0;
                while offset < copy_len {
                    let end = std::cmp::min(copy_len, offset + DEFAULT_BUFFER_SIZE as u64);
                    let chunk = self.operator.read_with(path).range(offset..end).await?;
                    if chunk.is_empty() {
                        break;
                    }
                    writer.write(chunk).await?;
                    offset = end;
                }

                // Zero padding if we need to extend
                if size > copy_len {
                    let pad = size - copy_len;
                    self.write_zeros(&mut writer, pad).await?;
                }

                writer.close().await?;

                // Move temp over original
                let mover = OpenDalMover::new(self.operator.clone());
                if let Err(e) = Mover::mover(&mover, &temp_path, path).await {
                    // Best-effort cleanup of temp object
                    let _ = self.operator.delete(&temp_path).await;
                    return Err(e);
                }
                println!("Truncated: {} -> {}", path, size);
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                if no_create {
                    return Ok(());
                }

                if parents
                    && let Some(parent) = parent_dir_of(path)
                    && !parent.is_empty()
                {
                    let mkdirer = OpenDalMkdirer::new(self.operator.clone());
                    Mkdirer::mkdir(&mkdirer, &parent, true).await?;
                }

                // Create new file with given size
                if size == 0 {
                    let mut writer = self.operator.writer(path).await?;
                    writer.close().await?;
                    println!("Created: {} (size 0)", path);
                    return Ok(());
                }

                let mut writer = self.operator.writer(path).await?;
                self.write_zeros(&mut writer, size).await?;
                writer.close().await?;
                println!("Created: {} (size {})", path, size);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
