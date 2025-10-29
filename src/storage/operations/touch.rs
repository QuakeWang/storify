use crate::error::{Error, Result};
use crate::storage::operations::Mkdirer;
use crate::storage::operations::mkdir::OpenDalMkdirer;
use crate::storage::utils::path::parent_dir_of;
use opendal::{ErrorKind, Operator};

/// Trait for touching files in storage (create or truncate)
pub trait Toucher {
    /// Ensure a file exists. Optionally truncate existing files.
    ///
    /// - When `no_create` is true and the path does not exist, this is a no-op.
    /// - When `truncate` is true and the path exists as a file, it will be truncated to 0 bytes.
    /// - When `parents` is true, try to create parent directories when needed.
    async fn touch(&self, path: &str, no_create: bool, truncate: bool, parents: bool)
    -> Result<()>;
}

pub struct OpenDalToucher {
    operator: Operator,
}

impl OpenDalToucher {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

impl Toucher for OpenDalToucher {
    async fn touch(
        &self,
        path: &str,
        no_create: bool,
        truncate: bool,
        parents: bool,
    ) -> Result<()> {
        if path.ends_with('/') {
            return Err(Error::InvalidArgument {
                message: "touch does not support directories; use mkdir".to_string(),
            });
        }

        match self.operator.stat(path).await {
            Ok(meta) => {
                if meta.mode().is_dir() {
                    return Err(Error::InvalidArgument {
                        message: "Path is a directory; use mkdir".to_string(),
                    });
                }

                if truncate {
                    let mut writer = self.operator.writer(path).await?;
                    writer.close().await?;
                    println!("Truncated: {}", path);
                }
                // else: exists and not truncating -> no-op
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                if no_create {
                    // no-op when file is missing
                    return Ok(());
                }

                if parents
                    && let Some(parent) = parent_dir_of(path)
                    && !parent.is_empty()
                {
                    let mkdirer = OpenDalMkdirer::new(self.operator.clone());
                    Mkdirer::mkdir(&mkdirer, &parent, true).await?;
                }
                let mut writer = self.operator.writer(path).await?;
                writer.close().await?;
                println!("Created: {}", path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
