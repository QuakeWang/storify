use crate::error::{Error, Result};
use opendal::Operator;
use std::path::Path;
use tokio::io::AsyncReadExt as TokioAsyncReadExt;

pub trait Appender {
    async fn append_from_local(
        &self,
        local: &str,
        remote: &str,
        opts: &AppendOptions,
    ) -> Result<()>;

    async fn append_from_stdin(&self, remote: &str, opts: &AppendOptions) -> Result<()>;
}

pub struct OpenDalAppender {
    operator: Operator,
}

impl OpenDalAppender {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn stat_remote(&self, remote: &str) -> Result<Option<opendal::Metadata>> {
        match self.operator.stat(remote).await {
            Ok(meta) => {
                if meta.mode().is_dir() {
                    return Err(Error::InvalidArgument {
                        message: "Path is a directory; append supports files only".to_string(),
                    });
                }
                Ok(Some(meta))
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn write_remote(&self, remote: &str, data: Vec<u8>, parents: bool) -> Result<()> {
        match self.operator.write(remote, data.clone()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if parents
                    && e.kind() == opendal::ErrorKind::NotFound
                    && let Some(parent) = Path::new(remote).parent().and_then(|p| p.to_str())
                {
                    // Best-effort create parent dir then retry
                    let _ = self.operator.create_dir(parent).await;
                    let _ = self.operator.write(remote, data).await?;
                    return Ok(());
                }
                Err(e.into())
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AppendOptions {
    pub no_create: bool,
    pub parents: bool,
}

impl Appender for OpenDalAppender {
    async fn append_from_local(
        &self,
        local: &str,
        remote: &str,
        opts: &AppendOptions,
    ) -> Result<()> {
        let meta = self.stat_remote(remote).await?;
        if meta.is_none() && opts.no_create {
            return Err(Error::PathNotFound {
                path: std::path::PathBuf::from(remote),
            });
        }

        // Merge existing + new
        let mut merged = if meta.is_some() {
            self.operator.read(remote).await?.to_vec()
        } else {
            Vec::new()
        };
        let src = tokio::fs::read(local).await?;
        merged.extend_from_slice(&src);

        if let Some(meta_initial) = meta.as_ref() {
            let meta_after = self.stat_remote(remote).await?;
            match meta_after {
                Some(ref latest) => {
                    let etag1 = meta_initial.etag();
                    let etag2 = latest.etag();
                    if etag1.is_some() && etag2.is_some() {
                        if etag1 != etag2 {
                            return Err(Error::InvalidArgument { message: "Destination object was modified by another client before write (ETag changed), append aborted to avoid overwriting concurrent updates.".into() });
                        }
                    } else if meta_initial.content_length() != latest.content_length() {
                        return Err(Error::InvalidArgument { message: "Destination object was modified by another client before write (size changed), append aborted to avoid overwriting concurrent updates.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Destination object was deleted before append, operation aborted.".into(),
                    });
                }
            }
        }
        self.write_remote(remote, merged, opts.parents).await
    }

    async fn append_from_stdin(&self, remote: &str, opts: &AppendOptions) -> Result<()> {
        let meta = self.stat_remote(remote).await?;
        if meta.is_none() && opts.no_create {
            return Err(Error::PathNotFound {
                path: std::path::PathBuf::from(remote),
            });
        }

        let mut stdin = tokio::io::stdin();
        let mut new_data = Vec::new();
        stdin.read_to_end(&mut new_data).await?;

        let mut merged = if meta.is_some() {
            self.operator.read(remote).await?.to_vec()
        } else {
            Vec::new()
        };
        merged.extend_from_slice(&new_data);

        // 再次 stat 目标，防止并发覆盖
        if let Some(meta_initial) = meta.as_ref() {
            let meta_after = self.stat_remote(remote).await?;
            match meta_after {
                Some(ref latest) => {
                    let etag1 = meta_initial.etag();
                    let etag2 = latest.etag();
                    if etag1.is_some() && etag2.is_some() {
                        if etag1 != etag2 {
                            return Err(Error::InvalidArgument { message: "Destination object was modified by another client before write (ETag changed), append aborted to avoid overwriting concurrent updates.".into() });
                        }
                    } else if meta_initial.content_length() != latest.content_length() {
                        return Err(Error::InvalidArgument { message: "Destination object was modified by another client before write (size changed), append aborted to avoid overwriting concurrent updates.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Destination object was deleted before append, operation aborted.".into(),
                    });
                }
            }
        }
        self.write_remote(remote, merged, opts.parents).await
    }
}
