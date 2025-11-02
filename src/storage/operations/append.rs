use crate::error::{Error, Result};
use crate::storage::constants::DEFAULT_BUFFER_SIZE;
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

    fn enforce_size_limit(
        existing: u64,
        added: u64,
        size_limit_mb: u64,
        force: bool,
    ) -> Result<()> {
        if size_limit_mb == 0 {
            return Ok(());
        }
        let total_mb = (existing + added).div_ceil(1024 * 1024);
        if total_mb > size_limit_mb && !force {
            return Err(Error::InvalidArgument {
                message: format!(
                    "Files too large ({}MB > {}MB). Use --force to override",
                    total_mb, size_limit_mb
                ),
            });
        }
        Ok(())
    }

    fn enforce_preconditions(
        meta: Option<&opendal::Metadata>,
        if_size: &Option<u64>,
        if_etag: &Option<String>,
    ) -> Result<()> {
        if let Some(size) = if_size.as_ref() {
            let actual = meta.map(|m| m.content_length()).unwrap_or(0);
            if actual != *size {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "Precondition failed: expected size={}, actual={}",
                        size, actual
                    ),
                });
            }
        }
        if let Some(expect) = if_etag.as_ref() {
            let actual = meta.and_then(|m| m.etag()).unwrap_or("");
            if actual.is_empty() {
                return Err(Error::InvalidArgument {
                    message: "Precondition failed: backend does not provide ETag".to_string(),
                });
            }
            if actual != expect {
                return Err(Error::InvalidArgument {
                    message: format!(
                        "Precondition failed: expected etag={}, actual={}",
                        expect, actual
                    ),
                });
            }
        }
        Ok(())
    }

    fn ensure_unmodified(
        before: Option<&opendal::Metadata>,
        now: Option<&opendal::Metadata>,
    ) -> Result<()> {
        match (before, now) {
            (None, None) => Ok(()),
            (Some(_), None) | (None, Some(_)) => Err(Error::InvalidArgument {
                message: "Concurrent modification detected: destination presence changed"
                    .to_string(),
            }),
            (Some(b), Some(n)) => {
                let b_etag = b.etag();
                let n_etag = n.etag();
                if b_etag.is_some() && n_etag.is_some() && b_etag != n_etag {
                    return Err(Error::InvalidArgument {
                        message: "Concurrent modification detected: destination ETag changed"
                            .to_string(),
                    });
                }
                let b_len = b.content_length();
                let n_len = n.content_length();
                if b_len != n_len {
                    return Err(Error::InvalidArgument {
                        message: "Concurrent modification detected: destination size changed"
                            .to_string(),
                    });
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendOptions {
    pub no_create: bool,
    pub parents: bool,
    pub size_limit_mb: u64,
    pub force: bool,
    pub if_size: Option<u64>,
    pub if_etag: Option<String>,
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

        // Preconditions
        Self::enforce_preconditions(meta.as_ref(), &opts.if_size, &opts.if_etag)?;

        // Early size check using local file size
        let src_len = tokio::fs::metadata(local).await?.len();
        let existing_len = meta.as_ref().map(|m| m.content_length()).unwrap_or(0);
        Self::enforce_size_limit(existing_len, src_len, opts.size_limit_mb, opts.force)?;

        // Merge existing + new
        let mut merged = if meta.is_some() {
            self.operator.read(remote).await?.to_vec()
        } else {
            Vec::new()
        };
        merged.reserve(src_len as usize);
        let src = tokio::fs::read(local).await?;
        merged.extend_from_slice(&src);

        // Revalidate just before write to detect concurrent modifications and
        // ensure preconditions and size limits still hold against the latest metadata.
        let meta_now = self.stat_remote(remote).await?;
        Self::ensure_unmodified(meta.as_ref(), meta_now.as_ref())?;
        Self::enforce_preconditions(meta_now.as_ref(), &opts.if_size, &opts.if_etag)?;
        let existing_now = meta_now.as_ref().map(|m| m.content_length()).unwrap_or(0);
        Self::enforce_size_limit(existing_now, src_len, opts.size_limit_mb, opts.force)?;

        self.write_remote(remote, merged, opts.parents).await
    }

    async fn append_from_stdin(&self, remote: &str, opts: &AppendOptions) -> Result<()> {
        let meta = self.stat_remote(remote).await?;
        if meta.is_none() && opts.no_create {
            return Err(Error::PathNotFound {
                path: std::path::PathBuf::from(remote),
            });
        }

        // Preconditions
        Self::enforce_preconditions(meta.as_ref(), &opts.if_size, &opts.if_etag)?;

        let existing_len = meta.as_ref().map(|m| m.content_length()).unwrap_or(0);

        // Incrementally read stdin to enforce size limit
        let mut stdin = tokio::io::stdin();
        let mut appended = Vec::new();
        let mut buf = vec![0u8; DEFAULT_BUFFER_SIZE];
        let mut total_new: u64 = 0;
        loop {
            let n = stdin.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            total_new += n as u64;
            Self::enforce_size_limit(existing_len, total_new, opts.size_limit_mb, opts.force)?;
            appended.extend_from_slice(&buf[..n]);
        }

        let mut merged = if meta.is_some() {
            self.operator.read(remote).await?.to_vec()
        } else {
            Vec::new()
        };
        merged.reserve(appended.len());
        merged.extend_from_slice(&appended);

        // Revalidate just before write to detect concurrent modifications and
        // ensure preconditions and size limits still hold against the latest metadata.
        let meta_now = self.stat_remote(remote).await?;
        Self::ensure_unmodified(meta.as_ref(), meta_now.as_ref())?;
        Self::enforce_preconditions(meta_now.as_ref(), &opts.if_size, &opts.if_etag)?;
        let existing_now = meta_now.as_ref().map(|m| m.content_length()).unwrap_or(0);
        Self::enforce_size_limit(existing_now, total_new, opts.size_limit_mb, opts.force)?;

        self.write_remote(remote, merged, opts.parents).await
    }
}
