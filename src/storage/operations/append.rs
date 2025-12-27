use crate::error::{Error, Result};
use opendal::Operator;
use std::path::Path;
use tokio::io::AsyncReadExt as TokioAsyncReadExt;

/// Trait for appending data to remote objects (files) in object storage.
pub trait Appender {
    /// Append content from a local file to the remote object.
    ///
    /// # Arguments
    /// * `local` - Local file path to append
    /// * `remote` - Remote object path to append to
    /// * `opts` - Options for creation/parent behavior
    async fn append_from_local(
        &self,
        local: &str,
        remote: &str,
        opts: &AppendOptions,
    ) -> Result<()>;

    /// Append data from process stdin to the remote object.
    ///
    /// # Arguments
    /// * `remote` - Remote object path to append to
    /// * `opts` - Options for creation/parent behavior
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
        let meta1 = self.stat_remote(remote).await?;
        if meta1.is_none() && opts.no_create {
            return Err(Error::PathNotFound {
                path: std::path::PathBuf::from(remote),
            });
        }

        let (merged, etag_stat2, size_stat2) = if let Some(meta) = meta1.as_ref() {
            let buf = self.operator.read(remote).await?.to_vec();
            let meta2 = self.stat_remote(remote).await?;
            match meta2 {
                Some(ref m2) => {
                    let etag1 = meta.etag();
                    let etag2 = m2.etag();
                    let local_bytes = tokio::fs::read(local).await?;
                    if etag1.is_some() && etag2.is_some() {
                        if etag1 != etag2 {
                            return Err(Error::InvalidArgument { message: "Remote object changed during read (ETag mismatch after read), append aborted.".into() });
                        }
                        let mut merged = buf;
                        merged.extend_from_slice(&local_bytes);
                        (merged, etag2.map(|s| s.to_owned()), None)
                    } else {
                        let s1 = meta.content_length();
                        let s2 = m2.content_length();
                        if s1 != s2 || s2 != buf.len() as u64 {
                            return Err(Error::InvalidArgument { message: "Remote object changed during read (size or byte count mismatch after read), append aborted.".into() });
                        }
                        let mut merged = buf;
                        merged.extend_from_slice(&local_bytes);
                        (merged, None, Some(s2))
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted during read, append aborted.".into(),
                    });
                }
            }
        } else {
            let local_bytes = tokio::fs::read(local).await?;
            let meta2 = self.stat_remote(remote).await?;
            if let Some(ref m2) = meta2 {
                let mut remote_buf = self.operator.read(remote).await?.to_vec();
                let meta3 = self.stat_remote(remote).await?;
                match meta3 {
                    Some(ref m3) => {
                        let etag2 = m2.etag();
                        let etag3 = m3.etag();
                        if etag2.is_some() && etag3.is_some() {
                            if etag2 != etag3 {
                                return Err(Error::InvalidArgument { message: "Remote object changed during concurrent creation (ETag mismatch), append aborted.".into() });
                            }
                            remote_buf.extend_from_slice(&local_bytes);
                            (remote_buf, etag3.map(|s| s.to_owned()), None)
                        } else {
                            let s2 = m2.content_length();
                            let s3 = m3.content_length();
                            if s2 != s3 || s3 != remote_buf.len() as u64 {
                                return Err(Error::InvalidArgument { message: "Remote object changed during concurrent creation (size or byte count mismatch), append aborted.".into() });
                            }
                            remote_buf.extend_from_slice(&local_bytes);
                            (remote_buf, None, Some(s3))
                        }
                    }
                    None => {
                        return Err(Error::InvalidArgument { message: "Remote object was concurrently deleted after creation, append aborted.".into() });
                    }
                }
            } else {
                (local_bytes, None, None)
            }
        };

        if let Some(meta2_etag) = etag_stat2.as_deref() {
            let meta3 = self.stat_remote(remote).await?;
            match meta3 {
                Some(ref latest) => {
                    let etag3 = latest.etag();
                    if Some(meta2_etag) != etag3 {
                        return Err(Error::InvalidArgument { message: "Remote object changed by another client before write (ETag mismatch), append aborted.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted before append, operation aborted."
                            .into(),
                    });
                }
            }
        } else if let Some(size2) = size_stat2 {
            let meta3 = self.stat_remote(remote).await?;
            match meta3 {
                Some(ref latest) => {
                    if latest.content_length() != size2 {
                        return Err(Error::InvalidArgument { message: "Remote object changed by another client before write (size mismatch), append aborted.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted before append, operation aborted."
                            .into(),
                    });
                }
            }
        } else if self.stat_remote(remote).await?.is_some() {
            return Err(Error::InvalidArgument {
                message: "Remote object was concurrently created before append, operation aborted."
                    .into(),
            });
        }
        self.write_remote(remote, merged, opts.parents).await
    }

    async fn append_from_stdin(&self, remote: &str, opts: &AppendOptions) -> Result<()> {
        let meta1 = self.stat_remote(remote).await?;
        if meta1.is_none() && opts.no_create {
            return Err(Error::PathNotFound {
                path: std::path::PathBuf::from(remote),
            });
        }

        let mut stdin = tokio::io::stdin();
        let mut new_data = Vec::new();
        stdin.read_to_end(&mut new_data).await?;

        let (merged, etag_stat2, size_stat2) = if let Some(meta0) = meta1.as_ref() {
            let buf = self.operator.read(remote).await?.to_vec();
            let meta2 = self.stat_remote(remote).await?;
            match meta2 {
                Some(ref m2) => {
                    let etag1 = meta0.etag();
                    let etag2 = m2.etag();
                    if etag1.is_some() && etag2.is_some() {
                        if etag1 != etag2 {
                            return Err(Error::InvalidArgument { message: "Remote object changed during read (ETag mismatch after read), append aborted.".into() });
                        }
                        let mut merged = buf;
                        merged.extend_from_slice(&new_data);
                        (merged, etag2.map(|s| s.to_owned()), None)
                    } else {
                        let s1 = meta0.content_length();
                        let s2 = m2.content_length();
                        if s1 != s2 || s2 != buf.len() as u64 {
                            return Err(Error::InvalidArgument { message: "Remote object changed during read (size or byte count mismatch after read), append aborted.".into() });
                        }
                        let mut merged = buf;
                        merged.extend_from_slice(&new_data);
                        (merged, None, Some(s2))
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted during read, append aborted.".into(),
                    });
                }
            }
        } else {
            let meta2 = self.stat_remote(remote).await?;
            if let Some(ref m2) = meta2 {
                let mut remote_buf = self.operator.read(remote).await?.to_vec();
                let meta3 = self.stat_remote(remote).await?;
                match meta3 {
                    Some(ref m3) => {
                        let etag2 = m2.etag();
                        let etag3 = m3.etag();
                        if etag2.is_some() && etag3.is_some() {
                            if etag2 != etag3 {
                                return Err(Error::InvalidArgument { message: "Remote object changed during concurrent creation (ETag mismatch), append aborted.".into() });
                            }
                            remote_buf.extend_from_slice(&new_data);
                            (remote_buf, etag3.map(|s| s.to_owned()), None)
                        } else {
                            let s2 = m2.content_length();
                            let s3 = m3.content_length();
                            if s2 != s3 || s3 != remote_buf.len() as u64 {
                                return Err(Error::InvalidArgument { message: "Remote object changed during concurrent creation (size or byte count mismatch), append aborted.".into() });
                            }
                            remote_buf.extend_from_slice(&new_data);
                            (remote_buf, None, Some(s3))
                        }
                    }
                    None => {
                        return Err(Error::InvalidArgument { message: "Remote object was concurrently deleted after creation, append aborted.".into() });
                    }
                }
            } else {
                (new_data, None, None)
            }
        };

        if let Some(meta2_etag) = etag_stat2.as_deref() {
            let meta3 = self.stat_remote(remote).await?;
            match meta3 {
                Some(ref latest) => {
                    let etag3 = latest.etag();
                    if Some(meta2_etag) != etag3 {
                        return Err(Error::InvalidArgument { message: "Remote object changed by another client before write (ETag mismatch), append aborted.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted before append, operation aborted."
                            .into(),
                    });
                }
            }
        } else if let Some(size2) = size_stat2 {
            let meta3 = self.stat_remote(remote).await?;
            match meta3 {
                Some(ref latest) => {
                    if latest.content_length() != size2 {
                        return Err(Error::InvalidArgument { message: "Remote object changed by another client before write (size mismatch), append aborted.".into() });
                    }
                }
                None => {
                    return Err(Error::InvalidArgument {
                        message: "Remote object was deleted before append, operation aborted."
                            .into(),
                    });
                }
            }
        } else if self.stat_remote(remote).await?.is_some() {
            return Err(Error::InvalidArgument {
                message: "Remote object was concurrently created before append, operation aborted."
                    .into(),
            });
        }
        self.write_remote(remote, merged, opts.parents).await
    }
}
