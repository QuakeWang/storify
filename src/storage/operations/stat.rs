use crate::error::Result;
use opendal::{EntryMode, Operator};

/// Object metadata used by `stat` command output.
///
/// - `path`: The queried object path (as provided by caller)
/// - `entry_type`: One of `file`, `dir`, or `other`
/// - `size`: Content length in bytes
/// - `last_modified`: RFC3339 string if available
/// - `etag`: Backend provided entity tag if available
/// - `content_type`: MIME type if available
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub path: String,
    pub entry_type: String, // file | dir | other
    pub size: u64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

/// Trait for fetching object metadata from storage.
pub trait Stater {
    /// Create a new stater with the given OpenDAL operator.
    fn new(operator: Operator) -> Self;

    /// Fetch metadata for a single object or directory.
    ///
    /// # Arguments
    /// * `path` - Object path to query. Accepts any type implementing `AsRef<str>`.
    ///
    /// # Returns
    /// * `Result<ObjectMeta>` - Collected metadata for the provided path
    async fn stat<P: AsRef<str>>(&self, path: P) -> Result<ObjectMeta>;
}

/// Implementation of `Stater` for OpenDAL `Operator`.
#[derive(Clone)]
pub struct OpenDalStater {
    operator: Operator,
}

impl Stater for OpenDalStater {
    /// Create a new `OpenDalStater` with the given operator.
    fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Fetch object metadata via OpenDAL's `stat` API, and normalize fields to printable types.
    async fn stat<P: AsRef<str>>(&self, path: P) -> Result<ObjectMeta> {
        let meta = self.operator.stat(path.as_ref()).await?;

        let entry_type = match meta.mode() {
            EntryMode::FILE => "file".to_string(),
            EntryMode::DIR => "dir".to_string(),
            _ => "other".to_string(),
        };

        let last_modified = meta.last_modified().map(|t| t.to_string());
        let etag = meta.etag().map(|s| s.to_string());
        let content_type = meta.content_type().map(|s| s.to_string());

        Ok(ObjectMeta {
            path: path.as_ref().to_owned(),
            entry_type,
            size: meta.content_length(),
            last_modified,
            etag,
            content_type,
        })
    }
}
