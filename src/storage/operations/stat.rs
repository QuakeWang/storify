use crate::error::Result;
use opendal::{EntryMode, Operator};

#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub path: String,
    pub entry_type: String, // file | dir | other
    pub size: u64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

pub trait Stater {
    fn new(operator: Operator) -> Self;
    async fn stat(&self, path: &str) -> Result<ObjectMeta>;
}

#[derive(Clone)]
pub struct OpenDalStater {
    operator: Operator,
}

impl Stater for OpenDalStater {
    fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn stat(&self, path: &str) -> Result<ObjectMeta> {
        let meta = self.operator.stat(path).await?;

        let entry_type = match meta.mode() {
            EntryMode::FILE => "file".to_string(),
            EntryMode::DIR => "dir".to_string(),
            _ => "other".to_string(),
        };

        let last_modified = meta.last_modified().map(|t| t.to_string());
        let etag = meta.etag().map(|s| s.to_string());
        let content_type = meta.content_type().map(|s| s.to_string());

        Ok(ObjectMeta {
            path: path.to_string(),
            entry_type,
            size: meta.content_length(),
            last_modified,
            etag,
            content_type,
        })
    }
}
