use crate::error::Result;
use crate::storage::utils::error::IntoOssifyError;
use crate::wrap_err;
use futures::stream::TryStreamExt;
use opendal::Operator;
use std::fmt;

/// Trait for listing directory contents in object storage.
pub trait Lister {
    /// List contents of a directory in object storage.
    ///
    /// # Arguments
    /// * `path` - Directory path to list
    /// * `long` - Whether to show detailed information
    /// * `recursive` - Whether to list recursively
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn list(&self, path: &str, long: bool, recursive: bool) -> Result<()>;
}

/// Implementation of Lister for OpenDAL Operator.
pub struct OpenDalLister {
    operator: Operator,
}

impl OpenDalLister {
    /// Create a new lister with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Print a single entry with optional detailed formatting.
    fn print_entry(&self, entry: &opendal::Entry, long: bool) {
        if long {
            let file_info = FileInfo::from_entry(entry);
            println!("{file_info}");
        } else {
            println!("{}", entry.path());
        }
    }
}

impl Lister for OpenDalLister {
    async fn list(&self, path: &str, long: bool, recursive: bool) -> Result<()> {
        let lister = wrap_err!(
            self.operator.lister_with(path).recursive(recursive).await,
            ListDirectoryFailed {
                path: path.to_string()
            }
        )?;

        lister
            .map_err(|e| crate::error::Error::ListDirectoryFailed {
                path: path.to_string(),
                source: Box::new(e.into_error()),
            })
            .try_for_each(|entry| async move {
                self.print_entry(&entry, long);
                Ok(())
            })
            .await
    }
}

/// File information for detailed listing output.
struct FileInfo {
    path: String,
    size: u64,
    modified: Option<String>,
    is_dir: bool,
}

impl FileInfo {
    fn from_entry(entry: &opendal::Entry) -> Self {
        let meta = entry.metadata();
        Self {
            path: entry.path().to_string(),
            size: meta.content_length(),
            modified: meta.last_modified().map(|t| t.to_rfc3339()),
            is_dir: meta.mode().is_dir(),
        }
    }
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_type = if self.is_dir { "DIR" } else { "FILE" };
        let size_str = if self.is_dir {
            "-".to_string()
        } else {
            crate::storage::utils::size::format_size(self.size)
        };
        let modified = self.modified.as_deref().unwrap_or("Unknown");
        write!(f, "{file_type:<6} {size_str:>10} {modified} {}", self.path)
    }
}
