// Directory creation operation trait and implementation
use crate::error::{Error, Result};
use opendal::Operator;

/// Trait for creating directories in storage.
/// Provides a clean interface for directory creation operations with proper error handling.
pub trait Mkdirer {
    /// Create a directory in storage.
    ///
    /// # Arguments
    /// * `path` - Path of the directory to create
    /// * `parents` - Whether to create parent directories as needed
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn mkdir(&self, path: &str, parents: bool) -> Result<()>;
}

/// Implementation of Mkdirer for OpenDAL Operator.
pub struct OpenDalMkdirer {
    operator: Operator,
}

impl OpenDalMkdirer {
    /// Create a new mkdirer with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Normalize directory path by ensuring it ends with a slash.
    fn normalize_directory_path(&self, path: &str) -> String {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{}/", trimmed)
        }
    }

    /// Check if a directory already exists.
    async fn directory_exists(&self, path: &str) -> Result<bool> {
        if path.is_empty() {
            return Ok(true); // Root directory always exists
        }

        // Try to stat the directory path
        match self.operator.stat(path).await {
            Ok(_) => Ok(true),
            Err(_) => {
                // If stat fails, try to list the directory to see if it exists
                // This handles cases where the directory exists but doesn't have a marker object
                match self.operator.list_with(path).await {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false),
                }
            }
        }
    }

    /// Create a single directory.
    async fn create_single_directory(&self, path: &str) -> Result<()> {
        if path.is_empty() {
            println!("Note: Root directory '/' already exists (bucket root)");
            return Ok(());
        }

        match self.operator.create_dir(path).await {
            Ok(_) => {
                println!("Created directory: {}", path);
                Ok(())
            }
            Err(e) => {
                // Handle common cases where directory might already exist
                if e.to_string().contains("already exists")
                    || e.to_string().contains("BucketAlreadyOwnedByYou")
                {
                    println!("Directory already exists: {}", path);
                    Ok(())
                } else {
                    Err(Error::DirectoryCreationFailed {
                        path: path.to_string(),
                        source: Box::new(Error::OpenDal { source: e }),
                    })
                }
            }
        }
    }

    /// Create parent directories recursively.
    async fn create_parent_directories(&self, path: &str) -> Result<()> {
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        // Create all parent directories
        for i in 1..=components.len() {
            let current_path = components[..i].join("/") + "/";
            if !self.directory_exists(&current_path).await? {
                self.create_single_directory(&current_path).await?;
            }
        }

        // Also create the final directory if it's not empty
        if !path.is_empty() {
            self.create_single_directory(path).await?;
        }

        Ok(())
    }
}

impl Mkdirer for OpenDalMkdirer {
    async fn mkdir(&self, path: &str, parents: bool) -> Result<()> {
        let normalized_path = self.normalize_directory_path(path);

        if parents {
            self.create_parent_directories(&normalized_path).await
        } else {
            self.create_single_directory(&normalized_path).await
        }
    }
}
