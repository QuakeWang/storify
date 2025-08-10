// Delete operation trait and implementation
use crate::error::{DirectoryDeletionNotRecursiveSnafu, PartialDeletionSnafu, Result};
use opendal::Operator;

/// Trait for deleting files and directories from storage.
/// Provides a clean interface for delete operations with proper error handling.
pub trait Deleter {
    /// Delete one or more files/directories from storage.
    ///
    /// # Arguments
    /// * `paths` - List of paths to delete
    /// * `recursive` - Whether to delete directories recursively
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn delete(&self, paths: &[String], recursive: bool) -> Result<()>;
}

/// Implementation of Deleter for OpenDAL Operator.
pub struct OpenDalDeleter {
    operator: Operator,
}

impl OpenDalDeleter {
    /// Create a new deleter with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Check if a path exists in storage.
    async fn path_exists(&self, path: &str) -> Result<bool> {
        match self.operator.stat(path).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Check if a path is a directory.
    async fn is_directory(&self, path: &str) -> Result<bool> {
        match self.operator.stat(path).await {
            Ok(metadata) => Ok(metadata.mode().is_dir()),
            Err(_) => Ok(false),
        }
    }
}

impl Deleter for OpenDalDeleter {
    async fn delete(&self, paths: &[String], recursive: bool) -> Result<()> {
        let mut failed_paths = Vec::new();

        for path in paths {
            if !self.path_exists(path).await? {
                eprintln!("Path not found: {path}");
                failed_paths.push(path.clone());
                continue;
            }

            if self.is_directory(path).await? && !recursive {
                return DirectoryDeletionNotRecursiveSnafu { path: path.clone() }.fail();
            }

            match self.operator.remove_all(path).await {
                Ok(_) => println!("Deleted: {path}"),
                Err(e) => {
                    eprintln!("Failed to delete {path}: {e}");
                    failed_paths.push(path.clone());
                }
            }
        }

        if !failed_paths.is_empty() {
            return PartialDeletionSnafu { failed_paths }.fail();
        }

        Ok(())
    }
}
