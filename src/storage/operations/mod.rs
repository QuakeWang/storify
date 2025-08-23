// Storage operation traits and implementations
pub mod copy;
pub mod delete;
pub mod download;
pub mod list;
pub mod mkdir;
pub mod upload;
pub mod usage;

// Re-export all operation traits - all are now implemented
pub use copy::Copier;
pub use delete::Deleter;
pub use download::Downloader;
pub use list::Lister;
pub use mkdir::Mkdirer;
pub use upload::Uploader;
pub use usage::UsageCalculator;
