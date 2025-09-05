// Storage operation traits and implementations
pub mod cat;
pub mod copy;
pub mod delete;
pub mod download;
pub mod list;
pub mod mkdir;
pub mod mv;
pub mod stat;
pub mod upload;
pub mod usage;

// Re-export all operation traits - all are now implemented
pub use cat::Cater;
pub use copy::Copier;
pub use delete::Deleter;
pub use download::Downloader;
pub use list::Lister;
pub use mkdir::Mkdirer;
pub use mv::Mover;
pub use stat::Stater;
pub use upload::Uploader;
pub use usage::UsageCalculator;
