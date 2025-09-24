// Storage operation traits and implementations
pub mod cat;
pub mod copy;
pub mod delete;
pub mod download;
pub mod grep;
pub mod head;
pub mod list;
pub mod mkdir;
pub mod mv;
pub mod stat;
pub mod tail;
pub mod upload;
pub mod usage;

// Re-export all operation traits - all are now implemented
pub use cat::Cater;
pub use copy::Copier;
pub use delete::Deleter;
pub use download::Downloader;
pub use grep::Greper;
pub use head::Header;
pub use list::Lister;
pub use mkdir::Mkdirer;
pub use mv::Mover;
pub use stat::Stater;
pub use tail::Tailer;
pub use upload::Uploader;
pub use usage::UsageCalculator;
