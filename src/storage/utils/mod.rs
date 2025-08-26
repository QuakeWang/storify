// Utilities for storage module
pub mod error;
pub mod path;
pub mod progress;
pub mod size;

/// Output format for CLI commands that can render machine-readable results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human friendly multi-line output
    Human,
    /// Key-value lines compatible with opendal-mkdir's raw output
    Raw,
    /// Single-line JSON output
    Json,
}
