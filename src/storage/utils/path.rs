// Path helper utilities shared across storage operations
use std::path::Path;

/// Build a remote path by joining base and file name.
pub fn build_remote_path(base: &str, file_name: &str) -> String {
    Path::new(base)
        .join(file_name)
        .to_string_lossy()
        .to_string()
}

/// Strip a prefix from the given path safely. Returns the original if strip fails.
pub fn strip_prefix_safe<'a>(path: &'a str, prefix: &str) -> &'a str {
    path.strip_prefix(prefix).unwrap_or(path)
}

/// Get relative path string between a full path and base path.
pub fn get_relative_path(full_path: &str, base_path: &str) -> String {
    strip_prefix_safe(full_path, base_path).to_string()
}
