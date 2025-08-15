// Path helper utilities shared across storage operations
use std::path::Path;

/// Build a remote path by joining base and file name.
pub fn build_remote_path(base: &str, file_name: &str) -> String {
    Path::new(base)
        .join(file_name)
        .to_string_lossy()
        .to_string()
}

/// Get relative path string considering the root directory between a full path and base path.
pub fn get_root_relative_path(full_path: &str, base_path: &str) -> String {
    let full_path = Path::new(full_path.trim_start_matches('/'));
    let base_path = Path::new(base_path.trim_start_matches('/'));

    if full_path == base_path {
        // For single-file case, return the file name to avoid empty relative path
        return Path::new(full_path)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
    }

    full_path
        .strip_prefix(base_path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| {
            full_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default()
        })
}
