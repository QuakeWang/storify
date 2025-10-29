// Path helper utilities shared across storage operations
use std::path::Path;

/// Build a remote path by joining base and file name.
pub fn build_remote_path(base: &str, file_name: &str) -> String {
    Path::new(base)
        .join(file_name)
        .to_string_lossy()
        .to_string()
}

/// Extract a normalized basename from a remote path.
pub fn basename(path: &str) -> String {
    Path::new(path.trim_start_matches('/'))
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| path.trim_matches('/').to_string())
}

/// Return a new String that guarantees a trailing '/'.
pub fn ensure_trailing_slash(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{}/", path)
    }
}

/// Get relative path string considering the root directory between a full path and base path.
pub fn get_root_relative_path(full_path: &str, base_path: &str) -> String {
    let full_path = Path::new(full_path.trim_start_matches('/'));
    let base_path = Path::new(base_path.trim_start_matches('/'));

    let mut rel = if full_path == base_path {
        full_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        full_path
            .strip_prefix(base_path)
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| {
                full_path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default()
            })
    };

    // Normalize for safe local joining: remove leading slash and collapse duplicated slashes
    if rel.starts_with('/') {
        rel = rel.trim_start_matches('/').to_string();
    }
    rel = rel.replace("//", "/");
    rel
}

/// Return parent directory (with trailing '/') for a remote path, if any.
/// Returns `None` when the path has no parent component.
pub fn parent_dir_of(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if let Some(idx) = trimmed.rfind('/') {
        let (dir, _) = trimmed.split_at(idx);
        if dir.is_empty() {
            Some(String::new())
        } else {
            Some(format!("{}/", dir))
        }
    } else {
        None
    }
}
