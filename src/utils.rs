//! Utility functions for user interaction and common operations.

/// Format deletion confirmation message with path list
pub fn format_deletion_message(paths: &[String]) -> String {
    let mut message = format!("About to delete {} item(s):\n", paths.len());
    for path in paths.iter().take(5) {
        message.push_str(&format!("  {}\n", path));
    }
    if paths.len() > 5 {
        message.push_str(&format!("  ... and {} more\n", paths.len() - 5));
    }
    message.push_str("Continue?");
    message
}
