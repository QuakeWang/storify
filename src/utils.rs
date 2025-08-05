/// Utility functions for user interaction and common operations.
use crate::error::Result;
use std::io::{self, Write};

/// Prompt user for confirmation before performing potentially destructive operations.
pub fn confirm_deletion(paths: &[String], force: bool) -> Result<bool> {
    if force {
        return Ok(true);
    }

    println!("About to delete {} item(s):", paths.len());
    for path in paths.iter().take(5) {
        println!("  {path}");
    }
    if paths.len() > 5 {
        println!("  ... and {} more", paths.len() - 5);
    }

    print!("Continue? (y/N): ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let trimmed = input.trim().to_lowercase();
    Ok(trimmed == "y" || trimmed == "yes")
}
