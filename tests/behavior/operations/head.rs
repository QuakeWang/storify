use crate::*;
use assert_cmd::prelude::*;
use std::path::Path;
use storify::error::Result;
use storify::storage::StorageClient;
use tokio::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_head_default_1kb,
        test_head_by_lines,
        test_head_by_bytes,
        test_head_nonexistent_file,
        test_head_large_file_force
    ));
}

// Verify head displays first 1KB by default
async fn test_head_default_1kb(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();

    // Upload via CLI to ensure end-to-end path
    storify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_prefix)
        .assert()
        .success();

    let file_name = source_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let remote_path = join_remote_path(&dest_prefix, &file_name);

    let assert = storify_cmd()
        .arg("head")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    // Verify output length doesn't exceed 1KB
    assert!(output.len() <= 1024);

    // Verify output is not empty for non-empty files
    let source_content = fs::read(&source_path).await?;
    if !source_content.is_empty() {
        assert!(!output.is_empty());
    }

    Ok(())
}

// Verify head displays first N lines
async fn test_head_by_lines(_client: StorageClient) -> Result<()> {
    // Create a test file with multiple lines
    let test_content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n";
    let temp_file = TEST_FIXTURE.new_file_path();
    fs::write(&temp_file, test_content).await?;

    let dest_prefix = TEST_FIXTURE.new_file_path();

    // Upload via CLI
    storify_cmd()
        .arg("put")
        .arg(&temp_file)
        .arg(&dest_prefix)
        .assert()
        .success();

    let file_name = Path::new(&temp_file)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let remote_path = join_remote_path(&dest_prefix, &file_name);

    // Test head -n 3
    let assert = storify_cmd()
        .arg("head")
        .arg("-n")
        .arg("3")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    // Verify output has exactly 3 lines
    let line_count = output_str.lines().count();
    assert_eq!(line_count, 3);

    // Verify content
    assert!(output_str.contains("Line 1"));
    assert!(output_str.contains("Line 2"));
    assert!(output_str.contains("Line 3"));
    assert!(!output_str.contains("Line 4"));

    Ok(())
}

// Verify head displays first N bytes
async fn test_head_by_bytes(_client: StorageClient) -> Result<()> {
    // Create a test file with known content
    let test_content = "Hello, World! ".repeat(100); // Create content longer than 50 bytes
    let temp_file = TEST_FIXTURE.new_file_path();
    fs::write(&temp_file, test_content.as_bytes()).await?;

    let dest_prefix = TEST_FIXTURE.new_file_path();

    // Upload via CLI
    storify_cmd()
        .arg("put")
        .arg(&temp_file)
        .arg(&dest_prefix)
        .assert()
        .success();

    let file_name = Path::new(&temp_file)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let remote_path = join_remote_path(&dest_prefix, &file_name);

    // Test head -c 50
    let assert = storify_cmd()
        .arg("head")
        .arg("-c")
        .arg("50")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    // Verify output has exactly 50 bytes
    assert_eq!(output.len(), 50);

    // Verify content starts correctly
    let output_str = String::from_utf8_lossy(&output);
    assert!(output_str.starts_with("Hello, World! Hello, World! Hello, World! Hello"));

    Ok(())
}

// Verify head fails gracefully for non-existent files
async fn test_head_nonexistent_file(_client: StorageClient) -> Result<()> {
    let non_existent_path = "nonexistent_file.txt";

    let assert = storify_cmd()
        .arg("head")
        .arg(non_existent_path)
        .assert()
        .failure();

    let stderr = assert.get_output().stderr.clone();
    let stderr_str = String::from_utf8_lossy(&stderr);

    // Verify error message indicates file not found
    assert!(stderr_str.contains("not found") || stderr_str.contains("NotFound"));

    Ok(())
}

// Verify head works with force flag for large files
async fn test_head_large_file_force(_client: StorageClient) -> Result<()> {
    // Create a larger test file
    let test_content = "Hello, World!\n".repeat(1000); // Create content longer than 1KB
    let temp_file = TEST_FIXTURE.new_file_path();
    fs::write(&temp_file, test_content.as_bytes()).await?;

    let dest_prefix = TEST_FIXTURE.new_file_path();

    // Upload via CLI
    storify_cmd()
        .arg("put")
        .arg(&temp_file)
        .arg(&dest_prefix)
        .assert()
        .success();

    let file_name = Path::new(&temp_file)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let remote_path = join_remote_path(&dest_prefix, &file_name);

    // Test head with force flag
    let assert = storify_cmd()
        .arg("head")
        .arg("-f")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    // Verify output length is exactly 1KB (default)
    assert_eq!(output.len(), 1024);

    // Verify content starts correctly
    let output_str = String::from_utf8_lossy(&output);
    assert!(output_str.starts_with("Hello, World!"));

    Ok(())
}
