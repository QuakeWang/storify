use crate::*;
use assert_cmd::prelude::*;
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

/// Create a temporary file under system temp dir with given content and return its path.
fn create_temp_file_with_content(content: &[u8]) -> String {
    let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::write(&path, content).expect("write temp file");
    path.to_string_lossy().to_string()
}

/// Upload a local file to a generated remote prefix via CLI and return the full remote path.
fn upload_and_remote_path(local_path: &str, dest_prefix: &str) -> String {
    storify_cmd()
        .arg("put")
        .arg(local_path)
        .arg(dest_prefix)
        .assert()
        .success();

    let file_name = std::path::Path::new(local_path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    join_remote_path(dest_prefix, &file_name)
}

// Verify head displays first 1KB by default
async fn test_head_default_1kb(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();

    let remote_path = upload_and_remote_path(&source_path.to_string_lossy(), &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    assert!(output.len() <= 1024);

    let source_content = fs::read(&source_path).await?;
    if !source_content.is_empty() {
        assert!(!output.is_empty());
    }

    Ok(())
}

// Verify head displays first N lines
async fn test_head_by_lines(_client: StorageClient) -> Result<()> {
    let test_content = b"Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n";
    let temp_file = create_temp_file_with_content(test_content);

    let dest_prefix = TEST_FIXTURE.new_file_path();

    let remote_path = upload_and_remote_path(&temp_file, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg("-n")
        .arg("3")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    let output_str = String::from_utf8_lossy(&output);

    let line_count = output_str.lines().count();
    assert_eq!(line_count, 3);

    assert!(output_str.contains("Line 1"));
    assert!(output_str.contains("Line 2"));
    assert!(output_str.contains("Line 3"));
    assert!(!output_str.contains("Line 4"));

    Ok(())
}

// Verify head displays first N bytes
async fn test_head_by_bytes(_client: StorageClient) -> Result<()> {
    let test_content = "Hello, World! ".repeat(100); // longer than 50 bytes
    let temp_file = create_temp_file_with_content(test_content.as_bytes());

    let dest_prefix = TEST_FIXTURE.new_file_path();

    let remote_path = upload_and_remote_path(&temp_file, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg("-c")
        .arg("50")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    assert_eq!(output.len(), 50);

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

    assert!(stderr_str.contains("not found") || stderr_str.contains("NotFound"));

    Ok(())
}

// Verify head works with force flag for large files
async fn test_head_large_file_force(_client: StorageClient) -> Result<()> {
    let test_content = "Hello, World!\n".repeat(1000);
    let temp_file = create_temp_file_with_content(test_content.as_bytes());

    let dest_prefix = TEST_FIXTURE.new_file_path();

    let remote_path = upload_and_remote_path(&temp_file, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg("-f")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();

    assert_eq!(output.len(), 1024);

    let output_str = String::from_utf8_lossy(&output);
    assert!(output_str.starts_with("Hello, World!"));

    Ok(())
}
