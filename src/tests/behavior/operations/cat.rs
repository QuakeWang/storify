use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;

register_behavior_tests!(
    test_cat_small_file_prints_content,
    test_cat_large_file_force_streams,
);

// Verify cat prints the content of a small text file
async fn test_cat_small_file_prints_content(_client: StorageClient) -> Result<()> {
    let content = b"cat small file\nHello world\n".to_vec();
    let source_path = write_temp_file(&content, ".txt");
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
    let expected = content.clone();

    let assert = storify_cmd()
        .arg("cat")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    assert_eq!(output, expected);

    Ok(())
}

// Verify --force streams content when exceeding size limit
async fn test_cat_large_file_force_streams(_client: StorageClient) -> Result<()> {
    let content = "Force stream content\n".repeat(4);
    let source_path = write_temp_file(content.as_bytes(), ".txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let file_name = source_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Upload under a directory to construct remote path
    storify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_prefix)
        .assert()
        .success();

    let remote_path = join_remote_path(&dest_prefix, &file_name);
    let expected = content.into_bytes();

    // Force with a very small size-limit to ensure the guard would trigger
    let assert = storify_cmd()
        .arg("cat")
        .arg("--size-limit")
        .arg("1")
        .arg("-f")
        .arg(&remote_path)
        .assert()
        .success();

    // Since small.txt is text and modest size, we can compare output exactly
    let output = assert.get_output().stdout.clone();
    assert_eq!(output, expected);

    Ok(())
}
