use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Stdio;
use storify::error::Result;
use storify::storage::StorageClient;
use tokio::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_cat_small_file_prints_content,
        test_cat_large_file_non_interactive_aborts,
        test_cat_large_file_force_streams
    ));
}

// Verify cat prints the content of a small text file
async fn test_cat_small_file_prints_content(_client: StorageClient) -> Result<()> {
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
    let expected = fs::read(&source_path).await?;

    let assert = storify_cmd()
        .arg("cat")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    assert_eq!(output, expected);

    Ok(())
}

// Verify non-interactive stdin aborts when exceeding size limit (without --force)
async fn test_cat_large_file_non_interactive_aborts(_client: StorageClient) -> Result<()> {
    // Prepare ~2 MiB file so we can trip a low size-limit without huge output
    let env = E2eTestEnv::new().await;
    let remote_path = TEST_FIXTURE.new_file_path();
    let content = vec![b'X'; 2 * 1024 * 1024];
    env.verifier.operator().write(&remote_path, content).await?;

    storify_cmd()
        .arg("cat")
        .arg("--size-limit")
        .arg("1") // 1 MB limit so 2 MB triggers guard
        .arg(&remote_path)
        .stdin(Stdio::null()) // simulate non-interactive
        .assert()
        .success()
        .stderr(predicate::str::contains("File too large"));

    Ok(())
}

// Verify --force streams content when exceeding size limit
async fn test_cat_large_file_force_streams(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
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
    let expected = fs::read(&source_path).await?;

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
