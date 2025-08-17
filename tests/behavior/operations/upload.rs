use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use std::path::Path;
use tokio::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    // Library-level integration tests
    tests.extend(async_trials!(
        client,
        test_storage_client_write,
        test_storage_client_write_from_special_dir
    ));

    // E2E behavior tests
    tests.extend(async_trials!(client, e2e_test_upload_command_succeeds));
}

/// A helper function to get the full path of a test data file.
fn get_test_data_path(file_name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(file_name)
}

// --- Integration Tests (testing StorageClient directly) ---

async fn test_storage_client_write(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
    let dest_path = TEST_FIXTURE.new_file_path();
    let content = fs::read(&source_path).await?;
    // Use the verifier from a new E2E env to write to the correct bucket
    let env = E2eTestEnv::new().await;
    env.verifier
        .operator()
        .write(&dest_path, content.clone())
        .await?;
    let uploaded_content = env.verifier.operator().read(&dest_path).await?;
    assert_eq!(content, uploaded_content.to_vec());
    Ok(())
}

async fn test_storage_client_write_from_special_dir(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("special_dir !@#$%^&()_+-=;'/file_in_special_dir.txt");
    let dest_path = TEST_FIXTURE.new_file_path();
    let content = fs::read(&source_path).await?;
    // Use the verifier from a new E2E env to write to the correct bucket
    let env = E2eTestEnv::new().await;
    env.verifier
        .operator()
        .write(&dest_path, content.clone())
        .await?;
    let uploaded_content = env.verifier.operator().read(&dest_path).await?;
    assert_eq!(content, uploaded_content.to_vec());
    Ok(())
}

// --- E2E Behavior Tests (testing the ossify binary) ---

async fn e2e_test_upload_command_succeeds(_client: StorageClient) -> Result<()> {
    // Arrange: A single line to get the full E2E environment.
    let env = E2eTestEnv::new().await;

    let source_path = get_test_data_path("small.txt");
    let dest_path = TEST_FIXTURE.new_file_path();
    let final_dest_path = format!("{}", source_path.file_name().unwrap().to_string_lossy());
    let final_dest_path = build_remote_path(&dest_path, &final_dest_path);

    // Act: Get a pre-configured command and add specific arguments.
    env.command()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    // Assert: Use the pre-configured verifier client.
    let expected_content = fs::read(&source_path).await?;
    let actual_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(expected_content, actual_content.to_vec());

    Ok(())
}

fn build_remote_path(remote_path: &str, file_name: &str) -> String {
    if remote_path.ends_with('/') {
        format!("{}{}", remote_path, file_name)
    } else {
        format!("{}/{}", remote_path, file_name)
    }
}
