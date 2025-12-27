use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(
    test_append_from_local_creates_when_missing,
    test_append_no_create_fails,
);

async fn test_append_from_local_creates_when_missing(client: StorageClient) -> Result<()> {
    let remote_path = TEST_FIXTURE.new_file_path();
    let local_path = write_temp_file(b"append-data", ".txt");

    storify_cmd()
        .arg("append")
        .arg(&remote_path)
        .arg("--src")
        .arg(local_path.to_string_lossy().to_string())
        .assert()
        .success();

    let actual = client.operator().read(&remote_path).await?;
    assert_eq!(actual.to_vec(), b"append-data");
    Ok(())
}

async fn test_append_no_create_fails(_client: StorageClient) -> Result<()> {
    let remote_path = TEST_FIXTURE.new_file_path();
    let local_path = write_temp_file(b"blocked", ".txt");

    storify_cmd()
        .arg("append")
        .arg(&remote_path)
        .arg("--src")
        .arg(local_path.to_string_lossy().to_string())
        .arg("-c")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Path not found")
                .or(predicate::str::contains("Failed to append")),
        );

    Ok(())
}
