use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;
use tokio::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_append_from_local_creates_when_missing,
        test_append_from_local_appends_existing,
        test_append_no_create_fails
    ));
}

async fn test_append_from_local_creates_when_missing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let local_path = std::env::temp_dir().join(format!("{}-append.txt", uuid::Uuid::new_v4()));
    let content = b"hello";
    fs::write(&local_path, content).await?;

    env.command()
        .arg("append")
        .arg(&dest_path)
        .arg("--src")
        .arg(&local_path)
        .assert()
        .success();

    let actual = env.verifier.operator().read(&dest_path).await?;
    assert_eq!(actual.to_vec(), content);
    Ok(())
}

async fn test_append_from_local_appends_existing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let initial = b"base-".to_vec();
    env.verifier
        .operator()
        .write(&dest_path, initial.clone())
        .await?;

    let local_path = std::env::temp_dir().join(format!("{}-append2.txt", uuid::Uuid::new_v4()));
    let appended = b"tail".to_vec();
    fs::write(&local_path, &appended).await?;

    env.command()
        .arg("append")
        .arg(&dest_path)
        .arg("--src")
        .arg(&local_path)
        .assert()
        .success();

    let actual = env.verifier.operator().read(&dest_path).await?;
    let mut expected = initial.clone();
    expected.extend_from_slice(&appended);
    assert_eq!(actual.to_vec(), expected);
    Ok(())
}

async fn test_append_no_create_fails(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let dest_path = TEST_FIXTURE.new_file_path();

    let local_path = std::env::temp_dir().join(format!("{}-append3.txt", uuid::Uuid::new_v4()));
    fs::write(&local_path, b"data").await?;

    env.command()
        .arg("append")
        .arg(&dest_path)
        .arg("--src")
        .arg(&local_path)
        .arg("-c")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Path not found")
                .or(predicate::str::contains("Failed to append")),
        );

    Ok(())
}
