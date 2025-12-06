use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::io::Write;
use std::process::Stdio;
use tokio::fs;
use uuid::Uuid;

register_behavior_tests!(
    test_append_from_local_creates_when_missing,
    test_append_from_local_appends_existing,
    test_append_no_create_fails,
    test_append_from_stdin_creates_when_missing,
    test_append_from_stdin_appends_existing,
    test_append_alias_positional_creates_when_missing,
    test_append_alias_positional_appends_existing,
);

async fn test_append_from_local_creates_when_missing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let local_path = std::env::temp_dir().join(format!("{}-append.txt", Uuid::new_v4()));
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

    let local_path = std::env::temp_dir().join(format!("{}-append2.txt", Uuid::new_v4()));
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

    let local_path = std::env::temp_dir().join(format!("{}-append3.txt", Uuid::new_v4()));
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

async fn test_append_from_stdin_creates_when_missing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let content = b"stdin-hello";

    let mut cmd = env.command();
    cmd.arg("append").arg(&dest_path).arg("--stdin");

    let mut child = cmd.stdin(Stdio::piped()).spawn().unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(content).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let actual = env.verifier.operator().read(&dest_path).await?;
    assert_eq!(actual.to_vec(), content);
    Ok(())
}

async fn test_append_from_stdin_appends_existing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let initial = b"base-".to_vec();
    env.verifier
        .operator()
        .write(&dest_path, initial.clone())
        .await?;

    let appended = b"stdin-tail".to_vec();

    let mut cmd = env.command();
    cmd.arg("append").arg(&dest_path).arg("--stdin");

    let mut child = cmd.stdin(Stdio::piped()).spawn().unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(&appended).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let actual = env.verifier.operator().read(&dest_path).await?;
    let mut expected = initial.clone();
    expected.extend_from_slice(&appended);
    assert_eq!(actual.to_vec(), expected);
    Ok(())
}

async fn test_append_alias_positional_creates_when_missing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let local_path = std::env::temp_dir().join(format!("{}-append-pos1.txt", Uuid::new_v4()));
    let content = b"alias-create";
    fs::write(&local_path, content).await?;

    env.command()
        .arg("append")
        .arg(&local_path)
        .arg(&dest_path)
        .assert()
        .success();

    let actual = env.verifier.operator().read(&dest_path).await?;
    assert_eq!(actual.to_vec(), content);
    Ok(())
}

async fn test_append_alias_positional_appends_existing(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;

    let dest_path = TEST_FIXTURE.new_file_path();
    let initial = b"base-".to_vec();
    env.verifier
        .operator()
        .write(&dest_path, initial.clone())
        .await?;

    let local_path = std::env::temp_dir().join(format!("{}-append-pos2.txt", Uuid::new_v4()));
    let appended = b"alias-append".to_vec();
    fs::write(&local_path, &appended).await?;

    env.command()
        .arg("append")
        .arg(&local_path)
        .arg(&dest_path)
        .assert()
        .success();

    let actual = env.verifier.operator().read(&dest_path).await?;
    let mut expected = initial.clone();
    expected.extend_from_slice(&appended);
    assert_eq!(actual.to_vec(), expected);
    Ok(())
}
