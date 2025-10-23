use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_diff_identical_files_produces_no_output,
        test_diff_basic_unified_output,
        test_diff_disallows_directories,
        test_diff_size_limit_blocks_without_force,
        test_diff_ignore_space_trims_trailing_whitespace
    ));
}

async fn upload_text_file(env: &E2eTestEnv, content: &str) -> Result<String> {
    let path = TEST_FIXTURE.new_file_path();
    env.verifier
        .operator()
        .write(&path, content.as_bytes().to_vec())
        .await?;
    Ok(path)
}

async fn test_diff_identical_files_produces_no_output(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let left = upload_text_file(&env, "line1\nline2\n").await?;
    let right = upload_text_file(&env, "line1\nline2\n").await?;

    storify_cmd()
        .arg("diff")
        .arg(&left)
        .arg(&right)
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    Ok(())
}

async fn test_diff_basic_unified_output(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let left = upload_text_file(&env, "a\nb\nc\n").await?;
    let right = upload_text_file(&env, "a\nB\nc\n").await?;

    storify_cmd()
        .arg("diff")
        .arg(&left)
        .arg(&right)
        .arg("-U")
        .arg("1")
        .assert()
        .success()
        .stdout(
            // Expect unified diff markers
            predicate::str::contains("@@")
                .and(predicate::str::contains("-b"))
                .and(predicate::str::contains("+B")),
        );

    Ok(())
}

async fn test_diff_disallows_directories(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let dir = TEST_FIXTURE.new_dir_path();
    env.verifier.operator().create_dir(&dir).await?;
    let file = upload_text_file(&env, "x\n").await?;

    storify_cmd()
        .arg("diff")
        .arg(&dir)
        .arg(&file)
        .assert()
        .failure()
        .stderr(predicate::str::contains("diff only supports files"));

    Ok(())
}

async fn test_diff_size_limit_blocks_without_force(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let left = upload_text_file(&env, &"X".repeat(2 * 1024 * 1024)).await?; // 2MB
    let right = upload_text_file(&env, &"Y".repeat(2 * 1024 * 1024)).await?; // 2MB

    storify_cmd()
        .arg("diff")
        .arg(&left)
        .arg(&right)
        .arg("--size-limit")
        .arg("1") // 1MB total limit so blocked
        .assert()
        .failure()
        .stderr(predicate::str::contains("Files too large"));

    Ok(())
}

async fn test_diff_ignore_space_trims_trailing_whitespace(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let left = upload_text_file(&env, "hello  \nworld\n").await?; // trailing spaces
    let right = upload_text_file(&env, "hello\nworld\n").await?;

    storify_cmd()
        .arg("diff")
        .arg(&left)
        .arg(&right)
        .arg("-w")
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    Ok(())
}
