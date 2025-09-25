use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_grep_basic,
        test_grep_ignore_case,
        test_grep_line_number,
        test_grep_chunk_boundary
    ));
}

async fn prepare_remote_file(verifier: &StorageClient, content: &[u8]) -> Result<String> {
    let path = TEST_FIXTURE.new_file_path();
    verifier.operator().write(&path, content.to_vec()).await?;
    Ok(path)
}

async fn test_grep_basic(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let content = b"alpha\nbeta\ngamma\nAlpha Beta\n";
    let remote_path = prepare_remote_file(&env.verifier, content).await?;

    storify_cmd()
        .arg("grep")
        .arg("beta")
        .arg(&remote_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("beta").and(predicate::str::contains("Alpha Beta").not()));

    Ok(())
}

async fn test_grep_ignore_case(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let content = b"alpha\nbeta\ngamma\nAlpha Beta\n";
    let remote_path = prepare_remote_file(&env.verifier, content).await?;

    storify_cmd()
        .arg("grep")
        .arg("-i")
        .arg("beta")
        .arg(&remote_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("beta").and(predicate::str::contains("Alpha Beta")));

    Ok(())
}

async fn test_grep_line_number(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let content = b"first\nsecond\nthird\n";
    let remote_path = prepare_remote_file(&env.verifier, content).await?;

    storify_cmd()
        .arg("grep")
        .arg("-n")
        .arg("second")
        .arg(&remote_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("2:second"));

    Ok(())
}

async fn test_grep_chunk_boundary(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    // Construct content that crosses typical chunk boundary using repeated lines
    let mut content = Vec::new();
    for _ in 0..5000 {
        content.extend_from_slice(b"lorem ipsum dolor sit amet\n");
    }

    content.extend_from_slice(b"TARGET line here\n");
    for _ in 0..5000 {
        content.extend_from_slice(b"lorem ipsum dolor sit amet\n");
    }

    let remote_path = prepare_remote_file(&env.verifier, &content).await?;

    storify_cmd()
        .arg("grep")
        .arg("TARGET")
        .arg(&remote_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("TARGET line here"));

    Ok(())
}
