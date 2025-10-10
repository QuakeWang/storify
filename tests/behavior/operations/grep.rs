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
        test_grep_chunk_boundary,
        test_grep_recursive_basic,
        test_grep_recursive_ignore_case,
        test_grep_recursive_line_number,
        test_grep_directory_without_recursive_flag
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

async fn test_grep_recursive_basic(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root_dir = TEST_FIXTURE.new_dir_path();
    let sub_dir = format!("{root_dir}sub/");
    env.verifier.operator().create_dir(&root_dir).await?;
    env.verifier.operator().create_dir(&sub_dir).await?;

    let root_file = format!("{root_dir}a.txt");
    let sub_file = format!("{sub_dir}b.txt");
    env.verifier
        .operator()
        .write(&root_file, b"foo\nmatch here\nbar\n".to_vec())
        .await?;
    env.verifier
        .operator()
        .write(&sub_file, b"nope\nTARGET in sub\n".to_vec())
        .await?;

    storify_cmd()
        .arg("grep")
        .arg("-R")
        .arg("TARGET")
        .arg(&root_dir)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(format!("{}:", sub_file))
                .and(predicate::str::contains("TARGET in sub")),
        );

    Ok(())
}

async fn test_grep_recursive_ignore_case(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root_dir = TEST_FIXTURE.new_dir_path();
    let sub_dir = format!("{root_dir}nested/");
    env.verifier.operator().create_dir(&root_dir).await?;
    env.verifier.operator().create_dir(&sub_dir).await?;

    let f1 = format!("{root_dir}x.txt");
    let f2 = format!("{sub_dir}y.txt");
    env.verifier
        .operator()
        .write(&f1, b"Alpha\nBeta\n".to_vec())
        .await?;
    env.verifier
        .operator()
        .write(&f2, b"gamma\nalpha\n".to_vec())
        .await?;

    storify_cmd()
        .arg("grep")
        .arg("-R")
        .arg("-i")
        .arg("alpha")
        .arg(&root_dir)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(format!("{}:", f1))
                .and(predicate::str::contains(format!("{}:", f2))),
        );

    Ok(())
}

async fn test_grep_recursive_line_number(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root_dir = TEST_FIXTURE.new_dir_path();
    env.verifier.operator().create_dir(&root_dir).await?;
    let f = format!("{root_dir}ln.txt");
    env.verifier
        .operator()
        .write(&f, b"first\nneedle\nthird\n".to_vec())
        .await?;

    storify_cmd()
        .arg("grep")
        .arg("-R")
        .arg("-n")
        .arg("needle")
        .arg(&root_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("{}:2:needle", f)));

    Ok(())
}

async fn test_grep_directory_without_recursive_flag(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root_dir = TEST_FIXTURE.new_dir_path();
    env.verifier.operator().create_dir(&root_dir).await?;
    let f = format!("{root_dir}a.txt");
    env.verifier
        .operator()
        .write(&f, b"hello\nworld\n".to_vec())
        .await?;

    storify_cmd()
        .arg("grep")
        .arg("hello")
        .arg(&root_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("use -R"));

    Ok(())
}
