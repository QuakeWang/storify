use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(
    test_grep_basic,
    test_grep_recursive_basic,
    test_grep_directory_without_recursive_flag,
);

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
