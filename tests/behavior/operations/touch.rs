use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_touch_create_and_truncate,
        test_touch_no_create_is_noop,
        test_touch_parents
    ));
}

async fn test_touch_create_and_truncate(_client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    storify_cmd().arg("touch").arg(&path).assert().success();

    storify_cmd()
        .arg("touch")
        .args(["-t", &path])
        .assert()
        .success();

    Ok(())
}

async fn test_touch_no_create_is_noop(_client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    storify_cmd()
        .arg("touch")
        .args(["-c", &path])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created:").not())
        .stdout(predicate::str::contains("Truncated:").not());
    Ok(())
}

async fn test_touch_parents(_client: StorageClient) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();
    let nested = format!("{dir}a/b/c.txt");
    storify_cmd()
        .arg("touch")
        .args(["-p", &nested])
        .assert()
        .success();
    Ok(())
}
