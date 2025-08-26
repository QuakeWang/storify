use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_stat_file_human,
        test_stat_file_json,
        test_stat_dir_raw,
        test_stat_not_found
    ));
}

pub async fn test_stat_file_human(client: StorageClient) -> Result<()> {
    let (path, content, _size) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    storify_cmd()
        .arg("stat")
        .arg(&path)
        .assert()
        .success()
        .stdout(predicate::str::contains("type=file"))
        .stdout(predicate::str::contains("size="));
    Ok(())
}

pub async fn test_stat_file_json(client: StorageClient) -> Result<()> {
    let (path, content, _size) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    storify_cmd()
        .arg("stat")
        .arg(&path)
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("{"))
        .stdout(predicate::str::contains("\"entry_type\":\"file\""));
    Ok(())
}

pub async fn test_stat_dir_raw(client: StorageClient) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dir).await?;

    storify_cmd()
        .arg("stat")
        .arg(format!("/{dir}"))
        .arg("--raw")
        .assert()
        .success()
        .stdout(predicate::str::contains("type=dir"));
    Ok(())
}

pub async fn test_stat_not_found(_client: StorageClient) -> Result<()> {
    storify_cmd()
        .arg("stat")
        .arg("/no_such_file")
        .assert()
        .failure();
    Ok(())
}
