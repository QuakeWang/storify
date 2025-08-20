use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use std::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_download_existing_file,
        test_download_non_existent_file
    ));
}

async fn test_download_existing_file(client: StorageClient) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content.clone()).await?;

    let local_dir = get_test_data_path("download");
    let local_path = local_dir.join(&path);

    let mut cmd = ossify_cmd();
    cmd.arg("get").arg(&path).arg(&local_dir);
    cmd.assert().success();

    let downloaded_content = fs::read(&local_path)?;
    assert_eq!(downloaded_content, content);

    fs::remove_file(local_path)?;

    Ok(())
}

async fn test_download_non_existent_file(_client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    let local_path = get_test_data_path("download").join("non_existent_file.txt");

    let mut cmd = ossify_cmd();
    cmd.arg("get").arg(&path).arg(&local_path);
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Path does not exist"));

    Ok(())
}
