use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;

register_behavior_tests!(
    test_delete_single_file,
    test_delete_non_empty_directory_recursively,
);

async fn test_delete_single_file(client: StorageClient) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    storify_cmd()
        .arg("rm")
        .arg("--force")
        .arg(&path)
        .assert()
        .success();

    let result = client.operator().stat(&path).await;
    assert!(result.is_err(), "File should be deleted");
    assert!(
        matches!(result.unwrap_err().kind(), opendal::ErrorKind::NotFound),
        "Error should be NotFound"
    );

    Ok(())
}

async fn test_delete_non_empty_directory_recursively(client: StorageClient) -> Result<()> {
    let root_dir = TEST_FIXTURE.new_dir_path();
    let file_path = format!("{root_dir}test.txt");
    let (path, content, _) = TEST_FIXTURE.new_file_with_range(file_path, 1..1024);
    client.operator().write(&path, content).await?;

    E2eTestEnv::new()
        .await
        .command()
        .arg("rm")
        .arg("-R")
        .arg("--force")
        .arg(&root_dir)
        .assert()
        .success();

    let result = client.operator().stat(&root_dir).await;
    assert!(result.is_err(), "Root directory should be deleted");

    let file_result = client.operator().stat(&path).await;
    assert!(
        file_result.is_err(),
        "File within directory should be deleted"
    );

    Ok(())
}
