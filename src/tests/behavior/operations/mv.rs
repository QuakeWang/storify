use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(
    test_move_file_to_existing_directory,
    test_move_non_existent_file,
);

async fn test_move_file_to_existing_directory(client: StorageClient) -> Result<()> {
    let src_dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&src_dir).await?;
    let (file, content, _) = TEST_FIXTURE.new_file(client.operator());
    let src_path = format!("{src_dir}{file}");
    client.operator().write(&src_path, content.clone()).await?;

    let dest_dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_dir).await?;

    storify_cmd()
        .arg("mv")
        .arg(&src_path)
        .arg(&dest_dir)
        .assert()
        .success();

    let dest_path = format!("{dest_dir}{file}");
    let dst_content = client.operator().read(&dest_path).await?;
    assert_eq!(content, dst_content.to_vec());
    assert!(client.operator().read(&src_path).await.is_err());
    Ok(())
}

async fn test_move_non_existent_file(client: StorageClient) -> Result<()> {
    let dest_dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_dir).await?;

    storify_cmd()
        .arg("mv")
        .arg("missing-file")
        .arg(&dest_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid path"));
    Ok(())
}
