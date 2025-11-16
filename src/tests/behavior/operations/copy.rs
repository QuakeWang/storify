use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(
    test_copy_file_to_existing_directory,
    test_copy_non_existent_file,
);

async fn test_copy_file_to_existing_directory(client: StorageClient) -> Result<()> {
    let src_dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&src_dir).await?;

    let (src_file, content, _) = TEST_FIXTURE.new_file(client.operator());
    let src_file_path = format!("{}{}", src_dir, src_file);
    client
        .operator()
        .write(&src_file_path, content.clone())
        .await?;

    let dest_dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_dir).await?;

    storify_cmd()
        .arg("cp")
        .arg(&src_file_path)
        .arg(&dest_dir)
        .assert()
        .success();

    let dest_file_path = format!("{}{}", dest_dir, src_file);
    let dst_content = client.operator().read(&dest_file_path).await?;
    assert_eq!(content, dst_content.to_vec());

    let src_content = client.operator().read(&src_file_path).await?;
    assert_eq!(content, src_content.to_vec());

    Ok(())
}

async fn test_copy_non_existent_file(client: StorageClient) -> Result<()> {
    let non_existent_src = TEST_FIXTURE.new_dir_path();
    let non_exist_src_file = TEST_FIXTURE.new_file_path();
    client.operator().create_dir(&non_existent_src).await?;
    let final_src_file = format!("{}{}", non_existent_src, non_exist_src_file);

    let dest_path = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_path).await?;

    storify_cmd()
        .arg("cp")
        .arg(&final_src_file)
        .arg(&dest_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid path"));

    Ok(())
}
