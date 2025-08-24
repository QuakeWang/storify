use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::path::Path;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_copy_file_to_existing_directory,
        test_copy_file_to_new_path,
        test_copy_across_directory,
        test_copy_overwrite_existing_file,
        test_copy_to_nonexistent_directory,
        test_copy_non_existent_file
    ));
}

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

async fn test_copy_file_to_new_path(client: StorageClient) -> Result<()> {
    let (src_file, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&src_file, content.clone()).await?;

    let dest_file = TEST_FIXTURE.new_file_path();

    storify_cmd()
        .arg("cp")
        .arg(&src_file)
        .arg(&dest_file)
        .assert()
        .success();

    let dst_content = client.operator().read(&dest_file).await?;
    assert_eq!(content, dst_content.to_vec());

    let src_content = client.operator().read(&src_file).await?;
    assert_eq!(content, src_content.to_vec());

    Ok(())
}

async fn test_copy_across_directory(client: StorageClient) -> Result<()> {
    let (src_path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&src_path, content.clone()).await?;

    let dest_path = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_path).await?;
    let final_dest_path = format!(
        "{}",
        Path::new(&src_path).file_name().unwrap().to_string_lossy()
    );

    let final_dest_path = join_remote_path(&dest_path, &final_dest_path);

    storify_cmd()
        .arg("cp")
        .arg(&src_path)
        .arg(&dest_path)
        .assert()
        .success();

    let dest_content = client.operator().read(&final_dest_path).await?;
    assert_eq!(content, dest_content.to_vec());

    let src_content = client.operator().read(&src_path).await?;
    assert_eq!(content, src_content.to_vec());

    Ok(())
}

async fn test_copy_overwrite_existing_file(client: StorageClient) -> Result<()> {
    let (src_file_path, src_content, _) = TEST_FIXTURE.new_file(client.operator());
    client
        .operator()
        .write(&src_file_path, src_content.clone())
        .await?;

    let (dst_file_path, dst_content, _) = TEST_FIXTURE.new_file(client.operator());
    client
        .operator()
        .write(&dst_file_path, dst_content.clone())
        .await?;

    let initial_dst_content = client.operator().read(&dst_file_path).await?;
    assert_eq!(dst_content, initial_dst_content.to_vec());
    assert_ne!(src_content, dst_content);

    storify_cmd()
        .arg("cp")
        .arg(&src_file_path)
        .arg(&dst_file_path)
        .assert()
        .success();

    let final_dst_content = client.operator().read(&dst_file_path).await?;
    assert_eq!(src_content, final_dst_content.to_vec());
    assert_ne!(dst_content, final_dst_content.to_vec());

    let src_content_after = client.operator().read(&src_file_path).await?;
    assert_eq!(src_content, src_content_after.to_vec());

    Ok(())
}

async fn test_copy_to_nonexistent_directory(client: StorageClient) -> Result<()> {
    let (src_file, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&src_file, content.clone()).await?;

    let nonexistent_dir = format!("{}/", TEST_FIXTURE.new_dir_path());

    storify_cmd()
        .arg("cp")
        .arg(&src_file)
        .arg(&nonexistent_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid path"));

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
