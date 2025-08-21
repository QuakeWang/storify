use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use std::path::Path;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_copy_some_directory,
        test_copy_across_directory,
        test_copy_overwrite_existing_file,
        test_copy_non_existent_file
    ));
}

async fn test_copy_some_directory(client: StorageClient) -> Result<()> {
    let (src_file_path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client
        .operator()
        .write(&src_file_path, content.clone())
        .await?;

    let dest_path = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_path).await?;

    let final_dest_path = format!(
        "{}",
        Path::new(&src_file_path)
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    let final_dest_path = join_remote_path(&dest_path, &final_dest_path);

    ossify_cmd()
        .arg("cp")
        .arg(&src_file_path)
        .arg(&dest_path)
        .assert()
        .success();

    let dst_content = client.operator().read(&final_dest_path).await?;
    assert_eq!(content, dst_content.to_vec());

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

    ossify_cmd()
        .arg("cp")
        .arg(&src_path)
        .arg(&dest_path)
        .assert()
        .success();

    let dst_content = client.operator().read(&final_dest_path).await?;
    assert_eq!(content, dst_content.to_vec());

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

    ossify_cmd()
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

async fn test_copy_non_existent_file(client: StorageClient) -> Result<()> {
    let non_existent_src = TEST_FIXTURE.new_dir_path();
    let non_exist_src_file = TEST_FIXTURE.new_file_path();
    client.operator().create_dir(&non_existent_src).await?;
    let final_src_file = format!("{}{}", non_existent_src, non_exist_src_file);

    let dest_path = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dest_path).await?;

    ossify_cmd()
        .arg("cp")
        .arg(&final_src_file)
        .arg(&dest_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid path"));

    Ok(())
}
