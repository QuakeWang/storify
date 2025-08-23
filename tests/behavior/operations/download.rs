use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use tokio::fs;
use uuid::Uuid;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_download_existing_file_to_directory,
        test_download_directory_recursive,
        test_download_non_existent_file,
        test_download_large_file,
        test_download_with_special_chars
    ));
}

#[derive(Clone)]
struct StagedFile {
    remote_path: String,
    content: Vec<u8>,
    file_name: String,
}

async fn stage_remote_file(client: &StorageClient) -> Result<StagedFile> {
    let (src_file, content, _) = TEST_FIXTURE.new_file(client.operator());
    let src_path = TEST_FIXTURE.new_dir_path();

    client.operator().create_dir(&src_path).await?;
    let src_file_path = format!("{}{}", src_path, src_file);
    client
        .operator()
        .write(&src_file_path, content.clone())
        .await?;

    Ok(StagedFile {
        remote_path: src_file_path,
        content,
        file_name: src_file,
    })
}

async fn stage_remote_directory(client: &StorageClient) -> Result<(String, Vec<u8>)> {
    let src_dir = TEST_FIXTURE.new_dir_path();
    let file_name = "test_file.txt";
    let file_path = format!("{}{}", src_dir, file_name);
    let content = b"test directory content".to_vec();

    client.operator().create_dir(&src_dir).await?;
    client.operator().write(&file_path, content.clone()).await?;

    Ok((src_dir, content))
}

async fn stage_large_file(client: &StorageClient) -> Result<StagedFile> {
    let src_file = "large_file.bin";
    let src_path = TEST_FIXTURE.new_dir_path();
    let content = vec![0x42; 1024 * 1024 * 100];

    client.operator().create_dir(&src_path).await?;
    let src_file_path = format!("{}{}", src_path, src_file);
    client
        .operator()
        .write(&src_file_path, content.clone())
        .await?;

    Ok(StagedFile {
        remote_path: src_file_path,
        content,
        file_name: src_file.to_string(),
    })
}

async fn stage_special_char_file(client: &StorageClient) -> Result<StagedFile> {
    let src_file = "special!@#$%^&()_+-=;'file.txt";
    let src_path = TEST_FIXTURE.new_dir_path();
    let content = b"special character file content".to_vec();

    client.operator().create_dir(&src_path).await?;
    let src_file_path = format!("{}{}", src_path, src_file);
    client
        .operator()
        .write(&src_file_path, content.clone())
        .await?;

    Ok(StagedFile {
        remote_path: src_file_path,
        content,
        file_name: src_file.to_string(),
    })
}

async fn test_download_existing_file_to_directory(client: StorageClient) -> Result<()> {
    let staged_file = stage_remote_file(&client).await?;
    let local_dir = std::env::temp_dir().join(format!("ossify-dl-{}", Uuid::new_v4()));

    ossify_cmd()
        .arg("get")
        .arg(&staged_file.remote_path)
        .arg(&local_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("Downloaded:"));

    let downloaded_file = local_dir.join(&staged_file.file_name);
    let actual_content = fs::read(&downloaded_file).await?;
    assert_eq!(staged_file.content, actual_content);

    let _ = fs::remove_dir_all(&local_dir).await;
    Ok(())
}

async fn test_download_directory_recursive(client: StorageClient) -> Result<()> {
    let (remote_dir, expected_content) = stage_remote_directory(&client).await?;
    let local_dest = std::env::temp_dir().join(format!("ossify-dl-dir-{}", Uuid::new_v4()));

    ossify_cmd()
        .arg("get")
        .arg(&remote_dir)
        .arg(&local_dest)
        .assert()
        .success();

    let dest_file = local_dest.join("test_file.txt");
    let dest_content = fs::read(&dest_file).await?;
    assert_eq!(expected_content, dest_content);

    let _ = fs::remove_dir_all(&local_dest).await;
    Ok(())
}

async fn test_download_non_existent_file(_client: StorageClient) -> Result<()> {
    let remote_path = TEST_FIXTURE.new_file_path();
    let local_dir = std::env::temp_dir().join(format!("ossify-dl-miss-{}", Uuid::new_v4()));

    ossify_cmd()
        .arg("get")
        .arg(&remote_path)
        .arg(&local_dir)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Failed to download"));

    assert!(!local_dir.exists());
    Ok(())
}

async fn test_download_large_file(client: StorageClient) -> Result<()> {
    let staged_file = stage_large_file(&client).await?;
    let local_dir = std::env::temp_dir().join(format!("ossify-dl-large-{}", Uuid::new_v4()));

    ossify_cmd()
        .arg("get")
        .arg(&staged_file.remote_path)
        .arg(&local_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("Downloaded:"));

    let downloaded_file = local_dir.join(&staged_file.file_name);
    let actual_content = fs::read(&downloaded_file).await?;
    assert_eq!(staged_file.content.len(), actual_content.len());
    assert_eq!(staged_file.content, actual_content);

    let _ = fs::remove_dir_all(&local_dir).await;
    Ok(())
}

async fn test_download_with_special_chars(client: StorageClient) -> Result<()> {
    let staged_file = stage_special_char_file(&client).await?;
    let local_dir = std::env::temp_dir().join(format!("ossify-dl-special-{}", Uuid::new_v4()));

    ossify_cmd()
        .arg("get")
        .arg(&staged_file.remote_path)
        .arg(&local_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("Downloaded:"));

    let downloaded_file = local_dir.join(&staged_file.file_name);
    let actual_content = fs::read(&downloaded_file).await?;
    assert_eq!(staged_file.content, actual_content);

    let _ = fs::remove_dir_all(&local_dir).await;
    Ok(())
}
