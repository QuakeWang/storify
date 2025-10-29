use crate::*;
use assert_cmd::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_truncate_create_default_zero,
        test_truncate_to_smaller_size,
        test_truncate_to_larger_with_padding,
        test_truncate_no_create_is_noop,
        test_truncate_parents,
        test_truncate_size_limit_guard
    ));
}

async fn test_truncate_create_default_zero(client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    storify_cmd().arg("truncate").arg(&path).assert().success();

    let meta = client.operator().stat(&path).await?;
    assert!(meta.mode().is_file());
    assert_eq!(meta.content_length(), 0);
    Ok(())
}

async fn test_truncate_to_smaller_size(client: StorageClient) -> Result<()> {
    let (path, content, _size) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    let target: u64 = 128;
    storify_cmd()
        .arg("truncate")
        .args(["--size", &target.to_string(), &path])
        .assert()
        .success();

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), target);
    Ok(())
}

async fn test_truncate_to_larger_with_padding(client: StorageClient) -> Result<()> {
    let (path, content, _size) = TEST_FIXTURE.new_file_with_range("pad-file", 64..256);
    client.operator().write(&path, content).await?;

    let target: u64 = 2048;
    storify_cmd()
        .arg("truncate")
        .args(["--size", &target.to_string(), &path])
        .assert()
        .success();

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), target);
    Ok(())
}

async fn test_truncate_no_create_is_noop(client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    storify_cmd()
        .arg("truncate")
        .args(["-c", "--size", "0", &path])
        .assert()
        .success();

    // Should still not exist
    let res = client.operator().stat(&path).await;
    assert!(res.is_err());
    Ok(())
}

async fn test_truncate_parents(client: StorageClient) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();
    let nested = format!("{dir}nested/dirs/file.bin");
    storify_cmd()
        .arg("truncate")
        .args(["-p", "--size", "100", &nested])
        .assert()
        .success();

    let meta = client.operator().stat(&nested).await?;
    assert_eq!(meta.content_length(), 100);
    Ok(())
}

async fn test_truncate_size_limit_guard(_client: StorageClient) -> Result<()> {
    // This should fail fast due to size-limit without writing
    storify_cmd()
        .arg("truncate")
        .args(["-s", "2000000", "--size-limit", "1", "/guard-too-big"])
        .assert()
        .failure();
    Ok(())
}
