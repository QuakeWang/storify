use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;
use uuid::Uuid;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_create_single_directory,
        test_create_directory_with_parents,
        test_create_root_directory,
        test_create_existing_directory,
        test_create_nested_directories
    ));
}

async fn test_create_single_directory(_client: StorageClient) -> Result<()> {
    let dir_name = format!("test-dir-{}", Uuid::new_v4());

    storify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    let _list_result = storify_cmd().arg("ls").arg(&dir_name).assert().success();

    let _ = storify_cmd().arg("rm").arg("-R").arg(&dir_name).output();

    Ok(())
}

async fn test_create_directory_with_parents(_client: StorageClient) -> Result<()> {
    let parent_dir = format!("parent-{}", Uuid::new_v4());
    let nested_path = format!("{}/nested/subdir", parent_dir);

    storify_cmd()
        .arg("mkdir")
        .arg("-p")
        .arg(&nested_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    let _list_result = storify_cmd().arg("ls").arg(&parent_dir).assert().success();

    let _ = storify_cmd().arg("rm").arg("-R").arg(&parent_dir).output();

    Ok(())
}

async fn test_create_root_directory(_client: StorageClient) -> Result<()> {
    storify_cmd()
        .arg("mkdir")
        .arg("/")
        .assert()
        .success()
        .stdout(predicate::str::contains("already exists"));

    Ok(())
}

async fn test_create_existing_directory(_client: StorageClient) -> Result<()> {
    let dir_name = format!("existing-dir-{}", Uuid::new_v4());

    storify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    let result = storify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .output()
        .expect("Failed to execute command");

    assert!(result.status.success(), "Second mkdir should succeed");

    let _ = storify_cmd().arg("rm").arg("-R").arg(&dir_name).output();

    Ok(())
}

async fn test_create_nested_directories(_client: StorageClient) -> Result<()> {
    let base_dir = format!("nested-{}", Uuid::new_v4());
    let nested_path = format!("{}/a/b/c/d", base_dir);

    storify_cmd()
        .arg("mkdir")
        .arg("-p")
        .arg(&nested_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    let _list_result = storify_cmd().arg("ls").arg(&nested_path).assert().success();

    let _ = storify_cmd().arg("rm").arg("-R").arg(&base_dir).output();

    Ok(())
}
