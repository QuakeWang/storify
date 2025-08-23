use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
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

    ossify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    // Verify directory exists by listing it
    let _list_result = ossify_cmd().arg("ls").arg(&dir_name).assert().success();

    // Clean up
    let _ = ossify_cmd().arg("rm").arg("-R").arg(&dir_name).output();

    Ok(())
}

async fn test_create_directory_with_parents(_client: StorageClient) -> Result<()> {
    let parent_dir = format!("parent-{}", Uuid::new_v4());
    let nested_path = format!("{}/nested/subdir", parent_dir);

    ossify_cmd()
        .arg("mkdir")
        .arg("-p")
        .arg(&nested_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    // Verify all directories exist
    let _list_result = ossify_cmd().arg("ls").arg(&parent_dir).assert().success();

    // Clean up
    let _ = ossify_cmd().arg("rm").arg("-R").arg(&parent_dir).output();

    Ok(())
}

async fn test_create_root_directory(_client: StorageClient) -> Result<()> {
    ossify_cmd()
        .arg("mkdir")
        .arg("/")
        .assert()
        .success()
        .stdout(predicate::str::contains("already exists"));

    Ok(())
}

async fn test_create_existing_directory(_client: StorageClient) -> Result<()> {
    let dir_name = format!("existing-dir-{}", Uuid::new_v4());

    // Create directory first time
    ossify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    // Try to create the same directory again
    // In object storage, this might succeed again or show "already exists"
    // Both behaviors are acceptable
    let result = ossify_cmd()
        .arg("mkdir")
        .arg(&dir_name)
        .output()
        .expect("Failed to execute command");

    assert!(result.status.success(), "Second mkdir should succeed");

    // Clean up
    let _ = ossify_cmd().arg("rm").arg("-R").arg(&dir_name).output();

    Ok(())
}

async fn test_create_nested_directories(_client: StorageClient) -> Result<()> {
    let base_dir = format!("nested-{}", Uuid::new_v4());
    let nested_path = format!("{}/a/b/c/d", base_dir);

    ossify_cmd()
        .arg("mkdir")
        .arg("-p")
        .arg(&nested_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory:"));

    // Verify the deepest directory exists
    let _list_result = ossify_cmd().arg("ls").arg(&nested_path).assert().success();

    // Clean up
    let _ = ossify_cmd().arg("rm").arg("-R").arg(&base_dir).output();

    Ok(())
}
