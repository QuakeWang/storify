use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use uuid::Uuid;

register_behavior_tests!(
    test_create_single_directory,
    test_create_directory_with_parents,
);

async fn test_create_single_directory(_client: StorageClient) -> Result<()> {
    let dir = format!("test-dir-{}", Uuid::new_v4());

    storify_cmd()
        .arg("mkdir")
        .arg(&dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory"));

    storify_cmd().arg("ls").arg(&dir).assert().success();
    storify_cmd().arg("rm").arg("-R").arg(&dir).output().ok();
    Ok(())
}

async fn test_create_directory_with_parents(_client: StorageClient) -> Result<()> {
    let nested = format!("parent-{}/child/sub", Uuid::new_v4());

    storify_cmd()
        .arg("mkdir")
        .arg("-p")
        .arg(&nested)
        .assert()
        .success()
        .stdout(predicate::str::contains("Created directory"));

    storify_cmd()
        .arg("ls")
        .arg(nested.trim_end_matches("sub"))
        .assert()
        .success();
    Ok(())
}
