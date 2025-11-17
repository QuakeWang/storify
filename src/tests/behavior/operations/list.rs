use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use futures::TryStreamExt;
use opendal::EntryMode;
use predicates::prelude::*;

register_behavior_tests!(
    test_list_empty_directory,
    test_list_single_file,
    test_list_invalid_path,
);

async fn test_list_empty_directory(client: StorageClient) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dir).await?;

    storify_cmd().arg("ls").arg(&dir).assert().success();

    let mut lister = client.operator().lister(&dir).await?;
    while let Some(entry) = lister.try_next().await? {
        assert!(entry.path() == dir || entry.path().is_empty());
    }
    Ok(())
}

async fn test_list_single_file(client: StorageClient) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    let parent = path.rsplit('/').nth(1).unwrap_or("");
    storify_cmd()
        .arg("ls")
        .arg(if parent.is_empty() { "/" } else { parent })
        .assert()
        .success()
        .stdout(predicate::str::contains(&path));

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);
    Ok(())
}

async fn test_list_invalid_path(_client: StorageClient) -> Result<()> {
    storify_cmd()
        .arg("ls")
        .arg("nonexistent-dir/")
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
    Ok(())
}
