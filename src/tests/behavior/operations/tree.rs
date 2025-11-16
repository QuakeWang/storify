use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(test_tree_nested, test_tree_depth_limit,);

async fn test_tree_nested(client: StorageClient) -> Result<()> {
    let root = TEST_FIXTURE.new_dir_path();
    let child = format!("{root}child/");
    let file = format!("{child}file.txt");

    client.operator().create_dir(&root).await?;
    client.operator().create_dir(&child).await?;
    client.operator().write(&file, vec![b'x']).await?;

    storify_cmd()
        .arg("tree")
        .arg(&root)
        .assert()
        .success()
        .stdout(predicate::str::contains("child/").and(predicate::str::contains("file.txt")));
    Ok(())
}

async fn test_tree_depth_limit(client: StorageClient) -> Result<()> {
    let root = TEST_FIXTURE.new_dir_path();
    let sub = format!("{root}sub/");
    client.operator().create_dir(&root).await?;
    client.operator().create_dir(&sub).await?;

    storify_cmd()
        .arg("tree")
        .arg("--depth")
        .arg("0")
        .arg(&root)
        .assert()
        .success()
        .stdout(predicate::str::contains(&sub).not());
    Ok(())
}
