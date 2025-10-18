use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_tree_empty,
        test_tree_nested,
        test_tree_depth_limit,
        test_tree_dirs_only
    ));
}

pub async fn test_tree_empty(client: StorageClient) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dir).await?;

    storify_cmd()
        .arg("tree")
        .arg(&dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("/"));
    Ok(())
}

pub async fn test_tree_nested(client: StorageClient) -> Result<()> {
    let root = TEST_FIXTURE.new_dir_path();
    let d1 = format!("{root}a/");
    let d2 = format!("{d1}b/");
    let f1 = format!("{d2}c.txt");

    client.operator().create_dir(&root).await?;
    client.operator().create_dir(&d1).await?;
    client.operator().create_dir(&d2).await?;
    client.operator().write(&f1, vec![b'x']).await?;

    storify_cmd()
        .arg("tree")
        .arg(&root)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("/")
                .and(predicate::str::contains("a/"))
                .and(predicate::str::contains("b/"))
                .and(predicate::str::contains("c.txt")),
        );
    Ok(())
}

pub async fn test_tree_depth_limit(client: StorageClient) -> Result<()> {
    let root = TEST_FIXTURE.new_dir_path();
    let d1 = format!("{root}a/");
    let d2 = format!("{d1}b/");
    let f1 = format!("{d2}c.txt");

    client.operator().create_dir(&root).await?;
    client.operator().create_dir(&d1).await?;
    client.operator().create_dir(&d2).await?;
    client.operator().write(&f1, vec![b'x']).await?;

    storify_cmd()
        .arg("tree")
        .arg(&root)
        .arg("-d")
        .arg("1")
        .assert()
        .success()
        .stdout(
            predicate::str::contains("a/").and(
                predicate::str::is_match(r"(?m)^\s*[├└]── b/$")
                    .unwrap()
                    .not(),
            ),
        );
    Ok(())
}

pub async fn test_tree_dirs_only(client: StorageClient) -> Result<()> {
    let root = TEST_FIXTURE.new_dir_path();
    let d1 = format!("{root}a/");
    let f1 = format!("{root}file.txt");

    client.operator().create_dir(&root).await?;
    client.operator().create_dir(&d1).await?;
    client.operator().write(&f1, vec![b'x']).await?;

    storify_cmd()
        .arg("tree")
        .arg(&root)
        .arg("--dirs-only")
        .assert()
        .success()
        .stdout(predicate::str::contains("a/").and(predicate::str::contains("file.txt").not()));
    Ok(())
}
