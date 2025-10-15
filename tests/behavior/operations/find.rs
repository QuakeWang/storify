use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_find_by_name_glob,
        test_find_by_regex,
        test_find_type_file,
        test_find_type_dir,
        test_find_type_other_empty
    ));
}

async fn test_find_by_name_glob(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root = TEST_FIXTURE.new_dir_path();
    let sub = format!("{root}sub/");
    env.verifier.operator().create_dir(&root).await?;
    env.verifier.operator().create_dir(&sub).await?;

    let f1 = format!("{root}a.log");
    let f2 = format!("{sub}b.log");
    let f3 = format!("{sub}c.txt");
    env.verifier.operator().write(&f1, b"x".to_vec()).await?;
    env.verifier.operator().write(&f2, b"y".to_vec()).await?;
    env.verifier.operator().write(&f3, b"z".to_vec()).await?;

    storify_cmd()
        .arg("find")
        .arg(&root)
        .arg("--name")
        .arg("**/*.log")
        .assert()
        .success()
        .stdout(
            predicate::str::contains(&f1)
                .and(predicate::str::contains(&f2))
                .and(predicate::str::contains(&f3).not()),
        );

    Ok(())
}

async fn test_find_by_regex(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root = TEST_FIXTURE.new_dir_path();
    env.verifier.operator().create_dir(&root).await?;
    let f1 = format!("{root}x.csv");
    let f2 = format!("{root}y.parquet");
    let f3 = format!("{root}z.txt");
    env.verifier.operator().write(&f1, b"1".to_vec()).await?;
    env.verifier.operator().write(&f2, b"2".to_vec()).await?;
    env.verifier.operator().write(&f3, b"3".to_vec()).await?;

    storify_cmd()
        .arg("find")
        .arg(&root)
        .arg("--regex")
        .arg(".*\\.(csv|parquet)$")
        .assert()
        .success()
        .stdout(
            predicate::str::contains(&f1)
                .and(predicate::str::contains(&f2))
                .and(predicate::str::contains(&f3).not()),
        );

    Ok(())
}

async fn test_find_type_file(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root = TEST_FIXTURE.new_dir_path();
    let sub = format!("{root}d/");
    env.verifier.operator().create_dir(&root).await?;
    env.verifier.operator().create_dir(&sub).await?;
    let f = format!("{root}file.bin");
    env.verifier.operator().write(&f, b"data".to_vec()).await?;

    storify_cmd()
        .arg("find")
        .arg(&root)
        .arg("--type")
        .arg("f")
        .assert()
        .success()
        .stdout(predicate::str::contains(&f).and(predicate::str::contains(&sub).not()));

    Ok(())
}

async fn test_find_type_dir(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root = TEST_FIXTURE.new_dir_path();
    let sub = format!("{root}sub/");
    env.verifier.operator().create_dir(&root).await?;
    env.verifier.operator().create_dir(&sub).await?;
    let f = format!("{root}f.txt");
    env.verifier.operator().write(&f, b"1".to_vec()).await?;

    storify_cmd()
        .arg("find")
        .arg(&root)
        .arg("--type")
        .arg("d")
        .assert()
        .success()
        .stdout(predicate::str::contains(&sub).and(predicate::str::contains(&f).not()));

    Ok(())
}

// OpenDAL typically returns only file/dir; other should be empty
async fn test_find_type_other_empty(_client: StorageClient) -> Result<()> {
    let env = E2eTestEnv::new().await;
    let root = TEST_FIXTURE.new_dir_path();
    env.verifier.operator().create_dir(&root).await?;
    let f = format!("{root}x.txt");
    env.verifier.operator().write(&f, b"1".to_vec()).await?;

    storify_cmd()
        .arg("find")
        .arg(&root)
        .arg("--type")
        .arg("o")
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    Ok(())
}
