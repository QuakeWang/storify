use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(test_find_by_name_glob, test_find_type_file,);

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
