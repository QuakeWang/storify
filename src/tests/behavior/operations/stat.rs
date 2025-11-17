use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(test_stat_file_human, test_stat_not_found,);

async fn test_stat_file_human(client: StorageClient) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    storify_cmd()
        .arg("stat")
        .arg(&path)
        .assert()
        .success()
        .stdout(predicate::str::contains("type=file"));
    Ok(())
}

async fn test_stat_not_found(_client: StorageClient) -> Result<()> {
    storify_cmd()
        .arg("stat")
        .arg("missing")
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("OpenDAL error")
                .or(predicate::str::contains("Path not found")),
        );
    Ok(())
}
