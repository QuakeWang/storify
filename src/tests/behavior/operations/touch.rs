use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;

register_behavior_tests!(test_touch_create_and_truncate,);

async fn test_touch_create_and_truncate(_client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    storify_cmd().arg("touch").arg(&path).assert().success();

    storify_cmd()
        .arg("touch")
        .args(["-t", &path])
        .assert()
        .success();

    Ok(())
}
