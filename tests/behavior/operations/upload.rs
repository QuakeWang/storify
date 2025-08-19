use crate::*;
use crate::{get_test_data_path, join_remote_path};
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use tokio::fs;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_storage_client_write,
        test_storage_client_write_from_special_dir
    ));

    tests.extend(async_trials!(client, e2e_test_upload_command_succeeds));
}

async fn test_storage_client_write(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let file_name = source_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let expected_content = fs::read(&source_path).await?;

    ossify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_prefix)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    let env = E2eTestEnv::new().await;
    let final_dest_path = join_remote_path(&dest_prefix, &file_name);
    let uploaded_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(expected_content, uploaded_content.to_vec());
    Ok(())
}

async fn test_storage_client_write_from_special_dir(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("special_dir !@#$%^&()_+-=;'/file_in_special_dir.txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let file_name = source_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let expected_content = fs::read(&source_path).await?;

    ossify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_prefix)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    let env = E2eTestEnv::new().await;
    let final_dest_path = join_remote_path(&dest_prefix, &file_name);
    let uploaded_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(expected_content, uploaded_content.to_vec());
    Ok(())
}

async fn e2e_test_upload_command_succeeds(_client: StorageClient) -> Result<()> {
    let source_path = get_test_data_path("small.txt");
    let dest_path = TEST_FIXTURE.new_file_path();
    let final_dest_path = format!("{}", source_path.file_name().unwrap().to_string_lossy());
    let final_dest_path = join_remote_path(&dest_path, &final_dest_path);

    ossify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    let env = E2eTestEnv::new().await;
    let expected_content = fs::read(&source_path).await?;
    let actual_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(expected_content, actual_content.to_vec());

    Ok(())
}
