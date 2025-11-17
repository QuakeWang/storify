use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
register_behavior_tests!(test_storage_client_write, e2e_test_upload_command_succeeds,);

async fn test_storage_client_write(_client: StorageClient) -> Result<()> {
    let content = b"upload small file\n".to_vec();
    let source_path = write_temp_file(&content, ".txt");
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let file_name = source_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    storify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_prefix)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    let env = E2eTestEnv::new().await;
    let final_dest_path = join_remote_path(&dest_prefix, &file_name);
    let uploaded_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(content, uploaded_content.to_vec());
    Ok(())
}

async fn e2e_test_upload_command_succeeds(_client: StorageClient) -> Result<()> {
    let content = b"upload e2e file\n".to_vec();
    let source_path = write_temp_file(&content, ".txt");
    let dest_path = TEST_FIXTURE.new_file_path();
    let final_dest_path = format!("{}", source_path.file_name().unwrap().to_string_lossy());
    let final_dest_path = join_remote_path(&dest_path, &final_dest_path);

    storify_cmd()
        .arg("put")
        .arg(&source_path)
        .arg(&dest_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Upload"));

    let env = E2eTestEnv::new().await;
    let actual_content = env.verifier.operator().read(&final_dest_path).await?;
    assert_eq!(content, actual_content.to_vec());

    Ok(())
}
