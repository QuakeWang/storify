use crate::async_trials;
use crate::error::Result;
use crate::storage::StorageClient;
use crate::tests::behavior::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;

register_behavior_tests!(
    test_head_default_10_lines,
    test_head_n_lines,
    test_head_nonexistent_file,
);

fn create_temp_file_with_content(content: &[u8]) -> String {
    let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::write(&path, content).expect("write temp file");
    path.to_string_lossy().to_string()
}

fn upload_and_remote_path(local_path: &str, dest_prefix: &str) -> String {
    storify_cmd()
        .arg("put")
        .arg(local_path)
        .arg(dest_prefix)
        .assert()
        .success();

    let file_name = std::path::Path::new(local_path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    join_remote_path(dest_prefix, &file_name)
}

async fn test_head_default_10_lines(_client: StorageClient) -> Result<()> {
    let lines: Vec<String> = (1..=20).map(|i| format!("line-{i}")).collect();
    let local = create_temp_file_with_content((lines.join("\n") + "\n").as_bytes());

    let remote = upload_and_remote_path(&local, &TEST_FIXTURE.new_file_path());
    let assert = storify_cmd().arg("head").arg(&remote).assert().success();
    let output = String::from_utf8_lossy(&assert.get_output().stdout);

    assert_eq!(output.lines().count(), 10);
    assert!(output.contains("line-10"));
    assert!(!output.contains("line-11"));
    Ok(())
}

async fn test_head_n_lines(_client: StorageClient) -> Result<()> {
    let local = create_temp_file_with_content(b"A\nB\nC\nD\n");
    let remote = upload_and_remote_path(&local, &TEST_FIXTURE.new_file_path());

    let assert = storify_cmd()
        .arg("head")
        .arg("-n")
        .arg("2")
        .arg(&remote)
        .assert()
        .success();
    let output = String::from_utf8_lossy(&assert.get_output().stdout);

    assert_eq!(output.lines().collect::<Vec<_>>(), ["A", "B"]);
    Ok(())
}

async fn test_head_nonexistent_file(_client: StorageClient) -> Result<()> {
    storify_cmd()
        .arg("head")
        .arg("missing-file.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Failed to read head of file"));
    Ok(())
}
