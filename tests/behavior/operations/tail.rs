use crate::*;
use assert_cmd::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_tail_default_10_lines,
        test_tail_n_lines,
        test_tail_c_bytes,
        test_tail_zero_lines_and_zero_bytes,
        test_tail_nonexistent_file,
        test_tail_multi_files_with_headers,
        test_tail_multi_files_quiet,
        test_tail_multi_files_verbose,
        test_tail_follow_append_growth,
        test_tail_follow_truncate_and_rewrite
    ));
}

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

async fn test_tail_default_10_lines(_client: StorageClient) -> Result<()> {
    let lines: Vec<String> = (1..=20).map(|i| format!("line-{i}")).collect();
    let content = lines.join("\n") + "\n";
    let local = create_temp_file_with_content(content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    let out = String::from_utf8_lossy(&output);
    let expected: Vec<String> = (11..=20).map(|i| format!("line-{i}")).collect();
    let tail = expected.join("\n") + "\n";
    assert_eq!(out, tail);
    Ok(())
}

async fn test_tail_n_lines(_client: StorageClient) -> Result<()> {
    let content = b"A\nB\nC\nD\nE\n";
    let local = create_temp_file_with_content(content);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg("-n")
        .arg("3")
        .arg(&remote_path)
        .assert()
        .success();
    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(out, "C\nD\nE\n");
    Ok(())
}

async fn test_tail_c_bytes(_client: StorageClient) -> Result<()> {
    let content = "Hello, World! ".repeat(10);
    let local = create_temp_file_with_content(content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg("-c")
        .arg("50")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    assert_eq!(output.len(), 50);
    Ok(())
}

async fn test_tail_zero_lines_and_zero_bytes(_client: StorageClient) -> Result<()> {
    let content = b"X\nY\nZ\n";
    let local = create_temp_file_with_content(content);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let out_n0 = storify_cmd()
        .arg("tail")
        .arg("-n")
        .arg("0")
        .arg(&remote_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(out_n0.is_empty());

    let out_c0 = storify_cmd()
        .arg("tail")
        .arg("-c")
        .arg("0")
        .arg(&remote_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(out_c0.is_empty());
    Ok(())
}

async fn test_tail_nonexistent_file(_client: StorageClient) -> Result<()> {
    let non_existent_path = "nonexistent_file.txt";
    let assert = storify_cmd()
        .arg("tail")
        .arg(non_existent_path)
        .assert()
        .failure();
    let stderr = assert.get_output().stderr.clone();
    let stderr_str = String::from_utf8_lossy(&stderr);
    assert!(
        stderr_str.contains("not found")
            || stderr_str.contains("NotFound")
            || stderr_str.contains("Path does not exist")
    );
    Ok(())
}

async fn test_tail_multi_files_with_headers(_client: StorageClient) -> Result<()> {
    let src1 = get_test_data_path("small.txt");
    let src2_content = (1..=5)
        .map(|i| format!("L{i}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let src2 = create_temp_file_with_content(src2_content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote1 = upload_and_remote_path(&src1.to_string_lossy(), &dest_prefix);
    let remote2 = upload_and_remote_path(&src2, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg(&remote1)
        .arg(&remote2)
        .assert()
        .success();
    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(out.contains(&format!("==> {} <==", remote1)));
    assert!(out.contains(&format!("==> {} <==", remote2)));
    Ok(())
}

async fn test_tail_multi_files_quiet(_client: StorageClient) -> Result<()> {
    let src1 = get_test_data_path("small.txt");
    let src2_content = (1..=3)
        .map(|i| format!("q{i}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let src2 = create_temp_file_with_content(src2_content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote1 = upload_and_remote_path(&src1.to_string_lossy(), &dest_prefix);
    let remote2 = upload_and_remote_path(&src2, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg("-q")
        .arg(&remote1)
        .arg(&remote2)
        .assert()
        .success();
    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(!out.contains("==>"));
    Ok(())
}

async fn test_tail_multi_files_verbose(_client: StorageClient) -> Result<()> {
    let src1 = get_test_data_path("small.txt");
    let src2_content = (1..=2)
        .map(|i| format!("v{i}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let src2 = create_temp_file_with_content(src2_content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote1 = upload_and_remote_path(&src1.to_string_lossy(), &dest_prefix);
    let remote2 = upload_and_remote_path(&src2, &dest_prefix);

    let assert = storify_cmd()
        .arg("tail")
        .arg("-v")
        .arg(&remote1)
        .arg(&remote2)
        .assert()
        .success();
    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(out.contains(&format!("==> {} <==", remote1)));
    assert!(out.contains(&format!("==> {} <==", remote2)));
    Ok(())
}

async fn test_tail_follow_append_growth(_client: StorageClient) -> Result<()> {
    let initial = b"A\n";
    let tmp = create_temp_file_with_content(initial);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&tmp, &dest_prefix);

    std::fs::write(&tmp, b"A\nB\nC\n").expect("rewrite local file");
    storify_cmd()
        .arg("put")
        .arg(&tmp)
        .arg(&dest_prefix)
        .assert()
        .success();

    use std::process::Stdio;
    let mut child = storify_cmd()
        .arg("tail")
        .arg("-f")
        .arg("-n")
        .arg("2")
        .arg(&remote_path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn tail -f");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait output");
    let s = String::from_utf8_lossy(&output.stdout);
    assert!(s.contains("B\n") || s.contains("C\n"));
    Ok(())
}

async fn test_tail_follow_truncate_and_rewrite(_client: StorageClient) -> Result<()> {
    let lines: Vec<String> = (1..=20).map(|i| format!("L{i}")).collect();
    let initial = (lines.join("\n") + "\n").into_bytes();
    let tmp = create_temp_file_with_content(&initial);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&tmp, &dest_prefix);

    std::fs::write(&tmp, b"X\nY\nZ\n").expect("rewrite local file");
    storify_cmd()
        .arg("put")
        .arg(&tmp)
        .arg(&dest_prefix)
        .assert()
        .success();

    use std::process::Stdio;
    let mut child = storify_cmd()
        .arg("tail")
        .arg("-f")
        .arg("-n")
        .arg("2")
        .arg(&remote_path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn tail -f");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait output");
    let s = String::from_utf8_lossy(&output.stdout);
    assert!(s.contains("Y\n") || s.contains("Z\n"));
    Ok(())
}
