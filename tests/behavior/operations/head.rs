use crate::*;
use assert_cmd::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_head_default_10_lines,
        test_head_n_lines,
        test_head_c_bytes,
        test_head_zero_lines_and_zero_bytes,
        test_head_nonexistent_file,
        test_head_multi_files_with_headers,
        test_head_multi_files_quiet,
        test_head_multi_files_verbose
    ));
}

/// Create a temporary file under system temp dir with given content and return its path.
fn create_temp_file_with_content(content: &[u8]) -> String {
    let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::write(&path, content).expect("write temp file");
    path.to_string_lossy().to_string()
}

/// Upload a local file to a generated remote prefix via CLI and return the full remote path.
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

// Default: first 10 lines
async fn test_head_default_10_lines(_client: StorageClient) -> Result<()> {
    // Build a file with >10 lines
    let lines: Vec<String> = (1..=20).map(|i| format!("line-{i}")).collect();
    let content = lines.join("\n") + "\n"; // newline-terminated
    let local = create_temp_file_with_content(content.as_bytes());

    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    let out = String::from_utf8_lossy(&output);

    // Expect exactly 10 lines (if file has >=10)
    assert_eq!(out.lines().count(), 10);
    // Check first and last emitted line
    assert!(out.starts_with("line-1\n"));
    assert!(out.contains("line-10\n"));
    assert!(!out.contains("line-11\n"));

    Ok(())
}

// -n N lines
async fn test_head_n_lines(_client: StorageClient) -> Result<()> {
    let content = b"A\nB\nC\nD\nE\n"; // 5 lines
    let local = create_temp_file_with_content(content);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg("-n")
        .arg("3")
        .arg(&remote_path)
        .assert()
        .success();
    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(out.lines().count(), 3);
    assert!(out.starts_with("A\n"));
    assert!(out.contains("B\n"));
    assert!(out.contains("C\n"));
    assert!(!out.contains("D\n"));

    Ok(())
}

// -c N bytes
async fn test_head_c_bytes(_client: StorageClient) -> Result<()> {
    let content = "Hello, World! ".repeat(10); // 150 bytes
    let local = create_temp_file_with_content(content.as_bytes());
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    let assert = storify_cmd()
        .arg("head")
        .arg("-c")
        .arg("50")
        .arg(&remote_path)
        .assert()
        .success();
    let output = assert.get_output().stdout.clone();
    assert_eq!(output.len(), 50);

    let output_str = String::from_utf8_lossy(&output);
    assert!(output_str.starts_with("Hello, World! Hello, World! Hello, World! Hello"));

    Ok(())
}

// Zero lines and zero bytes should output nothing
async fn test_head_zero_lines_and_zero_bytes(_client: StorageClient) -> Result<()> {
    let content = b"X\nY\nZ\n";
    let local = create_temp_file_with_content(content);
    let dest_prefix = TEST_FIXTURE.new_file_path();
    let remote_path = upload_and_remote_path(&local, &dest_prefix);

    // -n 0
    let out_n0 = storify_cmd()
        .arg("head")
        .arg("-n")
        .arg("0")
        .arg(&remote_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(out_n0.is_empty());

    // -c 0
    let out_c0 = storify_cmd()
        .arg("head")
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

// Non-existent file should fail with not found message
async fn test_head_nonexistent_file(_client: StorageClient) -> Result<()> {
    let non_existent_path = "nonexistent_file.txt";

    let assert = storify_cmd()
        .arg("head")
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

// Multiple files: default shows headers when >1 files
async fn test_head_multi_files_with_headers(_client: StorageClient) -> Result<()> {
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
        .arg("head")
        .arg(&remote1)
        .arg(&remote2)
        .assert()
        .success();

    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    // Expect headers like ==> path <== present twice
    assert!(out.contains(&format!("==> {} <==", remote1)));
    assert!(out.contains(&format!("==> {} <==", remote2)));

    Ok(())
}

// Multiple files with -q: no headers
async fn test_head_multi_files_quiet(_client: StorageClient) -> Result<()> {
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
        .arg("head")
        .arg("-q")
        .arg(&remote1)
        .arg(&remote2)
        .assert()
        .success();

    let out = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(!out.contains("==>"));

    Ok(())
}

// Multiple files with -v: always show headers
async fn test_head_multi_files_verbose(_client: StorageClient) -> Result<()> {
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
        .arg("head")
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
