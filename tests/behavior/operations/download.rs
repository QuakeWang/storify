use crate::*;
use assert_cmd::prelude::*;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_download_directory,
        test_download_non_existent_file
    ));
}

async fn test_download_directory(_client: StorageClient) -> Result<()> {
    let source_dir = get_test_data_path("special_dir !@#$%^&()_+-=;'");
    let remote_dir = TEST_FIXTURE.new_dir_path();
    let local_dest_dir = get_test_data_path("download").join("downloaded_dir");

    ossify_cmd()
        .arg("put")
        .arg("-R")
        .arg(&source_dir)
        .arg(&remote_dir)
        .assert()
        .success();

    ossify_cmd()
        .arg("get")
        .arg(&remote_dir)
        .arg(&local_dest_dir)
        .assert()
        .success();

    let mut source_entries = get_dir_entries(&source_dir, &source_dir)?;
    let mut dest_entries = get_dir_entries(&local_dest_dir, &local_dest_dir)?;
    source_entries.sort_by(|a, b| a.0.cmp(&b.0));
    dest_entries.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(source_entries, dest_entries);

    fs::remove_dir_all(&local_dest_dir)?;

    Ok(())
}

async fn test_download_non_existent_file(_client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    let local_path = get_test_data_path("download").join("non_existent_file.txt");

    let mut cmd = ossify_cmd();
    cmd.arg("get").arg(&path).arg(&local_path);
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Path does not exist"));

    Ok(())
}

fn get_dir_entries(
    path: impl AsRef<Path>,
    base_path: impl AsRef<Path>,
) -> Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            entries.extend(get_dir_entries(&path, base_path.as_ref())?);
        } else {
            let content = fs::read(&path)?;
            let relative_path = path.strip_prefix(base_path.as_ref()).unwrap().to_path_buf();
            entries.push((relative_path, content));
        }
    }
    Ok(entries)
}
