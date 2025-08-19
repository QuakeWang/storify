use crate::*;
use assert_cmd::prelude::*;
use futures::TryStreamExt;
use opendal::EntryMode;
use ossify::error::Result;
use ossify::storage::StorageClient;
use predicates::prelude::*;
use uuid::Uuid;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_list_empty_directory,
        test_list_single_file,
        test_list_multiple_files,
        test_list_nested_directories,
        test_list_with_special_chars,
        test_list_invalid_path,
        test_list_recursive
    ));
}

pub async fn test_list_empty_directory(client: StorageClient) -> Result<()> {
    let dir_path = TEST_FIXTURE.new_dir_path();

    client.operator().create_dir(&dir_path).await?;

    ossify_cmd().arg("ls").arg(&dir_path).assert().success();

    let mut obs = client.operator().lister(&dir_path).await?;
    let mut entries = Vec::new();
    while let Some(de) = obs.try_next().await? {
        entries.push(de.path().to_string());
    }

    let is_empty = entries.is_empty()
        || (entries.len() == 1 && entries[0] == dir_path)
        || (entries.len() == 1 && entries[0].ends_with('/'));

    assert!(
        is_empty,
        "empty directory should have no actual content, found: {entries:?}"
    );

    Ok(())
}

pub async fn test_list_single_file(client: StorageClient) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(client.operator());

    client.operator().write(&path, content).await?;

    let parent = path.rsplit('/').nth(1).unwrap_or("").to_string();
    let parent_path = if parent.is_empty() { "" } else { &parent };

    let cli_path = if parent_path.is_empty() {
        "/"
    } else {
        parent_path
    };
    ossify_cmd()
        .arg("ls")
        .arg(cli_path)
        .assert()
        .success()
        .stdout(predicate::str::contains(&path));

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    Ok(())
}

pub async fn test_list_multiple_files(client: StorageClient) -> Result<()> {
    let parent = TEST_FIXTURE.new_dir_path();
    let mut expected_files = Vec::new();

    for _ in 0..5 {
        let file_path = format!("{parent}{}", Uuid::new_v4());
        let (_, content, _) = TEST_FIXTURE.new_file_with_range(&file_path, 100..1000);
        client.operator().write(&file_path, content).await?;
        expected_files.push(file_path);
    }

    let mut assert = ossify_cmd();
    assert.arg("ls").arg(&parent);
    let mut a = assert.assert();
    a = a.success();
    for expected in &expected_files {
        a = a.stdout(predicate::str::contains(expected));
    }

    Ok(())
}

pub async fn test_list_nested_directories(client: StorageClient) -> Result<()> {
    let root_dir = TEST_FIXTURE.new_dir_path();
    let sub_dir = format!("{root_dir}subdir/");
    let nested_dir = format!("{sub_dir}nested/");

    client.operator().create_dir(&root_dir).await?;
    client.operator().create_dir(&sub_dir).await?;
    client.operator().create_dir(&nested_dir).await?;

    let file_path = format!("{nested_dir}test.txt");
    let (_, content, _) = TEST_FIXTURE.new_file_with_range(&file_path, 100..500);
    client.operator().write(&file_path, content).await?;

    ossify_cmd()
        .arg("ls")
        .arg(&root_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains(&sub_dir));

    let meta = client.operator().stat(&sub_dir).await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

pub async fn test_list_with_special_chars(client: StorageClient) -> Result<()> {
    let parent = TEST_FIXTURE.new_dir_path();
    let special_names = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.with.dots.txt",
        "file@#$%^&*().txt",
    ];

    for name in &special_names {
        let file_path = format!("{parent}{name}");
        let (_, content, _) = TEST_FIXTURE.new_file_with_range(&file_path, 50..200);
        client.operator().write(&file_path, content).await?;
    }

    let mut assert = ossify_cmd();
    assert.arg("ls").arg(&parent);
    let mut a = assert.assert();
    a = a.success();
    for name in &special_names {
        let expected_path = format!("{parent}{name}");
        a = a.stdout(predicate::str::contains(&expected_path));
    }

    Ok(())
}

pub async fn test_list_invalid_path(client: StorageClient) -> Result<()> {
    let invalid_path = format!("{}/non_existent_dir/", Uuid::new_v4());

    ossify_cmd().arg("ls").arg(&invalid_path).assert().success();

    if let Ok(mut obs) = client.operator().lister(&invalid_path).await {
        let mut count = 0;
        while (obs.try_next().await?).is_some() {
            count += 1;
        }
        assert_eq!(count, 0, "non-existent directory should have no entries");
    }

    Ok(())
}

pub async fn test_list_recursive(client: StorageClient) -> Result<()> {
    let root_dir = TEST_FIXTURE.new_dir_path();
    let sub_dir = format!("{root_dir}subdir/");
    let nested_dir = format!("{sub_dir}nested/");

    client.operator().create_dir(&root_dir).await?;
    client.operator().create_dir(&sub_dir).await?;
    client.operator().create_dir(&nested_dir).await?;

    let root_file = format!("{root_dir}root.txt");
    let sub_file = format!("{sub_dir}sub.txt");
    let nested_file = format!("{nested_dir}nested.txt");

    for file_path in &[&root_file, &sub_file, &nested_file] {
        let (_, content, _) = TEST_FIXTURE.new_file_with_range(*file_path, 100..300);
        client.operator().write(file_path, content).await?;
    }

    ossify_cmd()
        .arg("ls")
        .arg("-R")
        .arg(&root_dir)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(&root_file)
                .and(predicate::str::contains(&sub_file))
                .and(predicate::str::contains(&nested_file)),
        );

    Ok(())
}
