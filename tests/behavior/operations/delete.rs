use crate::*;
use ossify::error::Result;
use ossify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_delete_single_file,
        test_delete_non_existent_file,
        test_delete_empty_directory,
        test_delete_non_empty_directory_recursively,
        test_delete_multiple_files_bulk
    ));
}

async fn test_delete_single_file(client: StorageClient) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(client.operator());
    client.operator().write(&path, content).await?;

    // Act
    client.operator().delete(&path).await?;

    // Assert
    let result = client.operator().stat(&path).await;
    assert!(result.is_err(), "File should be deleted");
    assert!(
        matches!(result.unwrap_err().kind(), opendal::ErrorKind::NotFound),
        "Error should be NotFound"
    );

    Ok(())
}

async fn test_delete_non_existent_file(client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    // Act & Assert: Deleting a non-existent file should not return an error.
    client.operator().delete(&path).await?;

    Ok(())
}

async fn test_delete_empty_directory(client: StorageClient) -> Result<()> {
    let dir_path = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dir_path).await?;

    // Act
    // Note: OpenDAL treats deleting a directory as deleting a file.
    client.operator().delete(&dir_path).await?;

    // Assert
    let result = client.operator().stat(&dir_path).await;
    assert!(result.is_err(), "Directory should be deleted");
    assert!(
        matches!(result.unwrap_err().kind(), opendal::ErrorKind::NotFound),
        "Error should be NotFound for deleted directory"
    );

    Ok(())
}

async fn test_delete_non_empty_directory_recursively(client: StorageClient) -> Result<()> {
    let root_dir = TEST_FIXTURE.new_dir_path();
    let file_path = format!("{root_dir}test.txt");
    let (path, content, _) = TEST_FIXTURE.new_file_with_range(file_path, 1..1024);
    client.operator().write(&path, content).await?;

    // Act
    client.operator().remove_all(&root_dir).await?;

    // Assert
    let result = client.operator().stat(&root_dir).await;
    assert!(result.is_err(), "Root directory should be deleted");

    let file_result = client.operator().stat(&path).await;
    assert!(
        file_result.is_err(),
        "File within directory should be deleted"
    );

    Ok(())
}

async fn test_delete_multiple_files_bulk(client: StorageClient) -> Result<()> {
    let mut paths = Vec::new();
    for _ in 0..5 {
        let (path, content, _) = TEST_FIXTURE.new_file(client.operator());
        client.operator().write(&path, content).await?;
        paths.push(path);
    }

    // Act
    for path in &paths {
        client.operator().delete(path).await?;
    }

    // Assert
    for path in paths {
        let result = client.operator().stat(&path).await;
        assert!(result.is_err(), "File {path} should be deleted");
    }

    Ok(())
}
