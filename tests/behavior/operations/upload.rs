use crate::*;
use opendal::EntryMode;
use ossify::error::Result;
use ossify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        client,
        test_upload_single_small_file,
        test_upload_empty_file,
        test_overwrite_existing_file,
        test_upload_to_non_existent_subdirectory,
        test_upload_large_file
    ));
}

/// Upload a single small file: Verify the file exists and its metadata is correct.
pub async fn test_upload_single_small_file(client: StorageClient) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file_with_range("small_file.txt", 1..1024);

    client.operator().write(&path, content.clone()).await?;

    // Verify metadata
    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    // Verify content
    let read_content = client.operator().read(&path).await?;
    assert_eq!(read_content.to_vec(), content);

    Ok(())
}

/// Upload an empty file: Verify that a 0-byte file can be created correctly.
pub async fn test_upload_empty_file(client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    let content: Vec<u8> = Vec::new();

    client.operator().write(&path, content).await?;

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), 0);
    assert_eq!(meta.mode(), EntryMode::FILE);

    // Verify content is empty
    let read_content = client.operator().read(&path).await?;
    assert!(read_content.is_empty());

    Ok(())
}

/// Overwrite an existing file: Verify that the old file's content is replaced by the new file's content.
pub async fn test_overwrite_existing_file(client: StorageClient) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();

    // Write initial content
    let (path, content_v1, _) = TEST_FIXTURE.new_file_with_range(path, 100..200);
    client.operator().write(&path, content_v1).await?;

    // Write new content to overwrite
    let (path, content_v2, size_v2) = TEST_FIXTURE.new_file_with_range(path, 300..400);
    client.operator().write(&path, content_v2.clone()).await?;

    // Verify content is overwritten
    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), size_v2 as u64);

    let read_content = client.operator().read(&path).await?;
    assert_eq!(read_content.to_vec(), content_v2);

    Ok(())
}

/// Upload to a non-existent subdirectory: Verify that the directory structure is created automatically.
pub async fn test_upload_to_non_existent_subdirectory(client: StorageClient) -> Result<()> {
    let path = format!("{}/{}/test.txt", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    TEST_FIXTURE.add_path(path.clone());

    let (_, content, size) = TEST_FIXTURE.new_file_with_range(&path, 1..1024);

    client.operator().write(&path, content.clone()).await?;

    // Verify file exists and metadata is correct
    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), size as u64);

    // Verify content
    let read_content = client.operator().read(&path).await?;
    assert_eq!(read_content.to_vec(), content);

    Ok(())
}

/// Upload a large file: (Optional) Test the multipart upload logic.
pub async fn test_upload_large_file(client: StorageClient) -> Result<()> {
    // 6MB to ensure it triggers multipart upload (default threshold is 5MB)
    let large_size = 6 * 1024 * 1024;
    let (path, content, size) =
        TEST_FIXTURE.new_file_with_range("large_file.bin", large_size..large_size + 1);

    assert_eq!(size, large_size);

    client.operator().write(&path, content.clone()).await?;

    let meta = client.operator().stat(&path).await?;
    assert_eq!(meta.content_length(), size as u64);

    // To save test time, we don't verify the content for large file.
    // The correctness of multipart upload is guaranteed by opendal.

    Ok(())
}
