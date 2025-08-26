use crate::*;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use storify::error::Result;
use storify::storage::StorageClient;

pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(client, test_du_summary_total_size));
}

pub async fn test_du_summary_total_size(client: StorageClient) -> Result<()> {
    // Prepare a directory with files of deterministic sizes
    let dir = TEST_FIXTURE.new_dir_path();
    client.operator().create_dir(&dir).await?;

    let sizes: [usize; 3] = [1000, 2048, 3];
    for (idx, size) in sizes.iter().enumerate() {
        let path = format!("{dir}f{idx}");
        let content = vec![b'a'; *size];
        client.operator().write(&path, content).await?;
    }

    fn format_size(size: u64) -> String {
        const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
        const THRESHOLD: u64 = 1024;
        if size < THRESHOLD {
            return format!("{size}B");
        }
        let mut size_f = size as f64;
        let mut unit_index = 0;
        while size_f >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
            size_f /= THRESHOLD as f64;
            unit_index += 1;
        }
        format!("{size_f:.1}{}", UNITS[unit_index])
    }

    let expected_total: u64 = sizes.iter().map(|s| *s as u64).sum();
    let expected_files = sizes.len();
    let alt_expected_files = expected_files + 1; // some backends may include the directory itself
    let expected_human = format_size(expected_total);

    let mut cmd = storify_cmd();
    cmd.arg("du")
        .arg("-s")
        .arg(&dir)
        .assert()
        .success()
        // first summary line like: "<size> <path>"
        .stdout(predicate::str::contains(format!("{expected_human} {dir}")))
        .stdout(
            predicate::str::contains(format!("Total files: {expected_files}")).or(
                predicate::str::contains(format!("Total files: {alt_expected_files}")),
            ),
        );

    Ok(())
}
