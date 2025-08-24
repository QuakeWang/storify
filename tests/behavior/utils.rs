use assert_cmd::prelude::*;
use libtest_mimic::{Failed, Trial};
use opendal::Operator;
use rand::Rng;
use rand::prelude::*;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::LazyLock;
use storify::error::Result;
use storify::storage::StorageClient;
use uuid::Uuid;

const TEST_DEFAULT_BUCKET: &str = "test";
const TEST_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9000";
const TEST_DEFAULT_ACCESS_KEY_ID: &str = "minioadmin";
const TEST_DEFAULT_ACCESS_KEY_SECRET: &str = "minioadmin";
const TEST_DEFAULT_REGION: &str = "us-east-1";

pub static TEST_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

// Cache MinIO config for tests to avoid repeated env reads
static TEST_MINIO_CONFIG: LazyLock<storify::storage::StorageConfig> =
    LazyLock::new(|| build_minio_config_from_env().expect("minio config"));

pub async fn init_test_service() -> Result<StorageClient> {
    // This ensures behavior tests run against local MinIO without relying on global env mutation.
    let config = build_minio_config_from_env()?;
    let client = StorageClient::new(config).await?;

    ensure_bucket_exists(client.operator()).await?;

    Ok(client)
}

/// Get the absolute path to a file under `tests/data/`.
pub fn get_test_data_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
        .join(file_name)
}

fn build_minio_config_from_env() -> Result<storify::storage::StorageConfig> {
    let bucket = env::var("STORAGE_BUCKET").unwrap_or_else(|_| TEST_DEFAULT_BUCKET.to_string());
    let access_key_id = env::var("STORAGE_ACCESS_KEY_ID")
        .unwrap_or_else(|_| TEST_DEFAULT_ACCESS_KEY_ID.to_string());
    let access_key_secret = env::var("STORAGE_ACCESS_KEY_SECRET")
        .unwrap_or_else(|_| TEST_DEFAULT_ACCESS_KEY_SECRET.to_string());
    let region = env::var("STORAGE_REGION")
        .ok()
        .unwrap_or_else(|| TEST_DEFAULT_REGION.to_string());
    let endpoint = env::var("STORAGE_ENDPOINT")
        .ok()
        .unwrap_or_else(|| TEST_DEFAULT_ENDPOINT.to_string());

    let mut config =
        storify::storage::StorageConfig::s3(bucket, access_key_id, access_key_secret, Some(region));
    config.endpoint = Some(endpoint);

    Ok(config)
}

/// Apply MinIO config to a command as environment variables
fn apply_minio_env<'a>(
    cmd: &'a mut Command,
    cfg: &storify::storage::StorageConfig,
) -> &'a mut Command {
    cmd.env("STORAGE_PROVIDER", "minio")
        .env("STORAGE_BUCKET", &cfg.bucket)
        .env(
            "STORAGE_ENDPOINT",
            cfg.endpoint.as_deref().unwrap_or(TEST_DEFAULT_ENDPOINT),
        )
        .env(
            "STORAGE_ACCESS_KEY_ID",
            cfg.access_key_id
                .as_deref()
                .unwrap_or(TEST_DEFAULT_ACCESS_KEY_ID),
        )
        .env(
            "STORAGE_ACCESS_KEY_SECRET",
            cfg.access_key_secret
                .as_deref()
                .unwrap_or(TEST_DEFAULT_ACCESS_KEY_SECRET),
        )
        .env(
            "STORAGE_REGION",
            cfg.region.as_deref().unwrap_or(TEST_DEFAULT_REGION),
        )
}

/// Create a base storify Command with clean environment and logging configured
fn base_cmd() -> Command {
    let mut cmd = Command::cargo_bin("storify").unwrap();
    cmd.env_clear().env("RUST_LOG", "info");
    cmd
}

/// Ensure the target bucket exists for tests. Ignores 'already exists' errors.
pub async fn ensure_bucket_exists(op: &Operator) -> Result<()> {
    match op.create_dir("").await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == opendal::ErrorKind::Unexpected => Ok(()),
        Err(e) => Err(storify::error::Error::from(e)),
    }
}

pub fn join_remote_path(remote_path: &str, file_name: &str) -> String {
    if remote_path.ends_with('/') {
        format!("{remote_path}{file_name}")
    } else {
        format!("{remote_path}/{file_name}")
    }
}

pub struct Fixture {
    pub paths: std::sync::Mutex<Vec<String>>,
}

impl Fixture {
    pub const fn new() -> Self {
        Self {
            paths: std::sync::Mutex::new(vec![]),
        }
    }

    pub fn add_path(&self, path: String) {
        self.paths.lock().unwrap().push(path);
    }

    pub fn new_dir_path(&self) -> String {
        let path = format!("{}/", Uuid::new_v4());
        self.paths.lock().unwrap().push(path.clone());
        path
    }

    pub fn new_file_path(&self) -> String {
        let path = format!("{}", Uuid::new_v4());
        self.paths.lock().unwrap().push(path.clone());
        path
    }

    pub fn new_file(&self, op: &Operator) -> (String, Vec<u8>, usize) {
        let max_size = op
            .info()
            .full_capability()
            .write_total_max_size
            .unwrap_or(4 * 1024 * 1024);

        self.new_file_with_range(Uuid::new_v4().to_string(), 1..max_size)
    }

    pub fn new_file_with_range(
        &self,
        path: impl Into<String>,
        range: std::ops::Range<usize>,
    ) -> (String, Vec<u8>, usize) {
        let path = path.into();
        self.paths.lock().unwrap().push(path.clone());

        let mut rng = rand::rng();
        let size = rng.random_range(range);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        (path, content, size)
    }

    pub async fn cleanup(&self, op: &Operator) {
        let paths: Vec<_> = std::mem::take(self.paths.lock().unwrap().as_mut());
        if !paths.is_empty() {
            let _ = op.delete_iter(paths).await;
        }
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

/// A helper struct for managing End-to-End test environments.
pub struct E2eTestEnv {
    pub config: storify::storage::StorageConfig,
    pub verifier: StorageClient,
}

impl E2eTestEnv {
    pub async fn new() -> Self {
        let cfg = TEST_MINIO_CONFIG.clone();
        let verifier = StorageClient::new(cfg.clone())
            .await
            .expect("failed to create verifier client");
        ensure_bucket_exists(verifier.operator())
            .await
            .expect("Failed to create E2E test bucket");

        Self {
            config: cfg,
            verifier,
        }
    }

    /// Returns a Command pre-configured with all necessary environment variables.
    pub fn command(&self) -> Command {
        let mut cmd = base_cmd();
        apply_minio_env(&mut cmd, &self.config);
        cmd
    }
}

pub fn build_async_trial<F, Fut>(name: &str, client: &StorageClient, f: F) -> Trial
where
    F: FnOnce(StorageClient) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let handle = TEST_RUNTIME.handle().clone();
    let client = client.clone();

    Trial::test(format!("behavior::{name}"), move || {
        handle
            .block_on(f(client))
            .map_err(|err| Failed::from(err.to_string()))
    })
}

#[macro_export]
macro_rules! async_trials {
    ($client:ident, $($test:ident),*) => {
        vec![$(build_async_trial(stringify!($test), $client, $test),)*]
    };
}

pub static TEST_FIXTURE: Fixture = Fixture::new();

pub fn storify_cmd() -> Command {
    let cfg = TEST_MINIO_CONFIG.clone();
    let mut cmd = base_cmd();
    apply_minio_env(&mut cmd, &cfg);
    cmd
}
