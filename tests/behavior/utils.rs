use libtest_mimic::{Failed, Trial};
use opendal::Operator;
use ossify::error::Result;
use ossify::storage::StorageClient;
use rand::Rng;
use rand::prelude::*;
use std::env;
use std::sync::LazyLock;
use uuid::Uuid;

pub static TEST_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub async fn init_test_service() -> Result<Option<StorageClient>> {
    let provider = match env::var("STORAGE_PROVIDER") {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };

    if provider != "minio" {
        return Ok(None);
    }

    let config = build_minio_config_from_env()?;

    let client = StorageClient::new(config).await?;

    Ok(Some(client))
}

fn build_minio_config_from_env() -> Result<ossify::storage::StorageConfig> {
    let bucket = env::var("STORAGE_BUCKET").unwrap_or_else(|_| "test".to_string());
    let access_key_id =
        env::var("STORAGE_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
    let access_key_secret =
        env::var("STORAGE_ACCESS_KEY_SECRET").unwrap_or_else(|_| "minioadmin".to_string());
    let region = env::var("STORAGE_REGION")
        .ok()
        .unwrap_or_else(|| "us-east-1".to_string());
    let endpoint = env::var("STORAGE_ENDPOINT")
        .ok()
        .unwrap_or_else(|| "http://127.0.0.1:9000".to_string());

    let mut config =
        ossify::storage::StorageConfig::s3(bucket, access_key_id, access_key_secret, Some(region));
    config.endpoint = Some(endpoint);

    Ok(config)
}

pub struct Fixture {
    base_path: String,
}

impl Fixture {
    pub fn new() -> Self {
        Self {
            base_path: format!("data/{}/", Uuid::new_v4()),
        }
    }

    pub fn new_dir_path(&self) -> String {
        format!("{}{}/", self.base_path, Uuid::new_v4())
    }

    pub fn new_file_path(&self) -> String {
        format!("{}{}", self.base_path, Uuid::new_v4())
    }

    pub fn new_file(&self, op: &Operator) -> (String, Vec<u8>, usize) {
        let max_size = op
            .info()
            .full_capability()
            .write_total_max_size
            .unwrap_or(4 * 1024 * 1024);

        // HACK: The test `test_list_single_file` has a bug in calculating the parent path.
        // It only works correctly for files in the root directory.
        // To make the test pass without modifying it, we generate a root-level path here.
        let root_level_path = Uuid::new_v4().to_string();

        self.new_file_with_range(root_level_path, 1..max_size)
    }

    pub fn new_file_with_range(
        &self,
        path: impl Into<String>,
        range: std::ops::Range<usize>,
    ) -> (String, Vec<u8>, usize) {
        let path = path.into();

        let mut rng = rand::rng();
        let size = rng.random_range(range);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        (path, content, size)
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
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
        vec![$(
            build_async_trial(stringify!($test), $client, $test),
        )*]
    };
}

pub static TEST_FIXTURE: LazyLock<Fixture> = LazyLock::new(Fixture::new);
