use crate::config::{ProviderBackend, prepare_storage_backend};
pub use crate::config::{StorageProvider, storage_config::StorageConfig};
use crate::error::{Error, Result};
use opendal::Operator;

pub mod constants;
mod operations;
mod utils;
pub use self::utils::OutputFormat;

use self::operations::cat::OpenDalFileReader;
use self::operations::copy::OpenDalCopier;
use self::operations::delete::OpenDalDeleter;
use self::operations::download::OpenDalDownloader;
use self::operations::find::OpenDalFinder;
use self::operations::grep::OpenDalGreper;
use self::operations::head::OpenDalHeadReader;
use self::operations::list::OpenDalLister;
use self::operations::mkdir::OpenDalMkdirer;
use self::operations::mv::OpenDalMover;
use self::operations::tail::OpenDalTailReader;
use self::operations::tree::OpenDalTreer;
use self::operations::upload::OpenDalUploader;
use self::operations::usage::OpenDalUsageCalculator;
use self::operations::{
    Cater, Copier, Deleter, Downloader, Greper, Header, Lister, Mkdirer, Mover, Stater, Tailer,
    Treer, Uploader, UsageCalculator,
};
use crate::storage::utils::error::IntoStorifyError;
use crate::wrap_err;
use futures::stream::TryStreamExt;

/// Unified storage client using OpenDAL
#[derive(Clone)]
pub struct StorageClient {
    operator: Operator,
    provider: StorageProvider,
}

impl StorageClient {
    pub async fn new(mut config: StorageConfig) -> Result<Self> {
        let provider = config.provider;
        let backend = prepare_storage_backend(&mut config)?;
        let operator = Self::build_operator(provider, &backend)?;
        Ok(Self { operator, provider })
    }

    pub fn provider(&self) -> StorageProvider {
        self.provider
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    #[allow(unused_variables)]
    fn build_operator(provider: StorageProvider, backend: &ProviderBackend) -> Result<Operator> {
        match backend {
            ProviderBackend::Oss {
                bucket,
                access_key,
                secret_key,
                endpoint,
                anonymous,
            } => {
                let mut builder = opendal::services::Oss::default().bucket(bucket);
                if *anonymous {
                    builder = builder.allow_anonymous();
                }
                if let Some(access_key_id) = access_key.as_deref() {
                    builder = builder.access_key_id(access_key_id);
                }
                if let Some(access_key_secret) = secret_key.as_deref() {
                    builder = builder.access_key_secret(access_key_secret);
                }
                if let Some(endpoint) = endpoint.as_deref() {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)?.finish())
            }
            ProviderBackend::S3 {
                bucket,
                access_key,
                secret_key,
                region,
                endpoint,
                anonymous,
            } => {
                let mut builder = opendal::services::S3::default().bucket(bucket);
                if *anonymous {
                    builder = builder.allow_anonymous();
                }
                if let Some(access_key_id) = access_key.as_deref() {
                    builder = builder.access_key_id(access_key_id);
                }
                if let Some(secret_access_key) = secret_key.as_deref() {
                    builder = builder.secret_access_key(secret_access_key);
                }
                if let Some(region) = region.as_deref() {
                    builder = builder.region(region);
                }
                if let Some(endpoint) = endpoint.as_deref() {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)?.finish())
            }
            ProviderBackend::Cos {
                bucket,
                secret_id,
                secret_key,
                endpoint,
            } => {
                let mut builder = opendal::services::Cos::default().bucket(bucket);
                builder = builder.secret_id(secret_id).secret_key(secret_key);
                if let Some(endpoint) = endpoint.as_deref() {
                    builder = builder.endpoint(endpoint);
                }
                log::debug!(
                    "COS builder config: bucket={}, endpoint={:?}",
                    bucket,
                    endpoint,
                );
                Ok(Operator::new(builder)?.finish())
            }
            ProviderBackend::Fs { root } => {
                let builder = opendal::services::Fs::default().root(root);
                Ok(Operator::new(builder)?.finish())
            }
            ProviderBackend::Hdfs { root, name_node } => {
                #[cfg(feature = "hdfs")]
                {
                    let builder = opendal::services::Hdfs::default()
                        .root(root)
                        .name_node(name_node);
                    Ok(Operator::new(builder)?.finish())
                }

                #[cfg(not(feature = "hdfs"))]
                {
                    let _ = (root, name_node);
                    Err(Error::UnsupportedProvider {
                        provider: format!("{} (feature disabled)", provider.as_str()),
                    })
                }
            }
            ProviderBackend::Azblob {
                container,
                account_name,
                account_key,
                endpoint,
            } => {
                let mut builder = opendal::services::Azblob::default().container(container);
                if let Some(account_name) = account_name.as_deref() {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = account_key.as_deref() {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = endpoint.as_deref() {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)?.finish())
            }
        }
    }

    pub async fn list_directory(&self, path: &str, long: bool, recursive: bool) -> Result<()> {
        log::debug!(
            "list_directory provider={:?} path={} long={} recursive={}",
            self.provider,
            path,
            long,
            recursive
        );
        let lister = OpenDalLister::new(self.operator.clone());
        wrap_err!(
            lister.list(path, long, recursive).await,
            ListDirectoryFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn print_tree(
        &self,
        path: &str,
        depth: Option<usize>,
        dirs_only: bool,
    ) -> Result<()> {
        let treer = OpenDalTreer::new(self.operator.clone());
        treer.tree(path, depth, dirs_only).await
    }

    pub async fn download_files(&self, remote_path: &str, local_path: &str) -> Result<()> {
        log::debug!(
            "download_files provider={:?} remote_path={} local_path={}",
            self.provider,
            remote_path,
            local_path
        );
        let downloader = OpenDalDownloader::new(self.operator.clone());
        wrap_err!(
            downloader.download(remote_path, local_path).await,
            DownloadFailed {
                remote_path: remote_path.to_string(),
                local_path: local_path.to_string()
            }
        )
    }

    pub async fn disk_usage(&self, path: &str, summary: bool) -> Result<()> {
        log::debug!(
            "disk_usage provider={:?} path={} summary={}",
            self.provider,
            path,
            summary
        );
        let calculator = OpenDalUsageCalculator::new(self.operator.clone());
        wrap_err!(
            calculator.calculate_usage(path, summary).await,
            DiskUsageFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn upload_files(
        &self,
        local_path: &str,
        remote_path: &str,
        is_recursive: bool,
    ) -> Result<()> {
        log::debug!(
            "upload_files provider={:?} local_path={} remote_path={} recursive={}",
            self.provider,
            local_path,
            remote_path,
            is_recursive
        );
        let uploader = OpenDalUploader::new(self.operator.clone());
        wrap_err!(
            uploader.upload(local_path, remote_path, is_recursive).await,
            UploadFailed {
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string()
            }
        )
    }

    pub async fn delete_files(&self, paths: &[String], recursive: bool) -> Result<()> {
        log::debug!(
            "delete_files provider={:?} paths_count={} recursive={}",
            self.provider,
            paths.len(),
            recursive
        );
        let deleter = OpenDalDeleter::new(self.operator.clone());
        wrap_err!(
            deleter.delete(paths, recursive).await,
            DeleteFailed {
                // summarize inputs to avoid huge error strings
                paths: paths.iter().take(5).cloned().collect::<Vec<_>>().join(","),
                recursive: recursive
            }
        )
    }

    pub async fn copy_files(&self, src_path: &str, dest_path: &str) -> Result<()> {
        log::debug!(
            "copy_files provider={:?} src_path={} dest_path={}",
            self.provider,
            src_path,
            dest_path
        );
        let copier = OpenDalCopier::new(self.operator.clone());
        wrap_err!(
            copier.copy(src_path, dest_path).await,
            CopyFailed {
                src_path: src_path.to_string(),
                dest_path: dest_path.to_string()
            }
        )
    }

    pub async fn move_files(&self, src_path: &str, dest_path: &str) -> Result<()> {
        log::debug!(
            "move_files provider={:?} src_path={} dest_path={}",
            self.provider,
            src_path,
            dest_path
        );
        let mover = OpenDalMover::new(self.operator.clone());
        wrap_err!(
            mover.mover(src_path, dest_path).await,
            MoveFailed {
                src_path: src_path.to_string(),
                dest_path: dest_path.to_string()
            }
        )
    }

    pub async fn create_directory(&self, path: &str, parents: bool) -> Result<()> {
        log::debug!(
            "create_directory provider={:?} path={} parents={}",
            self.provider,
            path,
            parents
        );
        let mkdirer = OpenDalMkdirer::new(self.operator.clone());
        wrap_err!(
            mkdirer.mkdir(path, parents).await,
            DirectoryCreationFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn cat_file(&self, path: &str, force: bool, size_limit_mb: u64) -> Result<()> {
        log::debug!(
            "cat_file provider={:?} path={},force={},size_limit_mb={}",
            self.provider,
            path,
            force,
            size_limit_mb
        );
        let reader = OpenDalFileReader::new(self.operator.clone());
        wrap_err!(
            reader.cat(path, force, size_limit_mb).await,
            CatFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn head_file(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
    ) -> Result<()> {
        log::debug!(
            "head_file provider={:?} path={} lines={:?} bytes={:?}",
            self.provider,
            path,
            lines,
            bytes
        );
        let reader = OpenDalHeadReader::new(self.operator.clone());
        wrap_err!(
            reader.head(path, lines, bytes).await,
            HeadFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn head_files(
        &self,
        paths: &[String],
        lines: Option<usize>,
        bytes: Option<usize>,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        log::debug!(
            "head_files provider={:?} paths_count={} lines={:?} bytes={:?} quiet={} verbose={}",
            self.provider,
            paths.len(),
            lines,
            bytes,
            quiet,
            verbose
        );
        let reader = OpenDalHeadReader::new(self.operator.clone());
        wrap_err!(
            reader.head_many(paths, lines, bytes, quiet, verbose).await,
            HeadFailed {
                path: paths.iter().take(5).cloned().collect::<Vec<_>>().join(",")
            }
        )
    }

    pub async fn tail_file(
        &self,
        path: &str,
        lines: Option<usize>,
        bytes: Option<usize>,
    ) -> Result<()> {
        log::debug!(
            "tail_file provider={:?} path={} lines={:?} bytes={:?}",
            self.provider,
            path,
            lines,
            bytes
        );
        let reader = OpenDalTailReader::new(self.operator.clone());
        wrap_err!(
            reader.tail(path, lines, bytes).await,
            TailFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn tail_files(
        &self,
        paths: &[String],
        lines: Option<usize>,
        bytes: Option<usize>,
        quiet: bool,
        verbose: bool,
    ) -> Result<()> {
        log::debug!(
            "tail_files provider={:?} paths_count={} lines={:?} bytes={:?} quiet={} verbose={}",
            self.provider,
            paths.len(),
            lines,
            bytes,
            quiet,
            verbose
        );
        let reader = OpenDalTailReader::new(self.operator.clone());
        wrap_err!(
            reader.tail_many(paths, lines, bytes, quiet, verbose).await,
            TailFailed {
                path: paths.iter().take(5).cloned().collect::<Vec<_>>().join(",")
            }
        )
    }

    pub async fn stat_metadata(&self, path: &str, format: OutputFormat) -> Result<()> {
        log::debug!(
            "stat_metadata provider={:?} path={} format={:?}",
            self.provider,
            path,
            format
        );
        let stater = self::operations::stat::OpenDalStater::new(self.operator.clone());
        let meta = stater.stat(path).await?;

        match format {
            OutputFormat::Human => {
                println!("path={}", meta.path);
                println!("type={}", meta.entry_type);
                println!("size={}", meta.size);
                if let Some(t) = meta.last_modified {
                    println!("last_modified={}", t);
                }
                if let Some(etag) = meta.etag {
                    println!("etag=\"{}\"", etag);
                }
                if let Some(ct) = meta.content_type {
                    println!("content_type={}", ct);
                }
            }
            OutputFormat::Raw => {
                println!("path={}", meta.path);
                println!("type={}", meta.entry_type);
                println!("size={}", meta.size);
                if let Some(t) = meta.last_modified {
                    println!("last_modified={}", t);
                }
                if let Some(etag) = meta.etag {
                    println!("etag=\"{}\"", etag);
                }
                if let Some(ct) = meta.content_type {
                    println!("content_type={}", ct);
                }
            }
            OutputFormat::Json => {
                #[derive(serde::Serialize)]
                struct JsonMeta<'a> {
                    path: &'a str,
                    entry_type: &'a str,
                    size: u64,
                    last_modified: Option<String>,
                    etag: Option<String>,
                    content_type: Option<String>,
                }
                let json = JsonMeta {
                    path: &meta.path,
                    entry_type: &meta.entry_type,
                    size: meta.size,
                    last_modified: meta.last_modified,
                    etag: meta.etag,
                    content_type: meta.content_type,
                };
                println!("{}", serde_json::to_string(&json)?);
            }
        }

        Ok(())
    }

    pub async fn grep_file(
        &self,
        path: &str,
        pattern: &str,
        ignore_case: bool,
        line_number: bool,
    ) -> Result<()> {
        log::debug!(
            "grep_file provider={:?} path={} pattern={} ignore_case={} line_number={}",
            self.provider,
            path,
            pattern,
            ignore_case,
            line_number
        );
        let greper = OpenDalGreper::new(self.operator.clone());
        wrap_err!(
            greper
                .grep(path, pattern, ignore_case, line_number, false)
                .await,
            GrepFailed {
                path: path.to_string()
            }
        )
    }

    pub async fn grep_path(
        &self,
        path: &str,
        pattern: &str,
        ignore_case: bool,
        line_number: bool,
        recursive: bool,
    ) -> Result<()> {
        log::debug!(
            "grep_path provider={:?} path={} pattern={} ignore_case={} line_number={} recursive={}",
            self.provider,
            path,
            pattern,
            ignore_case,
            line_number,
            recursive
        );

        // When recursive is requested, avoid failing on NotFound for virtual prefixes (S3/OSS).
        if recursive {
            match self.operator.stat(path).await {
                Ok(meta) => {
                    if meta.mode().is_file() {
                        return self
                            .grep_file(path, pattern, ignore_case, line_number)
                            .await;
                    }
                    // If it's a directory or other type, fall through to recursive listing.
                }
                Err(e) => {
                    // NotFound likely indicates a virtual prefix; proceed to listing.
                    if e.kind() != opendal::ErrorKind::NotFound {
                        return Err(Error::GrepFailed {
                            path: path.to_string(),
                            source: Box::new(e.into()),
                        });
                    }
                }
            }

            let lister = wrap_err!(
                self.operator.lister_with(path).recursive(true).await,
                ListDirectoryFailed {
                    path: path.to_string()
                }
            )?;

            futures::pin_mut!(lister);
            while let Some(entry) =
                lister
                    .try_next()
                    .await
                    .map_err(|e| Error::ListDirectoryFailed {
                        path: path.to_string(),
                        source: Box::new(e.into_error()),
                    })?
            {
                if entry.metadata().mode().is_file() {
                    let greper = OpenDalGreper::new(self.operator.clone());
                    greper
                        .grep(entry.path(), pattern, ignore_case, line_number, true)
                        .await?;
                }
            }
            return Ok(());
        }

        // Non-recursive: require a real file; directories must use -R.
        let meta = self.operator.stat(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: std::path::PathBuf::from(path),
                }
            } else {
                Error::GrepFailed {
                    path: path.to_string(),
                    source: Box::new(e.into()),
                }
            }
        })?;

        if meta.mode().is_file() {
            return self
                .grep_file(path, pattern, ignore_case, line_number)
                .await;
        }
        if meta.mode().is_dir() {
            return Err(Error::InvalidArgument {
                message: "Path is a directory; use -R to grep recursively".to_string(),
            });
        }

        Err(Error::InvalidArgument {
            message: format!("Unsupported object type for grep: {}", path),
        })
    }

    pub async fn find_paths(&self, args: &crate::cli::storage::FindArgs) -> Result<()> {
        log::debug!(
            "find_paths provider={:?} path={} name={:?} regex_present={} type={:?}",
            self.provider,
            args.path,
            args.name,
            args.regex.is_some(),
            args.r#type,
        );

        // Prepare filters
        let name_glob = if let Some(pattern) = &args.name {
            let g = globset::Glob::new(pattern).map_err(|e| Error::InvalidArgument {
                message: format!("invalid --name glob: {}", e),
            })?;
            Some(g.compile_matcher())
        } else {
            None
        };

        let regex = if let Some(re) = &args.regex {
            Some(regex::Regex::new(re).map_err(|e| Error::InvalidArgument {
                message: format!("invalid --regex: {}", e),
            })?)
        } else {
            None
        };

        let type_filter = match args.r#type.as_deref() {
            None => None,
            Some("f") => Some(self::operations::find::EntryTypeFilter::File),
            Some("d") => Some(self::operations::find::EntryTypeFilter::Dir),
            Some("o") => Some(self::operations::find::EntryTypeFilter::Other),
            Some(other) => {
                return Err(Error::InvalidArgument {
                    message: format!("invalid --type: {} (expected f|d|o)", other),
                });
            }
        };

        let finder = OpenDalFinder::new(self.operator.clone());
        let opts = self::operations::find::FindOptions {
            path: args.path.clone(),
            name_glob,
            regex,
            type_filter,
        };

        self::operations::find::Finder::find(&finder, &opts)
            .await
            .map_err(|e| match e {
                Error::PathNotFound { .. } => e,
                other => Error::FindFailed {
                    path: args.path.clone(),
                    source: Box::new(other),
                },
            })
    }
}
