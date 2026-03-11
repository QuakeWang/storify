use crate::error::{Error, Result};
use crate::storage::constants::SYNC_CHUNK_SIZE;
use crate::storage::utils::path::ensure_trailing_slash;
use crate::storage::utils::retry::read_range_with_retry;
use futures::stream::{self, StreamExt, TryStreamExt};
use opendal::{EntryMode, Operator};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Direction of a sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Local filesystem → remote object storage.
    Upload,
    /// Remote object storage → local filesystem.
    Download,
}

impl std::str::FromStr for SyncDirection {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "upload" | "up" | "push" => Ok(Self::Upload),
            "download" | "down" | "pull" => Ok(Self::Download),
            other => Err(Error::InvalidArgument {
                message: format!("invalid sync direction '{other}': expected upload|download"),
            }),
        }
    }
}

/// Options that control how sync behaves.
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub direction: SyncDirection,
    pub dry_run: bool,
    pub delete: bool,
    pub concurrency: usize,
}

/// Per-file action decided during the diff phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncAction {
    Create,
    Update,
    Delete,
    Skip,
}

/// Lightweight metadata used for comparison.
#[derive(Debug, Clone)]
struct EntryMeta {
    size: u64,
    etag: Option<String>,
}

/// Summary of a completed sync operation.
#[derive(Debug, Default)]
pub struct SyncReport {
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
    pub bytes_transferred: u64,
}

impl fmt::Display for SyncReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Sync complete: {} created, {} updated, {} deleted, {} skipped ({} bytes transferred)",
            self.created, self.updated, self.deleted, self.skipped, self.bytes_transferred,
        )
    }
}

/// Trait for synchronising files between local and remote storage.
pub trait Syncer {
    /// Synchronise `source` with `target` according to `opts`.
    fn sync(
        &self,
        source: &str,
        target: &str,
        opts: &SyncOptions,
    ) -> impl std::future::Future<Output = Result<SyncReport>> + Send;
}

/// OpenDAL-backed syncer implementation.
pub struct OpenDalSyncer {
    operator: Operator,
}

impl OpenDalSyncer {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    // ------------------------------------------------------------------
    // Upload sync  (local → remote)
    // ------------------------------------------------------------------

    async fn sync_upload(
        &self,
        local_root: &str,
        remote_root: &str,
        opts: &SyncOptions,
    ) -> Result<SyncReport> {
        let local_root = Path::new(local_root);
        if !local_root.is_dir() {
            return Err(Error::InvalidArgument {
                message: format!("source '{}' is not a directory", local_root.display()),
            });
        }

        let remote_root_norm = ensure_trailing_slash(remote_root);

        // 1. Walk local FS (size only — MD5 computed lazily during diff)
        let local_entries = walk_local_dir(local_root).await?;

        // 2. List remote
        let remote_entries = self.list_remote_entries(&remote_root_norm).await?;

        // 3. Diff with lazy MD5
        let plan = diff_entries(
            &local_entries,
            &remote_entries,
            opts.delete,
            Some(local_root),
        )
        .await;

        if opts.dry_run {
            return Self::print_dry_run(&plan);
        }

        // 4. Execute transfers + deletes
        let report = self
            .execute_plan(
                plan,
                opts.concurrency,
                |op, rel_path| {
                    let local_file = local_root.join(&rel_path);
                    let remote_path = format!("{remote_root_norm}{rel_path}");
                    async move { upload_file(&op, &local_file, &remote_path).await }
                },
                |op, rel_path| {
                    let remote_path = format!("{remote_root_norm}{rel_path}");
                    async move {
                        op.delete(&remote_path).await?;
                        println!("  DELETE {remote_path}");
                        Ok(())
                    }
                },
            )
            .await?;

        println!("\n{report}");
        Ok(report)
    }

    // ------------------------------------------------------------------
    // Download sync  (remote → local)
    // ------------------------------------------------------------------

    async fn sync_download(
        &self,
        remote_root: &str,
        local_root: &str,
        opts: &SyncOptions,
    ) -> Result<SyncReport> {
        let local_root = Path::new(local_root);
        fs::create_dir_all(local_root).await?;

        let remote_root_norm = ensure_trailing_slash(remote_root);

        // 1. List remote
        let remote_entries = self.list_remote_entries(&remote_root_norm).await?;

        // 2. Walk local (size only)
        let local_entries = if local_root.exists() {
            walk_local_dir(local_root).await?
        } else {
            HashMap::new()
        };

        // 3. Diff (source = remote, target = local; no lazy MD5 needed — remote has ETags)
        let plan = diff_entries(&remote_entries, &local_entries, opts.delete, None).await;

        if opts.dry_run {
            return Self::print_dry_run(&plan);
        }

        // 4. Execute transfers + deletes
        let report = self
            .execute_plan(
                plan,
                opts.concurrency,
                |op, rel_path| {
                    let local_file = local_root.join(&rel_path);
                    let remote_path = format!("{remote_root_norm}{rel_path}");
                    async move { download_file(&op, &remote_path, &local_file).await }
                },
                |_op, rel_path| {
                    let local_file = local_root.join(&rel_path);
                    async move {
                        if local_file.exists() {
                            fs::remove_file(&local_file).await?;
                            println!("  DELETE {}", local_file.display());
                        }
                        Ok(())
                    }
                },
            )
            .await?;

        println!("\n{report}");
        Ok(report)
    }

    // ------------------------------------------------------------------
    // Generic plan executor
    // ------------------------------------------------------------------

    /// Execute a sync plan with bounded concurrency.
    ///
    /// `transfer_fn` is called for Create/Update actions with `(Operator, rel_path)`.
    /// `delete_fn` is called for Delete actions.
    async fn execute_plan<TF, TFut, DF, DFut>(
        &self,
        plan: Vec<(String, SyncAction)>,
        concurrency: usize,
        transfer_fn: TF,
        delete_fn: DF,
    ) -> Result<SyncReport>
    where
        TF: Fn(Operator, String) -> TFut,
        TFut: std::future::Future<Output = Result<u64>> + Send,
        DF: Fn(Operator, String) -> DFut,
        DFut: std::future::Future<Output = Result<()>> + Send,
    {
        let mut report = SyncReport::default();

        // Separate transfers from other actions to process concurrently
        let mut transfers = Vec::new();
        let mut deletes = Vec::new();
        for (rel_path, action) in &plan {
            match action {
                SyncAction::Create | SyncAction::Update => {
                    transfers.push((rel_path.clone(), *action));
                }
                SyncAction::Delete => {
                    deletes.push(rel_path.clone());
                }
                SyncAction::Skip => {
                    report.skipped += 1;
                }
            }
        }

        // Execute transfers with bounded concurrency
        let transfer_results: Vec<(String, SyncAction, Result<u64>)> = stream::iter(transfers)
            .map(|(rel_path, action)| {
                let fut = transfer_fn(self.operator.clone(), rel_path.clone());
                async move { (rel_path, action, fut.await) }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for (rel_path, action, result) in transfer_results {
            match result {
                Ok(bytes) => {
                    report.bytes_transferred += bytes;
                    match action {
                        SyncAction::Create => report.created += 1,
                        SyncAction::Update => report.updated += 1,
                        _ => {}
                    }
                }
                Err(e) => {
                    return Err(Error::SyncFailed {
                        path: rel_path,
                        source: Box::new(e),
                    });
                }
            }
        }

        // Execute deletes sequentially (ordering matters for safety)
        for rel_path in deletes {
            delete_fn(self.operator.clone(), rel_path).await?;
            report.deleted += 1;
        }

        Ok(report)
    }

    // ------------------------------------------------------------------
    // Remote helpers
    // ------------------------------------------------------------------

    async fn list_remote_entries(&self, remote_root: &str) -> Result<HashMap<String, EntryMeta>> {
        let mut entries = HashMap::new();

        let lister = self
            .operator
            .lister_with(remote_root)
            .recursive(true)
            .await?;
        futures::pin_mut!(lister);

        while let Some(entry) = lister.try_next().await? {
            let meta = entry.metadata();
            if meta.mode() != EntryMode::FILE {
                continue;
            }

            let full_path = entry.path();
            let rel_path = match full_path.strip_prefix(remote_root) {
                Some(rel) if !rel.is_empty() => rel.to_string(),
                _ => {
                    log::warn!(
                        "skipping entry with unexpected prefix: path='{}' root='{}'",
                        full_path,
                        remote_root
                    );
                    continue;
                }
            };

            entries.insert(
                rel_path,
                EntryMeta {
                    size: meta.content_length(),
                    etag: meta.etag().map(str::to_string),
                },
            );
        }

        Ok(entries)
    }

    fn print_dry_run(plan: &[(String, SyncAction)]) -> Result<SyncReport> {
        let mut report = SyncReport::default();
        for (path, action) in plan {
            match action {
                SyncAction::Create => {
                    println!("  [DRY-RUN] CREATE {path}");
                    report.created += 1;
                }
                SyncAction::Update => {
                    println!("  [DRY-RUN] UPDATE {path}");
                    report.updated += 1;
                }
                SyncAction::Delete => {
                    println!("  [DRY-RUN] DELETE {path}");
                    report.deleted += 1;
                }
                SyncAction::Skip => {
                    report.skipped += 1;
                }
            }
        }
        println!("\n{report} (dry-run, no changes made)");
        Ok(report)
    }
}

impl Syncer for OpenDalSyncer {
    async fn sync(&self, source: &str, target: &str, opts: &SyncOptions) -> Result<SyncReport> {
        match opts.direction {
            SyncDirection::Upload => self.sync_upload(source, target, opts).await,
            SyncDirection::Download => self.sync_download(source, target, opts).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Diff logic
// ---------------------------------------------------------------------------

/// Compare source entries against target entries and produce a plan.
///
/// When `local_root` is provided, MD5 is computed lazily (only for files where
/// the size matches but we need a content-level check against the remote ETag).
/// This avoids computing MD5 for files that differ in size.
async fn diff_entries(
    source: &HashMap<String, EntryMeta>,
    target: &HashMap<String, EntryMeta>,
    delete: bool,
    local_root: Option<&Path>,
) -> Vec<(String, SyncAction)> {
    let mut plan = Vec::new();

    let mut sorted_keys: Vec<_> = source.keys().collect();
    sorted_keys.sort();
    for key in sorted_keys {
        let src_meta = &source[key];
        match target.get(key) {
            None => plan.push((key.clone(), SyncAction::Create)),
            Some(tgt_meta) => {
                let action = determine_action(src_meta, tgt_meta, key, local_root).await;
                plan.push((key.clone(), action));
            }
        }
    }

    if delete {
        let mut orphan_keys: Vec<_> = target.keys().filter(|k| !source.contains_key(*k)).collect();
        orphan_keys.sort();
        for key in orphan_keys {
            plan.push((key.clone(), SyncAction::Delete));
        }
    }

    plan
}

/// Determine whether a file needs to be synced.
///
/// For upload: source has no ETag (local file), target has ETag (remote).
/// When sizes match, we lazily compute the local MD5 and compare.
async fn determine_action(
    src: &EntryMeta,
    tgt: &EntryMeta,
    rel_path: &str,
    local_root: Option<&Path>,
) -> SyncAction {
    // Fast path: if both have ETags (download sync), compare directly
    if let (Some(se), Some(te)) = (&src.etag, &tgt.etag) {
        return if normalize_etag(se) == normalize_etag(te) {
            SyncAction::Skip
        } else {
            SyncAction::Update
        };
    }

    // Size differs → always transfer
    if src.size != tgt.size {
        return SyncAction::Update;
    }

    // Size matches but no ETag on source (upload sync, local file).
    // Lazy MD5: compute only when needed.
    if let (None, Some(root), Some(remote_etag)) = (&src.etag, local_root, &tgt.etag) {
        let local_path = root.join(rel_path);
        if let Ok(local_md5) = compute_file_md5(&local_path).await {
            return if local_md5 == normalize_etag(remote_etag) {
                SyncAction::Skip
            } else {
                SyncAction::Update
            };
        }
    }

    // Cannot determine content difference — assume unchanged
    SyncAction::Skip
}

/// Strip surrounding quotes from an ETag value.
///
/// S3/MinIO returns ETags like `"d41d8cd98f00b204e9800998ecf8427e"` with literal
/// quotes, while local MD5 computation produces bare hex. Normalise both to bare hex.
fn normalize_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

// ---------------------------------------------------------------------------
// Local FS helpers
// ---------------------------------------------------------------------------

/// Recursively walk a local directory and collect file metadata (size only).
///
/// Symlinks are skipped to avoid infinite loops. MD5 is NOT computed here;
/// it is done lazily during the diff phase only for files that need it.
async fn walk_local_dir(root: &Path) -> Result<HashMap<String, EntryMeta>> {
    let mut entries = HashMap::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let mut read_dir = fs::read_dir(&dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let file_type = entry.file_type().await?;

            // Skip symlinks to prevent infinite recursion
            if file_type.is_symlink() {
                log::debug!("skipping symlink: {}", entry.path().display());
                continue;
            }

            let path = entry.path();
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let meta = entry.metadata().await?;
                let rel_path = path
                    .strip_prefix(root)
                    .map_err(|_| Error::InvalidPath {
                        path: path.display().to_string(),
                    })?
                    .to_string_lossy()
                    .to_string();

                entries.insert(
                    rel_path,
                    EntryMeta {
                        size: meta.len(),
                        etag: None, // MD5 computed lazily during diff
                    },
                );
            }
        }
    }

    Ok(entries)
}

/// Compute MD5 hex digest of a local file using buffered I/O.
async fn compute_file_md5(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut ctx = md5::Context::new();
    let mut buf = vec![0u8; 64 * 1024]; // 64 KiB read buffer

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        ctx.consume(&buf[..n]);
    }

    Ok(format!("{:x}", ctx.finalize()))
}

// ---------------------------------------------------------------------------
// Transfer helpers
// ---------------------------------------------------------------------------

/// Upload a single local file to remote storage using streaming I/O.
///
/// Files <= `SYNC_CHUNK_SIZE` are read all at once; larger files are streamed
/// in chunks to avoid excessive memory usage.
async fn upload_file(op: &Operator, local_path: &Path, remote_path: &str) -> Result<u64> {
    let meta = fs::metadata(local_path).await?;
    let size = meta.len();

    if size <= SYNC_CHUNK_SIZE {
        let data = fs::read(local_path).await?;
        op.write(remote_path, data).await?;
    } else {
        let mut file = fs::File::open(local_path).await?;
        let mut buf = vec![0u8; SYNC_CHUNK_SIZE as usize];
        let mut writer = op.writer(remote_path).await?;

        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            writer.write(buf[..n].to_vec()).await?;
        }
        writer.close().await?;
    }

    println!(
        "  ✅ {} → {remote_path} ({size} bytes)",
        local_path.display()
    );
    Ok(size)
}

/// Download a single remote file to local filesystem using streaming I/O.
///
/// Files <= `SYNC_CHUNK_SIZE` are read all at once; larger files are streamed
/// via range requests with retry on transient errors.
async fn download_file(op: &Operator, remote_path: &str, local_path: &Path) -> Result<u64> {
    if let Some(parent) = local_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let metadata = op.stat(remote_path).await?;
    let size = metadata.content_length();

    if size <= SYNC_CHUNK_SIZE {
        let data = op.read(remote_path).await?;
        fs::write(local_path, data.to_bytes()).await?;
    } else {
        let mut offset = 0u64;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(local_path)
            .await?;

        while offset < size {
            let end = std::cmp::min(offset + SYNC_CHUNK_SIZE, size);
            let data = read_range_with_retry(op, remote_path, offset..end).await?;
            file.write_all(&data.to_bytes()).await?;
            offset = end;
        }
        file.flush().await?;
    }

    println!(
        "  ✅ {remote_path} → {} ({size} bytes)",
        local_path.display()
    );
    Ok(size)
}
