use crate::error::{Error, Result};
use crate::storage::{OutputFormat, StorageClient};
use crate::utils::format_deletion_message;
use clap::Args as ClapArgs;
use tokio::runtime::Handle;
use tokio::task;

use super::context::CliContext;
use super::entry::Command;

fn parse_validated_path(path_str: &str) -> Result<String> {
    if path_str.trim().is_empty() {
        Err(Error::InvalidPath {
            path: path_str.to_string(),
        })
    } else {
        Ok(path_str.to_string())
    }
}

#[derive(ClapArgs, Debug, Clone)]
pub struct LsArgs {
    /// The path to list
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Show detailed information (long format)
    #[arg(short = 'L', long)]
    pub long: bool,

    /// Process directories recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct GetArgs {
    /// The remote path to download from
    #[arg(value_name = "REMOTE", value_parser = parse_validated_path)]
    pub remote: String,

    /// The local path to download to
    #[arg(value_name = "LOCAL", value_parser = parse_validated_path)]
    pub local: String,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct DuArgs {
    /// The path to check usage for
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Show summary only
    #[arg(short = 's', long)]
    pub summary: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct PutArgs {
    /// The local path to upload from
    #[arg(value_name = "LOCAL", value_parser = parse_validated_path)]
    pub local: String,

    /// The remote path to upload to
    #[arg(value_name = "REMOTE", value_parser = parse_validated_path)]
    pub remote: String,

    /// Process directories recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct RmArgs {
    /// Remote path(s) to delete
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Remove directories and their contents recursively
    #[arg(short = 'R', long)]
    pub recursive: bool,

    /// Force deletion without confirmation
    #[arg(short = 'f', long)]
    pub force: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct CpArgs {
    /// The remote path to copy from
    #[arg(value_name = "SRC", value_parser = parse_validated_path)]
    pub src_path: String,

    /// The remote path to copy to
    #[arg(value_name = "DEST", value_parser = parse_validated_path)]
    pub dest_path: String,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct MvArgs {
    /// The remote path to move from
    #[arg(value_name = "SRC", value_parser = parse_validated_path)]
    pub src_path: String,

    /// The remote path to move to
    #[arg(value_name = "DEST", value_parser = parse_validated_path)]
    pub dest_path: String,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct MkdirArgs {
    /// The directory path to create
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Create parent directories as needed
    #[arg(short, long)]
    pub parents: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct CatArgs {
    /// The remote file path to display
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    #[arg(short = 'f', long)]
    pub force: bool,

    /// Limit file size in MB (default: 10)
    #[arg(short = 's', long = "size-limit", default_value_t = 10)]
    pub size_limit_mb: u64,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct StatArgs {
    /// The path to stat
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Output as JSON (machine-readable)
    #[arg(long, conflicts_with = "raw")]
    pub json: bool,

    /// Output as raw key=value lines (compatible with opendal-mkdir)
    #[arg(long, conflicts_with = "json")]
    pub raw: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct HeadArgs {
    /// Remote file path(s) to display
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Number of lines to display
    #[arg(short = 'n', long, conflicts_with = "bytes")]
    pub lines: Option<usize>,

    /// Number of bytes to display
    #[arg(short = 'c', long, conflicts_with = "lines")]
    pub bytes: Option<usize>,

    /// Do not print headers for multiple files
    #[arg(short = 'q', long, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Always print headers
    #[arg(short = 'v', long, conflicts_with = "quiet")]
    pub verbose: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct TailArgs {
    /// Remote file path(s) to display
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Number of lines to display from the end
    #[arg(short = 'n', long, conflicts_with = "bytes")]
    pub lines: Option<usize>,

    /// Number of bytes to display from the end
    #[arg(short = 'c', long, conflicts_with = "lines")]
    pub bytes: Option<usize>,

    /// Do not print headers for multiple files
    #[arg(short = 'q', long, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Always print headers
    #[arg(short = 'v', long, conflicts_with = "quiet")]
    pub verbose: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct GrepArgs {
    /// Pattern to search for
    pub pattern: String,

    /// The remote file path to search
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Case insensitive search
    #[arg(short = 'i', long = "ignore-case")]
    pub ignore_case: bool,

    /// Show line numbers
    #[arg(short = 'n', long = "line-number")]
    pub line_number: bool,

    /// Recursively search directories
    #[arg(short = 'R', long = "recursive")]
    pub recursive: bool,
}

#[derive(ClapArgs, Debug, Clone)]
#[command(group = clap::ArgGroup::new("name_or_regex").args(["name", "regex"]).multiple(false))]
pub struct FindArgs {
    /// The path to search under (file or directory)
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Glob pattern to match full path (e.g. **/*.log)
    #[arg(long)]
    pub name: Option<String>,

    /// Regex to match full path
    #[arg(long)]
    pub regex: Option<String>,

    /// Filter by entry type: f (file), d (dir), o (other)
    #[arg(long = "type", value_name = "f|d|o")]
    pub r#type: Option<String>,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct TreeArgs {
    /// The path to show as a tree
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub path: String,

    /// Maximum depth to display (0 for unlimited)
    #[arg(short = 'd', long = "depth")]
    pub depth: Option<usize>,

    /// Show directories only
    #[arg(long = "dirs-only")]
    pub dirs_only: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct DiffArgs {
    /// The left file path
    #[arg(value_name = "LEFT", value_parser = parse_validated_path)]
    pub left: String,

    /// The right file path
    #[arg(value_name = "RIGHT", value_parser = parse_validated_path)]
    pub right: String,

    /// Number of context lines to show around changes
    #[arg(short = 'U', long = "context", default_value_t = 3)]
    pub context: usize,

    /// Ignore whitespace differences
    #[arg(short = 'w', long = "ignore-space")]
    pub ignore_space: bool,

    /// Limit total size of compared files in MB (0 disables)
    #[arg(short = 's', long = "size-limit", default_value_t = 10)]
    pub size_limit_mb: u64,

    /// Bypass size-limit check
    #[arg(short = 'f', long)]
    pub force: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct TouchArgs {
    /// Remote path(s) to touch (create if not exists)
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Do not create files; succeed silently if they do not exist
    #[arg(short = 'c', long = "no-create")]
    pub no_create: bool,

    /// Truncate existing files to zero length (dangerous)
    #[arg(short = 't', long = "truncate")]
    pub truncate: bool,

    /// Create parent directories when needed (filesystem providers)
    #[arg(short = 'p', long = "parents")]
    pub parents: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct TruncateArgs {
    /// Remote path(s) to truncate
    #[arg(value_name = "PATH", value_parser = parse_validated_path)]
    pub paths: Vec<String>,

    /// Target size in bytes (default: 0)
    #[arg(long = "size", value_name = "BYTES", default_value_t = 0)]
    pub size: u64,

    /// Do not create files; succeed silently if they do not exist
    #[arg(short = 'c', long = "no-create")]
    pub no_create: bool,

    /// Create parent directories when needed (filesystem providers)
    #[arg(short = 'p', long = "parents")]
    pub parents: bool,

    /// Limit total bytes written per file in MB (0 disables)
    #[arg(short = 's', long = "size-limit", default_value_t = 10)]
    pub size_limit_mb: u64,

    /// Bypass size-limit check
    #[arg(short = 'f', long)]
    pub force: bool,
}

pub async fn execute(command: &Command, ctx: &CliContext) -> Result<()> {
    let config = ctx.storage_config()?;
    let client = StorageClient::new(config.clone()).await?;

    match command {
        Command::Ls(ls_args) => {
            client
                .list_directory(&ls_args.path, ls_args.long, ls_args.recursive)
                .await?;
        }
        Command::Get(get_args) => {
            client
                .download_files(&get_args.remote, &get_args.local)
                .await?;
        }
        Command::Du(du_args) => {
            client.disk_usage(&du_args.path, du_args.summary).await?;
        }
        Command::Put(put_args) => {
            client
                .upload_files(&put_args.local, &put_args.remote, put_args.recursive)
                .await?;
        }
        Command::Rm(rm_args) => {
            if !rm_args.force {
                let prompt = ctx.prompt();
                let message = format_deletion_message(&rm_args.paths);
                let confirmed = task::block_in_place(|| {
                    Handle::current().block_on(prompt.confirm(&message, false))
                })?;

                if !confirmed {
                    println!("Operation cancelled.");
                    return Ok(());
                }
            }
            client
                .delete_files(&rm_args.paths, rm_args.recursive)
                .await?;
        }
        Command::Cp(cp_args) => {
            client
                .copy_files(&cp_args.src_path, &cp_args.dest_path)
                .await?;
        }
        Command::Mv(mv_args) => {
            client
                .move_files(&mv_args.src_path, &mv_args.dest_path)
                .await?;
        }
        Command::Mkdir(mkdir_args) => {
            client
                .create_directory(&mkdir_args.path, mkdir_args.parents)
                .await?;
        }
        Command::Cat(cat_args) => {
            client
                .cat_file(&cat_args.path, cat_args.force, cat_args.size_limit_mb)
                .await?;
        }
        Command::Head(head_args) => {
            if head_args.paths.len() <= 1 {
                let path = head_args.paths.first().ok_or_else(|| Error::InvalidPath {
                    path: "".to_string(),
                })?;
                client
                    .head_file(path, head_args.lines, head_args.bytes)
                    .await?;
            } else {
                client
                    .head_files(
                        &head_args.paths,
                        head_args.lines,
                        head_args.bytes,
                        head_args.quiet,
                        head_args.verbose,
                    )
                    .await?;
            }
        }
        Command::Tail(tail_args) => {
            if tail_args.paths.len() <= 1 {
                let path = tail_args.paths.first().ok_or_else(|| Error::InvalidPath {
                    path: "".to_string(),
                })?;
                client
                    .tail_file(path, tail_args.lines, tail_args.bytes)
                    .await?;
            } else {
                client
                    .tail_files(
                        &tail_args.paths,
                        tail_args.lines,
                        tail_args.bytes,
                        tail_args.quiet,
                        tail_args.verbose,
                    )
                    .await?;
            }
        }
        Command::Stat(stat_args) => {
            let format = if stat_args.json {
                OutputFormat::Json
            } else if stat_args.raw {
                OutputFormat::Raw
            } else {
                OutputFormat::Human
            };
            client.stat_metadata(&stat_args.path, format).await?;
        }
        Command::Grep(grep_args) => {
            client
                .grep_path(
                    &grep_args.path,
                    &grep_args.pattern,
                    grep_args.ignore_case,
                    grep_args.line_number,
                    grep_args.recursive,
                )
                .await?;
        }
        Command::Find(find_args) => {
            client.find_paths(find_args).await?;
        }
        Command::Tree(tree_args) => {
            client
                .print_tree(&tree_args.path, tree_args.depth, tree_args.dirs_only)
                .await?;
        }
        Command::Diff(diff_args) => {
            client
                .diff_files(
                    &diff_args.left,
                    &diff_args.right,
                    diff_args.context,
                    diff_args.ignore_space,
                    diff_args.size_limit_mb,
                    diff_args.force,
                )
                .await?;
        }
        Command::Touch(touch_args) => {
            if touch_args.paths.is_empty() {
                return Err(Error::InvalidArgument {
                    message: "missing PATH".to_string(),
                });
            }
            client
                .touch_files(
                    &touch_args.paths,
                    touch_args.no_create,
                    touch_args.truncate,
                    touch_args.parents,
                )
                .await?;
        }
        Command::Truncate(trunc_args) => {
            if trunc_args.paths.is_empty() {
                return Err(Error::InvalidArgument {
                    message: "missing PATH".to_string(),
                });
            }
            client
                .truncate_files(
                    &trunc_args.paths,
                    trunc_args.size,
                    trunc_args.no_create,
                    trunc_args.parents,
                    trunc_args.size_limit_mb,
                    trunc_args.force,
                )
                .await?;
        }
        Command::Config(_) => {
            unreachable!("Config commands are handled separately")
        }
    }
    Ok(())
}
