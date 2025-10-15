use clap::{ArgGroup, Args as ClapArgs, Parser, Subcommand};

use crate::error::Result;

use super::{
    config,
    context::CliContext,
    prompts::Prompt,
    storage::{
        self, CatArgs, CpArgs, DuArgs, GetArgs, GrepArgs, HeadArgs, LsArgs, MkdirArgs, MvArgs,
        PutArgs, RmArgs, StatArgs, TailArgs,
    },
};

#[derive(Parser, Debug, Clone)]
#[command(
    version = env!("CARGO_PKG_VERSION"),
    author = "WangErxi",
    about = "A unified tool for managing object storage (OSS, S3, etc.)",
    after_help = "Enjoy the unified experience!"
)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalOptions,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(ClapArgs, Debug, Clone, Default)]
pub struct GlobalOptions {
    /// Disable interactive prompts (fail fast when input is required)
    #[arg(long)]
    pub non_interactive: bool,
    /// Preferred profile name when resolving configuration
    #[arg(short = 'p', long = "profile", value_name = "NAME")]
    pub profile: Option<String>,

    /// Override the profile store path
    #[arg(long = "profile-store", value_name = "PATH")]
    pub profile_store: Option<std::path::PathBuf>,
    /// Master password for encrypted profile store
    #[arg(long = "master-password", value_name = "PASS", hide_env_values = true)]
    pub master_password: Option<String>,
    /// Environment variable used to read master password (empty to disable)
    #[arg(
        long = "profile-pass-env",
        value_name = "ENV",
        default_value = "STORIFY_PROFILE_PASS"
    )]
    pub profile_pass_env: String,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Inspect Storify configuration
    #[command(subcommand)]
    Config(ConfigCommand),

    /// List directory contents
    Ls(LsArgs),
    /// Download files from remote to local
    Get(GetArgs),
    /// Show disk usage statistics
    Du(DuArgs),
    /// Upload files from local to remote
    Put(PutArgs),
    /// Remove files/directories from remote storage
    Rm(RmArgs),
    /// Copy files/directories from remote to remote
    Cp(CpArgs),
    /// Move files/directories from remote to remote
    Mv(MvArgs),
    /// Create directories in remote storage
    Mkdir(MkdirArgs),
    /// Display object metadata
    Stat(StatArgs),
    /// Display file contents
    Cat(CatArgs),
    /// Display beginning of file contents
    Head(HeadArgs),
    /// Display end of file contents
    Tail(TailArgs),
    /// Search for patterns in files
    Grep(GrepArgs),
    /// Find objects by name/regex/type
    Find(super::storage::FindArgs),
}

#[derive(Subcommand, Debug, Clone)]
pub enum ConfigCommand {
    /// Show configuration information
    Show(ShowArgs),
    /// Create or update a profile in the profile store
    Create(CreateArgs),
    /// Mutate configuration settings (e.g. default profile)
    Set(SetArgs),
    /// List profiles in the profile store
    List(ListArgs),
    /// Delete a profile from the profile store
    Delete(DeleteArgs),
}

#[derive(ClapArgs, Debug, Clone)]
#[command(group = ArgGroup::new("show_target").args(["profile", "default"]).multiple(false))]
pub struct ShowArgs {
    /// Show a stored profile by name
    #[arg(long)]
    pub profile: Option<String>,
    /// Show the default profile from the profile store (ignoring environment variables)
    #[arg(long)]
    pub default: bool,
    /// Show secrets in plaintext (access_key_id, access_key_secret). Default: redacted
    #[arg(long)]
    pub show_secrets: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct CreateArgs {
    /// Profile name to create or update
    #[arg(value_name = "NAME")]
    pub name: Option<String>,
    /// Storage provider (oss|s3|minio|cos|fs|hdfs)
    #[arg(long, value_name = "PROVIDER")]
    pub provider: Option<String>,
    /// Bucket name (cloud providers)
    #[arg(long)]
    pub bucket: Option<String>,
    /// Access key id / secret id
    #[arg(long = "access-key-id")]
    pub access_key_id: Option<String>,
    /// Access key secret / secret key
    #[arg(long = "access-key-secret")]
    pub access_key_secret: Option<String>,
    /// Endpoint override
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Region setting (S3/OSS)
    #[arg(long)]
    pub region: Option<String>,
    /// Local/HDFS root path
    #[arg(long = "root-path")]
    pub root_path: Option<String>,
    /// HDFS name node address
    #[arg(long = "name-node")]
    pub name_node: Option<String>,
    /// Allow anonymous access when supported
    #[arg(long)]
    pub anonymous: bool,
    /// Overwrite existing profile without prompting
    #[arg(long)]
    pub force: bool,
    /// Mark the profile as default after creation
    #[arg(long = "make-default")]
    pub make_default: bool,
}

#[derive(ClapArgs, Debug, Clone)]
#[command(group = ArgGroup::new("default_target").required(true).args(["name", "clear"]))]
pub struct SetArgs {
    /// Profile name to mark as default
    #[arg(value_name = "NAME")]
    pub name: Option<String>,
    /// Clear the default profile setting
    #[arg(long)]
    pub clear: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct ListArgs {
    /// Show secrets in plaintext (access_key_id, access_key_secret). Default: redacted
    #[arg(long)]
    pub show_secrets: bool,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct DeleteArgs {
    /// Profile name to delete (optional, will prompt if not provided)
    #[arg(value_name = "NAME")]
    pub name: Option<String>,
    /// Skip confirmation prompt
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: Args) -> Result<()> {
    run_with_prompt(args, None).await
}

pub async fn run_with_prompt(args: Args, prompt: Option<Prompt>) -> Result<()> {
    let prompt = prompt.unwrap_or_else(|| Prompt::new(!args.global.non_interactive));

    let ctx = CliContext::from_args(args, prompt).await?;
    match ctx.command() {
        Command::Config(cmd) => config::execute(cmd, &ctx),
        storage_cmd => storage::execute(storage_cmd, &ctx).await,
    }
}
