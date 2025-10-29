use snafu::Snafu;
use std::path::PathBuf;
use std::string::FromUtf8Error;
use toml::{de::Error as TomlDeError, ser::Error as TomlSerError};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Environment variable '{key}' is required but not found"))]
    MissingEnvVar { key: String },

    #[snafu(display("Missing required configuration field '{field}' for provider '{provider}'"))]
    MissingConfigField { provider: String, field: String },

    #[snafu(display(
        "Unsupported storage provider: {provider}. Allowed: 'oss' | 's3' | 'minio' | 'cos' | 'fs' | 'hdfs'"
    ))]
    UnsupportedProvider { provider: String },

    #[snafu(display("Path not found: {}", path.display()))]
    PathNotFound { path: PathBuf },

    #[snafu(display("Invalid path: {path}"))]
    InvalidPath { path: String },

    #[snafu(display("Cannot delete directory without -R flag: {path}"))]
    DirectoryDeletionNotRecursive { path: String },

    #[snafu(display("Use -R to upload directories"))]
    DirectoryUploadNotRecursive,

    #[snafu(display("Partial deletion failure: {} path(s) failed to delete", failed_paths.len()))]
    PartialDeletion { failed_paths: Vec<String> },

    #[snafu(display("Failed to delete '{paths}' (recursive: {recursive}): {source}"))]
    DeleteFailed {
        paths: String,
        recursive: bool,
        source: Box<Error>,
    },

    #[snafu(display("Failed to download '{remote_path}' to '{local_path}': {source}"))]
    DownloadFailed {
        remote_path: String,
        local_path: String,
        source: Box<Error>,
    },

    #[snafu(display("Failed to upload '{local_path}' to '{remote_path}': {source}"))]
    UploadFailed {
        local_path: String,
        remote_path: String,
        source: Box<Error>,
    },

    #[snafu(display("Failed to copy '{src_path}' to '{dest_path}': {source}"))]
    CopyFailed {
        src_path: String,
        dest_path: String,
        source: Box<Error>,
    },

    #[snafu(display("Failed to move '{src_path}' to '{dest_path}': {source}"))]
    MoveFailed {
        src_path: String,
        dest_path: String,
        source: Box<Error>,
    },

    #[snafu(display("Failed to list directory '{path}': {source}"))]
    ListDirectoryFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to get disk usage for '{path}': {source}"))]
    DiskUsageFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to create directory '{path}': {source}"))]
    DirectoryCreationFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to read file '{path}': {source}"))]
    CatFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to read head of file '{path}': {source}"))]
    HeadFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to read tail of file '{path}': {source}"))]
    TailFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to grep file '{path}': {source}"))]
    GrepFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to find under '{path}': {source}"))]
    FindFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to diff '{src_path}' and '{dest_path}': {source}"))]
    DiffFailed {
        src_path: String,
        dest_path: String,
        source: Box<Error>,
    },

    #[snafu(display("Failed to touch '{path}': {source}"))]
    TouchFailed { path: String, source: Box<Error> },

    #[snafu(display("Failed to truncate '{path}': {source}"))]
    TruncateFailed { path: String, source: Box<Error> },

    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    #[snafu(display("OpenDAL error: {source}"))]
    OpenDal { source: opendal::Error },

    #[snafu(display("IO error: {source}"))]
    Io { source: std::io::Error },

    #[snafu(display("JSON serialization error: {source}"))]
    Json { source: serde_json::Error },

    #[snafu(display("Failed to access profile store '{}': {source}", path.display()))]
    ProfileStoreIo {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse profile store '{}': {source}", path.display()))]
    ProfileStoreParse { path: PathBuf, source: TomlDeError },

    #[snafu(display("Failed to serialize profile store '{}': {source}", path.display()))]
    ProfileStoreSerialize { path: PathBuf, source: TomlSerError },

    #[snafu(display("Invalid UTF-8 in profile store '{}': {source}", path.display()))]
    ProfileStoreUtf8 {
        path: PathBuf,
        source: FromUtf8Error,
    },

    #[snafu(display("Profile encryption error: {message}"))]
    ProfileEncryption { message: String },

    #[snafu(display("Profile decryption error: {message}"))]
    ProfileDecryption { message: String },

    #[snafu(display(
        "Profile store '{}' is encrypted; supply a master password",
        path.display()
    ))]
    ProfileStoreLocked { path: PathBuf },

    #[snafu(display("Profile '{name}' not found"))]
    ProfileNotFound { name: String },

    #[snafu(display(
        "No configuration resolves. Available profiles: {profiles}. Hint: run `storify config` or supply --profile"
    ))]
    NoConfiguration { profiles: String },
}

impl From<opendal::Error> for Error {
    fn from(error: opendal::Error) -> Self {
        Error::OpenDal { source: error }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io { source: error }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json { source: error }
    }
}

impl Error {
    pub fn non_interactive(action: &str) -> Self {
        Error::InvalidArgument {
            message: format!(
                "{action} requires interactive input. Hint: rerun without --non-interactive or supply the needed flags (e.g. --bucket, --profile, --config-file)."
            ),
        }
    }
}
