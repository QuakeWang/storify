# Storify

A unified command-line tool for managing object storage with HDFS-like interface.

## Features

- **Multi-cloud support**: OSS, S3, MinIO, COS, HDFS, and local filesystem
- **HDFS-compatible commands**: Familiar interface for Hadoop users
- **Profile management**: Encrypted storage for multiple configurations
- **Unified configuration**: Single tool for all storage providers
- **High performance**: Async I/O with progress reporting
- **Cross-platform**: Works on Linux, macOS, and Windows

## Installation

### From Source

```bash
git clone https://github.com/QuakeWang/storify.git
cd storify
cargo build --release
```

The binary will be available at `target/release/storify`.

### From Cargo (when published)

```bash
cargo install storify
```

## Quick Start

### Using Profiles (Recommended)

Create and manage encrypted configuration profiles:

```bash
# Create a new profile interactively
storify config create myprofile

# Create with flags
storify config create prod --provider oss --bucket my-bucket

# List all profiles
storify config list

# Set default profile
storify config set myprofile
```

### Using Environment Variables

Set your storage provider and credentials:

```bash
# Choose provider: oss, s3, minio, cos, fs, hdfs or azblob
export STORAGE_PROVIDER=oss

# Common configuration
export STORAGE_BUCKET=your-bucket
export STORAGE_ACCESS_KEY_ID=your-access-key
export STORAGE_ACCESS_KEY_SECRET=your-secret-key

# Optional
export STORAGE_ENDPOINT=your-endpoint
export STORAGE_REGION=your-region
```

### Provider-Specific Variables

```bash
# OSS
OSS_BUCKET, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_REGION

# AWS S3  
AWS_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

# MinIO
MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT, MINIO_DEFAULT_REGION

# COS (Tencent Cloud)
COS_BUCKET, COS_SECRET_ID, COS_SECRET_KEY

# Filesystem
STORAGE_ROOT_PATH=./storage

# HDFS
HDFS_NAME_NODE=hdfs://namenode:8020
HDFS_ROOT_PATH=/user/data
```

**Variable Priority**: `STORAGE_*` overrides provider-specific variables (e.g., `STORAGE_BUCKET` overrides `OSS_BUCKET`).

## Usage

### Storage Operations

```bash
# List directory contents
storify ls path/to/dir
storify ls path/to/dir -L          # detailed format
storify ls path/to/dir -R          # recursive

# Download files/directories  
storify get remote/path local/path

# Upload files/directories
storify put local/path remote/path
storify put local/dir remote/dir -R # recursive

# Copy within storage
storify cp source/path dest/path

# Move/rename within storage
storify mv source/path dest/path

# Create directories
storify mkdir path/to/dir
storify mkdir -p path/to/nested/dir  # create parents

# Display file contents
storify cat path/to/file

# Display beginning of file
storify head path/to/file          # first 10 lines (default)
storify head -n 20 path/to/file    # first 20 lines
storify head -c 1024 path/to/file  # first 1024 bytes
storify head -q file1 file2        # suppress headers

# Display end of file
storify tail path/to/file          # last 10 lines (default)
storify tail -n 20 path/to/file    # last 20 lines
storify tail -c 1024 path/to/file  # last 1024 bytes
storify tail -v path/to/file       # always show header

# Search for patterns
storify grep "pattern" path/to/file       # basic search
storify grep -i "pattern" path/to/file    # case-insensitive
storify grep -n "pattern" path/to/file    # show line numbers
storify grep -R "pattern" path/           # recursive

# Find objects by name/regex/type
storify find path/ --name '**/*.log'     # glob on full path
storify find path/ --regex '.*\\.(csv|parquet)$'  # regex on full path
storify find path/ --type f                # filter by type: f|d|o

# Show disk usage
storify du path/to/dir
storify du path/to/dir -s          # summary only

# Delete files/directories
storify rm path/to/file
storify rm path/to/dir -R          # recursive
storify rm path/to/dir -Rf         # recursive + force (no confirmation)

# Show object metadata
storify stat path/to/file          # human-readable
storify stat path/to/file --json   # JSON output
storify stat path/to/file --raw    # raw key=value format
```

## Command Reference

### Storage Commands

| Command | Description | Options |
|---------|-------------|---------|
| `ls` | List directory contents | `-L` (detailed), `-R` (recursive) |
| `get` | Download files from remote | |
| `put` | Upload files to remote | `-R` (recursive) |
| `cp` | Copy files within storage | |
| `mv` | Move/rename files within storage | |
| `mkdir` | Create directories | `-p` (create parents) |
| `cat` | Display file contents | |
| `head` | Display beginning of file | `-n` (lines), `-c` (bytes), `-q` (quiet), `-v` (verbose) |
| `tail` | Display end of file | `-n` (lines), `-c` (bytes), `-q` (quiet), `-v` (verbose) |
| `grep` | Search for patterns in files | `-i` (case-insensitive), `-n` (line numbers) ,`-R` (recursive) |
| `find` | Find objects by name/regex/type | `--name <GLOB>`, `--regex <RE>`, `--type <f|d|o>` |
| `rm` | Delete files/directories | `-R` (recursive), `-f` (force) |
| `du` | Show disk usage | `-s` (summary) |
| `stat` | Show object metadata | `--json`, `--raw` |

### Config Commands

| Command | Description | Options |
|---------|-------------|---------|
| `config create` | Create/update profile | Provider-specific flags, `--anonymous`, `--make-default`, `--force` |
| `config list` | List all profiles | `--show-secrets` |
| `config show` | Show configuration | `--profile <NAME>`, `--default`, `--show-secrets` |
| `config set` | Set/clear default profile | `<NAME>` or `--clear` |
| `config delete` | Delete a profile | `<NAME>`, `--force` |

## Supported Providers

| Provider | Type | Anonymous Support |
|----------|------|-------------------|
| **OSS** | Alibaba Cloud Object Storage | ✅ Yes |
| **S3** | Amazon S3 | ✅ Yes |
| **MinIO** | Self-hosted S3-compatible | ✅ Yes |
| **COS** | Tencent Cloud Object Storage | ❌ No |
| **FS** | Local Filesystem | ✅ Yes (always) |
| **HDFS** | Hadoop Distributed File System | ❌ No |
| **Azblob** | Azure Cloud Object Storage | ❌ No |

## Architecture

Built on [OpenDAL](https://github.com/apache/opendal) for unified storage access.

```
┌─────────────────────────────────────┐
│            Storify CLI              │
├─────────────────────────────────────┤
│       Profile Store (Encrypted)     │
├─────────────────────────────────────┤
│           Config Loader             │
├─────────────────────────────────────┤
│          Storage Client             │
├─────────────────────────────────────┤
│             OpenDAL                 │
├─────────────────────────────────────┤
│ OSS │ S3 │ COS │ HDFS │ FS │ Azblob │
└─────────────────────────────────────┘
```

## Security

- **Encryption**: Profile store uses AES-256-GCM encryption
- **File Permissions**: Unix systems set 0600 permissions on profile store
- **Atomic Writes**: Configuration changes use atomic file operations
- **Backup**: Automatic backup (`.bak`) before modifications

## Development

### Prerequisites

- Rust 1.80+ (specified in `rust-toolchain.toml`)
- Cargo
- Git

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with clippy
cargo clippy --all-targets --workspace -- -D warnings
```


## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
