# Storify

Storify is a fast, convenient CLI for cloud object storage—profile-driven configs (encrypted and easy to override), portable commands that behave the same everywhere, and high-throughput async transfers with live progress so switching providers and moving data stays smooth.

## Features

- **Multi-cloud support**: OSS, S3, MinIO, COS, HDFS, and local filesystem
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

### From Cargo

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

## Common commands

```bash
# List directory contents
storify ls path/to/dir

# Download files/directories  
storify get remote/path local/path

# Upload directory recursively
storify put -R local/dir remote/dir

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

# Diff two files (unified diff)
storify diff left/file right/file          # unified diff with 3 lines context
storify diff -U 1 left/file right/file     # set context lines
storify diff -w left/file right/file       # ignore trailing whitespace
storify diff --size-limit 1 -f left right  # size guard and force

# Find objects by name/regex/type
storify find path/ --name '**/*.log'     # glob on full path
storify find path/ --regex '.*\\.(csv|parquet)$'  # regex on full path
storify find path/ --type f                # filter by type: f|d|o

# Show directory structure as a tree
storify tree path/to/dir              # show full tree
storify tree path/to/dir -d 1         # limit depth to 1
storify tree path/to/dir --dirs-only  # show directories only

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

# Create files (touch)
storify touch path/to/file                 # create if missing; no change if exists
storify touch -t path/to/file              # truncate to 0 bytes if exists
storify touch -c path/to/missing           # do not create; succeed silently
storify touch -p path/to/nested/file       # create parents when applicable

# Append data to file
storify append local.txt path/to/file            # alias: local first, dest second
storify append path/to/file --src local.txt      # append local file to remote file (canonical)
echo "line" | storify append path/to/file --stdin  # append from stdin
storify append path/to/file --src local.txt -c     # fail if missing (no-create)
```

## Command Reference

### Storage Commands

| Command | Description | Options |
|---------|-------------|---------|
| `ls` | List directory contents | `-L` (detailed), `-R` (recursive) |
| `get` | Download files from remote |
| `put` | Upload files to remote | `-R` (recursive) |
| `cp` | Copy files within storage |
| `mv` | Move/rename files within storage | 
| `mkdir` | Create directories | `-p` (create parents) |
| `touch` | Create files |
| `append` | Append data to a remote file | `--src <PATH>` or `--stdin`, `-c` (no-create), `-p` (parents) |
| `cat` | Display file contents |
| `head` | Display beginning of file | `-n` (lines), `-c` (bytes), `-q` (quiet), `-v` (verbose) |
| `tail` | Display end of file | `-n` (lines), `-c` (bytes), `-q` (quiet), `-v` (verbose) |
| `grep` | Search for patterns in files | `-i` (case-insensitive), `-n` (line numbers) ,`-R` (recursive) |
| `find` | Find objects by name/regex/type | `--name <GLOB>`, `--regex <RE>`, `--type <f|d|o>` |
| `rm` | Delete files/directories | `-R` (recursive), `-f` (force) |
| `tree` | View directory structure as a tree | `-d <DEPTH>`, `--dirs-only` |
| `du` | Show disk usage | `-s` (summary) |
| `stat` | Show object metadata | `--json`, `--raw` |
| `diff` | Compare two files (unified diff) | `-U <N>` (context), `-w` (ignore-space), `--size-limit <MB>`, `-f` (force) |

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

More commands: see [`docs/usage.md`](docs/usage.md).

## Documentation

- Usage guide: [`docs/usage.md`](docs/usage.md)
- Configuration and providers: [`docs/config-providers.md`](docs/config-providers.md)
- Architecture and development: [`docs/dev-arch.md`](docs/dev-arch.md)
- FAQ: [`docs/faq.md`](docs/faq.md)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
