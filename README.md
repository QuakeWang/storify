# Ossify

A unified command-line tool for managing object storage with HDFS-like interface.

## Features

- **Multi-cloud support**: OSS, S3, MinIO, and local filesystem
- **HDFS-compatible commands**: Familiar interface for Hadoop users
- **Unified configuration**: Single tool for all storage providers
- **High performance**: Async I/O with progress reporting

## Installation

```bash
git clone https://github.com/QuakeWang/ossify.git
cd ossify
cargo build --release
```

## Configuration

Set your storage provider and credentials:

```bash
# Choose provider: oss, s3, minio, or fs
export STORAGE_PROVIDER=oss

# Common configuration
export STORAGE_BUCKET=your-bucket
export STORAGE_ACCESS_KEY_ID=your-access-key
export STORAGE_ACCESS_KEY_SECRET=your-secret-key

# Optional
export STORAGE_ENDPOINT=your-endpoint
export STORAGE_REGION=your-region
```

### Provider-specific variables (legacy support)

```bash
# OSS
OSS_BUCKET, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET

# AWS S3  
AWS_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# MinIO
MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

# Filesystem
STORAGE_ROOT_PATH=./storage
```

## Usage

```bash
# List directory contents
ossify ls path/to/dir
ossify ls path/to/dir -L          # detailed format
ossify ls path/to/dir -R          # recursive

# Download files/directories  
ossify get remote/path local/path

# Upload files/directories
ossify put local/path remote/path
ossify put local/dir remote/dir -R # recursive

# Copy within storage
ossify cp source/path dest/path

# Show disk usage
ossify du path/to/dir
ossify du path/to/dir -s          # summary only

# Delete files/directories
ossify rm path/to/file
ossify rm path/to/dir -R          # recursive
```

## Command Reference

| Command | Description |
|---------|-------------|
| `ls` | List directory contents |
| `get` | Download files from remote |
| `put` | Upload files to remote |
| `cp` | Copy files within storage |
| `rm` | Delete files/directories |
| `du` | Show disk usage |

## Architecture

Built on [OpenDAL](https://github.com/apache/opendal) for unified storage access.

```
┌─────────────────┐
│   Ossify CLI    │
├─────────────────┤
│ Storage Client  │
├─────────────────┤
│    OpenDAL      │
├─────────────────┤
│ OSS │ S3 │ MinIO│
└─────────────────┘
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.