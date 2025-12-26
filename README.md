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

### Temporary Config Cache (TTL)

Use an encrypted, TTL-based temporary cache when you don't want to create a named profile:

```bash
# Set a temporary config (default TTL 24h)
storify config create --temp --provider cos --bucket my-bucket

# Clear temporary config
storify config temp clear
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
```

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
