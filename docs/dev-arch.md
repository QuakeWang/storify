# Architecture and Development

Storify is built on [OpenDAL](https://github.com/apache/opendal) to provide a unified storage layer.

```mermaid
flowchart TB
    cli[Storify CLI]
    profiles["Profile Store<br/>(encrypted, per-user)"]
    loader["Config Loader<br/>(profiles + env)"]
    client["Storage Client<br/>(async I/O + commands)"]
    opendal[OpenDAL abstraction]
    subgraph providers[Storage Providers]
        oss[OSS]
        s3[S3]
        minio[MinIO]
        cos[COS]
        hdfs[HDFS]
        fs[FS]
        azb[Azblob]
    end

    cli --> profiles --> loader --> client --> opendal
    opendal --> oss
    opendal --> s3
    opendal --> minio
    opendal --> cos
    opendal --> hdfs
    opendal --> fs
    opendal --> azb
```

## Components
- Profile Store: encrypted, ownership-locked store for multiple profiles.
- Config Loader: merges profile values with environment variables (env overrides).
- Storage Client: executes HDFS-like commands with progress-aware async I/O.
- OpenDAL: provider abstraction covering OSS, S3, MinIO, COS, HDFS, FS, Azblob.

## Development
- Prerequisites: Rust 1.80+ (see `rust-toolchain.toml`), Cargo, Git.
- Build (debug): `cargo build`
- Build (release): `cargo build --release`
- Tests: `cargo test`
- Lints: `cargo clippy --all-targets --workspace -- -D warnings`
- Contributions: see `CONTRIBUTING.md`
