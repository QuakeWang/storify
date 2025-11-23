# Configuration and Providers

Storify supports two configuration styles: encrypted profiles and environment variables. Environment variables always win over stored profile values.

## Profiles (recommended)
- Create interactively: `storify config create myprofile`
- Create with flags: `storify config create prod --provider oss --bucket my-bucket`
- List profiles: `storify config list`
- Set default: `storify config set myprofile`
- Show config: `storify config show --profile myprofile`
- Delete: `storify config delete myprofile`

## Environment variables
- Choose provider: `STORAGE_PROVIDER` (`oss`, `s3`, `minio`, `cos`, `fs`, `hdfs`, `azblob`)
- Common variables:
  - `STORAGE_BUCKET`
  - `STORAGE_ACCESS_KEY_ID`
  - `STORAGE_ACCESS_KEY_SECRET`
  - Optional: `STORAGE_ENDPOINT`, `STORAGE_REGION`
- Precedence: `STORAGE_*` overrides provider-specific variables (for example `STORAGE_BUCKET` overrides `OSS_BUCKET`).

### Provider-specific variables
- OSS: `OSS_BUCKET`, `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ENDPOINT`, `OSS_REGION`
- AWS S3: `AWS_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- MinIO: `MINIO_BUCKET`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ENDPOINT`, `MINIO_DEFAULT_REGION`
- COS: `COS_BUCKET`, `COS_SECRET_ID`, `COS_SECRET_KEY`
- Filesystem: `STORAGE_ROOT_PATH=./storage`
- HDFS: `HDFS_NAME_NODE`, `HDFS_ROOT_PATH`

### Anonymous support
- OSS, S3, MinIO, FS: Yes (supported)
- COS, HDFS, Azblob: No (not supported)

## Security
- Profile store is encrypted with AES-256-GCM.
- On Unix, profile store permissions are set to 0600.
- Writes are atomic; a `.bak` backup is created before modifying the store.
