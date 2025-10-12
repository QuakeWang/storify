use crate::error::Error;
use std::str::FromStr;

/// Storage provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    Oss,
    S3,
    Cos,
    Fs,
    Hdfs,
    Azblob,
}

impl FromStr for StorageProvider {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "oss" => Ok(Self::Oss),
            "s3" | "minio" => Ok(Self::S3),
            "cos" => Ok(Self::Cos),
            "fs" => Ok(Self::Fs),
            "hdfs" => Ok(Self::Hdfs),
            "azblob" => Ok(Self::Azblob),
            _ => Err(Error::UnsupportedProvider {
                provider: s.to_string(),
            }),
        }
    }
}

impl StorageProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageProvider::Oss => "oss",
            StorageProvider::S3 => "s3",
            StorageProvider::Cos => "cos",
            StorageProvider::Fs => "fs",
            StorageProvider::Hdfs => "hdfs",
            StorageProvider::Azblob => "azblob",
        }
    }
}
