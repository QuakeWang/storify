use crate::config::{
    StorageProvider,
    crypto::{
        EncryptionMetadata, decrypt_field_auto, derive_master_key, encrypt_field,
        encrypt_field_with_salt, extract_salt, generate_salt, resolve_master_password,
    },
    storage_config::StorageConfig,
};
use crate::error::{Error, Result};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredProfile {
    pub provider: String,
    pub bucket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name_node: Option<String>,
    // Not serialized to file; derived at runtime based on presence of credentials
    #[serde(skip)]
    pub anonymous: bool,
}

impl StoredProfile {
    pub fn from_config(config: &StorageConfig) -> Self {
        Self {
            provider: config.provider.as_str().to_string(),
            bucket: config.bucket.clone(),
            access_key_id: config.access_key_id.clone(),
            access_key_secret: config.access_key_secret.clone(),
            endpoint: config.endpoint.clone(),
            region: config.region.clone(),
            root_path: config.root_path.clone(),
            name_node: config.name_node.clone(),
            anonymous: config.anonymous,
        }
    }

    pub fn into_config(self) -> Result<StorageConfig> {
        let provider = StorageProvider::from_str(&self.provider)?;
        let mut config = StorageConfig {
            provider,
            bucket: self.bucket,
            access_key_id: self.access_key_id,
            access_key_secret: self.access_key_secret,
            endpoint: self.endpoint,
            region: self.region,
            root_path: self.root_path,
            name_node: self.name_node,
            anonymous: self.anonymous,
        };
        crate::config::prepare_storage_config(&mut config)?;
        Ok(config)
    }
}

/// Profile store file structure (in-memory and persisted)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ProfileStoreFile {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, StoredProfile>,
}

impl ProfileStoreFile {
    fn normalize_default(&mut self) {
        let should_clear = self
            .default
            .as_ref()
            .map(|d| !self.profiles.contains_key(d))
            .unwrap_or(false);

        if should_clear {
            self.default = None;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProfileStoreOpenOptions {
    pub path: Option<PathBuf>,
    pub master_password: Option<SecretString>,
}

/// Persistent profile storage with best-effort secure defaults (XDG paths, 0600 files).
#[derive(Debug, Clone)]
pub struct ProfileStore {
    path: PathBuf,
    file: ProfileStoreFile,
    encryption: EncryptionMetadata,
}

impl ProfileStore {
    pub fn default_path() -> PathBuf {
        default_store_path()
    }

    pub fn open_default() -> Result<Self> {
        Self::open_with_options(ProfileStoreOpenOptions::default())
    }

    pub fn open(path: Option<PathBuf>) -> Result<Self> {
        Self::open_with_options(ProfileStoreOpenOptions {
            path,
            ..ProfileStoreOpenOptions::default()
        })
    }

    pub fn open_with_password(
        path: Option<PathBuf>,
        master_password: Option<SecretString>,
    ) -> Result<Self> {
        Self::open_with_options(ProfileStoreOpenOptions {
            path,
            master_password,
        })
    }

    pub fn open_with_options(options: ProfileStoreOpenOptions) -> Result<Self> {
        let path = options.path.unwrap_or_else(default_store_path);
        if path.is_dir() {
            return Err(Error::ProfileStoreIo {
                path,
                source: std::io::Error::other("profile store path points to a directory"),
            });
        }

        let (file, encryption) = if path.exists() {
            Self::read_file(&path, options.master_password.as_ref())?
        } else {
            let password = resolve_master_password(options.master_password.clone(), &path);
            let salt = generate_salt();
            let key = derive_master_key(&password, &salt)?;
            let encryption = EncryptionMetadata::new(key, salt.to_vec());
            (ProfileStoreFile::default(), encryption)
        };

        Ok(Self {
            path,
            file,
            encryption,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn profile(&self, name: &str) -> Option<&StoredProfile> {
        self.file.profiles.get(name)
    }

    /// Get a cloned profile by name (returns error if not found)
    pub fn get_profile(&self, name: &str) -> Result<StoredProfile> {
        self.profile(name)
            .cloned()
            .ok_or_else(|| Error::ProfileNotFound {
                name: name.to_string(),
            })
    }

    pub fn available_profiles(&self) -> Vec<String> {
        let mut names: Vec<String> = self.file.profiles.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn default_profile(&self) -> Option<&str> {
        self.file.default.as_deref()
    }

    pub fn save_profile(
        &mut self,
        name: String,
        profile: StoredProfile,
        make_default: bool,
    ) -> Result<()> {
        if make_default {
            self.file.default = Some(name.clone());
        }
        self.file.profiles.insert(name, profile);
        self.persist()
    }

    /// Delete a profile (returns error if not found)
    pub fn delete_profile(&mut self, name: &str) -> Result<()> {
        self.file.profiles.remove(name).ok_or_else(|| {
            Error::ProfileNotFound {
                name: name.to_string(),
            }
        })?;

        // Clear default if deleting the default profile
        if self.file.default.as_deref() == Some(name) {
            self.file.default = None;
        }

        self.persist()
    }

    pub fn set_default_profile(&mut self, name: Option<String>) -> Result<()> {
        if let Some(ref candidate) = name
            && !self.file.profiles.contains_key(candidate)
        {
            return Err(Error::ProfileNotFound {
                name: candidate.clone(),
            });
        }
        self.file.default = name;
        self.persist()
    }

    /// Re-derive encryption key (for key rotation or master password change)
    pub fn set_encryption(&mut self, master_password: Option<SecretString>) -> Result<()> {
        let password = resolve_master_password(master_password, &self.path);
        let salt = generate_salt();
        let key = derive_master_key(&password, &salt)?;
        self.encryption = EncryptionMetadata::new(key, salt.to_vec());
        self.persist()
    }

    fn persist(&mut self) -> Result<()> {
        let mut payload = self.file.clone();
        payload.normalize_default();

        let key = self.encryption.key();
        let salt = self.encryption.salt();

        encrypt_all_profiles(&mut payload.profiles, key, salt)?;

        let serialized =
            toml::to_string_pretty(&payload).map_err(|source| Error::ProfileStoreSerialize {
                path: self.path.clone(),
                source,
            })?;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::ProfileStoreIo {
                path: parent.to_path_buf(),
                source,
            })?;
            #[cfg(unix)]
            {
                let perms = fs::Permissions::from_mode(0o700);
                fs::set_permissions(parent, perms).ok();
            }
        }

        write_atomic(&self.path, serialized.as_bytes())?;
        Ok(())
    }

    /// Read config file (field-level encryption)
    fn read_file(
        path: &Path,
        master_password: Option<&SecretString>,
    ) -> Result<(ProfileStoreFile, EncryptionMetadata)> {
        // Resolve password once at the beginning
        let password = resolve_master_password(master_password.cloned(), path);

        let raw = fs::read(path).map_err(|source| Error::ProfileStoreIo {
            path: path.to_path_buf(),
            source,
        })?;

        // Empty file: initialize with encryption
        if raw.is_empty() {
            let salt = generate_salt();
            let key = derive_master_key(&password, &salt)?;
            let encryption = EncryptionMetadata::new(key, salt.to_vec());
            return Ok((ProfileStoreFile::default(), encryption));
        }

        let text = String::from_utf8(raw).map_err(|source| Error::ProfileStoreUtf8 {
            path: path.to_path_buf(),
            source,
        })?;

        let mut file: ProfileStoreFile =
            toml::from_str(&text).map_err(|source| Error::ProfileStoreParse {
                path: path.to_path_buf(),
                source,
            })?;

        let salt = extract_salt_from_profiles(&file.profiles)?;

        let key = derive_master_key(&password, &salt)?;

        for profile in file.profiles.values_mut() {
            decrypt_sensitive_field(&mut profile.access_key_id, &key)?;
            decrypt_sensitive_field(&mut profile.access_key_secret, &key)?;
        }

        file.normalize_default();
        let metadata = EncryptionMetadata::new(key, salt);
        Ok((file, metadata))
    }
}

/// Extract salt from profile store (searches all profiles)
fn extract_salt_from_profiles(profiles: &BTreeMap<String, StoredProfile>) -> Result<Vec<u8>> {
    for profile in profiles.values() {
        if let Some(encrypted_ak) = &profile.access_key_id
            && let Some(salt) = extract_salt(encrypted_ak)?
        {
            return Ok(salt);
        }
        if let Some(encrypted_sk) = &profile.access_key_secret
            && let Some(salt) = extract_salt(encrypted_sk)?
        {
            return Ok(salt);
        }
    }

    Err(Error::ProfileDecryption {
        message:
            "no encrypted fields found with embedded salt (expected format: ENC:v1:salt:ciphertext)"
                .into(),
    })
}

/// Encrypt all profiles' sensitive fields
fn encrypt_all_profiles(
    profiles: &mut BTreeMap<String, StoredProfile>,
    key: &[u8; 32],
    salt: &[u8],
) -> Result<()> {
    if profiles.is_empty() {
        return Ok(());
    }

    let mut salt_embedded = false;

    for profile in profiles.values_mut() {
        // Encrypt access_key_id (embed salt if first field)
        if let Some(value) = &profile.access_key_id {
            profile.access_key_id = Some(if !salt_embedded {
                salt_embedded = true;
                encrypt_field_with_salt(value, key, salt)?
            } else {
                encrypt_field(value, key)?
            });
        }

        // Encrypt access_key_secret (embed salt if first field)
        if let Some(value) = &profile.access_key_secret {
            profile.access_key_secret = Some(if !salt_embedded {
                salt_embedded = true;
                encrypt_field_with_salt(value, key, salt)?
            } else {
                encrypt_field(value, key)?
            });
        }
    }

    Ok(())
}

/// Decrypt sensitive field (automatically handles all formats)
fn decrypt_sensitive_field(field: &mut Option<String>, key: &[u8; 32]) -> Result<()> {
    if let Some(encrypted) = field {
        *field = decrypt_field_auto(encrypted, key)?;
    }
    Ok(())
}

/// RAII guard for temporary files (auto-cleanup on drop)
struct TempFile {
    path: PathBuf,
    should_cleanup: bool,
}

impl TempFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            should_cleanup: true,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Mark file as persistent (don't cleanup on drop)
    fn keep(mut self) {
        self.should_cleanup = false;
        // Drop will not trigger cleanup
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.should_cleanup && self.path.exists() {
            // Best-effort cleanup (ignore errors)
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Get default profile store path
fn default_store_path() -> PathBuf {
    const ENV_VARS: &[&str] = &["STORIFY_PROFILE_PATH", "STORIFY_CONFIG"];

    ENV_VARS
        .iter()
        .find_map(|&var| env::var(var).ok().map(PathBuf::from))
        .or_else(|| {
            directories::BaseDirs::new().map(|base_dirs| {
                base_dirs
                    .home_dir()
                    .join(".config")
                    .join("storify")
                    .join("profiles.toml")
            })
        })
        .unwrap_or_else(|| {
            // Fallback to current directory
            env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("storify-profiles.toml")
        })
}

fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let tmp_path = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("storify"),
        Uuid::new_v4().simple()
    ));

    let temp_file = TempFile::new(tmp_path);

    write_atomic_inner(path, temp_file.path(), data)?;

    temp_file.keep();
    Ok(())
}

fn write_atomic_inner(path: &Path, tmp_path: &Path, data: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);

    // Set permissions atomically on creation (Unix only)
    #[cfg(unix)]
    {
        options.mode(0o600);
    }

    let mut file = options
        .open(tmp_path)
        .map_err(|source| Error::ProfileStoreIo {
            path: tmp_path.to_path_buf(),
            source,
        })?;

    file.write_all(data)
        .map_err(|source| Error::ProfileStoreIo {
            path: tmp_path.to_path_buf(),
            source,
        })?;

    file.sync_all().map_err(|source| Error::ProfileStoreIo {
        path: tmp_path.to_path_buf(),
        source,
    })?;

    drop(file);

    // Backup existing file with proper permissions
    if path.exists() {
        let backup = backup_path(path);
        fs::copy(path, &backup).map_err(|source| Error::ProfileStoreIo {
            path: backup.clone(),
            source,
        })?;

        #[cfg(unix)]
        {
            let perms = fs::Permissions::from_mode(0o600);
            fs::set_permissions(&backup, perms).map_err(|source| Error::ProfileStoreIo {
                path: backup,
                source,
            })?;
        }
    }

    // Atomic rename
    fs::rename(tmp_path, path).map_err(|source| Error::ProfileStoreIo {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(())
}

fn backup_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("profiles.toml");
    path.with_file_name(format!("{file_name}.bak"))
}
