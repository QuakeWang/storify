use crate::config::{StorageProvider, storage_config::StorageConfig};
use crate::error::{Error, Result};
use argon2::{Algorithm, Argon2, Params as Argon2Params, Version};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use directories::ProjectDirs;
use rand::{RngCore, rng};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
#[cfg(not(unix))]
use std::sync::Once;

const VERSION: u8 = 1;
const FILE_NAME: &str = "profiles.toml";
const BACKUP_SUFFIX: &str = "bak";
const KEY_LENGTH: usize = 32;
const ARGON2_ALGORITHM: &str = "ARGON2ID";
const DEFAULT_ARGON2_MEMORY_KIB: u32 = 64 * 1024;
const DEFAULT_ARGON2_ITERATIONS: u32 = 3;
const DEFAULT_ARGON2_LANES: u32 = 1;

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
    #[serde(default)]
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

    pub fn redacted(&self) -> Self {
        let mut clone = self.clone();
        if clone.access_key_id.is_some() {
            clone.access_key_id = Some("****".to_string());
        }
        if clone.access_key_secret.is_some() {
            clone.access_key_secret = Some("****".to_string());
        }
        clone
    }
}

#[derive(Debug, Clone)]
pub struct ProfileStore {
    path: PathBuf,
    encryption: Encryption,
}

#[derive(Debug, Clone)]
enum Encryption {
    Plaintext,
    MasterPassword(MasterPasswordEncryption),
}

#[derive(Debug, Clone)]
struct MasterPasswordEncryption {
    password: SecretString,
}

#[derive(Debug, Clone)]
struct KdfParams {
    memory_kib: u32,
    iterations: u32,
    lanes: u32,
}

impl KdfParams {
    fn default_argon2() -> Self {
        Self {
            memory_kib: DEFAULT_ARGON2_MEMORY_KIB,
            iterations: DEFAULT_ARGON2_ITERATIONS,
            lanes: DEFAULT_ARGON2_LANES,
        }
    }

    fn to_info(&self) -> KdfInfo {
        KdfInfo {
            algorithm: ARGON2_ALGORITHM.to_string(),
            memory_kib: self.memory_kib,
            iterations: self.iterations,
            lanes: self.lanes,
        }
    }

    fn parse(info: KdfInfo) -> Result<Self> {
        if info.algorithm.eq_ignore_ascii_case(ARGON2_ALGORITHM) {
            Ok(Self {
                memory_kib: info.memory_kib,
                iterations: info.iterations,
                lanes: info.lanes,
            })
        } else {
            Err(Error::ProfileDecryption {
                message: format!("unsupported kdf: {}", info.algorithm),
            })
        }
    }
}

#[derive(Default)]
struct ProfilesState {
    default_profile: Option<String>,
    profiles: BTreeMap<String, StoredProfile>,
}

#[derive(Serialize, Deserialize)]
struct PlainProfilesFile {
    version: u8,
    default_profile: Option<String>,
    profiles: BTreeMap<String, StoredProfile>,
}

#[derive(Serialize, Deserialize)]
struct EncryptedProfilesFile {
    version: u8,
    encryption: EncryptedPayload,
}

#[derive(Serialize, Deserialize)]
struct EncryptedPayload {
    algorithm: String,
    kdf: KdfInfo,
    salt: String,
    nonce: String,
    data: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct KdfInfo {
    algorithm: String,
    memory_kib: u32,
    iterations: u32,
    lanes: u32,
}

impl ProfileStore {
    pub fn open_default(password: Option<String>) -> Result<Self> {
        let path = default_profiles_path()?;
        Ok(Self::with_path(path, password))
    }

    pub fn with_path(path: PathBuf, password: Option<String>) -> Self {
        Self {
            path,
            encryption: Encryption::from_password(password),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self, name: &str) -> Result<Option<StoredProfile>> {
        let state = self.read_state()?;
        Ok(state.profiles.get(name).cloned())
    }

    pub fn save(&self, name: &str, profile: StoredProfile, set_default: bool) -> Result<()> {
        let mut state = self.read_state()?;
        state.profiles.insert(name.to_string(), profile);
        if set_default {
            state.default_profile = Some(name.to_string());
        }
        self.write_state(&state)
    }

    pub fn remove(&self, name: &str) -> Result<()> {
        let mut state = self.read_state()?;
        state.profiles.remove(name);
        if state
            .default_profile
            .as_ref()
            .map(|default| default == name)
            .unwrap_or(false)
        {
            state.default_profile = None;
        }
        self.write_state(&state)
    }

    pub fn list_profiles(&self) -> Result<Vec<String>> {
        let state = self.read_state()?;
        Ok(state.profiles.keys().cloned().collect())
    }

    pub fn list_profiles_redacted(&self) -> Result<Vec<(String, StoredProfile)>> {
        let state = self.read_state()?;
        Ok(state
            .profiles
            .iter()
            .map(|(name, p)| (name.clone(), p.redacted()))
            .collect())
    }

    pub fn default_profile(&self) -> Result<Option<String>> {
        let state = self.read_state()?;
        Ok(state.default_profile)
    }

    pub fn set_default_profile(&self, profile: Option<&str>) -> Result<()> {
        let mut state = self.read_state()?;
        state.default_profile = profile.map(|p| p.to_string());
        self.write_state(&state)
    }

    fn read_state(&self) -> Result<ProfilesState> {
        if !self.path.exists() {
            return Ok(ProfilesState::default());
        }
        let bytes = fs::read(&self.path).map_err(|source| Error::ProfileStoreIo {
            path: self.path.clone(),
            source,
        })?;
        let content = String::from_utf8(bytes).map_err(|err| Error::ProfileStoreUtf8 {
            path: self.path.clone(),
            source: err,
        })?;

        if content.trim().is_empty() {
            return Ok(ProfilesState::default());
        }

        if let Some(state) = self.try_read_encrypted(&content)? {
            return Ok(state);
        }

        let file = self.decode_plain_profiles(&content)?;
        Ok(ProfilesState {
            default_profile: file.default_profile,
            profiles: file.profiles,
        })
    }

    fn write_state(&self, state: &ProfilesState) -> Result<()> {
        let plaintext = PlainProfilesFile {
            version: VERSION,
            default_profile: state.default_profile.clone(),
            profiles: state.profiles.clone(),
        };

        ensure_dir(self.path.parent())?;
        let serialized = match &self.encryption {
            Encryption::Plaintext => toml::to_string_pretty(&plaintext).map_err(|source| {
                Error::ProfileStoreSerialize {
                    path: self.path.clone(),
                    source,
                }
            })?,
            Encryption::MasterPassword(strategy) => {
                let plain =
                    toml::to_string(&plaintext).map_err(|source| Error::ProfileStoreSerialize {
                        path: self.path.clone(),
                        source,
                    })?;
                let encrypted = strategy.encrypt(plain.as_bytes())?;
                let file = EncryptedProfilesFile {
                    version: VERSION,
                    encryption: encrypted,
                };
                toml::to_string_pretty(&file).map_err(|source| Error::ProfileStoreSerialize {
                    path: self.path.clone(),
                    source,
                })?
            }
        };

        if self.path.exists() {
            let backup = self.path.with_extension(BACKUP_SUFFIX);
            fs::copy(&self.path, &backup).map_err(|source| Error::ProfileStoreIo {
                path: backup.clone(),
                source,
            })?;
            set_secure_permissions(&backup)?;
        }

        let tmp_path = self.path.with_extension("tmp");
        let write_result = (|| -> Result<()> {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|source| Error::ProfileStoreIo {
                    path: tmp_path.clone(),
                    source,
                })?;
            tmp_file
                .write_all(serialized.as_bytes())
                .map_err(|source| Error::ProfileStoreIo {
                    path: tmp_path.clone(),
                    source,
                })?;
            tmp_file
                .sync_all()
                .map_err(|source| Error::ProfileStoreIo {
                    path: tmp_path.clone(),
                    source,
                })?;
            Ok(())
        })();

        if let Err(err) = write_result {
            fs::remove_file(&tmp_path).ok();
            return Err(err);
        }

        set_secure_permissions(&tmp_path)?;
        fs::rename(&tmp_path, &self.path).map_err(|source| Error::ProfileStoreIo {
            path: self.path.clone(),
            source,
        })?;
        set_secure_permissions(&self.path)?;
        Ok(())
    }

    fn try_read_encrypted(&self, content: &str) -> Result<Option<ProfilesState>> {
        if let Ok(encrypted) = toml::from_str::<EncryptedProfilesFile>(content) {
            return self.decrypt_state(encrypted).map(Some);
        }
        Ok(None)
    }

    fn decrypt_state(&self, encrypted: EncryptedProfilesFile) -> Result<ProfilesState> {
        if encrypted.encryption.algorithm != MasterPasswordEncryption::ALGORITHM {
            return Err(Error::ProfileDecryption {
                message: "Unsupported encryption algorithm".to_string(),
            });
        }
        let plain = self.encryption.decrypt(&encrypted, &self.path)?;
        let file = self.decode_plain_profiles(&plain)?;
        Ok(ProfilesState {
            default_profile: file.default_profile,
            profiles: file.profiles,
        })
    }

    fn decode_plain_profiles(&self, plain: &str) -> Result<PlainProfilesFile> {
        toml::from_str::<PlainProfilesFile>(plain)
            .map_err(|source| Error::ProfileStoreParse {
                path: self.path.clone(),
                source,
            })
            .and_then(|file| {
                ensure_version(file.version)?;
                Ok(file)
            })
    }
}

impl Encryption {
    fn from_password(password: Option<String>) -> Self {
        match password {
            Some(pwd) if !pwd.is_empty() => Encryption::MasterPassword(MasterPasswordEncryption {
                password: SecretString::new(pwd.into_boxed_str()),
            }),
            _ => Encryption::Plaintext,
        }
    }

    fn decrypt(&self, file: &EncryptedProfilesFile, path: &Path) -> Result<String> {
        match self {
            Encryption::MasterPassword(master) => master.decrypt(&file.encryption, path),
            Encryption::Plaintext => Err(Error::ProfileDecryption {
                message: "profiles are encrypted, provide a master password".to_string(),
            }),
        }
    }
}

impl MasterPasswordEncryption {
    const ALGORITHM: &'static str = "CHACHA20POLY1305";

    fn encrypt(&self, data: &[u8]) -> Result<EncryptedPayload> {
        let mut rng = rng();
        let mut salt = [0u8; 16];
        rng.fill_bytes(&mut salt);
        let params = KdfParams::default_argon2();
        let key = self.derive_key(&params, &salt)?;

        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
        let cipher = ChaCha20Poly1305::new(&key);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, data)
            .map_err(|_| Error::ProfileEncryption {
                message: "encryption failed".to_string(),
            })?;

        Ok(EncryptedPayload {
            algorithm: Self::ALGORITHM.to_string(),
            kdf: params.to_info(),
            salt: BASE64.encode(salt),
            nonce: BASE64.encode(nonce_bytes),
            data: BASE64.encode(ciphertext),
        })
    }

    fn decrypt(&self, payload: &EncryptedPayload, path: &Path) -> Result<String> {
        if payload.algorithm != Self::ALGORITHM {
            return Err(Error::ProfileDecryption {
                message: "unsupported encryption algorithm".to_string(),
            });
        }
        let params = KdfParams::parse(payload.kdf.clone())?;
        let salt =
            BASE64
                .decode(payload.salt.as_bytes())
                .map_err(|_| Error::ProfileDecryption {
                    message: "invalid salt".to_string(),
                })?;
        let nonce_bytes =
            BASE64
                .decode(payload.nonce.as_bytes())
                .map_err(|_| Error::ProfileDecryption {
                    message: "invalid nonce".to_string(),
                })?;
        let ciphertext =
            BASE64
                .decode(payload.data.as_bytes())
                .map_err(|_| Error::ProfileDecryption {
                    message: "invalid ciphertext".to_string(),
                })?;
        let key = self.derive_key(&params, &salt)?;
        let cipher = ChaCha20Poly1305::new(&key);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let plaintext =
            cipher
                .decrypt(nonce, ciphertext.as_ref())
                .map_err(|_| Error::ProfileDecryption {
                    message: "unable to decrypt profiles".to_string(),
                })?;
        String::from_utf8(plaintext).map_err(|err| Error::ProfileStoreUtf8 {
            path: path.to_path_buf(),
            source: err,
        })
    }

    fn derive_key(&self, params: &KdfParams, salt: &[u8]) -> Result<Key> {
        let argon_params = Argon2Params::new(
            params.memory_kib,
            params.iterations,
            params.lanes,
            Some(KEY_LENGTH),
        )
        .map_err(|_| Error::ProfileEncryption {
            message: "invalid argon2 parameters".to_string(),
        })?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, argon_params);
        let mut key_bytes = vec![0u8; KEY_LENGTH];
        argon2
            .hash_password_into(
                self.password.expose_secret().as_bytes(),
                salt,
                &mut key_bytes,
            )
            .map_err(|_| Error::ProfileEncryption {
                message: "argon2 derivation failed".to_string(),
            })?;
        let key = Key::from_slice(&key_bytes).to_owned();
        key_bytes.fill(0);
        Ok(key)
    }
}

fn default_profiles_path() -> Result<PathBuf> {
    if let Some(project_dirs) = ProjectDirs::from("dev", "Storify", "Storify") {
        let mut path = project_dirs.config_dir().to_path_buf();
        path.push(FILE_NAME);
        Ok(path)
    } else {
        Err(Error::ProfileStoreUnavailable)
    }
}

fn ensure_dir(path: Option<&Path>) -> Result<()> {
    if let Some(dir) = path
        && !dir.exists()
    {
        fs::create_dir_all(dir).map_err(|source| Error::ProfileStoreIo {
            path: dir.to_path_buf(),
            source,
        })?;
        set_secure_permissions(dir)?;
    }
    Ok(())
}

fn set_secure_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        let mode = if path.is_dir() { 0o700 } else { 0o600 };
        let permissions = Permissions::from_mode(mode);
        fs::set_permissions(path, permissions).map_err(|source| Error::ProfileStoreIo {
            path: path.to_path_buf(),
            source,
        })?;
    }
    #[cfg(not(unix))]
    {
        use log::warn;
        static PERMISSION_WARN_ONCE: Once = Once::new();
        PERMISSION_WARN_ONCE.call_once(|| {
            warn!(
                "profile store permissions hardening is not available on this platform; check file ACLs manually"
            );
        });
    }
    Ok(())
}

fn ensure_version(version: u8) -> Result<()> {
    if version == VERSION {
        Ok(())
    } else {
        Err(Error::ProfileStoreVersion {
            expected: VERSION,
            found: version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store(name: &str, password: Option<&str>) -> ProfileStore {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "storify-profile-test-{}-{}.toml",
            name,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        ProfileStore::with_path(path, password.map(|s| s.to_string()))
    }

    fn sample_profile(provider: &str) -> StoredProfile {
        StoredProfile {
            provider: provider.to_string(),
            bucket: "bucket".into(),
            access_key_id: Some("id".into()),
            access_key_secret: Some("secret".into()),
            endpoint: Some("https://endpoint".into()),
            region: Some("ap-southeast-1".into()),
            root_path: None,
            name_node: None,
            anonymous: false,
        }
    }

    fn cleanup(store: &ProfileStore) {
        let path = store.path().to_path_buf();
        fs::remove_file(&path).ok();
        fs::remove_file(path.with_extension("bak")).ok();
    }

    #[test]
    fn plain_round_trip() {
        let store = temp_store("plain", None);
        let profile = sample_profile("s3");
        store
            .save("dev", profile.clone(), true)
            .expect("save plain profile");
        let loaded = store
            .load("dev")
            .expect("load stored profile")
            .expect("profile exists");
        assert_eq!(loaded, profile);
        assert_eq!(store.default_profile().unwrap(), Some("dev".into()));
        cleanup(&store);
    }

    #[test]
    fn encrypted_round_trip() {
        let store = temp_store("enc", Some("password"));
        let profile = sample_profile("oss");
        store
            .save("prod", profile.clone(), true)
            .expect("save encrypted profile");
        let loaded = store
            .load("prod")
            .expect("load stored profile")
            .expect("profile exists");
        assert_eq!(loaded, profile);
        let content = fs::read_to_string(store.path()).expect("read encrypted file");
        assert!(content.contains("kdf"));
        assert!(!content.contains("default_profile"));
        cleanup(&store);
    }

    #[test]
    fn profile_listing_redacts_sensitive_values() {
        let store = temp_store("list", None);
        store
            .save("dev", sample_profile("s3"), true)
            .expect("save profile");
        let list = store.list_profiles_redacted().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "dev");
        assert_eq!(list[0].1.access_key_id.as_deref(), Some("****"));
        cleanup(&store);
    }

    #[test]
    fn encrypted_store_requires_password() {
        let store = temp_store("enc-required", Some("secret"));
        store
            .save("prod", sample_profile("oss"), true)
            .expect("save encrypted profile");
        let path = store.path().to_path_buf();
        let no_password = ProfileStore::with_path(path.clone(), None);
        let err = no_password
            .load("prod")
            .expect_err("missing password should fail");
        assert!(matches!(err, Error::ProfileDecryption { .. }));
        cleanup(&store);
    }
}
