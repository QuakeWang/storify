use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use blake3;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use rand::Rng;
use secrecy::{ExposeSecret, SecretString};
use std::env;
use std::path::Path;
use zeroize::Zeroizing;

use crate::error::Error;

/// Field encryption prefix (identifies encrypted fields)
pub const FIELD_ENCRYPTED_PREFIX: &str = "ENC:";

// Key and nonce sizes
const SALT_SIZE: usize = 16; // Argon2 recommended minimum (128 bits)
const NONCE_SIZE: usize = 12; // ChaCha20Poly1305 nonce (96 bits)
const KEY_SIZE: usize = 32; // AES-256 equivalent (256 bits)

// Argon2 parameters: balance security & performance (~100ms on modern CPU)
const ARGON2_M_COST: u32 = 19 * 1024; // Memory cost: 19 MiB
const ARGON2_T_COST: u32 = 2; // Time cost: 2 iterations
const ARGON2_P_COST: u32 = 1; // Parallelism: 1 thread

/// Internally uses `Zeroizing` to protect keys and ensure memory safety.
#[derive(Debug)]
pub struct EncryptionMetadata {
    key: Zeroizing<[u8; KEY_SIZE]>,
    salt: Vec<u8>,
}

impl EncryptionMetadata {
    pub fn new(key: [u8; KEY_SIZE], salt: impl Into<Vec<u8>>) -> Self {
        Self {
            key: Zeroizing::new(key),
            salt: salt.into(),
        }
    }

    pub fn key(&self) -> &[u8; KEY_SIZE] {
        &self.key
    }

    pub fn salt(&self) -> &[u8] {
        &self.salt
    }
}

impl Clone for EncryptionMetadata {
    fn clone(&self) -> Self {
        Self {
            key: Zeroizing::new(*self.key),
            salt: self.salt.clone(),
        }
    }
}

/// Generate cryptographically secure random salt
pub fn generate_salt() -> [u8; SALT_SIZE] {
    rand::rng().random()
}

/// Derive auto password from system context using Blake3
pub fn derive_auto_password(config_path: &Path) -> SecretString {
    let mut components = Vec::new();

    if let Ok(username) = env::var("USER").or_else(|_| env::var("USERNAME")) {
        components.push(username);
    }

    components.push(config_path.to_string_lossy().to_string());

    components.push("storify-auto-encryption-v1".to_string());

    let combined = components.join("::");

    let context_key = blake3::hash(b"storify-kdf-context-8964");
    let mut hasher = blake3::Hasher::new_keyed(context_key.as_bytes());
    hasher.update(combined.as_bytes());
    let hash = hasher.finalize();

    let password = BASE64_ENGINE.encode(&hash.as_bytes()[..32]);
    SecretString::new(password.into())
}

/// Resolve final master password to use
pub fn resolve_master_password(explicit: Option<SecretString>, config_path: &Path) -> SecretString {
    explicit.unwrap_or_else(|| derive_auto_password(config_path))
}

/// Derive encryption key from master password (using Argon2id KDF)
pub fn derive_master_key(password: &SecretString, salt: &[u8]) -> Result<[u8; KEY_SIZE], Error> {
    let params = Params::new(ARGON2_M_COST, ARGON2_T_COST, ARGON2_P_COST, Some(KEY_SIZE)).map_err(
        |err| Error::ProfileEncryption {
            message: format!("invalid argon2 parameters: {err}"),
        },
    )?;

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut key = Zeroizing::new([0u8; KEY_SIZE]);
    argon2
        .hash_password_into(password.expose_secret().as_bytes(), salt, &mut *key)
        .map_err(|err| Error::ProfileEncryption {
            message: format!("failed to derive master key: {err}"),
        })?;
    Ok(*key)
}

/// Encrypt a single field (using derived key)
///
/// **Format**: `ENC:<base64([nonce:12][ciphertext:var])>`
///
/// # Example
/// ```text
/// access_key_id = "ENC:ARAAwBkSAiJHC/2l5jfEG8..."
/// ```
pub fn encrypt_field(plaintext: &str, key: &[u8; KEY_SIZE]) -> Result<String, Error> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let nonce: [u8; NONCE_SIZE] = rand::rng().random();

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
        .map_err(|err| Error::ProfileEncryption {
            message: format!("field encryption failed: {err}"),
        })?;

    // Build payload: [nonce:12][ciphertext:var]
    let mut payload = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);

    Ok(format!(
        "{}{}",
        FIELD_ENCRYPTED_PREFIX,
        BASE64_ENGINE.encode(payload)
    ))
}

/// Encrypt a field and embed salt (for the first encrypted field)
///
/// **Format**: `ENC:v1:<base64_salt>:<base64([nonce:12][ciphertext:var])>`
///
/// # Example
/// ```text
/// access_key_id = "ENC:v1:4ndoZ9WUGD/c5y/Jx9Pnqw==:XyTaoQra3IUk..."
/// ```
pub fn encrypt_field_with_salt(
    plaintext: &str,
    key: &[u8; KEY_SIZE],
    salt: &[u8],
) -> Result<String, Error> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let nonce: [u8; NONCE_SIZE] = rand::rng().random();

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
        .map_err(|err| Error::ProfileEncryption {
            message: format!("field encryption failed: {err}"),
        })?;

    // Build payload: [nonce:12][ciphertext:var]
    let mut payload = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);

    Ok(format!(
        "{}v1:{}:{}",
        FIELD_ENCRYPTED_PREFIX,
        BASE64_ENGINE.encode(salt),
        BASE64_ENGINE.encode(payload)
    ))
}

/// Decrypt a single field
pub fn decrypt_field(encrypted: &str, key: &[u8; KEY_SIZE]) -> Result<Option<String>, Error> {
    // Check if field is encrypted
    let Some(encoded) = encrypted.strip_prefix(FIELD_ENCRYPTED_PREFIX) else {
        return Ok(None); // Plaintext field
    };

    let payload = BASE64_ENGINE
        .decode(encoded)
        .map_err(|err| Error::ProfileDecryption {
            message: format!("invalid base64 in encrypted field: {err}"),
        })?;

    if payload.len() < NONCE_SIZE {
        return Err(Error::ProfileDecryption {
            message: format!(
                "encrypted field too short: expected at least {} bytes, got {}",
                NONCE_SIZE,
                payload.len()
            ),
        });
    }

    let nonce = &payload[0..NONCE_SIZE];
    let ciphertext = &payload[NONCE_SIZE..];

    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|err| Error::ProfileDecryption {
            message: format!("field decryption failed: {err}"),
        })?;

    let text = String::from_utf8(plaintext).map_err(|err| Error::ProfileDecryption {
        message: format!("decrypted field is not valid UTF-8: {err}"),
    })?;

    Ok(Some(text))
}

/// Decrypt field and extract salt (for parsing first encrypted field)
///
/// **Format**: `ENC:v1:<base64_salt>:<base64([nonce:12][ciphertext:var])>`
///
/// Returns `(salt, plaintext)` or `None` if not a salt-embedded encrypted field
pub fn decrypt_field_with_salt(
    encrypted: &str,
    key: &[u8; KEY_SIZE],
) -> Result<Option<(Vec<u8>, String)>, Error> {
    // Check if field is salt-embedded encrypted (ENC:v1:...)
    let Some(rest) = encrypted.strip_prefix(FIELD_ENCRYPTED_PREFIX) else {
        return Ok(None); // Plaintext field
    };

    if !rest.starts_with("v1:") {
        return Ok(None); // Not salt-embedded format
    }

    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(Error::ProfileDecryption {
            message: "invalid encrypted field format: expected 'v1:salt:ciphertext'".into(),
        });
    }

    let salt_bytes = BASE64_ENGINE
        .decode(parts[1])
        .map_err(|err| Error::ProfileDecryption {
            message: format!("invalid base64 in salt: {err}"),
        })?;

    let payload = BASE64_ENGINE
        .decode(parts[2])
        .map_err(|err| Error::ProfileDecryption {
            message: format!("invalid base64 in encrypted field: {err}"),
        })?;

    if payload.len() < NONCE_SIZE {
        return Err(Error::ProfileDecryption {
            message: format!(
                "encrypted field too short: expected at least {} bytes, got {}",
                NONCE_SIZE,
                payload.len()
            ),
        });
    }

    let nonce = &payload[0..NONCE_SIZE];
    let ciphertext = &payload[NONCE_SIZE..];

    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|err| Error::ProfileDecryption {
            message: format!("field decryption failed: {err}"),
        })?;

    let text = String::from_utf8(plaintext).map_err(|err| Error::ProfileDecryption {
        message: format!("decrypted field is not valid UTF-8: {err}"),
    })?;

    Ok(Some((salt_bytes, text)))
}

/// Extract salt from an encrypted field (if present)
pub fn extract_salt(encrypted: &str) -> Result<Option<Vec<u8>>, Error> {
    let Some(rest) = encrypted.strip_prefix(FIELD_ENCRYPTED_PREFIX) else {
        return Ok(None); // Plaintext field
    };

    if !rest.starts_with("v1:") {
        return Ok(None); // Non-salt format (ENC:...)
    }

    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(Error::ProfileDecryption {
            message: "invalid salt-embedded format: expected 'ENC:v1:salt:ciphertext'".into(),
        });
    }

    let salt = BASE64_ENGINE
        .decode(parts[1])
        .map_err(|err| Error::ProfileDecryption {
            message: format!("invalid base64 in salt: {err}"),
        })?;

    Ok(Some(salt))
}

/// Decrypt a field automatically (tries all supported formats)
pub fn decrypt_field_auto(encrypted: &str, key: &[u8; KEY_SIZE]) -> Result<Option<String>, Error> {
    // Not encrypted (plaintext)
    if !encrypted.starts_with(FIELD_ENCRYPTED_PREFIX) {
        return Ok(None);
    }

    if let Some((_, plaintext)) = decrypt_field_with_salt(encrypted, key)? {
        return Ok(Some(plaintext));
    }

    decrypt_field(encrypted, key)
}
