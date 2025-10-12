use crate::config::{
    ConfigSource, ProfileStore, StorageConfig, StorageProvider, StoredProfile,
    loader::ResolvedConfig,
    prepare_storage_config,
    spec::{ProviderSpec, Requirement, provider_spec},
};
use crate::error::{Error, Result};
use std::env;
use std::str::FromStr;
use tokio::runtime::Handle;
use tokio::task;

use super::{
    context::CliContext,
    entry::{ConfigCommand, CreateArgs, DeleteArgs, ListArgs, SetArgs, ShowArgs},
    prompts::Prompt,
};

/// Encapsulates prompt logic to avoid manually managing cache and used state
struct PromptSession {
    prompt: Option<Prompt>,
    used: bool,
}

impl PromptSession {
    fn new() -> Self {
        Self {
            prompt: None,
            used: false,
        }
    }

    fn confirm(&mut self, ctx: &CliContext, message: &str, default: bool) -> Result<bool> {
        let prompt = self.get_or_init(ctx)?;
        self.used = true;
        task::block_in_place(|| Handle::current().block_on(prompt.confirm(message, default)))
    }

    fn input_required(&mut self, ctx: &CliContext, label: &str, secret: bool) -> Result<String> {
        loop {
            let value = self.input(ctx, label, secret)?;
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
            println!("{} cannot be empty.", label);
        }
    }

    fn input_optional(
        &mut self,
        ctx: &CliContext,
        label: &str,
        secret: bool,
    ) -> Result<Option<String>> {
        let value = self.input(ctx, label, secret)?;
        let trimmed = value.trim();
        Ok(if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        })
    }

    fn input(&mut self, ctx: &CliContext, label: &str, secret: bool) -> Result<String> {
        let prompt = self.get_or_init(ctx)?;
        self.used = true;
        task::block_in_place(|| Handle::current().block_on(prompt.input(label, secret)))
    }

    fn get_or_init(&mut self, ctx: &CliContext) -> Result<Prompt> {
        if self.prompt.is_none() {
            ctx.ensure_interactive("prompt required")?;
            self.prompt = Some(*ctx.prompt());
        }
        Ok(self.prompt.unwrap())
    }
}

pub fn execute(command: &ConfigCommand, ctx: &CliContext) -> Result<()> {
    match command {
        ConfigCommand::Show(args) => show_command(args, ctx),
        ConfigCommand::Create(args) => create_profile(args, ctx),
        ConfigCommand::Set(args) => set_default_profile(args, ctx),
        ConfigCommand::List(args) => list_profiles(args, ctx),
        ConfigCommand::Delete(args) => delete_profile(args, ctx),
    }
}

fn show_command(args: &ShowArgs, ctx: &CliContext) -> Result<()> {
    let credential_mode = if args.show_secrets {
        CredentialMode::PlainText
    } else {
        CredentialMode::Redacted
    };

    let (config, source_hint) = if args.default || args.profile.is_some() {
        let store = open_profile_store(ctx)?;
        let profile_name = if args.default {
            store
                .default_profile()
                .ok_or_else(|| Error::InvalidArgument {
                    message: "No default profile configured.".into(),
                })?
        } else {
            args.profile.as_ref().unwrap().as_str()
        };
        let config = store.get_profile(profile_name)?.into_config()?;
        let hint = format!("profile '{}'", profile_name);
        (config, Some(hint))
    } else {
        let config = ctx.storage_config()?.clone();
        let source = ctx.resolved().source;
        let hint = build_source_hint(source, ctx.resolved());
        (config, hint)
    };

    if let Some(hint) = source_hint {
        println!("# Configuration source: {}\n", hint);
    }

    print_config(&config, "", credential_mode);
    Ok(())
}

fn build_source_hint(source: Option<ConfigSource>, resolved: &ResolvedConfig) -> Option<String> {
    let has_env_provider = env::var("STORAGE_PROVIDER").is_ok();

    match source {
        Some(ConfigSource::ExplicitProfile) => {
            let profile = resolved.profile.as_deref().unwrap_or("unknown");
            Some(format!("--profile '{}'", profile))
        }
        Some(ConfigSource::DefaultProfile) => {
            let profile = resolved.profile.as_deref().unwrap_or("unknown");
            let mut hint = format!("default profile '{}'", profile);

            if has_env_provider {
                hint.push_str("\n# (overriding STORAGE_PROVIDER environment variable)");
                hint.push_str("\n# Tip: Run 'config set --clear' to use environment variables");
            }

            Some(hint)
        }
        Some(ConfigSource::Environment) => Some("environment variables".to_string()),
        None => None,
    }
}

fn create_profile(args: &CreateArgs, ctx: &CliContext) -> Result<()> {
    let mut store = open_profile_store(ctx)?;
    let mut session = PromptSession::new();

    let mut name = match &args.name {
        Some(name) => name.clone(),
        None => {
            println!("Enter a name for this profile.");
            session.input_required(ctx, "Profile name", false)?
        }
    };

    if name.trim().is_empty() {
        return Err(Error::InvalidArgument {
            message: "Profile name cannot be empty.".into(),
        });
    }
    name = name.trim().to_string();

    if store.profile(&name).is_some() && !args.force {
        let overwrite = session.confirm(
            ctx,
            &format!("Profile '{}' exists. Overwrite?", name),
            false,
        )?;
        if !overwrite {
            println!("Operation cancelled.");
            return Ok(());
        }
    }

    let provider_input = match &args.provider {
        Some(provider) => provider.clone(),
        None => {
            println!("Select a storage provider (oss, s3, minio, cos, fs, hdfs).");
            session.input_required(ctx, "Storage provider", false)?
        }
    };
    let provider = StorageProvider::from_str(&provider_input)?;
    print_provider_help(provider, provider_spec(provider));

    let mut bucket = args.bucket.clone();
    let mut root_path = args.root_path.clone();
    let mut name_node = args.name_node.clone();
    let mut access_key_id = args.access_key_id.clone();
    let mut access_key_secret = args.access_key_secret.clone();
    let mut endpoint = args.endpoint.clone();
    let mut region = args.region.clone();

    match provider {
        StorageProvider::Oss
        | StorageProvider::S3
        | StorageProvider::Cos
        | StorageProvider::Azblob => {
            if bucket.is_none() {
                println!("Bucket name (required).");
                bucket = Some(session.input_required(ctx, "Bucket", false)?);
            }

            if access_key_id.is_none() {
                if provider == StorageProvider::Cos {
                    println!("Secret ID (required for COS).");
                    access_key_id = Some(session.input_required(ctx, "Secret ID", false)?);
                } else {
                    println!("Access key ID (leave blank for anonymous).");
                    access_key_id = session.input_optional(ctx, "Access key ID", false)?;
                }
            }

            if access_key_secret.is_none() {
                if provider == StorageProvider::Cos {
                    println!("Secret key (required for COS).");
                    access_key_secret = Some(session.input_required(ctx, "Secret key", false)?);
                } else {
                    println!("Secret key (leave blank for anonymous).");
                    access_key_secret = session.input_optional(ctx, "Secret key", false)?;
                }
            }

            if endpoint.is_none() {
                println!("Endpoint URL (leave blank for provider default).");
                endpoint = session.input_optional(ctx, "Endpoint", false)?;
            }

            if region.is_none() && provider != StorageProvider::Cos {
                println!("Region (leave blank for provider default).");
                region = session.input_optional(ctx, "Region", false)?;
            }
        }
        StorageProvider::Fs => {
            if root_path.is_none() {
                println!("Root path (leave blank to use current directory).");
                root_path = session.input_optional(ctx, "Root path", false)?;
            }
        }
        StorageProvider::Hdfs => {
            if name_node.is_none() {
                println!("Name node address (required).");
                name_node = Some(session.input_required(ctx, "Name node", false)?);
            }
            if root_path.is_none() {
                println!("Root path (leave blank for default).");
                root_path = session.input_optional(ctx, "Root path", false)?;
            }
        }
    }

    let mut config = match provider {
        StorageProvider::Oss => StorageConfig::oss(bucket.expect("bucket required")),
        StorageProvider::S3 => StorageConfig::s3(bucket.expect("bucket required")),
        StorageProvider::Cos => StorageConfig::cos(bucket.expect("bucket required")),
        StorageProvider::Fs => StorageConfig::fs(root_path.clone()),
        StorageProvider::Hdfs => StorageConfig::hdfs(name_node.clone(), root_path.clone()),
        StorageProvider::Azblob => StorageConfig::azblob(bucket.expect("bucket required")),
    };

    config.access_key_id = access_key_id;
    config.access_key_secret = access_key_secret;
    config.endpoint = endpoint;
    config.region = region;
    config.root_path = root_path;
    config.name_node = name_node;
    config.anonymous = args.anonymous;

    prepare_storage_config(&mut config)?;

    let mut make_default = args.make_default;
    if !make_default && session.used {
        make_default =
            session.confirm(ctx, &format!("Set '{name}' as the default profile?"), false)?;
    }

    let stored = StoredProfile::from_config(&config);
    store.save_profile(name.clone(), stored, make_default)?;
    println!("Profile '{}' saved to {}", name, store.path().display());
    if make_default {
        println!("'{}' marked as default.", name);
    }
    Ok(())
}

fn print_provider_help(provider: StorageProvider, spec: ProviderSpec) {
    let anon = if spec.allows_anonymous() {
        "supported"
    } else {
        "not supported"
    };
    println!(
        "Provider '{}' selected (anonymous access {anon}). Field requirements:",
        provider.as_str()
    );
    for info in spec.field_matrix() {
        if info.rule.requirement() == Requirement::Unsupported {
            continue;
        }
        let label = requirement_label(info.rule.requirement());
        if let Some(default) = info.rule.default() {
            println!("  - {}: {} (default: {})", info.name, label, default);
        } else {
            println!("  - {}: {}", info.name, label);
        }
    }
}

fn set_default_profile(args: &SetArgs, ctx: &CliContext) -> Result<()> {
    let mut store = open_profile_store(ctx)?;
    let has_env_provider = env::var("STORAGE_PROVIDER").is_ok();

    if args.clear {
        store.set_default_profile(None)?;
        println!("✓ Default profile cleared");

        if has_env_provider {
            println!("ℹ Will now use STORAGE_PROVIDER environment variable");
        } else {
            println!(
                "ℹ No default profile set. You'll need to provide --profile or set environment variables"
            );
        }
    } else if let Some(name) = &args.name {
        store.set_default_profile(Some(name.clone()))?;
        println!("✓ Default profile set to '{}'", name);

        if has_env_provider {
            println!("ℹ This profile will override STORAGE_PROVIDER environment variable");
            println!("ℹ To temporarily use env vars, run: config set --clear");
        }
    }
    Ok(())
}

fn list_profiles(args: &ListArgs, ctx: &CliContext) -> Result<()> {
    let credential_mode = if args.show_secrets {
        CredentialMode::PlainText
    } else {
        CredentialMode::Hidden
    };

    let store = match open_profile_store(ctx) {
        Ok(store) => store,
        Err(Error::ProfileStoreLocked { .. }) => {
            println!(
                "Profiles: encrypted (supply --master-password or set {})",
                ctx.global_options().profile_pass_env
            );
            return Ok(());
        }
        Err(err) => return Err(err),
    };

    let names = store.available_profiles();
    let default = store.default_profile().map(str::to_string);
    let default_display = default.as_deref().unwrap_or("none");

    if names.is_empty() {
        println!("No profiles configured.");
        return Ok(());
    }

    println!("Profiles (default: {}):\n", default_display);
    for name in &names {
        let marker = if default.as_deref() == Some(name.as_str()) {
            '*'
        } else {
            ' '
        };

        match store.get_profile(name) {
            Ok(profile) => {
                let config = profile.into_config()?;
                println!("[{marker}] {name}");
                print_config(&config, "    ", credential_mode);
            }
            Err(err) => {
                println!("[{marker}] {name}");
                println!("    (failed to load: {err})");
            }
        }

        if name != names.last().unwrap() {
            println!();
        }
    }

    Ok(())
}

/// Credential display mode for configuration output
#[derive(Debug, Clone, Copy)]
enum CredentialMode {
    /// Hide credentials completely (used in `config list`)
    Hidden,
    /// Show redacted credentials (used in `config show`, default)
    Redacted,
    /// Show credentials in plaintext (used with `--show-secrets`)
    PlainText,
}

/// Print storage configuration with flexible formatting
fn print_config(config: &StorageConfig, indent: &str, credential_mode: CredentialMode) {
    // Basic fields
    println!("{}provider: {}", indent, config.provider.as_str());
    println!("{}bucket: {}", indent, config.bucket);

    // Optional fields
    if let Some(endpoint) = config.endpoint.as_deref() {
        println!("{}endpoint: {}", indent, endpoint);
    }

    if let Some(region) = config.region.as_deref() {
        println!("{}region: {}", indent, region);
    }

    if let Some(root) = config.root_path.as_deref() {
        println!("{}root_path: {}", indent, root);
    }

    if let Some(name_node) = config.name_node.as_deref() {
        println!("{}name_node: {}", indent, name_node);
    }

    // Credentials and anonymous mode
    if config.anonymous {
        println!("{}anonymous: true", indent);
    } else {
        print_credentials(config, indent, credential_mode);
    }
}

/// Print credentials based on the specified mode
fn print_credentials(config: &StorageConfig, indent: &str, mode: CredentialMode) {
    match mode {
        CredentialMode::Hidden => {
            // Don't display credentials at all
        }
        CredentialMode::Redacted => {
            // Show redacted credentials (first 4 chars + ***)
            if let Some(access_key) = config.access_key_id.as_deref() {
                let masked = mask_secret(access_key);
                println!("{}access_key_id: {}", indent, masked);
            }

            if config.access_key_secret.is_some() {
                println!("{}access_key_secret: ****", indent);
            }
        }
        CredentialMode::PlainText => {
            // Show credentials in plaintext
            if let Some(access_key) = config.access_key_id.as_deref() {
                println!("{}access_key_id: {}", indent, access_key);
            }

            if let Some(secret_key) = config.access_key_secret.as_deref() {
                println!("{}access_key_secret: {}", indent, secret_key);
            }
        }
    }
}

/// Mask a secret string by showing first 4 characters followed by ***
fn mask_secret(secret: &str) -> String {
    if secret.len() <= 4 {
        "****".to_string()
    } else {
        format!("{}***", &secret[..4])
    }
}

fn open_profile_store(ctx: &CliContext) -> Result<ProfileStore> {
    let path = ctx
        .resolved()
        .profile_store_path
        .clone()
        .or_else(|| ctx.global_options().profile_store.clone());
    ProfileStore::open_with_password(path, ctx.master_password())
}

fn requirement_label(requirement: Requirement) -> &'static str {
    match requirement {
        Requirement::Required => "required",
        Requirement::Optional => "optional",
        Requirement::Unsupported => "unsupported",
    }
}

fn delete_profile(args: &DeleteArgs, ctx: &CliContext) -> Result<()> {
    let mut store = open_profile_store(ctx)?;
    let mut session = PromptSession::new();

    let name = match &args.name {
        Some(name) => name.clone(),
        None => {
            let available = store.available_profiles();

            if available.is_empty() {
                println!("No profiles configured.");
                return Ok(());
            }

            println!("Available profiles:");
            let default_profile = store.default_profile();
            for profile_name in &available {
                let marker = if Some(profile_name.as_str()) == default_profile {
                    "[*]"
                } else {
                    "[ ]"
                };
                println!("  {} {}", marker, profile_name);
            }
            println!();

            session.input_required(ctx, "Profile name to delete", false)?
        }
    };

    if store.profile(&name).is_none() {
        return Err(Error::InvalidArgument {
            message: format!("Profile '{}' does not exist.", name),
        });
    }

    if !args.force {
        ctx.ensure_interactive("delete profile")?;

        let confirm = session.confirm(ctx, &format!("Delete profile '{}'?", name), false)?;

        if !confirm {
            println!("Operation cancelled.");
            return Ok(());
        }
    }

    let was_default = store.default_profile() == Some(name.as_str());

    store.delete_profile(&name)?;

    println!("Profile '{}' deleted from {}", name, store.path().display());

    if was_default {
        println!(
            "Note: '{}' was the default profile. Use `storify config set <name>` to set a new default.",
            name
        );
    }

    Ok(())
}
