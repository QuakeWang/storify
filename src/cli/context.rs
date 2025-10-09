use std::env;

use crate::config::StorageConfig;
use crate::config::loader::{ConfigRequest, ResolvedConfig, resolve};
use crate::error::{Error, Result};
use secrecy::SecretString;

use super::entry::{Args, Command, GlobalOptions};
use super::prompts::Prompt;

pub struct CliContext {
    options: GlobalOptions,
    command: Command,
    resolved: ResolvedConfig,
    prompt: Prompt,
}

impl CliContext {
    pub async fn from_args(args: Args, prompt: Prompt) -> Result<Self> {
        let require_storage = !matches!(args.command, Command::Config(_));
        let non_interactive = args.global.non_interactive;
        let master_password = Self::resolve_master_password(&args.global);

        let request = ConfigRequest {
            profile: args.global.profile.clone(),
            profile_store_path: args.global.profile_store.clone(),
            non_interactive,
            require_storage,
            master_password,
        };

        let resolved = resolve(request)?;

        Ok(Self {
            options: args.global,
            command: args.command,
            resolved,
            prompt,
        })
    }

    pub fn command(&self) -> &Command {
        &self.command
    }

    pub fn resolved(&self) -> &ResolvedConfig {
        &self.resolved
    }

    pub fn is_non_interactive(&self) -> bool {
        self.options.non_interactive
    }

    pub fn ensure_interactive(&self, action: &str) -> Result<()> {
        if self.is_non_interactive() {
            Err(Error::non_interactive(action))
        } else {
            Ok(())
        }
    }

    pub fn prompt(&self) -> &Prompt {
        &self.prompt
    }

    pub fn master_password(&self) -> Option<SecretString> {
        Self::resolve_master_password(&self.options)
    }

    pub fn global_options(&self) -> &GlobalOptions {
        &self.options
    }

    pub fn storage_config(&self) -> Result<&StorageConfig> {
        self.resolved.storage.as_ref().ok_or_else(|| {
            let profiles = if self.resolved.available_profiles.is_empty() {
                "none".to_string()
            } else {
                self.resolved.available_profiles.join(", ")
            };
            Error::NoConfiguration { profiles }
        })
    }

    fn resolve_master_password(options: &GlobalOptions) -> Option<SecretString> {
        if let Some(explicit) = options.master_password.as_ref()
            && !explicit.is_empty()
        {
            return Some(SecretString::new(explicit.clone().into()));
        }

        let env_key = options.profile_pass_env.trim();
        if env_key.is_empty() {
            return None;
        }

        match env::var(env_key) {
            Ok(value) if !value.is_empty() => Some(SecretString::new(value.into())),
            _ => None,
        }
    }
}
