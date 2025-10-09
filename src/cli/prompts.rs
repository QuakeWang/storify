use std::io;

use dialoguer::{Confirm, Input, Password};
use tokio::task;

use crate::error::{Error, Result};

/// Interactive prompt mode for CLI operations
#[derive(Debug, Clone, Copy)]
pub enum Prompt {
    /// Console-based interactive prompts using dialoguer
    Console,
    /// Non-interactive mode that uses defaults or fails
    NonInteractive,
}

impl Prompt {
    pub fn new(interactive: bool) -> Self {
        if interactive {
            Self::Console
        } else {
            Self::NonInteractive
        }
    }

    pub async fn confirm(&self, message: &str, default: bool) -> Result<bool> {
        match self {
            Prompt::Console => {
                let prompt = message.to_string();
                let result = task::spawn_blocking(move || {
                    Confirm::new()
                        .with_prompt(prompt)
                        .default(default)
                        .interact()
                })
                .await
                .map_err(join_error)?;

                result.map_err(|err| Error::InvalidArgument {
                    message: err.to_string(),
                })
            }
            Prompt::NonInteractive => Ok(default),
        }
    }

    pub async fn input(&self, field: &str, secret: bool) -> Result<String> {
        match self {
            Prompt::Console => {
                let prompt = field.to_string();
                let result = task::spawn_blocking(move || {
                    if secret {
                        Password::new()
                            .with_prompt(prompt)
                            .allow_empty_password(true)
                            .interact()
                    } else {
                        Input::new()
                            .with_prompt(prompt)
                            .allow_empty(true)
                            .interact_text()
                    }
                })
                .await
                .map_err(join_error)?;

                result.map_err(|err| Error::InvalidArgument {
                    message: err.to_string(),
                })
            }
            Prompt::NonInteractive => Err(Error::InvalidArgument {
                message: format!(
                    "Input required for '{field}', but prompts are disabled. Provide values via flags or environment variables."
                ),
            }),
        }
    }
}

impl Default for Prompt {
    fn default() -> Self {
        Self::Console
    }
}

fn join_error(err: task::JoinError) -> Error {
    Error::Io {
        source: io::Error::other(err.to_string()),
    }
}
