use crate::error::{Error, Result};
use opendal::Operator;
use similar::TextDiff;
use std::path::PathBuf;

/// Trait for diffing two files and printing a unified diff
pub trait Differ {
    /// Diff two files and print the unified diff to stdout
    async fn diff(&self, left: &str, right: &str, context: usize, ignore_space: bool)
    -> Result<()>;
}

pub struct OpenDalDiffer {
    operator: Operator,
}

impl OpenDalDiffer {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn read_text(&self, path: &str, ignore_space: bool) -> Result<String> {
        let data = self.operator.read(path).await.map_err(|e| {
            if e.kind() == opendal::ErrorKind::NotFound {
                Error::PathNotFound {
                    path: PathBuf::from(path),
                }
            } else {
                Error::DiffFailed {
                    src_path: path.to_string(),
                    dest_path: path.to_string(),
                    source: Box::new(e.into()),
                }
            }
        })?;

        let mut s = String::from_utf8(data.to_vec()).map_err(|_| Error::InvalidArgument {
            message: format!("Non-UTF8 or binary file not supported: {}", path),
        })?;

        if ignore_space {
            // Trim trailing spaces/tabs for each line with minimal allocations.
            // Use an output String with reserved capacity.
            let mut out = String::with_capacity(s.len());
            let mut lines = s.split_inclusive('\n');
            for line in lines.by_ref() {
                if let Some(stripped) = line.strip_suffix('\n') {
                    out.push_str(stripped.trim_end_matches([' ', '\t']));
                    out.push('\n');
                } else {
                    out.push_str(line.trim_end_matches([' ', '\t']));
                }
            }
            s = out;
        }
        Ok(s)
    }
}

impl Differ for OpenDalDiffer {
    async fn diff(
        &self,
        left: &str,
        right: &str,
        context: usize,
        ignore_space: bool,
    ) -> Result<()> {
        let left_text = self.read_text(left, ignore_space).await?;
        let right_text = self.read_text(right, ignore_space).await?;

        // Produce unified diff via `similar`
        let diff = TextDiff::from_lines(&left_text, &right_text);
        let unified = diff
            .unified_diff()
            .context_radius(context)
            .header(left, right)
            .to_string();

        // If no differences, print nothing
        if unified.trim().is_empty() {
            return Ok(());
        }

        println!("{}", unified);
        Ok(())
    }
}
