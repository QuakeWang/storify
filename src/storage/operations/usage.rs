use crate::error::Result;
use futures::stream::TryStreamExt;
use opendal::Operator;

/// Trait for calculating disk usage in storage.
pub trait UsageCalculator {
    /// Calculate disk usage for a path in storage.
    ///
    /// # Arguments
    /// * `path` - Path to calculate usage for
    /// * `summary` - Whether to show summary only or detailed listing
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed error information
    async fn calculate_usage(&self, path: &str, summary: bool) -> Result<()>;
}

/// Implementation of UsageCalculator for OpenDAL Operator.
pub struct OpenDalUsageCalculator {
    operator: Operator,
}

impl OpenDalUsageCalculator {
    /// Create a new usage calculator with the given OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

impl UsageCalculator for OpenDalUsageCalculator {
    async fn calculate_usage(&self, path: &str, summary: bool) -> Result<()> {
        let lister = self.operator.lister_with(path).recursive(true).await?;
        let (total_size, total_files) = lister
            .try_fold((0, 0), |(size, count), entry| async move {
                let meta = entry.metadata();
                if !summary {
                    println!("{} {}", format_size(meta.content_length()), entry.path());
                }
                Ok((size + meta.content_length(), count + 1))
            })
            .await?;

        if summary {
            println!("{} {path}", format_size(total_size));
            println!("Total files: {total_files}");
        }
        Ok(())
    }
}

/// Format file size in human-readable format.
fn format_size(size: u64) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    const THRESHOLD: u64 = 1024;
    if size < THRESHOLD {
        return format!("{size}B");
    }
    let mut size_f = size as f64;
    let mut unit_index = 0;
    while size_f >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size_f /= THRESHOLD as f64;
        unit_index += 1;
    }
    format!("{size_f:.1}{}", UNITS[unit_index])
}
