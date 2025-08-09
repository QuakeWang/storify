use std::io::{self, Write};

/// A minimal progress reporter that prints percentage updates to stdout.
pub struct ConsoleProgressReporter {
    label: String,
    total_bytes: Option<u64>,
    step_bytes: u64,
}

impl ConsoleProgressReporter {
    pub fn new(label: impl Into<String>, total_bytes: Option<u64>, step_bytes: u64) -> Self {
        Self {
            label: label.into(),
            total_bytes,
            step_bytes: step_bytes.max(1),
        }
    }

    /// Print progress if a reporting threshold has been reached.
    pub fn maybe_report(&self, processed_bytes: u64) {
        if let Some(total) = self.total_bytes {
            if total == 0 {
                return;
            }
            if processed_bytes.is_multiple_of(self.step_bytes) {
                let progress = ((processed_bytes as f64 / total as f64) * 100.0) as u32;
                print!("\r {}: {}%", self.label, progress);
                let _ = io::stdout().flush();
            }
        }
    }
}
