use std::io::{self, Write};

/// A minimal progress reporter that prints percentage updates to stdout.
pub struct ConsoleProgressReporter {
    label: String,
    total_bytes: Option<u64>,
    step_bytes: u64,
    next_report_bytes: u64,
}

impl ConsoleProgressReporter {
    pub fn new(label: impl Into<String>, total_bytes: Option<u64>, step_bytes: u64) -> Self {
        let step_bytes = step_bytes.max(1);
        Self {
            label: label.into(),
            total_bytes,
            step_bytes,
            next_report_bytes: step_bytes,
        }
    }

    /// Print progress if a reporting threshold has been reached.
    pub fn maybe_report(&mut self, processed_bytes: u64) {
        if let Some(total) = self.total_bytes {
            if total == 0 {
                return;
            }

            if processed_bytes < self.next_report_bytes && processed_bytes != total {
                return;
            }

            let progress = (processed_bytes as f64 / total as f64) * 100.0;
            print!("\r {}: {:.1}%", self.label, progress);
            let _ = io::stdout().flush();

            while self.next_report_bytes <= processed_bytes {
                self.next_report_bytes = self.next_report_bytes.saturating_add(self.step_bytes);
            }
        }
    }
}
