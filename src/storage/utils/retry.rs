use crate::error::Result;
use std::time::Duration;
use tokio::time::sleep;

/// Maximum number of retries for transient read/write failures.
const MAX_RETRIES: usize = 6;

/// Read a byte range from `op` with exponential-backoff retry on transient errors.
///
/// Retries up to `MAX_RETRIES` times on `Temporary` or `RateLimited` errors,
/// with 200ms × 2^attempt delay capped at 5 seconds.
pub async fn read_range_with_retry(
    op: &opendal::Operator,
    path: &str,
    range: std::ops::Range<u64>,
) -> Result<opendal::Buffer> {
    let mut attempt = 0usize;

    loop {
        match op.read_with(path).range(range.clone()).await {
            Ok(buf) => return Ok(buf),
            Err(err) => {
                let retryable =
                    err.is_temporary() || matches!(err.kind(), opendal::ErrorKind::RateLimited);
                if !retryable || attempt >= MAX_RETRIES {
                    return Err(err.into());
                }

                let backoff = 1u64.checked_shl(attempt as u32).unwrap_or(u64::MAX);
                let delay_ms = 200u64.saturating_mul(backoff).min(5_000);
                log::warn!(
                    "Temporary read failure (attempt {}/{}), retrying in {}ms: path={} range={}-{} err={}",
                    attempt + 1,
                    MAX_RETRIES + 1,
                    delay_ms,
                    path,
                    range.start,
                    range.end,
                    err
                );
                sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
        }
    }
}
