// ---------------------------------------------------------------------------
// Retry with exponential backoff — matches Otto's withRetry from utils.ts.
// ---------------------------------------------------------------------------

use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry an async operation with exponential backoff.
///
/// - `max_retries`: maximum retry attempts (0 = no retries, just one attempt)
/// - `base_delay`: initial delay, doubled each retry
/// - `should_retry`: optional predicate — return false to stop retrying early
pub async fn with_retry<T, E, F, Fut>(
    mut f: F,
    max_retries: u32,
    base_delay: Duration,
    should_retry: Option<fn(&E) -> bool>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut last_err: Option<E> = None;

    for attempt in 0..=max_retries {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if attempt == max_retries {
                    return Err(e);
                }
                if let Some(predicate) = should_retry {
                    if !predicate(&e) {
                        return Err(e);
                    }
                }
                let delay = base_delay * 2u32.pow(attempt);
                sleep(delay).await;
                last_err = Some(e);
            }
        }
    }

    // Unreachable: the loop always returns on the final attempt (attempt == max_retries).
    // But if max_retries is somehow bypassed, return the last error safely.
    Err(last_err.expect("retry loop completed without any attempt"))
}
