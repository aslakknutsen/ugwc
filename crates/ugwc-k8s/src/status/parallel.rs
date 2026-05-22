//! Bounded concurrency for status patch futures within a single reconcile.

use std::future::Future;

use futures::stream::{self, StreamExt};

/// Run `futs` with at most `concurrency` in flight at once.
pub async fn run_bounded<Fut>(concurrency: usize, futs: impl IntoIterator<Item = Fut>)
where
    Fut: Future<Output = ()> + Send,
{
    let cap = concurrency.max(1);
    stream::iter(futs.into_iter())
        .buffer_unordered(cap)
        .collect::<Vec<_>>()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn run_bounded_caps_concurrency() {
        let n = 8usize;
        let cap = 2usize;
        let sleep_ms = 50u64;

        let futs = (0..n).map(|_| async {
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        });

        let started = Instant::now();
        run_bounded(cap, futs).await;
        let elapsed = started.elapsed();

        let min_expected = Duration::from_millis(sleep_ms * (n / cap) as u64);
        let max_expected = Duration::from_millis(sleep_ms * ((n + cap - 1) / cap) as u64 + 30);
        assert!(
            elapsed >= min_expected,
            "elapsed {elapsed:?} < min {min_expected:?}"
        );
        assert!(
            elapsed <= max_expected,
            "elapsed {elapsed:?} > max {max_expected:?} (serial would be ~{}ms)",
            sleep_ms * n as u64
        );
    }
}
