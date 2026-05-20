//! Helpers for spawning tasks with the current tracing span attached.

use tracing::Instrument;

/// Spawn a future on the Tokio runtime, preserving the active tracing span as parent.
///
/// When `tracing-opentelemetry` is installed as a subscriber layer, the span
/// automatically carries the OTel `Context`, so trace IDs propagate across task
/// boundaries without an explicit `Context::attach()` (which is `!Send` and
/// cannot be held across `.await` points).
pub fn spawn_with_trace_context<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.instrument(tracing::Span::current()))
}
