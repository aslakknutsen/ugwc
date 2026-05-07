//! Conservative defaults: do **not** publish partial snapshots unless explicitly enabled.

use crate::ir::IrSnapshot;

/// What to do when the publish pipeline hits an unrecoverable error after a compile succeeded.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[non_exhaustive]
pub enum OnPublishError {
    /// Do not advance the published snapshot / version.
    #[default]
    Abort,
    /// Best-effort retry with backoff in the caller (wired in later phases).
    Retry,
}

/// How to treat compile outcomes that contain recoverable issues for some objects.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[non_exhaustive]
pub enum OnPartialCompile {
    /// Treat as failure: do not install a new [`IrSnapshot`] for publish.
    #[default]
    Reject,
    /// Emit the snapshot but surface errors only via status / reports (dangerous; off by default).
    AllowBestEffort,
}

/// User/operator-selected failure handling dimensions (registry hook in later phases).
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct FailurePolicy {
    pub on_publish_error: OnPublishError,
    pub on_partial_compile: OnPartialCompile,
}

impl FailurePolicy {
    /// Default production-safe policy: never best-effort publish.
    #[must_use]
    pub fn conservative() -> Self {
        Self::default()
    }

    /// Whether `snapshot` may be installed as the next candidate for xDS publish.
    #[must_use]
    pub fn allow_snapshot_after_compile(&self, snapshot: &IrSnapshot) -> bool {
        if snapshot.meta.has_errors {
            return matches!(self.on_partial_compile, OnPartialCompile::AllowBestEffort);
        }
        true
    }
}
