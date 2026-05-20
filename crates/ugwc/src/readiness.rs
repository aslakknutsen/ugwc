//! Controller readiness (informer/cache sync gate for `/readyz`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::metrics;

/// Process readiness: cache synced after initial Kubernetes watch setup.
#[derive(Clone)]
pub struct Readiness {
    cache_synced: Arc<AtomicBool>,
}

impl Readiness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache_synced: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_cache_synced(&self, synced: bool) {
        self.cache_synced.store(synced, Ordering::SeqCst);
        metrics::set_cache_synced(synced);
    }

    #[must_use]
    pub fn is_cache_synced(&self) -> bool {
        self.cache_synced.load(Ordering::SeqCst)
    }

    /// Callback for `ugwc-k8s` to invoke after initial CRD list + Gateway watcher startup.
    #[must_use]
    pub fn on_cache_synced_callback(readiness: Arc<Self>) -> Arc<dyn Fn() + Send + Sync> {
        Arc::new(move || readiness.set_cache_synced(true))
    }
}
