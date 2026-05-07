//! Thread-safe holder for the “current” [`ugwc_core::IrSnapshot`] used by xDS.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ugwc_core::IrSnapshot;

/// Read-mostly snapshot storage used by the ADS service implementations.
pub trait SnapshotCache: Send + Sync {
    fn current(&self) -> Arc<IrSnapshot>;
}

/// Default cache backed by [`arc_swap::ArcSwap`].
pub struct SharedSnapshot {
    inner: ArcSwap<IrSnapshot>,
}

impl SharedSnapshot {
    #[must_use]
    pub fn new(initial: IrSnapshot) -> Self {
        Self {
            inner: ArcSwap::from_pointee(initial),
        }
    }

    pub fn store(&self, snap: IrSnapshot) {
        self.inner.store(Arc::new(snap));
    }
}

impl SnapshotCache for SharedSnapshot {
    fn current(&self) -> Arc<IrSnapshot> {
        self.inner.load_full()
    }
}
