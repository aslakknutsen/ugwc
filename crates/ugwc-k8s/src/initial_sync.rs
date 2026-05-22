//! Readiness gate: `/readyz` opens only after initial API lists for core dependencies complete.
//!
//! CRD-gated route/policy watchers may start later; readiness does not wait for them.
//! With reflector stores, `endpointslice_list` fires when the reflector's store becomes
//! ready (`wait_until_ready`).  The `crd_list` gate stays as-is (CRDs don't use reflector
//! stores).  `gateway_list` was removed — Gateway is CRD-gated and its store readiness
//! is implicit once the CRD supervisor starts the watcher.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Tracks initial list completion for core informer dependencies.
pub struct InitialSyncGate {
    crd_list: AtomicBool,
    endpointslice_list: AtomicBool,
    fired: AtomicBool,
    on_ready: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl InitialSyncGate {
    #[must_use]
    pub fn new(on_ready: Option<Arc<dyn Fn() + Send + Sync>>) -> Self {
        Self {
            crd_list: AtomicBool::new(false),
            endpointslice_list: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            on_ready,
        }
    }

    pub fn mark_crd_list(&self) {
        self.crd_list.store(true, Ordering::SeqCst);
        self.try_fire();
    }

    pub fn mark_endpointslice_list(&self) {
        self.endpointslice_list.store(true, Ordering::SeqCst);
        self.try_fire();
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.crd_list.load(Ordering::SeqCst)
            && self.endpointslice_list.load(Ordering::SeqCst)
    }

    fn try_fire(&self) {
        if !self.is_ready() {
            return;
        }
        if self
            .fired
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        if let Some(cb) = &self.on_ready {
            cb();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn fires_once_when_all_gates_set() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let gate = InitialSyncGate::new(Some(Arc::new(move || {
            c.fetch_add(1, Ordering::SeqCst);
        })));
        gate.mark_crd_list();
        assert_eq!(count.load(Ordering::SeqCst), 0);
        gate.mark_endpointslice_list();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        gate.mark_crd_list();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
