//! Thread-safe holders for IR snapshots consumed by xDS.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use prometheus_client::metrics::counter::Counter;
use tokio::sync::watch;
use ugwc_core::ir::{IrScopeId, IrSnapshot};

/// Read-mostly snapshot storage (legacy single-global view).
pub trait SnapshotCache: Send + Sync {
    fn current(&self) -> Arc<IrSnapshot>;
}

/// Resolve which IR to serve for an xDS stream (`node.id` → scope key).
pub trait XdsSnapshotSource: Send + Sync {
    /// `node_id`: `None` / empty / `global` / `*` → global snapshot; else `namespace/name` per [`IrScopeId`].
    fn resolve_for_discovery(&self, node_id: Option<&str>) -> Arc<IrSnapshot>;

    fn previous_for_scope(&self, scope: &IrScopeId) -> Option<Arc<IrSnapshot>>;

    fn store_scoped(&self, snap: IrSnapshot);
}

/// Default cache backed by [`arc_swap::ArcSwap`] (one global snapshot only).
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

impl XdsSnapshotSource for SharedSnapshot {
    fn resolve_for_discovery(&self, node_id: Option<&str>) -> Arc<IrSnapshot> {
        let _ = node_id;
        self.current()
    }

    fn previous_for_scope(&self, scope: &IrScopeId) -> Option<Arc<IrSnapshot>> {
        let cur = self.current();
        (cur.scope == *scope).then_some(cur)
    }

    fn store_scoped(&self, snap: IrSnapshot) {
        self.store(snap);
    }
}

/// Global snapshot plus per-gateway (`IrScopeId` string) entries for scoped xDS clients.
pub struct MultiScopeSnapshot {
    global: ArcSwap<IrSnapshot>,
    by_scope: RwLock<HashMap<String, Arc<IrSnapshot>>>,
    /// Bumps on every [`Self::store_scoped`] so ADS streams can push when IR changes.
    rev_tx: watch::Sender<u64>,
    scope_miss: Counter,
}

impl MultiScopeSnapshot {
    #[must_use]
    pub fn new(global_initial: IrSnapshot, scope_miss: Counter) -> Self {
        let (rev_tx, _rev_rx) = watch::channel(0u64);
        Self {
            global: ArcSwap::from_pointee(global_initial),
            by_scope: RwLock::new(HashMap::new()),
            rev_tx,
            scope_miss,
        }
    }

    /// Clone of the revision sender: each open ADS stream should call [`watch::Sender::subscribe`].
    #[must_use]
    pub fn revision_sender(&self) -> watch::Sender<u64> {
        self.rev_tx.clone()
    }
}

impl SnapshotCache for MultiScopeSnapshot {
    fn current(&self) -> Arc<IrSnapshot> {
        self.global.load_full()
    }
}

impl XdsSnapshotSource for MultiScopeSnapshot {
    fn resolve_for_discovery(&self, node_id: Option<&str>) -> Arc<IrSnapshot> {
        let key = node_id.map(str::trim).filter(|s| !s.is_empty());
        match key {
            None | Some("global") | Some("*") => self.global.load_full(),
            Some(k) => self.by_scope.read().get(k).cloned().unwrap_or_else(|| {
                self.scope_miss.inc();
                Arc::new(IrSnapshot::empty_for_scope(IrScopeId(k.to_string())))
            }),
        }
    }

    fn previous_for_scope(&self, scope: &IrScopeId) -> Option<Arc<IrSnapshot>> {
        self.by_scope.read().get(&scope.0).cloned()
    }

    fn store_scoped(&self, snap: IrSnapshot) {
        let key = snap.scope.0.clone();
        let arc = Arc::new(snap);
        self.by_scope.write().insert(key, arc);
        let cur = *self.rev_tx.borrow();
        let _ = self.rev_tx.send(cur.saturating_add(1));
    }
}

impl MultiScopeSnapshot {
    /// Remove a per-gateway scope from the cache and notify ADS watchers.
    #[must_use]
    pub fn remove_scoped(&self, scope: &IrScopeId) -> bool {
        let removed = self.by_scope.write().remove(&scope.0).is_some();
        if removed {
            let cur = *self.rev_tx.borrow();
            let _ = self.rev_tx.send(cur.saturating_add(1));
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::ir::IrScopeId;
    use ugwc_core::IrSnapshot;

    fn test_multi_scope(initial: IrSnapshot) -> MultiScopeSnapshot {
        MultiScopeSnapshot::new(initial, Counter::default())
    }

    #[test]
    fn multi_scope_global_aliases() {
        let initial = IrSnapshot::empty_for_scope(IrScopeId::new("default", "compile-stub"));
        let m = test_multi_scope(initial);
        assert_eq!(
            m.resolve_for_discovery(None).scope.0,
            "default/compile-stub"
        );
        assert_eq!(
            m.resolve_for_discovery(Some("global")).scope.0,
            "default/compile-stub"
        );
    }

    #[test]
    fn multi_scope_stores_and_resolves() {
        let initial = IrSnapshot::empty_for_scope(IrScopeId::new("default", "compile-stub"));
        let m = test_multi_scope(initial);
        m.store_scoped(IrSnapshot::empty_for_scope(IrScopeId::new("ns", "gw")));
        let r = m.resolve_for_discovery(Some("ns/gw"));
        assert_eq!(r.scope.0, "ns/gw");
    }

    #[test]
    fn remove_scoped_drops_entry() {
        let initial = IrSnapshot::empty_for_scope(IrScopeId::new("default", "a"));
        let m = test_multi_scope(initial);
        m.store_scoped(IrSnapshot::empty_for_scope(IrScopeId::new("ns", "gw")));
        assert_eq!(m.resolve_for_discovery(Some("ns/gw")).scope.0, "ns/gw");
        assert!(m.remove_scoped(&IrScopeId::new("ns", "gw")));
        assert_eq!(m.resolve_for_discovery(Some("ns/gw")).scope.0, "ns/gw");
    }

    #[tokio::test]
    async fn store_scoped_notifies_all_revision_watchers() {
        let initial = IrSnapshot::empty_for_scope(IrScopeId::new("default", "a"));
        let m = std::sync::Arc::new(test_multi_scope(initial));
        let mut rx1 = m.revision_sender().subscribe();
        let mut rx2 = m.revision_sender().subscribe();
        let h1 = tokio::spawn(async move {
            rx1.changed().await.expect("watch rx1");
            rx1.borrow_and_update();
        });
        let h2 = tokio::spawn(async move {
            rx2.changed().await.expect("watch rx2");
            rx2.borrow_and_update();
        });
        m.store_scoped(IrSnapshot::empty_for_scope(IrScopeId::new("ns", "gw")));
        h1.await.expect("watch task 1");
        h2.await.expect("watch task 2");
    }
}
