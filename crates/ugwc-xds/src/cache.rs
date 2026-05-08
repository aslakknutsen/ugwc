//! Thread-safe holders for IR snapshots consumed by xDS.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use metrics::counter;
use parking_lot::RwLock;
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
}

impl MultiScopeSnapshot {
    #[must_use]
    pub fn new(global_initial: IrSnapshot) -> Self {
        Self {
            global: ArcSwap::from_pointee(global_initial),
            by_scope: RwLock::new(HashMap::new()),
        }
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
                counter!("ugwc_xds_scope_miss_total").increment(1);
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::ir::IrScopeId;
    use ugwc_core::IrSnapshot;

    #[test]
    fn multi_scope_global_aliases() {
        let initial = IrSnapshot::empty_for_scope(IrScopeId::new("default", "compile-stub"));
        let m = MultiScopeSnapshot::new(initial);
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
        let m = MultiScopeSnapshot::new(initial);
        m.store_scoped(IrSnapshot::empty_for_scope(IrScopeId::new("ns", "gw")));
        let r = m.resolve_for_discovery(Some("ns/gw"));
        assert_eq!(r.scope.0, "ns/gw");
    }
}
