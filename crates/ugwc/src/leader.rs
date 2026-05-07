//! Phase 8 — Kubernetes Lease leader election (**stub** until full HA path).

use tokio::sync::watch;

/// Tracks whether this replica should publish IR/xDS updates.
#[derive(Clone)]
pub struct LeaderState {
    is_leader: watch::Receiver<bool>,
}

impl LeaderState {
    /// Standalone / tests: always leader.
    #[must_use]
    pub fn leader_always() -> Self {
        let (_tx, rx) = watch::channel(true);
        Self { is_leader: rx }
    }

    /// HA mode placeholder — followers stay false until lease wiring lands.
    #[must_use]
    pub fn follower_stub() -> Self {
        let (_tx, rx) = watch::channel(false);
        Self { is_leader: rx }
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        *self.is_leader.borrow()
    }
}
