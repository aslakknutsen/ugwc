//! Tracks whether this replica should publish IR/xDS updates (leader election).

use std::sync::Arc;

use tokio::sync::watch;

struct Inner {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

/// Whether this replica is the active leader (publishes xDS / dataplane changes).
#[derive(Clone)]
pub struct LeaderState {
    inner: Arc<Inner>,
}

impl LeaderState {
    #[must_use]
    pub fn leader_always() -> Self {
        let (tx, rx) = watch::channel(true);
        Self {
            inner: Arc::new(Inner { tx, rx }),
        }
    }

    #[must_use]
    pub fn new_follower() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            inner: Arc::new(Inner { tx, rx }),
        }
    }

    pub fn set_is_leader(&self, leader: bool) {
        let _ = self.inner.tx.send(leader);
    }

    #[must_use]
    pub fn is_leader(&self) -> bool {
        *self.inner.rx.borrow()
    }
}
