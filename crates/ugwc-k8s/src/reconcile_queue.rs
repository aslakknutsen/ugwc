//! Gateway reconcile queue with workqueue-style Prometheus metrics.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use metrics::{counter, gauge, histogram};
use tokio::sync::mpsc;

/// Queue name label for `ugwc_workqueue_*` metrics.
pub const GATEWAY_RECONCILE_QUEUE: &str = "gateway_reconcile";

struct QueueItem {
    ns: String,
    name: String,
    enqueued_at: Instant,
}

#[derive(Debug)]
struct QueueStats {
    depth: AtomicUsize,
    in_flight: AtomicUsize,
    longest_running_secs: AtomicU64,
    unfinished_work_secs: AtomicU64,
}

impl QueueStats {
    fn new() -> Self {
        Self {
            depth: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            longest_running_secs: AtomicU64::new(0),
            unfinished_work_secs: AtomicU64::new(0),
        }
    }

    fn publish_gauges(&self, queue_name: &str) {
        gauge!("ugwc_workqueue_depth", "queue_name" => queue_name.to_string())
            .set(self.depth.load(Ordering::Relaxed) as f64);
        gauge!(
            "ugwc_workqueue_longest_running_processor_seconds",
            "queue_name" => queue_name.to_string()
        )
        .set(self.longest_running_secs.load(Ordering::Relaxed) as f64);
        gauge!(
            "ugwc_workqueue_unfinished_work_seconds",
            "queue_name" => queue_name.to_string()
        )
        .set(self.unfinished_work_secs.load(Ordering::Relaxed) as f64);
    }
}

/// Cloneable handle for enqueueing gateway reconcile keys.
#[derive(Clone)]
pub struct ReconcileSender {
    tx: mpsc::Sender<QueueItem>,
    stats: Arc<QueueStats>,
}

impl ReconcileSender {
    pub async fn send(&self, ns: String, name: String) -> Result<(), ()> {
        counter!(
            "ugwc_workqueue_adds_total",
            "queue_name" => GATEWAY_RECONCILE_QUEUE.to_string()
        )
        .increment(1);
        let depth = self.stats.depth.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!(
            "ugwc_workqueue_depth",
            "queue_name" => GATEWAY_RECONCILE_QUEUE.to_string()
        )
        .set(depth as f64);
        self.stats.publish_gauges(GATEWAY_RECONCILE_QUEUE);
        self.tx
            .send(QueueItem {
                ns,
                name,
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(|_| ())
    }
}

/// Shared queue metrics and in-flight tracking (for graceful shutdown drain).
#[derive(Clone)]
pub struct ReconcileQueueMetrics {
    stats: Arc<QueueStats>,
}

impl ReconcileQueueMetrics {
    /// No-op metrics for non-Kubernetes runs.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            stats: Arc::new(QueueStats::new()),
        }
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.stats.in_flight.load(Ordering::Relaxed)
    }

    fn begin_processing(&self) {
        let n = self.stats.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.stats
            .unfinished_work_secs
            .store(n as u64, Ordering::Relaxed);
        self.stats.publish_gauges(GATEWAY_RECONCILE_QUEUE);
    }

    fn end_processing(&self, started: Instant) {
        let elapsed = started.elapsed().as_secs();
        let n = self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        let remaining = n.saturating_sub(1);
        self.stats
            .unfinished_work_secs
            .store(remaining as u64, Ordering::Relaxed);
        let prev_longest = self.stats.longest_running_secs.load(Ordering::Relaxed);
        if elapsed > prev_longest {
            self.stats
                .longest_running_secs
                .store(elapsed, Ordering::Relaxed);
        }
        self.stats.publish_gauges(GATEWAY_RECONCILE_QUEUE);
    }
}

/// Consumes reconcile keys from the workqueue.
pub struct ReconcileQueue {
    rx: mpsc::Receiver<QueueItem>,
    metrics: ReconcileQueueMetrics,
}

/// Create a queue and return `(sender, consumer, metrics)`.
#[must_use]
pub fn new_reconcile_queue(
    capacity: usize,
) -> (ReconcileSender, ReconcileQueue, ReconcileQueueMetrics) {
    let (tx, rx) = mpsc::channel(capacity);
    let stats = Arc::new(QueueStats::new());
    let sender = ReconcileSender {
        tx,
        stats: stats.clone(),
    };
    let metrics = ReconcileQueueMetrics { stats: stats.clone() };
    let queue = ReconcileQueue {
        rx,
        metrics: metrics.clone(),
    };
    (sender, queue, metrics)
}

impl ReconcileQueue {
    pub async fn recv(&mut self) -> Option<(String, String)> {
        let item = self.rx.recv().await?;
        let wait = item.enqueued_at.elapsed().as_secs_f64();
        histogram!(
            "ugwc_workqueue_queue_duration_seconds",
            "queue_name" => GATEWAY_RECONCILE_QUEUE.to_string()
        )
        .record(wait);
        let depth = self
            .metrics
            .stats
            .depth
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        gauge!(
            "ugwc_workqueue_depth",
            "queue_name" => GATEWAY_RECONCILE_QUEUE.to_string()
        )
        .set(depth as f64);
        self.metrics.stats.publish_gauges(GATEWAY_RECONCILE_QUEUE);
        Some((item.ns, item.name))
    }

    #[must_use]
    pub fn metrics(&self) -> ReconcileQueueMetrics {
        self.metrics.clone()
    }

    pub fn begin_reconcile(&self) -> Instant {
        self.metrics.begin_processing();
        Instant::now()
    }

    pub fn end_reconcile(&self, started: Instant) {
        self.metrics.end_processing(started);
    }
}

pub fn record_workqueue_retry(queue_name: &str) {
    counter!(
        "ugwc_workqueue_retries_total",
        "queue_name" => queue_name.to_string()
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn depth_tracks_send_and_recv() {
        let (tx, mut queue, metrics) = new_reconcile_queue(8);
        assert_eq!(metrics.in_flight(), 0);
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            1
        );
        let pair = queue.recv().await;
        assert_eq!(pair, Some(("ns".into(), "gw".into())));
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            0
        );
    }
}
