//! Gateway reconcile queue with workqueue-style Prometheus metrics.
//!
//! Workqueue metrics use the global `metrics` recorder (installed by `ugwc::metrics::install`).
//! Keep label sets fixed per metric name to avoid facade cardinality panics.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    /// Start time of each in-flight reconcile (sum of ages → unfinished_work gauge).
    in_flight_started: Mutex<Vec<Instant>>,
}

impl QueueStats {
    fn new() -> Self {
        Self {
            depth: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            longest_running_secs: AtomicU64::new(0),
            in_flight_started: Mutex::new(Vec::new()),
        }
    }

    fn unfinished_work_seconds(&self) -> f64 {
        let now = Instant::now();
        self.in_flight_started
            .lock()
            .expect("in_flight_started lock")
            .iter()
            .map(|s| now.duration_since(*s).as_secs_f64())
            .sum()
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
        .set(self.unfinished_work_seconds());
    }
}

/// Cloneable handle for enqueueing gateway reconcile keys.
#[derive(Clone)]
pub struct ReconcileSender {
    tx: mpsc::Sender<QueueItem>,
    stats: Arc<QueueStats>,
    draining: Arc<AtomicBool>,
}

impl ReconcileSender {
    pub async fn send(&self, ns: String, name: String) -> Result<(), ()> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(());
        }
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

    /// Stop accepting new reconcile keys (graceful shutdown).
    pub fn close(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
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

    #[cfg(test)]
    fn unfinished_work_seconds(&self) -> f64 {
        self.stats.unfinished_work_seconds()
    }

    fn begin_processing(&self) -> Instant {
        let started = Instant::now();
        self.stats.in_flight.fetch_add(1, Ordering::Relaxed);
        self.stats
            .in_flight_started
            .lock()
            .expect("in_flight_started lock")
            .push(started);
        self.stats.publish_gauges(GATEWAY_RECONCILE_QUEUE);
        started
    }

    fn end_processing(&self, started: Instant) {
        let elapsed = started.elapsed().as_secs();
        self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut v) = self.stats.in_flight_started.lock() {
            if let Some(pos) = v.iter().position(|t| *t == started) {
                v.remove(pos);
            }
        }
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
        draining: Arc::new(AtomicBool::new(false)),
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
        self.metrics.begin_processing()
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

    #[tokio::test]
    async fn unfinished_work_seconds_accumulates_while_reconcile_in_flight() {
        let (_tx, queue, metrics) = new_reconcile_queue(8);
        let started = queue.begin_reconcile();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let unfinished = metrics.unfinished_work_seconds();
        assert!(
            unfinished >= 0.04,
            "expected unfinished work >= 40ms, got {unfinished}"
        );
        queue.end_reconcile(started);
        assert!(
            metrics.unfinished_work_seconds() < 0.01,
            "expected ~0 unfinished work after end"
        );
    }

    #[tokio::test]
    async fn send_rejected_after_close() {
        let (tx, _queue, _metrics) = new_reconcile_queue(8);
        tx.close();
        assert!(tx.send("ns".into(), "gw".into()).await.is_err());
    }
}
