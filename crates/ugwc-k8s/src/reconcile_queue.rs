//! Gateway reconcile queue with workqueue-style Prometheus metrics.
//!
//! Metrics are prometheus-client Family types passed in at construction.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use tokio::sync::mpsc;

/// Queue name label for `ugwc_workqueue_*` metrics.
pub const GATEWAY_RECONCILE_QUEUE: &str = "gateway_reconcile";

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QueueNameLabels {
    pub queue_name: String,
}

/// Metric handles for the workqueue, passed from the binary crate.
#[derive(Clone)]
pub struct WorkqueueMetrics {
    pub adds: Family<QueueNameLabels, Counter>,
    pub retries: Family<QueueNameLabels, Counter>,
    pub depth: Family<QueueNameLabels, Gauge>,
    pub longest_running_processor_seconds: Family<QueueNameLabels, Gauge>,
    pub unfinished_work_seconds: Family<QueueNameLabels, Gauge>,
    pub queue_duration_seconds: Family<QueueNameLabels, Histogram>,
}

impl WorkqueueMetrics {
    /// Create a no-op metrics set (for tests or non-k8s mode).
    #[must_use]
    pub fn noop() -> Self {
        Self {
            adds: Family::default(),
            retries: Family::default(),
            depth: Family::default(),
            longest_running_processor_seconds: Family::default(),
            unfinished_work_seconds: Family::default(),
            queue_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(std::iter::once(1.0))
            }),
        }
    }
}

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

    fn publish_gauges(&self, wq_metrics: &WorkqueueMetrics, queue_name: &str) {
        let labels = QueueNameLabels {
            queue_name: queue_name.to_string(),
        };
        wq_metrics
            .depth
            .get_or_create(&labels)
            .set(self.depth.load(Ordering::Relaxed) as i64);
        wq_metrics
            .longest_running_processor_seconds
            .get_or_create(&labels)
            .set(self.longest_running_secs.load(Ordering::Relaxed) as i64);
        wq_metrics
            .unfinished_work_seconds
            .get_or_create(&labels)
            .set(self.unfinished_work_seconds() as i64);
    }
}

/// Cloneable handle for enqueueing gateway reconcile keys.
#[derive(Clone)]
pub struct ReconcileSender {
    tx: mpsc::Sender<QueueItem>,
    stats: Arc<QueueStats>,
    draining: Arc<AtomicBool>,
    wq_metrics: WorkqueueMetrics,
}

impl ReconcileSender {
    pub async fn send(&self, ns: String, name: String) -> Result<(), ()> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(());
        }
        let labels = QueueNameLabels {
            queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
        };
        self.wq_metrics.adds.get_or_create(&labels).inc();
        self.stats.depth.fetch_add(1, Ordering::Relaxed);
        self.stats.publish_gauges(&self.wq_metrics, GATEWAY_RECONCILE_QUEUE);
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
    wq_metrics: WorkqueueMetrics,
}

impl ReconcileQueueMetrics {
    /// No-op metrics for non-Kubernetes runs.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            stats: Arc::new(QueueStats::new()),
            wq_metrics: WorkqueueMetrics::noop(),
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
        self.stats.publish_gauges(&self.wq_metrics, GATEWAY_RECONCILE_QUEUE);
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
        self.stats.publish_gauges(&self.wq_metrics, GATEWAY_RECONCILE_QUEUE);
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
    wq_metrics: WorkqueueMetrics,
) -> (ReconcileSender, ReconcileQueue, ReconcileQueueMetrics) {
    let (tx, rx) = mpsc::channel(capacity);
    let stats = Arc::new(QueueStats::new());
    let sender = ReconcileSender {
        tx,
        stats: stats.clone(),
        draining: Arc::new(AtomicBool::new(false)),
        wq_metrics: wq_metrics.clone(),
    };
    let queue_metrics = ReconcileQueueMetrics {
        stats: stats.clone(),
        wq_metrics: wq_metrics.clone(),
    };
    let queue = ReconcileQueue {
        rx,
        metrics: queue_metrics.clone(),
    };
    (sender, queue, queue_metrics)
}

impl ReconcileQueue {
    pub async fn recv(&mut self) -> Option<(String, String)> {
        let item = self.rx.recv().await?;
        let wait = item.enqueued_at.elapsed().as_secs_f64();
        let labels = QueueNameLabels {
            queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
        };
        self.metrics
            .wq_metrics
            .queue_duration_seconds
            .get_or_create(&labels)
            .observe(wait);
        self.metrics
            .stats
            .depth
            .fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .stats
            .publish_gauges(&self.metrics.wq_metrics, GATEWAY_RECONCILE_QUEUE);
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

pub fn record_workqueue_retry(wq_metrics: &WorkqueueMetrics, queue_name: &str) {
    let labels = QueueNameLabels {
        queue_name: queue_name.to_string(),
    };
    wq_metrics.retries.get_or_create(&labels).inc();
}

impl ReconcileQueue {
    pub fn record_retry(&self) {
        record_workqueue_retry(&self.metrics.wq_metrics, GATEWAY_RECONCILE_QUEUE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn depth_tracks_send_and_recv() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, metrics) = new_reconcile_queue(8, wq);
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
        let wq = WorkqueueMetrics::noop();
        let (_tx, queue, metrics) = new_reconcile_queue(8, wq);
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
        let wq = WorkqueueMetrics::noop();
        let (tx, _queue, _metrics) = new_reconcile_queue(8, wq);
        tx.close();
        assert!(tx.send("ns".into(), "gw".into()).await.is_err());
    }
}
