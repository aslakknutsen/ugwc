//! Gateway reconcile queue with workqueue-style Prometheus metrics.
//!
//! Keys are deduplicated: pending duplicates are dropped, in-flight re-enqueues
//! are coalesced via a dirty bit (client-go workqueue semantics).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use tokio::sync::Notify;

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
    pub dedup: Family<QueueNameLabels, Counter>,
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
            dedup: Family::default(),
            depth: Family::default(),
            longest_running_processor_seconds: Family::default(),
            unfinished_work_seconds: Family::default(),
            queue_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(std::iter::once(1.0))
            }),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct QueueKey {
    ns: String,
    name: String,
}

impl QueueKey {
    fn new(ns: String, name: String) -> Self {
        Self { ns, name }
    }
}

#[derive(Debug)]
struct DedupInner {
    pending: VecDeque<QueueKey>,
    pending_set: HashSet<QueueKey>,
    in_flight: HashSet<QueueKey>,
    dirty: HashSet<QueueKey>,
    enqueued_at: HashMap<QueueKey, Instant>,
    closed: bool,
}

impl DedupInner {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            pending_set: HashSet::new(),
            in_flight: HashSet::new(),
            dirty: HashSet::new(),
            enqueued_at: HashMap::new(),
            closed: false,
        }
    }
}

#[derive(Debug)]
struct DedupQueue {
    inner: Mutex<DedupInner>,
    notify: Notify,
    capacity: usize,
}

enum EnqueueOutcome {
    Added,
    Deduped,
    MarkedDirty,
    Closed,
    Full,
}

impl DedupQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(DedupInner::new()),
            notify: Notify::new(),
            capacity,
        }
    }

    fn try_enqueue(&self, key: QueueKey) -> EnqueueOutcome {
        let mut inner = self.inner.lock().expect("dedup queue lock");
        if inner.closed {
            return EnqueueOutcome::Closed;
        }
        if inner.pending_set.contains(&key) {
            return EnqueueOutcome::Deduped;
        }
        if inner.in_flight.contains(&key) {
            inner.dirty.insert(key);
            return EnqueueOutcome::MarkedDirty;
        }
        if inner.pending.len() >= self.capacity {
            return EnqueueOutcome::Full;
        }
        let now = Instant::now();
        inner.pending.push_back(key.clone());
        inner.pending_set.insert(key.clone());
        inner.enqueued_at.insert(key, now);
        EnqueueOutcome::Added
    }

    async fn enqueue(
        &self,
        key: QueueKey,
        wq_metrics: &WorkqueueMetrics,
        stats: &QueueStats,
    ) -> Result<(), ()> {
        let labels = QueueNameLabels {
            queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
        };
        loop {
            match self.try_enqueue(key.clone()) {
                EnqueueOutcome::Added => {
                    wq_metrics.adds.get_or_create(&labels).inc();
                    sync_depth(stats, &self.inner.lock().expect("dedup queue lock"));
                    stats.publish_gauges(wq_metrics, GATEWAY_RECONCILE_QUEUE);
                    self.notify.notify_waiters();
                    return Ok(());
                }
                EnqueueOutcome::Deduped => {
                    wq_metrics.dedup.get_or_create(&labels).inc();
                    return Ok(());
                }
                EnqueueOutcome::MarkedDirty => return Ok(()),
                EnqueueOutcome::Closed => return Err(()),
                EnqueueOutcome::Full => {
                    self.notify.notified().await;
                }
            }
        }
    }

    async fn dequeue(&self, stats: &QueueStats, wq_metrics: &WorkqueueMetrics) -> Option<(String, String, Instant)> {
        loop {
            let item = {
                let mut inner = self.inner.lock().expect("dedup queue lock");
                if let Some(key) = inner.pending.pop_front() {
                    inner.pending_set.remove(&key);
                    let enqueued_at = inner
                        .enqueued_at
                        .remove(&key)
                        .unwrap_or_else(Instant::now);
                    inner.in_flight.insert(key.clone());
                    inner.dirty.remove(&key);
                    Some((key.ns, key.name, enqueued_at))
                } else if inner.closed {
                    return None;
                } else {
                    None
                }
            };
            if let Some((ns, name, enqueued_at)) = item {
                sync_depth(stats, &self.inner.lock().expect("dedup queue lock"));
                stats.publish_gauges(wq_metrics, GATEWAY_RECONCILE_QUEUE);
                self.notify.notify_waiters();
                return Some((ns, name, enqueued_at));
            }
            self.notify.notified().await;
        }
    }

    fn done(
        &self,
        ns: &str,
        name: &str,
        wq_metrics: &WorkqueueMetrics,
        stats: &QueueStats,
        draining: bool,
    ) {
        let key = QueueKey::new(ns.to_string(), name.to_string());
        let mut inner = self.inner.lock().expect("dedup queue lock");
        if !inner.in_flight.remove(&key) {
            debug_assert!(
                false,
                "done called for key not in flight: {}/{}",
                ns, name
            );
            return;
        }
        let was_dirty = inner.dirty.remove(&key);
        if was_dirty && !inner.closed && !draining {
            let labels = QueueNameLabels {
                queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
            };
            wq_metrics.retries.get_or_create(&labels).inc();
            let now = Instant::now();
            inner.pending.push_back(key.clone());
            inner.pending_set.insert(key.clone());
            inner.enqueued_at.insert(key, now);
            sync_depth(stats, &inner);
            stats.publish_gauges(wq_metrics, GATEWAY_RECONCILE_QUEUE);
            drop(inner);
            self.notify.notify_waiters();
        }
    }

    fn close(&self) {
        let mut inner = self.inner.lock().expect("dedup queue lock");
        inner.closed = true;
        drop(inner);
        self.notify.notify_waiters();
    }
}

fn sync_depth(stats: &QueueStats, inner: &DedupInner) {
    stats
        .depth
        .store(inner.pending.len(), Ordering::Relaxed);
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
    queue: Arc<DedupQueue>,
    stats: Arc<QueueStats>,
    draining: Arc<AtomicBool>,
    wq_metrics: WorkqueueMetrics,
}

impl ReconcileSender {
    pub async fn send(&self, ns: String, name: String) -> Result<(), ()> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(());
        }
        self.queue
            .enqueue(
                QueueKey::new(ns, name),
                &self.wq_metrics,
                &self.stats,
            )
            .await
    }

    /// Stop accepting new reconcile keys (graceful shutdown).
    pub fn close(&self) {
        self.draining.store(true, Ordering::Relaxed);
        self.queue.close();
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
    queue: Arc<DedupQueue>,
    metrics: ReconcileQueueMetrics,
    draining: Arc<AtomicBool>,
}

/// Create a queue and return `(sender, consumer, metrics)`.
#[must_use]
pub fn new_reconcile_queue(
    capacity: usize,
    wq_metrics: WorkqueueMetrics,
) -> (ReconcileSender, ReconcileQueue, ReconcileQueueMetrics) {
    let queue = Arc::new(DedupQueue::new(capacity));
    let stats = Arc::new(QueueStats::new());
    let draining = Arc::new(AtomicBool::new(false));
    let sender = ReconcileSender {
        queue: queue.clone(),
        stats: stats.clone(),
        draining: draining.clone(),
        wq_metrics: wq_metrics.clone(),
    };
    let queue_metrics = ReconcileQueueMetrics {
        stats: stats.clone(),
        wq_metrics: wq_metrics.clone(),
    };
    let consumer = ReconcileQueue {
        queue,
        metrics: queue_metrics.clone(),
        draining,
    };
    (sender, consumer, queue_metrics)
}

impl ReconcileQueue {
    pub async fn recv(&mut self) -> Option<(String, String)> {
        let (ns, name, enqueued_at) =
            self.queue
                .dequeue(&self.metrics.stats, &self.metrics.wq_metrics)
                .await?;
        let wait = enqueued_at.elapsed().as_secs_f64();
        let labels = QueueNameLabels {
            queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
        };
        self.metrics
            .wq_metrics
            .queue_duration_seconds
            .get_or_create(&labels)
            .observe(wait);
        Some((ns, name))
    }

    /// Mark a reconcile key complete; re-enqueues if it was marked dirty while in-flight.
    pub fn done(&self, ns: &str, name: &str) {
        let draining = self.draining.load(Ordering::Relaxed);
        self.queue.done(
            ns,
            name,
            &self.metrics.wq_metrics,
            &self.metrics.stats,
            draining,
        );
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

    fn queue_labels() -> QueueNameLabels {
        QueueNameLabels {
            queue_name: GATEWAY_RECONCILE_QUEUE.to_string(),
        }
    }

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
        queue.done("ns", "gw");
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pending_dedup_collapses_duplicate_sends() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq.clone());
        tx.send("ns".into(), "gw".into()).await.unwrap();
        tx.send("ns".into(), "gw".into()).await.unwrap();
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            1,
            "pending should hold one key"
        );
        assert_eq!(
            wq.dedup.get_or_create(&queue_labels()).get(),
            2,
            "two sends should be deduplicated"
        );
        assert!(queue.recv().await.is_some());
        queue.done("ns", "gw");
        tx.close();
        assert!(queue.recv().await.is_none());
    }

    #[tokio::test]
    async fn dirty_while_in_flight_requeues_once() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(queue.recv().await, Some(("ns".into(), "gw".into())));
        tx.send("ns".into(), "gw".into()).await.unwrap();
        queue.done("ns", "gw");
        assert_eq!(
            queue.recv().await,
            Some(("ns".into(), "gw".into())),
            "dirty key should be re-enqueued"
        );
        queue.done("ns", "gw");
        tx.close();
        assert!(queue.recv().await.is_none());
    }

    #[tokio::test]
    async fn dirty_coalesces_multiple_sends_while_in_flight() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("a".into(), "1".into()).await.unwrap();
        assert_eq!(queue.recv().await, Some(("a".into(), "1".into())));
        for _ in 0..5 {
            tx.send("a".into(), "1".into()).await.unwrap();
        }
        queue.done("a", "1");
        assert_eq!(queue.recv().await, Some(("a".into(), "1".into())));
        queue.done("a", "1");
        tx.close();
        assert!(queue.recv().await.is_none());
    }

    #[tokio::test]
    async fn fifo_ordering_across_keys() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("ns".into(), "a".into()).await.unwrap();
        tx.send("ns".into(), "b".into()).await.unwrap();
        tx.send("ns".into(), "a".into()).await.unwrap();
        assert_eq!(queue.recv().await, Some(("ns".into(), "a".into())));
        queue.done("ns", "a");
        assert_eq!(queue.recv().await, Some(("ns".into(), "b".into())));
        queue.done("ns", "b");
    }

    #[tokio::test]
    async fn in_flight_key_does_not_increment_depth() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(queue.recv().await, Some(("ns".into(), "gw".into())));
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            0,
            "in-flight key must not count toward depth"
        );
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            0,
            "dirty-only key must not count toward depth until done"
        );
        queue.done("ns", "gw");
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            1,
            "dirty re-enqueue increases pending depth"
        );
        assert_eq!(queue.recv().await, Some(("ns".into(), "gw".into())));
        queue.done("ns", "gw");
        assert_eq!(
            queue.metrics.stats.depth.load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn close_drains_pending_then_returns_none() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("ns".into(), "gw".into()).await.unwrap();
        tx.close();
        assert!(tx.send("ns".into(), "other".into()).await.is_err());
        assert_eq!(queue.recv().await, Some(("ns".into(), "gw".into())));
        queue.done("ns", "gw");
        assert!(queue.recv().await.is_none());
    }

    #[tokio::test]
    async fn close_drops_dirty_reenqueue() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(8, wq);
        tx.send("ns".into(), "gw".into()).await.unwrap();
        assert_eq!(queue.recv().await, Some(("ns".into(), "gw".into())));
        tx.send("ns".into(), "gw".into()).await.unwrap();
        tx.close();
        queue.done("ns", "gw");
        assert!(queue.recv().await.is_none());
    }

    #[tokio::test]
    async fn capacity_backpressure_unblocks_after_dequeue_and_done() {
        let wq = WorkqueueMetrics::noop();
        let (tx, mut queue, _metrics) = new_reconcile_queue(2, wq);
        tx.send("ns".into(), "a".into()).await.unwrap();
        tx.send("ns".into(), "b".into()).await.unwrap();
        let tx_bg = tx.clone();
        let blocked = tokio::spawn(async move {
            tx_bg.send("ns".into(), "c".into()).await
        });
        tokio::task::yield_now().await;
        assert!(
            !blocked.is_finished(),
            "third send should block at capacity"
        );
        assert_eq!(queue.recv().await, Some(("ns".into(), "a".into())));
        queue.done("ns", "a");
        assert_eq!(blocked.await.unwrap(), Ok(()));
        assert_eq!(queue.recv().await, Some(("ns".into(), "b".into())));
        queue.done("ns", "b");
        assert_eq!(queue.recv().await, Some(("ns".into(), "c".into())));
        queue.done("ns", "c");
        tx.close();
        assert!(queue.recv().await.is_none());
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
