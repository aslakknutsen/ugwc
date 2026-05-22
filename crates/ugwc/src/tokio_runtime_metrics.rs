//! Optional Tokio runtime metrics (`ugwc_tokio_*`). Enable with feature `tokio-metrics` and
//! `RUSTFLAGS='--cfg tokio_unstable'` for release images that need scheduler visibility.

use crate::metrics::TokioMetrics;

#[cfg(feature = "tokio-metrics")]
pub fn spawn(metrics: TokioMetrics) {
    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    ugwc_k8s::spawn_with_trace_context(async move {
        for m in runtime_monitor.intervals() {
            metrics.workers_count.set(m.workers_count as i64);
            metrics
                .total_busy_duration_seconds
                .set(m.total_busy_duration.as_secs_f64() as i64);
            metrics.total_polls_count.set(m.total_polls_count as i64);
            metrics.total_noop_count.set(m.total_noop_count as i64);
            metrics.total_steal_count.set(m.total_steal_count as i64);
            metrics
                .total_steal_operations
                .set(m.total_steal_operations as i64);
            metrics
                .total_local_schedule_count
                .set(m.total_local_schedule_count as i64);
            metrics
                .total_overflow_count
                .set(m.total_overflow_count as i64);
            metrics
                .injection_queue_depth
                .set(m.blocking_queue_depth as i64);
            metrics
                .budget_forced_yield_count
                .set(m.budget_forced_yield_count as i64);
            metrics
                .total_local_queue_depth
                .set(m.total_local_queue_depth as i64);
            let mean_poll_ns = if m.total_polls_count > 0 {
                m.total_busy_duration.as_nanos() as f64 / m.total_polls_count as f64
            } else {
                0.0
            };
            metrics
                .mean_poll_duration_seconds
                .set((mean_poll_ns / 1_000_000_000.0) as i64);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

#[cfg(not(feature = "tokio-metrics"))]
pub fn spawn(_metrics: TokioMetrics) {}
