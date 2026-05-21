//! Optional Tokio runtime metrics (`ugwc_tokio_*`). Enable with feature `tokio-metrics` and
//! `RUSTFLAGS='--cfg tokio_unstable'` for release images that need scheduler visibility.

#[cfg(feature = "tokio-metrics")]
pub fn spawn() {
    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    ugwc_k8s::spawn_with_trace_context(async move {
        for m in runtime_monitor.intervals() {
            metrics::gauge!("ugwc_tokio_workers_count").set(m.workers_count as f64);
            metrics::gauge!("ugwc_tokio_total_busy_duration_seconds")
                .set(m.total_busy_duration.as_secs_f64());
            metrics::gauge!("ugwc_tokio_total_polls_count").set(m.total_polls_count as f64);
            metrics::gauge!("ugwc_tokio_total_noop_count").set(m.total_noop_count as f64);
            metrics::gauge!("ugwc_tokio_total_steal_count").set(m.total_steal_count as f64);
            metrics::gauge!("ugwc_tokio_total_steal_operations")
                .set(m.total_steal_operations as f64);
            metrics::gauge!("ugwc_tokio_total_local_schedule_count")
                .set(m.total_local_schedule_count as f64);
            metrics::gauge!("ugwc_tokio_total_overflow_count")
                .set(m.total_overflow_count as f64);
            metrics::gauge!("ugwc_tokio_injection_queue_depth")
                .set(m.blocking_queue_depth as f64);
            metrics::gauge!("ugwc_tokio_budget_forced_yield_count")
                .set(m.budget_forced_yield_count as f64);
            metrics::gauge!("ugwc_tokio_total_local_queue_depth")
                .set(m.total_local_queue_depth as f64);
            let mean_poll_ns = if m.total_polls_count > 0 {
                m.total_busy_duration.as_nanos() as f64 / m.total_polls_count as f64
            } else {
                0.0
            };
            metrics::gauge!("ugwc_tokio_mean_poll_duration_seconds")
                .set(mean_poll_ns / 1_000_000_000.0);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

#[cfg(not(feature = "tokio-metrics"))]
pub fn spawn() {}
