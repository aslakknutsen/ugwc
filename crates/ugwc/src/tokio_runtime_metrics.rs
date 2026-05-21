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
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

#[cfg(not(feature = "tokio-metrics"))]
pub fn spawn() {}
