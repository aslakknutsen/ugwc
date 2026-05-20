//! Optional Tokio runtime metrics (`ugwc_tokio_*`). Enable with feature `tokio-metrics` and
//! `RUSTFLAGS='--cfg tokio_unstable'` for release images that need scheduler visibility.

#[cfg(feature = "tokio-metrics")]
pub fn spawn() {
    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    tokio::spawn(async move {
        for interval in runtime_monitor.intervals() {
            let m = interval.metrics();
            metrics::gauge!("ugwc_tokio_workers_count").set(m.workers_count as f64);
            metrics::gauge!("ugwc_tokio_total_busy_duration_seconds")
                .set(m.total_busy_duration.as_secs_f64());
            metrics::gauge!("ugwc_tokio_total_polls_count").set(m.total_polls_count as f64);
            metrics::gauge!("ugwc_tokio_total_noop_count").set(m.total_noop_count as f64);
        }
    });
}

#[cfg(not(feature = "tokio-metrics"))]
pub fn spawn() {}
