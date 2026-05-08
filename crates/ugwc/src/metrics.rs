//! Phase 10 — Prometheus metrics (`/metrics` on admin router).

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;

static PROM: OnceCell<PrometheusHandle> = OnceCell::new();

pub fn install() -> Result<&'static PrometheusHandle, String> {
    PROM.get_or_try_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .map_err(|e| e.to_string())
    })
}

pub fn record_reconcile() {
    counter!("ugwc_reconcile_total").increment(1);
}

pub fn record_compile_error() {
    counter!("ugwc_compile_errors_total").increment(1);
}

pub fn record_publish_skipped() {
    counter!("ugwc_publish_skipped_total").increment(1);
}

pub fn record_publish_applied() {
    counter!("ugwc_publish_applied_total").increment(1);
}

pub fn set_leader_active(active: bool) {
    gauge!("ugwc_leader_active").set(if active { 1.0 } else { 0.0 });
}

pub fn observe_compile_seconds(seconds: f64) {
    histogram!("ugwc_compile_duration_seconds").record(seconds);
}

pub fn observe_snapshot_load_seconds(seconds: f64) {
    histogram!("ugwc_snapshot_load_duration_seconds").record(seconds);
}
