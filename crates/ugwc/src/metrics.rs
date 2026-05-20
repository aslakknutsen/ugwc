//! Prometheus metrics (`/metrics` on admin router).
//!
//! All `counter!` / `histogram!` / `gauge!` macros for the `ugwc` binary live here
//! (snake_case names only). Workqueue metrics are recorded from `ugwc-k8s` via the
//! same global `metrics` recorder after [`install`].

use std::sync::Mutex;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;
use prometheus::Encoder;

static PROM: OnceCell<PrometheusHandle> = OnceCell::new();
static PROCESS_REGISTRY: OnceCell<Mutex<prometheus::Registry>> = OnceCell::new();

pub fn install() -> Result<&'static PrometheusHandle, String> {
    PROM.get_or_try_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .map_err(|e| e.to_string())
    })?;
    PROCESS_REGISTRY.get_or_init(|| {
        let registry = prometheus::Registry::new();
        let collector = prometheus::process_collector::ProcessCollector::for_self();
        registry
            .register(Box::new(collector))
            .expect("register process collector");
        Mutex::new(registry)
    });
    PROM.get()
        .ok_or_else(|| "metrics install failed".to_string())
}

/// Prometheus text exposition for process collector (appended to facade `/metrics`).
pub fn render_process_metrics() -> Result<String, String> {
    let registry = PROCESS_REGISTRY
        .get()
        .ok_or_else(|| "process registry not installed".to_string())?;
    let guard = registry.lock().map_err(|e| e.to_string())?;
    let metric_families = guard.gather();
    let encoder = prometheus::TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&metric_families, &mut buf)
        .map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

// --- Reconcile SLO (controller_name, status) ---

pub fn record_reconcile_run(controller_name: &str, success: bool) {
    let status = if success { "success" } else { "error" };
    counter!(
        "ugwc_reconcile_runs_total",
        "controller_name" => controller_name.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn observe_reconcile_duration_seconds(controller_name: &str, seconds: f64) {
    histogram!(
        "ugwc_reconcile_duration_seconds",
        "controller_name" => controller_name.to_string()
    )
    .record(seconds);
}

// --- Domain counters / histograms ---

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

pub fn set_cache_synced(synced: bool) {
    gauge!("ugwc_cache_synced").set(if synced { 1.0 } else { 0.0 });
}

pub fn observe_compile_seconds(seconds: f64) {
    histogram!("ugwc_compile_duration_seconds").record(seconds);
}

pub fn observe_snapshot_load_seconds(seconds: f64) {
    histogram!("ugwc_snapshot_load_duration_seconds").record(seconds);
}

pub fn record_dataplane_apply() {
    counter!("ugwc_dataplane_apply_total").increment(1);
}

pub fn record_dataplane_error() {
    counter!("ugwc_dataplane_error_total").increment(1);
}
