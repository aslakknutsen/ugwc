//! Prometheus metrics (`/metrics` on admin router).
//!
//! Facade macros bridge into a [`prometheus_client::Registry`] via
//! [`metrics_prometheus_client::MetricsCollector`]. Process metrics use the
//! `metrics-process` crate which registers through the `metrics` facade.

use std::sync::{Mutex, OnceLock};

use metrics::{counter, gauge, histogram};
use metrics_prometheus_client::MetricsCollector;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;

struct MetricsState {
    registry: Mutex<Registry>,
}

static STATE: OnceLock<MetricsState> = OnceLock::new();

/// Install the global metrics recorder and process collector.
pub fn install() -> Result<(), String> {
    if STATE.get().is_some() {
        return Ok(());
    }
    let mut registry = Registry::default();
    let collector: MetricsCollector = metrics_prometheus_client::install();
    registry.register_collector(Box::new(collector));
    let _ = STATE.set(MetricsState {
        registry: Mutex::new(registry),
    });
    metrics_process::Collector::default().describe();
    Ok(())
}

/// Full Prometheus text exposition for `/metrics`.
pub fn render() -> Result<String, String> {
    let state = STATE.get().ok_or_else(|| "metrics not installed".to_string())?;
    metrics_process::Collector::default().collect();
    let registry = state.registry.lock().map_err(|e| e.to_string())?;
    let mut body = String::new();
    encode(&mut body, &registry).map_err(|e| e.to_string())?;
    Ok(body)
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

/// Reconcile latency histogram. Exemplars linking to the active trace_id require
/// native histogram support (not yet available via the `metrics` facade bridge).
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

/// Increment per cross-namespace `RefNotPermitted` finding from a compile pass.
pub fn record_ungranted_references(report: &ugwc_core::CompileReport) {
    for err in &report.errors {
        if err.reason == "RefNotPermitted" {
            counter!(
                "ugwc_ungranted_reference_total",
                "route_kind" => err.kind.clone()
            )
            .increment(1);
        }
    }
}
