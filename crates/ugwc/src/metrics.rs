//! Prometheus metrics (`/metrics` on admin router).
//!
//! Facade macros bridge into a [`prometheus_client::Registry`] via
//! [`metrics_prometheus_client::MetricsCollector`]. Process metrics use the
//! `metrics-process` crate which registers through the `metrics` facade.
//!
//! `reconcile_duration_seconds` uses a native `prometheus_client` histogram so
//! that exemplars (with `trace_id`) can be attached — the facade bridge cannot
//! carry exemplar labels.

use std::sync::{Mutex, OnceLock};

use metrics::{counter, gauge, histogram};
use metrics_prometheus_client::MetricsCollector;
use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::exemplar::HistogramWithExemplars;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::exponential_buckets;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ControllerLabel {
    controller_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TraceIdLabel {
    trace_id: String,
}

type ReconcileHistogramFamily = Family<ControllerLabel, HistogramWithExemplars<TraceIdLabel>>;

struct MetricsState {
    registry: Mutex<Registry>,
    reconcile_duration: ReconcileHistogramFamily,
}

static STATE: OnceLock<MetricsState> = OnceLock::new();
static PROCESS_COLLECTOR: OnceLock<metrics_process::Collector> = OnceLock::new();

/// Install the global metrics recorder and process collector.
pub fn install() -> Result<(), String> {
    if STATE.get().is_some() {
        return Ok(());
    }
    let mut registry = Registry::default();
    let collector: MetricsCollector = metrics_prometheus_client::install();
    registry.register_collector(Box::new(collector));

    let reconcile_duration: ReconcileHistogramFamily = Family::new_with_constructor(|| {
        HistogramWithExemplars::new(exponential_buckets(0.005, 2.0, 12))
    });
    registry.register(
        "ugwc_reconcile_duration_seconds",
        "Duration of gateway reconcile in seconds",
        reconcile_duration.clone(),
    );

    let _ = STATE.set(MetricsState {
        registry: Mutex::new(registry),
        reconcile_duration,
    });
    let process = PROCESS_COLLECTOR.get_or_init(|| {
        let c = metrics_process::Collector::default();
        c.describe();
        c
    });
    let _ = process;
    Ok(())
}

/// Full Prometheus text exposition for `/metrics`.
pub fn render() -> Result<String, String> {
    let state = STATE.get().ok_or_else(|| "metrics not installed".to_string())?;
    if let Some(process) = PROCESS_COLLECTOR.get() {
        process.collect();
    }
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

/// Reconcile latency histogram with trace_id exemplar (when OTel is active).
pub fn observe_reconcile_duration_seconds(controller_name: &str, seconds: f64) {
    let Some(state) = STATE.get() else { return };
    let label = ControllerLabel {
        controller_name: controller_name.to_string(),
    };
    let exemplar = current_trace_id_exemplar();
    state
        .reconcile_duration
        .get_or_create(&label)
        .observe(seconds, exemplar);
}

/// Extract the active OTel trace_id (if any) for histogram exemplars.
fn current_trace_id_exemplar() -> Option<TraceIdLabel> {
    #[cfg(feature = "otlp-traces")]
    {
        use opentelemetry::trace::TraceContextExt;
        let cx = opentelemetry::Context::current();
        let sc = cx.span().span_context().clone();
        if sc.is_valid() {
            return Some(TraceIdLabel {
                trace_id: sc.trace_id().to_string(),
            });
        }
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_exports_facade_counters() {
        install().expect("metrics install");
        counter!(
            "ugwc_reconcile_runs_total",
            "controller_name" => "test",
            "status" => "success"
        )
        .increment(1);
        gauge!(
            "gatewayapi_gateway_status",
            "namespace" => "ns",
            "name" => "gw",
            "status" => "Programmed",
            "source" => "ugwc"
        )
        .set(1.0);
        let body = render().expect("render");
        assert!(
            body.contains("ugwc_reconcile_runs_total"),
            "missing reconcile counter in:\n{body}"
        );
        assert!(
            body.contains("gatewayapi_gateway_status"),
            "missing gateway status gauge in:\n{body}"
        );
        // metrics 0.24 re-registers on every macro call; must not panic on repeat.
        counter!(
            "ugwc_reconcile_runs_total",
            "controller_name" => "test",
            "status" => "success"
        )
        .increment(1);
        counter!(
            "ugwc_compile_errors_total",
        )
        .increment(1);
        counter!("ugwc_compile_errors_total").increment(1);
        let body = render().expect("render after repeat");
        assert!(body.contains("ugwc_compile_errors_total"));
    }
}
