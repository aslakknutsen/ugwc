//! Prometheus metrics (`/metrics` on admin router).
//!
//! All metrics use `prometheus-client` `Family` types directly — no global
//! recorder or facade bridge.  Process metrics via `kubert-prometheus-process`.
//!
//! `reconcile_duration_seconds` carries trace-id exemplars.

use std::sync::{Mutex, OnceLock};

use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::exemplar::HistogramWithExemplars;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

// ---------------------------------------------------------------------------
// Label sets
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ControllerStatusLabels {
    pub controller_name: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ControllerLabel {
    pub controller_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TraceIdLabel {
    pub trace_id: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteKindLabels {
    pub route_kind: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodStatusLabels {
    pub method: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PraxisFeatureLabels {
    pub feature: String,
}

// Empty label set for metrics without labels.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NoLabels;

impl prometheus_client::encoding::EncodeLabelSet for NoLabels {
    fn encode(
        &self,
        _encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Metrics handles — passed into subsystems at startup
// ---------------------------------------------------------------------------

type ReconcileHistogramFamily = Family<ControllerLabel, HistogramWithExemplars<TraceIdLabel>>;

/// Core reconcile/compile/publish metrics.
#[derive(Clone)]
pub struct ReconcileMetrics {
    pub reconcile_runs: Family<ControllerStatusLabels, Counter>,
    pub reconcile_duration: ReconcileHistogramFamily,
    pub compile_errors: Counter,
    pub compile_duration: Family<NoLabels, Histogram>,
    pub snapshot_load_duration: Family<NoLabels, Histogram>,
    pub status_patch_duration: Family<NoLabels, Histogram>,
    pub status_patch_count: Family<NoLabels, Histogram>,
    pub publish_skipped: Counter,
    pub publish_applied: Counter,
    pub leader_active: Gauge,
    pub cache_synced: Gauge,
    pub ir_listeners: Gauge,
    pub ir_routes: Gauge,
    pub ir_backends: Gauge,
    pub dataplane_apply: Counter,
    pub dataplane_error: Counter,
    pub praxis_config_encode_duration: Family<NoLabels, Histogram>,
    pub praxis_config_apply: Counter,
    pub praxis_config_error: Counter,
    pub praxis_config_unsupported: Family<PraxisFeatureLabels, Counter>,
    pub ungranted_references: Family<RouteKindLabels, Counter>,
}

/// Admin HTTP request metrics.
#[derive(Clone)]
pub struct AdminMetrics {
    pub request_duration: Family<MethodStatusLabels, Histogram>,
}

/// Workqueue metrics for ugwc-k8s (re-use the struct from ugwc_k8s).
pub type WorkqueueMetrics = ugwc_k8s::WorkqueueMetrics;

/// Kubernetes REST client metrics (re-use the struct from ugwc_k8s).
pub type KubeClientMetrics = ugwc_k8s::KubeClientMetrics;

/// xDS metrics: server-side handles + scope miss counter for cache.
#[derive(Clone)]
pub struct XdsMetrics {
    pub server: ugwc_xds::XdsMetrics,
    pub scope_miss: Counter,
}

/// Tokio runtime gauges (feature-gated).
#[derive(Clone)]
#[allow(dead_code)]
pub struct TokioMetrics {
    pub workers_count: Gauge,
    pub total_busy_duration_seconds: Gauge,
    pub total_polls_count: Gauge,
    pub total_noop_count: Gauge,
    pub total_steal_count: Gauge,
    pub total_steal_operations: Gauge,
    pub total_local_schedule_count: Gauge,
    pub total_overflow_count: Gauge,
    pub injection_queue_depth: Gauge,
    pub budget_forced_yield_count: Gauge,
    pub total_local_queue_depth: Gauge,
    pub mean_poll_duration_seconds: Gauge,
}

/// Top-level handle holding all metric families. Cheap to clone (all Arc-backed internally).
#[derive(Clone)]
pub struct Metrics {
    pub reconcile: ReconcileMetrics,
    pub admin: AdminMetrics,
    pub workqueue: WorkqueueMetrics,
    pub kube_client: KubeClientMetrics,
    pub xds: XdsMetrics,
    pub tokio: TokioMetrics,
    pub gateway_state: crate::gateway_state_metrics::GatewayStateMetrics,
    pub index: ugwc_k8s::GatewayIndexMetrics,
}

// ---------------------------------------------------------------------------
// Global state (registry + handles)
// ---------------------------------------------------------------------------

struct MetricsState {
    registry: Mutex<Registry>,
    handles: Metrics,
}

static STATE: OnceLock<MetricsState> = OnceLock::new();

/// Build the registry, register all families, and return the metrics handles.
/// Safe to call multiple times (idempotent).
pub fn install() -> Result<Metrics, String> {
    if let Some(s) = STATE.get() {
        return Ok(s.handles.clone());
    }

    let mut registry = Registry::default();

    // -- Process metrics (kubert-prometheus-process) --
    let _ = kubert_prometheus_process::register(registry.sub_registry_with_prefix("process"));

    // -- Reconcile --
    let reconcile_runs = Family::<ControllerStatusLabels, Counter>::default();
    registry.register(
        "ugwc_reconcile_runs",
        "Total gateway reconcile runs",
        reconcile_runs.clone(),
    );

    let reconcile_duration: ReconcileHistogramFamily = Family::new_with_constructor(|| {
        HistogramWithExemplars::new(exponential_buckets(0.005, 2.0, 12))
    });
    registry.register(
        "ugwc_reconcile_duration_seconds",
        "Duration of gateway reconcile in seconds",
        reconcile_duration.clone(),
    );

    let compile_errors = Counter::default();
    registry.register(
        "ugwc_compile_errors",
        "Compile errors total",
        compile_errors.clone(),
    );

    let compile_duration = Family::<NoLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.001, 2.0, 14))
    });
    registry.register(
        "ugwc_compile_duration_seconds",
        "Duration of compile step in seconds",
        compile_duration.clone(),
    );

    let snapshot_load_duration = Family::<NoLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.001, 2.0, 14))
    });
    registry.register(
        "ugwc_snapshot_load_duration_seconds",
        "Duration of cluster snapshot load in seconds",
        snapshot_load_duration.clone(),
    );

    let status_patch_duration = Family::<NoLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.001, 2.0, 14))
    });
    registry.register(
        "ugwc_status_patch_duration_seconds",
        "Duration of attached resource status patches per gateway reconcile",
        status_patch_duration.clone(),
    );

    let status_patch_count = Family::<NoLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(1.0, 2.0, 12))
    });
    registry.register(
        "ugwc_status_patch_count",
        "Number of status patch tasks per gateway reconcile",
        status_patch_count.clone(),
    );

    let publish_skipped = Counter::default();
    registry.register(
        "ugwc_publish_skipped",
        "Publish skipped total",
        publish_skipped.clone(),
    );

    let publish_applied = Counter::default();
    registry.register(
        "ugwc_publish_applied",
        "Publish applied total",
        publish_applied.clone(),
    );

    let leader_active = Gauge::default();
    registry.register(
        "ugwc_leader_active",
        "Leader election active",
        leader_active.clone(),
    );

    let cache_synced = Gauge::default();
    registry.register(
        "ugwc_cache_synced",
        "Cache synced (readiness)",
        cache_synced.clone(),
    );

    let ir_listeners = Gauge::default();
    registry.register(
        "ugwc_compile_ir_listeners",
        "IR listener count",
        ir_listeners.clone(),
    );

    let ir_routes = Gauge::default();
    registry.register(
        "ugwc_compile_ir_routes",
        "IR route count",
        ir_routes.clone(),
    );

    let ir_backends = Gauge::default();
    registry.register(
        "ugwc_compile_ir_backends",
        "IR backend count",
        ir_backends.clone(),
    );

    let dataplane_apply = Counter::default();
    registry.register(
        "ugwc_dataplane_apply",
        "Dataplane apply total",
        dataplane_apply.clone(),
    );

    let dataplane_error = Counter::default();
    registry.register(
        "ugwc_dataplane_error",
        "Dataplane error total",
        dataplane_error.clone(),
    );

    let praxis_config_encode_duration = Family::<NoLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.0001, 2.0, 14))
    });
    registry.register(
        "ugwc_praxis_config_encode_seconds",
        "Duration of encoding gateway IR into Praxis native configuration",
        praxis_config_encode_duration.clone(),
    );

    let praxis_config_apply = Counter::default();
    registry.register(
        "ugwc_praxis_config_apply",
        "Praxis ConfigMap apply total",
        praxis_config_apply.clone(),
    );

    let praxis_config_error = Counter::default();
    registry.register(
        "ugwc_praxis_config_error",
        "Praxis ConfigMap encode/apply error total",
        praxis_config_error.clone(),
    );

    let praxis_config_unsupported = Family::<PraxisFeatureLabels, Counter>::default();
    registry.register(
        "ugwc_praxis_config_unsupported",
        "Gateway API features unsupported by the Praxis native-config encoder",
        praxis_config_unsupported.clone(),
    );

    let ungranted_references = Family::<RouteKindLabels, Counter>::default();
    registry.register(
        "ugwc_ungranted_reference",
        "Cross-namespace RefNotPermitted findings",
        ungranted_references.clone(),
    );

    // -- Admin --
    let admin_request_duration =
        Family::<MethodStatusLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.001, 2.0, 12))
        });
    registry.register(
        "ugwc_admin_request_duration_seconds",
        "Admin HTTP request duration",
        admin_request_duration.clone(),
    );

    // -- Workqueue --
    use ugwc_k8s::reconcile_queue::QueueNameLabels;
    let wq_adds = Family::<QueueNameLabels, Counter>::default();
    registry.register(
        "ugwc_workqueue_adds",
        "Workqueue adds total",
        wq_adds.clone(),
    );

    let wq_retries = Family::<QueueNameLabels, Counter>::default();
    registry.register(
        "ugwc_workqueue_retries",
        "Workqueue retries total",
        wq_retries.clone(),
    );

    let wq_dedup = Family::<QueueNameLabels, Counter>::default();
    registry.register(
        "ugwc_workqueue_dedup",
        "Workqueue enqueue calls deduplicated (key already pending)",
        wq_dedup.clone(),
    );

    let wq_depth = Family::<QueueNameLabels, Gauge>::default();
    registry.register("ugwc_workqueue_depth", "Workqueue depth", wq_depth.clone());

    let wq_in_flight = Family::<QueueNameLabels, Gauge>::default();
    registry.register(
        "ugwc_workqueue_in_flight",
        "Reconcile keys currently being processed",
        wq_in_flight.clone(),
    );

    let wq_longest = Family::<QueueNameLabels, Gauge>::default();
    registry.register(
        "ugwc_workqueue_longest_running_processor_seconds",
        "Longest running processor seconds",
        wq_longest.clone(),
    );

    let wq_unfinished = Family::<QueueNameLabels, Gauge>::default();
    registry.register(
        "ugwc_workqueue_unfinished_work_seconds",
        "Unfinished work seconds",
        wq_unfinished.clone(),
    );

    let wq_queue_duration = Family::<QueueNameLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.001, 2.0, 14))
    });
    registry.register(
        "ugwc_workqueue_queue_duration_seconds",
        "Time spent in queue before processing",
        wq_queue_duration.clone(),
    );

    // -- Kube REST client --
    use ugwc_k8s::kube_metrics::RestClientLabels;
    let kube_requests = Family::<RestClientLabels, Counter>::default();
    registry.register(
        "ugwc_rest_client_requests",
        "Kubernetes REST client requests total",
        kube_requests.clone(),
    );

    let kube_request_duration = Family::<RestClientLabels, Histogram>::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.001, 2.0, 14))
    });
    registry.register(
        "ugwc_rest_client_request_duration_seconds",
        "Kubernetes REST client request duration",
        kube_request_duration.clone(),
    );

    // -- xDS --
    let xds_streams_active = Gauge::default();
    registry.register(
        "ugwc_xds_streams_active",
        "Active ADS streams",
        xds_streams_active.clone(),
    );

    let xds_disconnects = Family::<ugwc_xds::XdsDisconnectLabels, Counter>::default();
    registry.register(
        "ugwc_xds_stream_disconnects",
        "xDS stream disconnects total",
        xds_disconnects.clone(),
    );

    let xds_push_bytes = Counter::default();
    registry.register(
        "ugwc_xds_push_bytes",
        "xDS push bytes total",
        xds_push_bytes.clone(),
    );

    let xds_push_resources = Gauge::default();
    registry.register(
        "ugwc_xds_push_resources",
        "xDS push resource count",
        xds_push_resources.clone(),
    );

    let xds_push_duration =
        Family::<ugwc_xds::XdsNoLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.0001, 2.0, 14))
        });
    registry.register(
        "ugwc_xds_push_duration_seconds",
        "xDS push encoding duration",
        xds_push_duration.clone(),
    );

    let xds_scope_miss = Counter::default();
    registry.register(
        "ugwc_xds_scope_miss",
        "xDS scope miss total",
        xds_scope_miss.clone(),
    );

    // -- Tokio runtime --
    let tokio_workers = Gauge::default();
    registry.register(
        "ugwc_tokio_workers_count",
        "Tokio worker count",
        tokio_workers.clone(),
    );

    let tokio_busy = Gauge::default();
    registry.register(
        "ugwc_tokio_total_busy_duration_seconds",
        "Tokio total busy duration",
        tokio_busy.clone(),
    );

    let tokio_polls = Gauge::default();
    registry.register(
        "ugwc_tokio_total_polls_count",
        "Tokio total polls",
        tokio_polls.clone(),
    );

    let tokio_noop = Gauge::default();
    registry.register(
        "ugwc_tokio_total_noop_count",
        "Tokio total noop count",
        tokio_noop.clone(),
    );

    let tokio_steal = Gauge::default();
    registry.register(
        "ugwc_tokio_total_steal_count",
        "Tokio total steal count",
        tokio_steal.clone(),
    );

    let tokio_steal_ops = Gauge::default();
    registry.register(
        "ugwc_tokio_total_steal_operations",
        "Tokio total steal operations",
        tokio_steal_ops.clone(),
    );

    let tokio_local_sched = Gauge::default();
    registry.register(
        "ugwc_tokio_total_local_schedule_count",
        "Tokio local schedule count",
        tokio_local_sched.clone(),
    );

    let tokio_overflow = Gauge::default();
    registry.register(
        "ugwc_tokio_total_overflow_count",
        "Tokio overflow count",
        tokio_overflow.clone(),
    );

    let tokio_inject_depth = Gauge::default();
    registry.register(
        "ugwc_tokio_injection_queue_depth",
        "Tokio injection queue depth",
        tokio_inject_depth.clone(),
    );

    let tokio_yield = Gauge::default();
    registry.register(
        "ugwc_tokio_budget_forced_yield_count",
        "Tokio budget forced yield count",
        tokio_yield.clone(),
    );

    let tokio_local_depth = Gauge::default();
    registry.register(
        "ugwc_tokio_total_local_queue_depth",
        "Tokio total local queue depth",
        tokio_local_depth.clone(),
    );

    let tokio_mean_poll = Gauge::default();
    registry.register(
        "ugwc_tokio_mean_poll_duration_seconds",
        "Tokio mean poll duration",
        tokio_mean_poll.clone(),
    );

    // -- Gateway API state gauges --
    let gateway_state = crate::gateway_state_metrics::GatewayStateMetrics::register(&mut registry);

    // -- Reverse index fan-out --
    let index_fanout_size =
        Family::<ugwc_k8s::IndexEventKindLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(
                [
                    0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
                ]
                .into_iter(),
            )
        });
    registry.register(
        "ugwc_index_fanout_size",
        "Gateways enqueued per reverse-index fan-out event",
        index_fanout_size.clone(),
    );

    let index_gateways_indexed = Gauge::default();
    registry.register(
        "ugwc_index_gateways_indexed",
        "Gateways present in the reverse index",
        index_gateways_indexed.clone(),
    );

    let handles = Metrics {
        reconcile: ReconcileMetrics {
            reconcile_runs,
            reconcile_duration,
            compile_errors,
            compile_duration,
            snapshot_load_duration,
            status_patch_duration,
            status_patch_count,
            publish_skipped,
            publish_applied,
            leader_active,
            cache_synced,
            ir_listeners,
            ir_routes,
            ir_backends,
            dataplane_apply,
            dataplane_error,
            praxis_config_encode_duration,
            praxis_config_apply,
            praxis_config_error,
            praxis_config_unsupported,
            ungranted_references,
        },
        admin: AdminMetrics {
            request_duration: admin_request_duration,
        },
        workqueue: WorkqueueMetrics {
            adds: wq_adds,
            retries: wq_retries,
            dedup: wq_dedup,
            depth: wq_depth,
            in_flight: wq_in_flight,
            longest_running_processor_seconds: wq_longest,
            unfinished_work_seconds: wq_unfinished,
            queue_duration_seconds: wq_queue_duration,
        },
        kube_client: KubeClientMetrics {
            requests: kube_requests,
            request_duration: kube_request_duration,
        },
        xds: XdsMetrics {
            server: ugwc_xds::XdsMetrics {
                streams_active: xds_streams_active,
                stream_disconnects: xds_disconnects,
                push_bytes: xds_push_bytes,
                push_resources: xds_push_resources,
                push_duration: xds_push_duration,
            },
            scope_miss: xds_scope_miss,
        },
        tokio: TokioMetrics {
            workers_count: tokio_workers,
            total_busy_duration_seconds: tokio_busy,
            total_polls_count: tokio_polls,
            total_noop_count: tokio_noop,
            total_steal_count: tokio_steal,
            total_steal_operations: tokio_steal_ops,
            total_local_schedule_count: tokio_local_sched,
            total_overflow_count: tokio_overflow,
            injection_queue_depth: tokio_inject_depth,
            budget_forced_yield_count: tokio_yield,
            total_local_queue_depth: tokio_local_depth,
            mean_poll_duration_seconds: tokio_mean_poll,
        },
        gateway_state,
        index: ugwc_k8s::GatewayIndexMetrics {
            fanout_size: index_fanout_size,
            gateways_indexed: index_gateways_indexed,
        },
    };

    let _ = STATE.set(MetricsState {
        registry: Mutex::new(registry),
        handles: handles.clone(),
    });

    Ok(handles)
}

/// Get the installed metrics handles (panics if install() was not called).
pub fn get() -> &'static Metrics {
    &STATE.get().expect("metrics::install() not called").handles
}

/// Full Prometheus text exposition for `/metrics`.
pub fn render() -> Result<String, String> {
    let state = STATE
        .get()
        .ok_or_else(|| "metrics not installed".to_string())?;
    let registry = state.registry.lock().map_err(|e| e.to_string())?;
    let mut body = String::new();
    encode(&mut body, &registry).map_err(|e| e.to_string())?;
    Ok(body)
}

// ---------------------------------------------------------------------------
// Convenience functions (thin wrappers for backward compat from reconcile.rs)
// ---------------------------------------------------------------------------

pub fn record_reconcile_run(controller_name: &str, success: bool) {
    let m = get();
    let status = if success { "success" } else { "error" };
    m.reconcile
        .reconcile_runs
        .get_or_create(&ControllerStatusLabels {
            controller_name: controller_name.to_string(),
            status: status.to_string(),
        })
        .inc();
}

pub fn observe_reconcile_duration_seconds(controller_name: &str, seconds: f64) {
    let m = get();
    let label = ControllerLabel {
        controller_name: controller_name.to_string(),
    };
    let exemplar = current_trace_id_exemplar();
    m.reconcile
        .reconcile_duration
        .get_or_create(&label)
        .observe(seconds, exemplar);
}

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

pub fn record_compile_error() {
    get().reconcile.compile_errors.inc();
}

pub fn record_publish_skipped() {
    get().reconcile.publish_skipped.inc();
}

pub fn record_publish_applied() {
    get().reconcile.publish_applied.inc();
}

pub fn set_leader_active(active: bool) {
    let g = &get().reconcile.leader_active;
    g.set(if active { 1 } else { 0 });
}

pub fn set_cache_synced(synced: bool) {
    let g = &get().reconcile.cache_synced;
    g.set(if synced { 1 } else { 0 });
}

pub fn observe_compile_seconds(seconds: f64) {
    get()
        .reconcile
        .compile_duration
        .get_or_create(&NoLabels)
        .observe(seconds);
}

pub fn observe_snapshot_load_seconds(seconds: f64) {
    get()
        .reconcile
        .snapshot_load_duration
        .get_or_create(&NoLabels)
        .observe(seconds);
}

pub fn observe_status_patch_duration_seconds(seconds: f64) {
    get()
        .reconcile
        .status_patch_duration
        .get_or_create(&NoLabels)
        .observe(seconds);
}

pub fn record_status_patch_count(count: usize) {
    get()
        .reconcile
        .status_patch_count
        .get_or_create(&NoLabels)
        .observe(count as f64);
}

pub fn set_compile_output_size(listeners: usize, routes: usize, backends: usize) {
    let m = get();
    m.reconcile.ir_listeners.set(listeners as i64);
    m.reconcile.ir_routes.set(routes as i64);
    m.reconcile.ir_backends.set(backends as i64);
}

pub fn record_dataplane_apply() {
    get().reconcile.dataplane_apply.inc();
}

pub fn record_dataplane_error() {
    get().reconcile.dataplane_error.inc();
}

pub fn observe_praxis_config_encode_seconds(seconds: f64) {
    get()
        .reconcile
        .praxis_config_encode_duration
        .get_or_create(&NoLabels)
        .observe(seconds);
}

pub fn record_praxis_config_apply() {
    get().reconcile.praxis_config_apply.inc();
}

pub fn record_praxis_config_error() {
    get().reconcile.praxis_config_error.inc();
}

pub fn record_praxis_config_unsupported(feature: &str) {
    get()
        .reconcile
        .praxis_config_unsupported
        .get_or_create(&PraxisFeatureLabels {
            feature: feature.to_string(),
        })
        .inc();
}

pub fn record_ungranted_references(report: &ugwc_core::CompileReport) {
    let m = get();
    for err in &report.errors {
        if err.reason == "RefNotPermitted" {
            m.reconcile
                .ungranted_references
                .get_or_create(&RouteKindLabels {
                    route_kind: err.kind.clone(),
                })
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_exports_registered_metrics() {
        let _handles = install().expect("metrics install");
        record_reconcile_run("test", true);
        record_compile_error();
        set_leader_active(true);
        observe_status_patch_duration_seconds(0.05);
        record_status_patch_count(20);
        let body = render().expect("render");
        assert!(
            body.contains("ugwc_reconcile_runs"),
            "missing reconcile counter in:\n{body}"
        );
        assert!(
            body.contains("ugwc_compile_errors"),
            "missing compile errors in:\n{body}"
        );
        assert!(
            body.contains("ugwc_leader_active"),
            "missing leader active in:\n{body}"
        );
        assert!(
            body.contains("ugwc_status_patch_duration_seconds"),
            "missing status patch duration in:\n{body}"
        );
        assert!(
            body.contains("ugwc_status_patch_count"),
            "missing status patch count in:\n{body}"
        );
    }
}
