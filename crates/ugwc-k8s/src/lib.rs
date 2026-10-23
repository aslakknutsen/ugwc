//! Kubernetes watches, mappers, and status patches for Gateway API+.

mod crd_supervisor;
mod events;
mod gateway_index;
mod gateway_watchers;
mod initial_sync;
pub mod kube_metrics;
pub mod reconcile_queue;
mod spawn;
pub mod stores;
mod watch_enqueue;
mod watch_helpers;
mod watch_helpers_store;
mod watch_stream;

pub mod controller;
pub mod loader;
pub mod mappers;
pub mod provision;
pub mod resources;
pub mod status;

pub use controller::{build_shared_stores, run_gateway_controller, AlwaysOnWriters, ReconcileFn};
pub use crd_supervisor::{ActiveCrds, ManagedCrd};
pub use events::{
    GatewayEvents, REASON_COMPILE_BLOCKED, REASON_DATAPLANE_MISCONFIGURED,
    REASON_PRAXIS_CONFIG_FAILED, REASON_PRAXIS_CONFIG_UNSUPPORTED, REASON_PROVISION_FAILED,
    REASON_TLS_MATERIAL_MISSING,
};
pub use gateway_index::{
    refs_from_snapshot, GatewayIndex, GatewayIndexMetrics, GatewayRefs, IndexEventKindLabels,
};
pub use initial_sync::InitialSyncGate;
pub use kube_metrics::{instrumented_client, try_default_instrumented, KubeClientMetrics};
pub use loader::{load_cluster_snapshot, parent_reference_to_snap, LoaderError};
pub use mappers::{mapped_gateway_key, GatewayKey, WatchEventCause};
pub use provision::{
    collect_effective_listener_ports, deprovision_per_gateway_praxis, per_gateway_dataplane_name,
    praxis_config_map_name, praxis_istiod_dial_url, praxis_tls_secret_name,
    reconcile_per_gateway_praxis, reconcile_shared_deployment_stub,
    warn_per_gateway_without_enable, PraxisConfigSource, PraxisProvisionArgs, ProvisionError,
};
pub use reconcile_queue::{
    new_reconcile_queue, record_workqueue_retry, ReconcileQueue, ReconcileQueueMetrics,
    ReconcileSender, WorkqueueMetrics, GATEWAY_RECONCILE_QUEUE,
};
pub use spawn::spawn_with_trace_context;
pub use status::{
    apply_backend_tls_policies_for_gateway, apply_backend_tls_status_for_ca_ref_change,
    apply_gateway_class_accepted_stub, apply_gateway_reconciled_status,
    apply_grpc_route_merged_parent_status, apply_http_route_merged_parent_status,
    apply_inference_pools_for_gateway, apply_listenerset_reconciled_status,
    apply_listenerset_rejected_status, apply_listenerset_spec_only_status,
    apply_tcp_route_merged_parent_status, apply_tls_route_merged_parent_status,
    apply_udp_route_merged_parent_status, patch_gateway_programmed_false,
    GrpcRouteMergedParentStatusArgs, HttpRouteMergedParentStatusArgs,
    TcpRouteMergedParentStatusArgs, TlsRouteMergedParentStatusArgs, UdpRouteMergedParentStatusArgs,
};
pub use stores::SharedStores;

/// Stub symbol retained for crate-graph smoke tests.
pub fn kube_placeholder() -> &'static str {
    "ugwc-k8s"
}
