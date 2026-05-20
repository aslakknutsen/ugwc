//! Kubernetes watches, mappers, and status patches for Gateway API+.

mod crd_supervisor;
mod gateway_watchers;
mod initial_sync;
mod kube_metrics;
mod reconcile_queue;
mod watch_helpers;

pub mod controller;
pub mod loader;
pub mod mappers;
pub mod provision;
pub mod resources;
pub mod status;

pub use controller::{run_gateway_controller, ReconcileFn};
pub use reconcile_queue::{
    new_reconcile_queue, record_workqueue_retry, ReconcileQueue, ReconcileQueueMetrics,
    ReconcileSender, GATEWAY_RECONCILE_QUEUE,
};
pub use crd_supervisor::{ActiveCrds, ManagedCrd};
pub use initial_sync::InitialSyncGate;
pub use kube_metrics::{instrumented_client, try_default_instrumented};
pub use loader::{load_cluster_snapshot, parent_reference_to_snap, LoaderError};
pub use mappers::{mapped_gateway_key, GatewayKey, WatchEventCause};
pub use provision::{
    collect_effective_listener_ports, per_gateway_dataplane_name, praxis_istiod_dial_url,
    reconcile_per_gateway_praxis, reconcile_shared_deployment_stub, warn_per_gateway_without_enable,
    PraxisProvisionArgs, ProvisionError,
};
pub use status::{
    apply_backend_tls_policies_for_gateway, apply_gateway_class_accepted_stub,
    apply_gateway_reconciled_status, apply_grpc_route_merged_parent_status,
    apply_http_route_merged_parent_status, apply_inference_pools_for_gateway,
    apply_listenerset_reconciled_status, apply_listenerset_rejected_status,
    apply_tcp_route_merged_parent_status,
    apply_tls_route_merged_parent_status, apply_udp_route_merged_parent_status,
    patch_gateway_programmed_false, GrpcRouteMergedParentStatusArgs,
    HttpRouteMergedParentStatusArgs, TcpRouteMergedParentStatusArgs,
    TlsRouteMergedParentStatusArgs, UdpRouteMergedParentStatusArgs,
};

/// Stub symbol retained for crate-graph smoke tests.
pub fn kube_placeholder() -> &'static str {
    "ugwc-k8s"
}
