//! Kubernetes watches, mappers, and status patches for Gateway API+.

pub mod controller;
pub mod loader;
pub mod mappers;
pub mod provision;
pub mod resources;
pub mod status;

pub use controller::{run_gateway_controller, ReconcileFn};
pub use loader::{load_cluster_snapshot, parent_reference_to_snap, LoaderError};
pub use mappers::{mapped_gateway_key, GatewayKey, WatchEventCause};
pub use provision::{
    collect_effective_listener_ports, reconcile_per_gateway_praxis,
    reconcile_shared_deployment_stub, warn_per_gateway_without_enable, PraxisProvisionArgs,
    ProvisionError,
};
pub use status::{
    apply_gateway_class_accepted_stub, apply_gateway_reconciled_status,
    apply_grpc_route_merged_parent_status, apply_http_route_merged_parent_status,
    apply_listenerset_reconciled_status, apply_tls_route_merged_parent_status,
    patch_gateway_programmed_false,
    GrpcRouteMergedParentStatusArgs, HttpRouteMergedParentStatusArgs,
    TlsRouteMergedParentStatusArgs,
};

/// Stub symbol retained for crate-graph smoke tests.
pub fn kube_placeholder() -> &'static str {
    "ugwc-k8s"
}
