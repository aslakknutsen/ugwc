//! Kubernetes watches, mappers, and status patches for Gateway API+.

pub mod controller;
pub mod mappers;
pub mod resources;
pub mod status;

pub use controller::{run_gateway_controller, ReconcileFn};
pub use mappers::{mapped_gateway_key, GatewayKey, WatchEventCause};
pub use status::{apply_gateway_reconciled_status, patch_gateway_programmed_false};

/// Stub symbol retained for crate-graph smoke tests.
pub fn kube_placeholder() -> &'static str {
    "ugwc-k8s"
}
