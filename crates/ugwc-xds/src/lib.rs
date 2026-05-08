//! xDS control plane surface (`tonic` + `xds-api` generated types).
//! **Hot encode path:** keep Kubernetes types out — only [`ugwc_core::IrSnapshot`] inputs.

pub mod cache;
pub mod encode;
pub mod server;

pub use cache::{MultiScopeSnapshot, SharedSnapshot, SnapshotCache, XdsSnapshotSource};
pub use encode::{encode_delta, encode_sotw, XdsSnapshot};
pub use server::{serve, shutdown_gracefully, XdsSecurityConfig, XdsServerConfig};

/// Legacy placeholder retained for crate graph smoke tests.
pub fn xds_placeholder() -> &'static str {
    "ugwc-xds"
}
