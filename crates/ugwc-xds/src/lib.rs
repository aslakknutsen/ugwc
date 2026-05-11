//! xDS control plane surface (`tonic` + `xds-api` generated types).
//! **Hot encode path:** keep Kubernetes types out — only [`ugwc_core::IrSnapshot`] inputs.

pub mod cache;
pub mod encode;
pub mod server;

pub use cache::{MultiScopeSnapshot, SharedSnapshot, SnapshotCache, XdsSnapshotSource};
pub use encode::{encode_delta, encode_sotw, XdsSnapshot};
pub use server::{serve, shutdown_gracefully, XdsSecurityConfig, XdsServerConfig};
/// ADS `type_url` for Praxis gwxds wire (same as [`ugwc_gwxds::GW_TYPE_URL`]).
pub use ugwc_gwxds::GW_TYPE_URL;

/// Legacy placeholder retained for crate graph smoke tests.
pub fn xds_placeholder() -> &'static str {
    "ugwc-xds"
}
