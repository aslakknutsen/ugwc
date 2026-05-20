//! Kubernetes Event recording for operator-facing diagnostics.

use k8s_openapi::api::core::v1::ObjectReference;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Client, Resource};
use tracing::debug;

use crate::resources::Gateway;

pub const REASON_COMPILE_BLOCKED: &str = "CompileBlocked";
pub const REASON_PROVISION_FAILED: &str = "ProvisionFailed";
pub const REASON_TLS_MATERIAL_MISSING: &str = "TlsMaterialMissing";
pub const REASON_DATAPLANE_MISCONFIGURED: &str = "DataplaneMisconfigured";

/// Aggregated Kubernetes Events publisher (`events.k8s.io`).
#[derive(Clone)]
pub struct GatewayEvents {
    recorder: Recorder,
}

impl GatewayEvents {
    #[must_use]
    pub fn new(client: Client) -> Self {
        let reporter = Reporter {
            controller: "ugwc".into(),
            instance: std::env::var("HOSTNAME").ok(),
        };
        Self {
            recorder: Recorder::new(client, reporter),
        }
    }

    /// Publish a Warning event on `gateway` (deduplicated by kube-runtime series).
    pub async fn publish_warning(
        &self,
        gateway: &Gateway,
        reason: &str,
        message: &str,
        related: Option<ObjectReference>,
    ) {
        let regarding = gateway.object_ref(&());
        let ev = Event {
            type_: EventType::Warning,
            reason: reason.into(),
            action: "Reconcile".into(),
            note: Some(message.into()),
            secondary: related,
        };
        if let Err(e) = self.recorder.publish(&ev, &regarding).await {
            debug!(error = %e, %reason, "kubernetes event publish");
        }
    }
}
