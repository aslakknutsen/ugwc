//! Optional dataplane workload reconciliation (e.g. Praxis proxy per `Gateway`).

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use thiserror::Error;
use tracing::{debug, warn};

use crate::resources::Gateway;

#[derive(Debug, Error)]
pub enum ProvisionError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
    #[error("{0}")]
    Msg(String),
}

/// Bootstrap for a Praxis (or compatible) proxy that dials ugwc xDS with a scoped `node.id`.
#[derive(Debug)]
pub struct PraxisProvisionArgs {
    pub proxy_image: String,
    pub xds_bootstrap_addr: String,
    pub xds_insecure: bool,
    pub xds_wire: String,
    /// `namespace/name` matching [`ugwc_core::ir::IrScopeId`] string form.
    pub gateway_scope: String,
    pub service_port: u16,
}

fn deployment_name(gateway_name: &str) -> String {
    let base = gateway_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    let suffix = base.trim_matches('-');
    let raw = format!("ugwc-dp-{suffix}");
    raw.chars().take(63).collect()
}

/// Apply `Deployment` + `Service` for a per-gateway Praxis dataplane (idempotent server-side apply).
pub async fn reconcile_per_gateway_praxis(
    client: &Client,
    gateway: &Gateway,
    args: &PraxisProvisionArgs,
) -> Result<(), ProvisionError> {
    let ns = gateway
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| ProvisionError::Msg("gateway missing namespace".into()))?;
    let gw_name = gateway.metadata.name.as_deref().unwrap_or("");
    if gw_name.is_empty() {
        return Err(ProvisionError::Msg("gateway missing name".into()));
    }
    let uid = gateway
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| ProvisionError::Msg("gateway missing uid for ownerReference".into()))?;

    let dep_name = deployment_name(gw_name);
    let labels = serde_json::json!({
        "app.kubernetes.io/name": "ugwc-praxis-dataplane",
        "app.kubernetes.io/managed-by": "ugwc",
        "gateway.ugwc.io/gateway-namespace": ns,
        "gateway.ugwc.io/gateway-name": gw_name,
    });

    let owner = serde_json::json!([{
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "Gateway",
        "name": gw_name,
        "uid": uid,
        "controller": true,
        "blockOwnerDeletion": true
    }]);

    let insecure_str = if args.xds_insecure { "true" } else { "false" };
    let deployment_value = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": dep_name,
            "namespace": ns,
            "labels": labels,
            "ownerReferences": owner,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "gateway.ugwc.io/dataplane": dep_name,
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "gateway.ugwc.io/dataplane": dep_name,
                        "app.kubernetes.io/name": "ugwc-praxis-dataplane",
                    }
                },
                "spec": {
                    "containers": [{
                        "name": "proxy",
                        "image": args.proxy_image,
                        "ports": [{ "containerPort": args.service_port as i32, "name": "http" }],
                        "env": [
                            { "name": "UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR", "value": args.xds_bootstrap_addr },
                            { "name": "UGWC_XDS_NODE_ID", "value": args.gateway_scope },
                            { "name": "UGWC_XDS_INSECURE", "value": insecure_str },
                            { "name": "UGWC_XDS_WIRE", "value": args.xds_wire },
                        ]
                    }]
                }
            }
        }
    });

    let dep: Deployment = serde_json::from_value(deployment_value)
        .map_err(|e| ProvisionError::Msg(format!("deployment json: {e}")))?;

    let dep_api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let pp = PatchParams::apply("ugwc").force();
    dep_api
        .patch(&dep_name, &pp, &Patch::Apply(dep))
        .await?;

    let svc_value = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": dep_name,
            "namespace": ns,
            "labels": labels,
            "ownerReferences": owner,
        },
        "spec": {
            "selector": {
                "gateway.ugwc.io/dataplane": dep_name,
            },
            "ports": [{
                "name": "http",
                "port": args.service_port as i32,
                "targetPort": args.service_port as i32,
                "protocol": "TCP"
            }],
            "type": "ClusterIP"
        }
    });

    let svc: Service =
        serde_json::from_value(svc_value).map_err(|e| ProvisionError::Msg(format!("service json: {e}")))?;

    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns);
    svc_api
        .patch(&dep_name, &pp, &Patch::Apply(svc))
        .await?;

    Ok(())
}

/// `SharedDeployment` and future strategies: documented stub (no API writes).
pub fn reconcile_shared_deployment_stub(gateway_name: &str) {
    debug!(
        gateway = %gateway_name,
        "SharedDeployment dataplane strategy not implemented — skipping apply"
    );
}

/// Log once when external strategy would have been the only option (noise control left to caller).
pub fn warn_per_gateway_without_enable(gateway_name: &str) {
    warn!(
        gateway = %gateway_name,
        "profile requests per_gateway_praxis but UGWC_DATAPLANE_ENABLE is false — skipping provision"
    );
}
