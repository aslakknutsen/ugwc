//! Optional dataplane workload reconciliation (e.g. Praxis proxy per `Gateway`).
//!
//! Per-gateway manifests are rendered from an embedded Minijinja template
//! (`templates/per_gateway_praxis.yaml.j2`) and server-side applied as untyped
//! [`kube::core::DynamicObject`] resources.

mod template;

use std::collections::HashSet;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Service, ServiceAccount};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::DeleteParams;
use kube::api::PatchParams;
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::resources::Gateway;

pub use template::{InfraLabelRow, NamePort};

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
    /// `Gateway.spec.gatewayClassName` (for standard Gateway API labels).
    pub gateway_class_name: String,
}

/// Turn `host:port` into a URI tonic can dial. Praxis xDS uses `http::Uri` + `Channel::builder`;
/// cleartext gRPC needs an `http://` scheme, TLS needs `https://`.
#[must_use]
pub fn praxis_istiod_dial_url(bootstrap: &str, xds_insecure: bool) -> String {
    let t = bootstrap.trim();
    if t.is_empty() {
        return String::new();
    }
    if t.contains("://") {
        return t.to_string();
    }
    if xds_insecure {
        format!("http://{t}")
    } else {
        format!("https://{t}")
    }
}

/// Deployment/Service name for the per-gateway Praxis dataplane (`ugwc-dp-…`).
///
/// Must stay aligned with `templates/per_gateway_praxis.yaml.j2` (`deployment_name` / Service `metadata.name`).
#[must_use]
pub fn per_gateway_dataplane_name(gateway_name: &str) -> String {
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

fn service_account_name(dep_name: &str) -> String {
    let raw = format!("{dep_name}-sa");
    raw.chars().take(63).collect()
}

fn gateway_class_label(gateway_class_name: &str) -> String {
    let t = gateway_class_name.trim();
    if t.is_empty() {
        "unknown".into()
    } else {
        t.to_string()
    }
}

/// RFC 6335-ish port name: lowercase alnum + `-`, max 15 chars.
fn sanitize_port_name(raw: &str, port: i32) -> String {
    let base: String = raw
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let base = base.trim_matches('-');
    let s = if base.is_empty() {
        format!("p{port}")
    } else {
        base.to_string()
    };
    let truncated: String = s.chars().take(15).collect();
    let trimmed = truncated.trim_end_matches('-');
    if trimmed.is_empty() {
        format!("p{port}")
    } else {
        trimmed.to_string()
    }
}

/// Ports from merged Gateway + ListenerSet listeners (conflicts / duplicates skipped).
#[must_use]
pub fn collect_effective_listener_ports(
    snapshot: &ugwc_core::ClusterSnapshot,
) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let mut seen_port: HashSet<i32> = HashSet::new();
    for e in &snapshot.effective_listeners {
        if e.conflicted {
            continue;
        }
        let port = i32::from(e.port);
        if port <= 0 || !seen_port.insert(port) {
            continue;
        }
        let raw_label = match &e.parent {
            ugwc_core::snapshot::ListenerParentSnap::InlineGateway => e.section_name.clone(),
            ugwc_core::snapshot::ListenerParentSnap::ListenerSet { namespace, name } => {
                format!("{namespace}-{name}-{}", e.section_name)
            }
        };
        let name = sanitize_port_name(&raw_label, port);
        out.push((name, port));
    }
    if out.is_empty() {
        out.push(("http".into(), 8080));
    }
    out
}

/// Listener-derived service ports; default `http` 8080 when none valid.
fn collect_listener_ports(gateway: &Gateway) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    for l in &gateway.spec.listeners {
        let port = i32::from(l.port);
        if port <= 0 {
            continue;
        }
        let name = sanitize_port_name(&l.name, port);
        out.push((name, port));
    }
    if out.is_empty() {
        out.push(("http".into(), 8080));
    }
    out
}

fn container_port_rows(listener_ports: &[(String, i32)]) -> Vec<NamePort> {
    let mut rows = vec![NamePort {
        name: "status-port".into(),
        port: 15021,
    }];
    for (name, p) in listener_ports {
        if *p == 15021 {
            continue;
        }
        rows.push(NamePort {
            name: name.clone(),
            port: *p,
        });
    }
    rows
}

fn service_port_rows(listener_ports: &[(String, i32)]) -> Vec<NamePort> {
    listener_ports
        .iter()
        .map(|(name, port)| NamePort {
            name: name.clone(),
            port: *port,
        })
        .collect()
}

fn json_scalar(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

/// Apply ServiceAccount, Deployment, Service, HPA, and PDB from the embedded template (SSA).
pub async fn reconcile_per_gateway_praxis(
    client: &Client,
    gateway: &Gateway,
    args: &PraxisProvisionArgs,
    listener_port_override: Option<&[(String, i32)]>,
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

    let dep_name = per_gateway_dataplane_name(gw_name);
    let sa_name = service_account_name(&dep_name);
    let gw_class = gateway_class_label(&args.gateway_class_name);

    let listener_pairs = listener_port_override
        .map(<[_]>::to_vec)
        .unwrap_or_else(|| collect_listener_ports(gateway));
    let container_ports = container_port_rows(&listener_pairs);
    let service_ports = service_port_rows(&listener_pairs);

    let infrastructure_labels: Vec<InfraLabelRow> = gateway
        .spec
        .infrastructure
        .as_ref()
        .and_then(|inf| inf.labels.as_ref())
        .map(|labels| {
            labels
                .iter()
                .map(|(k, v)| InfraLabelRow {
                    key: k.clone(),
                    value_json: json_scalar(v.as_str()),
                })
                .collect()
        })
        .unwrap_or_default();

    let insecure_str = if args.xds_insecure { "true" } else { "false" };

    let ctx = template::PerGatewayTemplateContext {
        namespace: ns,
        gateway_name: gw_name,
        gateway_uid: uid,
        gateway_class: gw_class.as_str(),
        deployment_name: dep_name.as_str(),
        sa_name: sa_name.as_str(),
        proxy_image_json: json_scalar(args.proxy_image.as_str()),
        xds_bootstrap_addr_json: json_scalar(args.xds_bootstrap_addr.as_str()),
        gateway_scope_json: json_scalar(args.gateway_scope.as_str()),
        xds_insecure_json: json_scalar(insecure_str),
        xds_wire_json: json_scalar(args.xds_wire.as_str()),
        gateway_name_json: json_scalar(gw_name),
        container_ports: &container_ports,
        service_ports: &service_ports,
        infrastructure_labels: &infrastructure_labels,
    };

    let rendered = template::render_per_gateway_praxis(&ctx)?;
    let documents = template::parse_manifest_documents(&rendered)?;
    let pp = PatchParams::apply("ugwc").force();
    template::apply_manifests_dynamic(client, ns, documents, &pp).await?;

    Ok(())
}

/// Delete per-gateway Praxis workload objects when the owning Gateway is gone.
///
/// Ignores `NotFound` on each resource. Resources with Gateway `ownerReferences` may
/// already be garbage-collected; explicit delete covers races and partial provision.
pub async fn deprovision_per_gateway_praxis(
    client: &Client,
    gateway_namespace: &str,
    gateway_name: &str,
) -> Result<(), ProvisionError> {
    if gateway_namespace.is_empty() || gateway_name.is_empty() {
        return Ok(());
    }
    let dep_name = per_gateway_dataplane_name(gateway_name);
    let sa_name = service_account_name(&dep_name);
    let dp = DeleteParams::default();

    async fn delete_ignoring_not_found<T>(
        api: kube::Api<T>,
        name: &str,
        dp: &DeleteParams,
    ) -> Result<(), ProvisionError>
    where
        T: kube::Resource + std::fmt::Debug + Clone + DeserializeOwned + Send + Sync,
        T::DynamicType: Default,
    {
        match api.delete(name, dp).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(a)) if a.code == 404 => Ok(()),
            Err(e) => Err(ProvisionError::Kube(e)),
        }
    }

    delete_ignoring_not_found(
        Api::<PodDisruptionBudget>::namespaced(client.clone(), gateway_namespace),
        &dep_name,
        &dp,
    )
    .await?;
    delete_ignoring_not_found(
        Api::<HorizontalPodAutoscaler>::namespaced(client.clone(), gateway_namespace),
        &dep_name,
        &dp,
    )
    .await?;
    delete_ignoring_not_found(
        Api::<Service>::namespaced(client.clone(), gateway_namespace),
        &dep_name,
        &dp,
    )
    .await?;
    delete_ignoring_not_found(
        Api::<Deployment>::namespaced(client.clone(), gateway_namespace),
        &dep_name,
        &dp,
    )
    .await?;
    delete_ignoring_not_found(
        Api::<ServiceAccount>::namespaced(client.clone(), gateway_namespace),
        &sa_name,
        &dp,
    )
    .await?;

    info!(
        %gateway_namespace,
        %gateway_name,
        %dep_name,
        "deprovisioned per-gateway praxis dataplane"
    );
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

#[cfg(test)]
mod tests {
    use super::praxis_istiod_dial_url;

    #[test]
    fn praxis_istiod_dial_url_adds_scheme() {
        assert_eq!(
            praxis_istiod_dial_url("ugwc.ugwc-system.svc:18000", true),
            "http://ugwc.ugwc-system.svc:18000"
        );
        assert_eq!(
            praxis_istiod_dial_url("ugwc.ugwc-system.svc:18000", false),
            "https://ugwc.ugwc-system.svc:18000"
        );
    }

    #[test]
    fn praxis_istiod_dial_url_preserves_explicit_uri() {
        let u = "http://cp.example:18000";
        assert_eq!(praxis_istiod_dial_url(u, false), u);
        assert_eq!(praxis_istiod_dial_url(u, true), u);
    }

    #[test]
    fn praxis_istiod_dial_url_empty() {
        assert_eq!(praxis_istiod_dial_url("", true), "");
        assert_eq!(praxis_istiod_dial_url("  ", true), "");
    }

    #[test]
    fn sanitize_port_name_strips_trailing_hyphen_after_truncation() {
        use super::sanitize_port_name;
        // "listener-exact-hostname" truncated to 15 chars would be "listener-exact-"
        assert_eq!(sanitize_port_name("listener-exact-hostname", 443), "listener-exact");
        // "listener-empty-hostname" similarly
        assert_eq!(sanitize_port_name("listener-empty-hostname", 443), "listener-empty");
        // Short names stay unchanged
        assert_eq!(sanitize_port_name("https", 443), "https");
        // Names that fit exactly at 15 chars without trailing hyphen
        assert_eq!(sanitize_port_name("listener-wildca", 443), "listener-wildca");
        // All-hyphen input (after trim) falls back to port-based name
        assert_eq!(sanitize_port_name("---", 8080), "p8080");
    }
}
