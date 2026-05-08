//! Optional dataplane workload reconciliation (e.g. Praxis proxy per `Gateway`).
//!
//! Per-gateway manifests are rendered from an embedded Minijinja template
//! (`templates/per_gateway_praxis.yaml.j2`) and server-side applied as untyped
//! [`kube::core::DynamicObject`] resources.

mod template;

use std::collections::HashSet;

use kube::api::PatchParams;
use kube::Client;
use thiserror::Error;
use tracing::{debug, warn};

use crate::resources::Gateway;

pub use template::NamePort;

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
    s.chars().take(15).collect()
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

    let dep_name = deployment_name(gw_name);
    let sa_name = service_account_name(&dep_name);
    let gw_class = gateway_class_label(&args.gateway_class_name);

    let listener_pairs = listener_port_override
        .map(<[_]>::to_vec)
        .unwrap_or_else(|| collect_listener_ports(gateway));
    let container_ports = container_port_rows(&listener_pairs);
    let service_ports = service_port_rows(&listener_pairs);

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
    };

    let rendered = template::render_per_gateway_praxis(&ctx)?;
    let documents = template::parse_manifest_documents(&rendered)?;
    let pp = PatchParams::apply("ugwc").force();
    template::apply_manifests_dynamic(client, ns, documents, &pp).await?;

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
