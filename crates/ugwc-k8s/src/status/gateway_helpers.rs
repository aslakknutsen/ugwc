use k8s_openapi::api::core::v1::Service;
use kube::Client;
use kube::Api;
use tracing::warn;

use crate::provision::per_gateway_dataplane_name;
use crate::resources::{Gateway, GatewayListeners, ListenerSet, ListenerSetListeners};
use ugwc_core::compile::CompileReport;

use super::ListenerAllowedRouteKinds;

/// True when compile attached an `InvalidCertificateRef` (or related) error to this Gateway.
///
/// Used so listener `ResolvedRefs` matches compile TLS failures when snapshot fields lag.
pub(super) fn gateway_compile_reports_invalid_certificate_ref(
    report: &CompileReport,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    report.errors.iter().any(|e| {
        e.kind == "Gateway"
            && e.namespace == gw_ns
            && e.name == gw_name
            && e.reason
                .to_ascii_lowercase()
                .contains("invalidcertificateref")
    })
}

pub(super) fn gateway_compile_reports_ref_not_permitted(
    report: &CompileReport,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    report.errors.iter().any(|e| {
        e.kind == "Gateway"
            && e.namespace == gw_ns
            && e.name == gw_name
            && e.reason == "RefNotPermitted"
    })
}

/// HTTPS listeners with structurally invalid `certificateRefs` (before Secret fetch).
pub(super) fn gateway_spec_https_certificate_ref_obviously_invalid(
    listener: &GatewayListeners,
) -> bool {
    if listener.protocol.trim().to_ascii_uppercase() != "HTTPS" {
        return false;
    }
    let Some(ref tls) = listener.tls else {
        return true;
    };
    let refs = tls.certificate_refs.as_deref().unwrap_or_default();
    if refs.is_empty() {
        return true;
    }
    refs.iter().any(|r| {
        let kind = r.kind.as_deref().unwrap_or("Secret");
        let group = r.group.as_deref().unwrap_or("");
        let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
        !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok
    })
}

pub(super) fn gateway_listener_https_cert_unresolved(
    cluster: Option<&ugwc_core::ClusterSnapshot>,
    lname: &str,
) -> bool {
    let Some(c) = cluster else {
        return false;
    };
    let Some(eff) = c.effective_listeners.iter().find(|e| {
        matches!(
            e.parent,
            ugwc_core::snapshot::ListenerParentSnap::InlineGateway
        ) && e.section_name == lname
    }) else {
        return false;
    };
    if eff.protocol.trim().to_ascii_uppercase() != "HTTPS" {
        return false;
    }
    eff.tls_resolve_error.is_some() || eff.tls_cert_pem.is_empty() || eff.tls_key_pem.is_empty()
}

/// Check whether a listener's cross-namespace certificateRef is blocked by a
/// missing or invalid ReferenceGrant.  This is a status-time safety net on top
/// of the loader's `tls_resolve_error` — it reads the Gateway spec directly so
/// the result is correct even when the effective-listener snapshot lags.
pub(super) fn gateway_listener_cert_ref_not_permitted(
    spec_row: Option<&GatewayListeners>,
    gw_ns: &str,
    cluster: Option<&ugwc_core::ClusterSnapshot>,
) -> bool {
    let Some(l) = spec_row else { return false };
    if !l.protocol.trim().eq_ignore_ascii_case("HTTPS") {
        return false;
    }
    let refs = match l.tls.as_ref() {
        Some(t) => t.certificate_refs.as_deref().unwrap_or_default(),
        None => return false,
    };
    if refs.is_empty() {
        return false;
    }
    let grants = cluster
        .map(|c| c.reference_grants.as_slice())
        .unwrap_or(&[]);
    refs.iter().any(|r| {
        let kind = r.kind.as_deref().unwrap_or("Secret");
        let group = r.group.as_deref().unwrap_or("");
        let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
        if !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok {
            return false;
        }
        let secret_ns = r.namespace.as_deref().unwrap_or(gw_ns);
        if secret_ns == gw_ns {
            return false;
        }
        !ugwc_core::reference_grant_permits_listener_secret_ref(
            grants, gw_ns, "Gateway", secret_ns, &r.name,
        )
    })
}

/// Check whether a ListenerSet listener's cross-namespace certificateRef is blocked
/// by a missing or invalid ReferenceGrant.
pub(super) fn listenerset_listener_cert_ref_not_permitted(
    spec_row: Option<&ListenerSetListeners>,
    ls_ns: &str,
    cluster: Option<&ugwc_core::ClusterSnapshot>,
) -> bool {
    let Some(l) = spec_row else {
        return false;
    };
    if !l.protocol.trim().eq_ignore_ascii_case("HTTPS") {
        return false;
    }
    let refs = match l.tls.as_ref() {
        Some(t) => t.certificate_refs.as_deref().unwrap_or_default(),
        None => return false,
    };
    if refs.is_empty() {
        return false;
    }
    let grants = cluster
        .map(|c| c.reference_grants.as_slice())
        .unwrap_or(&[]);
    refs.iter().any(|r| {
        let kind = r.kind.as_deref().unwrap_or("Secret");
        let group = r.group.as_deref().unwrap_or("");
        let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
        if !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok {
            return false;
        }
        let secret_ns = r.namespace.as_deref().unwrap_or(ls_ns);
        if secret_ns == ls_ns {
            return false;
        }
        !ugwc_core::reference_grant_permits_listener_secret_ref(
            grants, ls_ns, "ListenerSet", secret_ns, &r.name,
        )
    })
}

pub(super) fn gateway_ip_address_json(ip: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "IPAddress",
        "value": ip,
    })
}

pub(super) fn gateway_hostname_address_json(host: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "Hostname",
        "value": host,
    })
}

pub(super) fn gateway_status_addresses_from_service(svc: &Service) -> Vec<serde_json::Value> {
    if let Some(st) = svc.status.as_ref() {
        if let Some(lb) = st.load_balancer.as_ref() {
            if let Some(ingress) = lb.ingress.as_ref() {
                for ing in ingress {
                    if let Some(ip) = ing.ip.as_deref() {
                        if !ip.is_empty() {
                            return vec![gateway_ip_address_json(ip)];
                        }
                    }
                }
                for ing in ingress {
                    if let Some(h) = ing.hostname.as_deref() {
                        if !h.is_empty() {
                            return vec![gateway_hostname_address_json(h)];
                        }
                    }
                }
            }
        }
    }
    // If the dataplane is a LoadBalancer, only publish once an ingress IP exists. Publishing
    // ClusterIP while the LB is pending breaks conformance (host runs `go test` against
    // `Gateway.status.addresses`).
    let is_load_balancer = svc
        .spec
        .as_ref()
        .and_then(|s| s.type_.as_deref())
        == Some("LoadBalancer");
    if is_load_balancer {
        return Vec::new();
    }
    if let Some(spec) = svc.spec.as_ref() {
        if let Some(ip) = spec.cluster_ip.as_deref() {
            if !ip.is_empty() && ip != "None" {
                return vec![gateway_ip_address_json(ip)];
            }
        }
    }
    Vec::new()
}

pub(super) fn listener_route_kinds_broken_for_status<L: ListenerAllowedRouteKinds>(
    eff: Option<&ugwc_core::snapshot::EffectiveListenerSnap>,
    spec_listener: Option<&L>,
) -> bool {
    let eff_broken = eff
        .map(ugwc_core::effective_listener_has_invalid_allowed_route_kinds)
        .unwrap_or(false);
    let spec_broken = spec_listener
        .map(|l| {
            let kinds = l.route_group_kinds();
            ugwc_core::allowed_route_kinds_invalid_for_gateway_protocol(
                l.protocol_for_route_kinds(),
                &kinds,
            )
        })
        .unwrap_or(false);
    eff_broken || spec_broken
}

pub(super) async fn fetch_gateway_status_addresses_json(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
) -> Vec<serde_json::Value> {
    let svc_name = per_gateway_dataplane_name(gateway_name);
    let api: Api<Service> = Api::namespaced(client.clone(), gateway_ns);
    match api.get(&svc_name).await {
        Ok(svc) => gateway_status_addresses_from_service(&svc),
        Err(e) => {
            let not_found = matches!(
                &e,
                kube::Error::Api(err) if err.code == 404
            );
            if !not_found {
                warn!(
                    error = %e,
                    %gateway_ns,
                    service = %svc_name,
                    "could not read dataplane Service for Gateway status.addresses"
                );
            }
            Vec::new()
        }
    }
}

/// Resolve `metadata.generation` for status `observedGeneration` (always refetch from API).
pub(super) async fn gateway_generation_for_status(
    api: &kube::Api<Gateway>,
    name: &str,
    gateway_ns: &str,
) -> Result<i64, kube::Error> {
    let refreshed = api.get(name).await?;
    let gen = refreshed
        .metadata
        .generation
        .filter(|&g| g > 0)
        .unwrap_or(1);
    if refreshed.metadata.generation.is_none() || refreshed.metadata.generation == Some(0) {
        warn!(
            gateway = %name,
            namespace = %gateway_ns,
            "Gateway metadata.generation missing or zero after refetch; using {gen} for observedGeneration"
        );
    }
    Ok(gen)
}

/// Resolve `metadata.generation` for ListenerSet status `observedGeneration`.
pub(super) async fn listenerset_generation_for_status(
    api: &kube::Api<ListenerSet>,
    name: &str,
    ls_ns: &str,
) -> Result<i64, kube::Error> {
    let refreshed = api.get(name).await?;
    let gen = refreshed
        .metadata
        .generation
        .filter(|&g| g > 0)
        .unwrap_or(1);
    if refreshed.metadata.generation.is_none() || refreshed.metadata.generation == Some(0) {
        warn!(
            listenerset = %name,
            namespace = %ls_ns,
            "ListenerSet metadata.generation missing or zero after refetch; using {gen} for observedGeneration"
        );
    }
    Ok(gen)
}
