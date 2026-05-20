//! Gateway API state gauges (`gatewayapi_*`) from reconciled cluster snapshots.
//!
//! Scope: resources in the fan-out of the gateway being reconciled (owned controller view).
//! Label `source="ugwc"` distinguishes these series from an optional external exporter.

use metrics::gauge;
use ugwc_core::snapshot::{ClusterSnapshot, ParentRefSnap};

const SOURCE: &str = "ugwc";

/// Update `gatewayapi_*` gauges after a successful compile for `snap`.
pub fn record_from_reconcile(snap: &ClusterSnapshot, programmed_ok: bool) {
    let gw_status = if programmed_ok {
        "Programmed"
    } else {
        "NotProgrammed"
    };
    gauge!(
        "gatewayapi_gateway_status",
        "namespace" => snap.gateway_namespace.clone(),
        "name" => snap.gateway_name.clone(),
        "status" => gw_status.to_string(),
        "source" => SOURCE.to_string()
    )
    .set(1.0);

    for r in &snap.http_routes {
        record_route(
            "HTTPRoute",
            &r.namespace,
            &r.name,
            &r.parent_refs,
            r.rules.len(),
            &snap.gateway_namespace,
            &snap.gateway_name,
        );
    }
    for r in &snap.grpc_routes {
        record_route(
            "GRPCRoute",
            &r.namespace,
            &r.name,
            &r.parent_refs,
            r.rules.len(),
            &snap.gateway_namespace,
            &snap.gateway_name,
        );
    }
    for r in &snap.tls_routes {
        record_route(
            "TLSRoute",
            &r.namespace,
            &r.name,
            &r.parent_refs,
            r.rules.len(),
            &snap.gateway_namespace,
            &snap.gateway_name,
        );
    }
    for r in &snap.tcp_routes {
        record_route(
            "TCPRoute",
            &r.namespace,
            &r.name,
            &r.parent_refs,
            r.rules.len(),
            &snap.gateway_namespace,
            &snap.gateway_name,
        );
    }
    for r in &snap.udp_routes {
        record_route(
            "UDPRoute",
            &r.namespace,
            &r.name,
            &r.parent_refs,
            r.rules.len(),
            &snap.gateway_namespace,
            &snap.gateway_name,
        );
    }

    for g in &snap.reference_grants {
        gauge!(
            "gatewayapi_referencegrant_status",
            "namespace" => g.namespace.clone(),
            "name" => g.name.clone(),
            "validated" => "present",
            "source" => SOURCE.to_string()
        )
        .set(1.0);
    }
}

fn record_route(
    kind: &str,
    route_ns: &str,
    route_name: &str,
    parent_refs: &[ParentRefSnap],
    rule_count: usize,
    gw_ns: &str,
    gw_name: &str,
) {
    let attached = parent_refs
        .iter()
        .filter(|p| parent_attaches_to_gateway(p, route_ns, gw_ns, gw_name))
        .count();
    let metric = match kind {
        "HTTPRoute" => "gatewayapi_httproute_attached_gateways",
        "GRPCRoute" => "gatewayapi_grpcroute_attached_gateways",
        "TLSRoute" => "gatewayapi_tlsroute_attached_gateways",
        "TCPRoute" => "gatewayapi_tcproute_attached_gateways",
        "UDPRoute" => "gatewayapi_udproute_attached_gateways",
        _ => "gatewayapi_route_attached_gateways",
    };
    gauge!(
        metric,
        "namespace" => route_ns.to_string(),
        "name" => route_name.to_string(),
        "source" => SOURCE.to_string()
    )
    .set(attached as f64);
    gauge!(
        "gatewayapi_route_rules_total",
        "namespace" => route_ns.to_string(),
        "name" => route_name.to_string(),
        "kind" => kind.to_string(),
        "source" => SOURCE.to_string()
    )
    .set(rule_count as f64);
}

fn parent_attaches_to_gateway(
    parent: &ParentRefSnap,
    route_ns: &str,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    let g = parent.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = parent.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = parent
        .namespace
        .as_deref()
        .unwrap_or(route_ns);
    pns == gw_ns && parent.name == gw_name
}
