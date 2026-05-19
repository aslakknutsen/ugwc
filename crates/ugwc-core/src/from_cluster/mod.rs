//! Build [`crate::ir::IrSnapshot`] + [`crate::compile::CompileReport`] from a [`crate::snapshot::ClusterSnapshot`].

mod attachment;
mod backend_resolve;
mod conflicts;
mod grpc_filters;
mod http_filters;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use uuid::Uuid;

use attachment::compute_listener_route_attachment_counts;
use backend_resolve::{
    collect_endpoints, find_service, resolve_grpc_route_backends, resolve_tcp_route_backends,
    resolve_tls_route_backends, resolve_udp_route_backends, resolve_weighted_backends,
    service_has_any_endpointslice,
};
use conflicts::{
    compute_http_grpc_conflict_losers, compute_tcp_route_conflict_losers,
    compute_tls_route_conflict_losers, compute_udp_route_conflict_losers,
    hosts_for_grpc_route_on_listener, hosts_for_route_on_listener,
    hosts_for_tls_route_on_listener, record_listener_conflicts,
};
use grpc_filters::{extract_grpc_rule_filters, validate_and_convert_grpc_matches};
use http_filters::{extract_rule_filters, rule_specificity, validate_and_convert_matches};

use crate::compile::{CompileReport, ObjectRefErr};
use crate::http_route_attachment::{
    find_effective_for_key, grpc_route_attachment_keys, intersect_route_listener_hosts,
    route_attachment_keys, tcp_route_attachment_keys, tcp_route_listener_ok,
    tls_route_attachment_keys, udp_route_attachment_keys, udp_route_listener_ok,
};
use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, IrSnapshot, ListenerIr, RouteFamily, RouteRuleIr, SnapshotMeta, TlsMode,
    IR_VERSION,
};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, EffectiveListenerSnap, GrpcRouteSnap, HttpRouteSnap, ListenerParentSnap,
    TcpRouteSnap, TlsRouteSnap, UdpRouteSnap,
};

/// Parse a Gateway API duration string (GEP-2257: e.g. `"10s"`, `"500ms"`, `"1h"`) to milliseconds.
fn parse_gateway_duration_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    if s == "0s" || s == "0" {
        return Some(0);
    }
    if let Some(rest) = s.strip_suffix("ms") {
        return rest.parse::<u64>().ok();
    }
    if let Some(rest) = s.strip_suffix('s') {
        return rest.parse::<u64>().ok().map(|v| v * 1000);
    }
    if let Some(rest) = s.strip_suffix('m') {
        return rest.parse::<u64>().ok().map(|v| v * 60_000);
    }
    if let Some(rest) = s.strip_suffix('h') {
        return rest.parse::<u64>().ok().map(|v| v * 3_600_000);
    }
    None
}

#[derive(Clone, Eq, PartialEq)]
struct RouteSortMeta {
    ts: String,
    route_ns: String,
    route_name: String,
    /// 0 = HTTPRoute, 1 = GRPCRoute, 2 = TLSRoute, 3 = TCPRoute, 4 = UDPRoute (stable ordering when timestamps match).
    route_kind_order: u8,
    rule_idx: usize,
    seq: usize,
}

impl Ord for RouteSortMeta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.route_ns.cmp(&other.route_ns))
            .then_with(|| self.route_name.cmp(&other.route_name))
            .then_with(|| self.route_kind_order.cmp(&other.route_kind_order))
            .then_with(|| self.rule_idx.cmp(&other.rule_idx))
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for RouteSortMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) fn normalize_protocol(p: &str) -> String {
    let t = p.trim();
    if t.is_empty() {
        "HTTP".into()
    } else {
        t.to_ascii_uppercase()
    }
}

pub(super) fn httproute_obj_err(route: &HttpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "HTTPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

pub(super) fn grpcroute_obj_err(route: &GrpcRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "GRPCRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

pub(super) fn tlsroute_obj_err(route: &TlsRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "TLSRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

pub(super) fn tcproute_obj_err(route: &TcpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1alpha2".into(),
        kind: "TCPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

pub(super) fn udproute_obj_err(route: &UdpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1alpha2".into(),
        kind: "UDPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn gateway_obj_err(snapshot: &ClusterSnapshot, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "Gateway".into(),
        namespace: snapshot.gateway_namespace.clone(),
        name: snapshot.gateway_name.clone(),
        reason: reason.into(),
    }
}

fn validate_gateway_addresses(snapshot: &ClusterSnapshot, report: &mut CompileReport) {
    for (i, a) in snapshot.gateway_addresses.iter().enumerate() {
        let t = a.type_.as_deref().unwrap_or("IPAddress");
        if t != "IPAddress" && t != "Hostname" {
            report.errors.push(gateway_obj_err(
                snapshot,
                format!("UnsupportedAddress: spec.addresses[{i}] type {t}"),
            ));
        }
    }
}

fn listener_tls_ir_fields(
    e: &EffectiveListenerSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> (TlsMode, Vec<u8>, Vec<u8>) {
    let proto = normalize_protocol(&e.protocol);
    match proto.as_str() {
        "HTTP" => {
            if e.tls.is_some() {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "Invalid: HTTP listener must not set tls",
                ));
            }
            (TlsMode::Plaintext, Vec::new(), Vec::new())
        }
        "HTTPS" => {
            let mode = e
                .tls
                .as_ref()
                .and_then(|t| t.mode.as_deref())
                .unwrap_or("Terminate");
            if mode.eq_ignore_ascii_case("Passthrough") {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "UnsupportedValue: HTTPS TLS Passthrough",
                ));
                return (TlsMode::Plaintext, Vec::new(), Vec::new());
            }
            if let Some(ref err) = e.tls_resolve_error {
                report.errors.push(gateway_obj_err(snapshot, err.as_str()));
                return (TlsMode::Terminate, Vec::new(), Vec::new());
            }
            if e.tls_cert_pem.is_empty() || e.tls_key_pem.is_empty() {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "InvalidCertificateRef: missing TLS certificate material",
                ));
                return (TlsMode::Terminate, Vec::new(), Vec::new());
            }
            (
                TlsMode::Terminate,
                e.tls_cert_pem.clone(),
                e.tls_key_pem.clone(),
            )
        }
        "TLS" => {
            let Some(ref tls_cfg) = e.tls else {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "Invalid: TLS listener requires tls configuration",
                ));
                return (TlsMode::Plaintext, Vec::new(), Vec::new());
            };
            let mode = tls_cfg.mode.as_deref().unwrap_or("").trim();
            if mode.is_empty() {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "Invalid: TLS listener requires tls.mode",
                ));
                return (TlsMode::Plaintext, Vec::new(), Vec::new());
            }
            if mode.eq_ignore_ascii_case("Terminate") {
                report.errors.push(gateway_obj_err(
                    snapshot,
                    "UnsupportedValue: TLS listener Terminate requires TLSRouteTermination support",
                ));
                return (TlsMode::Plaintext, Vec::new(), Vec::new());
            }
            if mode.eq_ignore_ascii_case("Passthrough") {
                return (TlsMode::Passthrough, Vec::new(), Vec::new());
            }
            report.errors.push(gateway_obj_err(
                snapshot,
                format!("UnsupportedValue: TLS listener tls.mode {mode}"),
            ));
            (TlsMode::Plaintext, Vec::new(), Vec::new())
        }
        "TCP" | "UDP" => (TlsMode::Plaintext, Vec::new(), Vec::new()),
        other => {
            report.errors.push(gateway_obj_err(
                snapshot,
                format!("UnsupportedProtocol: {other}"),
            ));
            (TlsMode::Plaintext, Vec::new(), Vec::new())
        }
    }
}

fn allowed_route_group_kinds_for_effective(e: &EffectiveListenerSnap) -> Vec<String> {
    let kinds = e
        .allowed_routes
        .as_ref()
        .map(|a| &a.kinds[..])
        .unwrap_or(&[]);
    if kinds.is_empty() {
        let proto = e.protocol.trim().to_ascii_uppercase();
        if proto == "HTTP" || proto == "HTTPS" {
            return vec![
                "gateway.networking.k8s.io/HTTPRoute".into(),
                "gateway.networking.k8s.io/GRPCRoute".into(),
            ];
        }
        if proto == "TLS" {
            return vec!["gateway.networking.k8s.io/TLSRoute".into()];
        }
        if proto == "TCP" {
            return vec!["gateway.networking.k8s.io/TCPRoute".into()];
        }
        if proto == "UDP" {
            return vec!["gateway.networking.k8s.io/UDPRoute".into()];
        }
        return vec![];
    }
    kinds
        .iter()
        .map(|k| {
            format!(
                "{}/{}",
                k.group.as_deref().unwrap_or("gateway.networking.k8s.io"),
                k.kind
            )
        })
        .collect()
}

/// Build IR + report from cluster snapshot (pure).
#[must_use]
pub fn ir_from_cluster(snapshot: &ClusterSnapshot) -> (IrSnapshot, CompileReport) {
    let correlation_id = Uuid::nil();
    let scope = crate::ir::IrScopeId::new(&snapshot.gateway_namespace, &snapshot.gateway_name);
    let mut report = CompileReport::default();

    record_listener_conflicts(snapshot, &mut report);
    validate_gateway_addresses(snapshot, &mut report);

    let attached_ls: HashSet<(String, String)> = snapshot
        .listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    report.http_grpc_conflict_losers = compute_http_grpc_conflict_losers(snapshot, &attached_ls);
    report.tls_route_conflict_losers = compute_tls_route_conflict_losers(snapshot, &attached_ls);
    report.tcp_route_conflict_losers = compute_tcp_route_conflict_losers(snapshot, &attached_ls);
    report.udp_route_conflict_losers = compute_udp_route_conflict_losers(snapshot, &attached_ls);

    let mut listeners = Vec::new();
    for e in &snapshot.effective_listeners {
        let (tls_mode, tls_cert_pem, tls_key_pem) =
            listener_tls_ir_fields(e, snapshot, &mut report);
        let (pk, pns, pn) = match &e.parent {
            ListenerParentSnap::InlineGateway => (
                "Gateway".to_string(),
                snapshot.gateway_namespace.clone(),
                snapshot.gateway_name.clone(),
            ),
            ListenerParentSnap::ListenerSet { namespace, name } => {
                ("ListenerSet".to_string(), namespace.clone(), name.clone())
            }
        };
        listeners.push(ListenerIr {
            section_name: e.section_name.clone(),
            bind_port: e.port,
            tls: tls_mode,
            gateway_protocol: normalize_protocol(&e.protocol),
            parent_kind: pk,
            parent_namespace: pns,
            parent_name: pn,
            conflicted: e.conflicted,
            hostname: e.hostname.clone(),
            tls_cert_pem,
            tls_key_pem,
            allowed_route_group_kinds: allowed_route_group_kinds_for_effective(e),
        });
    }

    let mut staged: Vec<(RouteSortMeta, RouteRuleIr)> = Vec::new();
    let mut seq = 0usize;

    for route in &snapshot.http_routes {
        let attach_keys = route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }

        for (rule_idx, rule) in route.rules.iter().enumerate() {
            let Some(matches_ir) =
                validate_and_convert_matches(route, rule_idx, &rule.matches, &mut report)
            else {
                continue;
            };

            let filters = extract_rule_filters(route, rule, rule_idx, &mut report);

            let backends_ir =
                resolve_weighted_backends(route, rule, rule_idx, snapshot, &mut report);

            if filters.redirect.is_some() && !rule.backend_refs.is_empty() {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "Invalid: RequestRedirect cannot be combined with backendRefs (rule {rule_idx})"
                    ),
                ));
                continue;
            }

            if filters.redirect.is_none()
                && backends_ir.is_empty()
                && rule.backend_refs.is_empty()
                && rule.filters.is_empty()
            {
                continue;
            }

            if filters.redirect.is_none() && backends_ir.is_empty() && !rule.backend_refs.is_empty() {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("Invalid: rule {rule_idx} has no valid backends"),
                ));
            }

            let meta_ts = route.creation_timestamp_rfc3339.clone().unwrap_or_default();

            for lk in &attach_keys {
                let Some(eff) = find_effective_for_key(snapshot, lk) else {
                    continue;
                };
                if eff.conflicted {
                    continue;
                }
                if report.http_grpc_conflict_loser_at(
                    "HTTPRoute",
                    &route.namespace,
                    &route.name,
                    lk,
                ) {
                    continue;
                }
                let hosts = hosts_for_route_on_listener(route, eff);
                if intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_none() {
                    continue;
                }

                let route_name = format!(
                    "{}/{}/{}:{}",
                    route.namespace, route.name, rule_idx, lk.section_name
                );

                let (request_timeout_ms, backend_timeout_ms) = match &rule.timeouts {
                    Some(t) => (
                        t.request.as_deref().and_then(parse_gateway_duration_ms),
                        t.backend_request.as_deref().and_then(parse_gateway_duration_ms),
                    ),
                    None => (None, None),
                };

                staged.push((
                    RouteSortMeta {
                        ts: meta_ts.clone(),
                        route_ns: route.namespace.clone(),
                        route_name: route.name.clone(),
                        route_kind_order: 0,
                        rule_idx,
                        seq,
                    },
                    RouteRuleIr {
                        name: route_name,
                        route_family: RouteFamily::Http,
                        hosts,
                        matches: matches_ir.clone(),
                        backends: backends_ir.clone(),
                        request_redirect: filters.redirect.clone(),
                        request_header_modifier: filters.request_header_modifier.clone(),
                        response_header_modifier: filters.response_header_modifier.clone(),
                        url_rewrite: filters.url_rewrite.clone(),
                        request_timeout_ms,
                        backend_timeout_ms,
                        cors: filters.cors.clone(),
                        listener: Some(lk.clone()),
                    },
                ));
                seq += 1;
            }
        }
    }

    for route in &snapshot.grpc_routes {
        let attach_keys = grpc_route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }

        for (rule_idx, rule) in route.rules.iter().enumerate() {
            let Some(matches_ir) =
                validate_and_convert_grpc_matches(route, rule_idx, &rule.matches, &mut report)
            else {
                continue;
            };

            let (hdr_mod_ir, resp_hdr_ir) =
                extract_grpc_rule_filters(route, rule, rule_idx, &mut report);

            let backends_ir = resolve_grpc_route_backends(route, rule, snapshot, &mut report);

            if backends_ir.is_empty()
                && rule.backend_refs.is_empty()
                && rule.filters.is_empty()
            {
                continue;
            }

            if backends_ir.is_empty() && !rule.backend_refs.is_empty() {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!("Invalid: rule {rule_idx} has no valid backends"),
                ));
            }

            let meta_ts = route.creation_timestamp_rfc3339.clone().unwrap_or_default();

            for lk in &attach_keys {
                let Some(eff) = find_effective_for_key(snapshot, lk) else {
                    continue;
                };
                if eff.conflicted {
                    continue;
                }
                if report.http_grpc_conflict_loser_at(
                    "GRPCRoute",
                    &route.namespace,
                    &route.name,
                    lk,
                ) {
                    continue;
                }
                let hosts = hosts_for_grpc_route_on_listener(route, eff);
                if intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_none() {
                    continue;
                }

                let route_name = format!(
                    "grpc/{}/{}/{}:{}",
                    route.namespace, route.name, rule_idx, lk.section_name
                );

                staged.push((
                    RouteSortMeta {
                        ts: meta_ts.clone(),
                        route_ns: route.namespace.clone(),
                        route_name: route.name.clone(),
                        route_kind_order: 1,
                        rule_idx,
                        seq,
                    },
                    RouteRuleIr {
                        name: route_name,
                        route_family: RouteFamily::Grpc,
                        hosts,
                        matches: matches_ir.clone(),
                        backends: backends_ir.clone(),
                        request_redirect: None,
                        request_header_modifier: hdr_mod_ir.clone(),
                        response_header_modifier: resp_hdr_ir.clone(),
                        url_rewrite: None,
                        request_timeout_ms: None,
                        backend_timeout_ms: None,
                        cors: None,
                        listener: Some(lk.clone()),
                    },
                ));
                seq += 1;
            }
        }
    }

    for route in &snapshot.tls_routes {
        let attach_keys = tls_route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }
        let Some(rule) = route.rules.first() else {
            continue;
        };
        let rule_idx = 0usize;
        let backends_ir = resolve_tls_route_backends(route, rule, snapshot, &mut report);

        if backends_ir.is_empty() && rule.backend_refs.is_empty() {
            continue;
        }

        if backends_ir.is_empty() && !rule.backend_refs.is_empty() {
            report.errors.push(tlsroute_obj_err(
                route,
                "Invalid: TLSRoute rule has no valid backends",
            ));
        }

        let meta_ts = route.creation_timestamp_rfc3339.clone().unwrap_or_default();

        for lk in &attach_keys {
            let Some(eff) = find_effective_for_key(snapshot, lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if normalize_protocol(&eff.protocol) != "TLS" {
                continue;
            }
            if report.tls_route_conflict_loser_at("TLSRoute", &route.namespace, &route.name, lk) {
                continue;
            }
            let hosts = hosts_for_tls_route_on_listener(route, eff);
            if intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_none() {
                continue;
            }

            let route_name = format!(
                "tls/{}/{}/{}:{}",
                route.namespace, route.name, rule_idx, lk.section_name
            );

            staged.push((
                RouteSortMeta {
                    ts: meta_ts.clone(),
                    route_ns: route.namespace.clone(),
                    route_name: route.name.clone(),
                    route_kind_order: 2,
                    rule_idx,
                    seq,
                },
                RouteRuleIr {
                    name: route_name,
                    route_family: RouteFamily::Tls,
                    hosts,
                    matches: vec![],
                    backends: backends_ir.clone(),
                    request_redirect: None,
                    request_header_modifier: None,
                    response_header_modifier: None,
                    url_rewrite: None,
                    request_timeout_ms: None,
                    backend_timeout_ms: None,
                    cors: None,
                    listener: Some(lk.clone()),
                },
            ));
            seq += 1;
        }
    }

    for route in &snapshot.tcp_routes {
        let attach_keys = tcp_route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }
        let Some(rule) = route.rules.first() else {
            continue;
        };
        let rule_idx = 0usize;
        let backends_ir = resolve_tcp_route_backends(route, rule, snapshot, &mut report);

        if backends_ir.is_empty() && rule.backend_refs.is_empty() {
            continue;
        }

        if backends_ir.is_empty() && !rule.backend_refs.is_empty() {
            report.errors.push(tcproute_obj_err(
                route,
                "Invalid: TCPRoute rule has no valid backends",
            ));
        }

        let meta_ts = route.creation_timestamp_rfc3339.clone().unwrap_or_default();

        for lk in &attach_keys {
            let Some(eff) = find_effective_for_key(snapshot, lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if normalize_protocol(&eff.protocol) != "TCP" {
                continue;
            }
            if report.tcp_route_conflict_loser_at("TCPRoute", &route.namespace, &route.name, lk) {
                continue;
            }
            if !tcp_route_listener_ok(route, eff) {
                continue;
            }

            let route_name = format!(
                "tcp/{}/{}/{}:{}",
                route.namespace, route.name, rule_idx, lk.section_name
            );

            staged.push((
                RouteSortMeta {
                    ts: meta_ts.clone(),
                    route_ns: route.namespace.clone(),
                    route_name: route.name.clone(),
                    route_kind_order: 3,
                    rule_idx,
                    seq,
                },
                RouteRuleIr {
                    name: route_name,
                    route_family: RouteFamily::Tcp,
                    hosts: vec![],
                    matches: vec![],
                    backends: backends_ir.clone(),
                    request_redirect: None,
                    request_header_modifier: None,
                    response_header_modifier: None,
                    url_rewrite: None,
                    request_timeout_ms: None,
                    backend_timeout_ms: None,
                    cors: None,
                    listener: Some(lk.clone()),
                },
            ));
            seq += 1;
        }
    }

    for route in &snapshot.udp_routes {
        let attach_keys = udp_route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }
        let Some(rule) = route.rules.first() else {
            continue;
        };
        let rule_idx = 0usize;
        let backends_ir = resolve_udp_route_backends(route, rule, snapshot, &mut report);

        if backends_ir.is_empty() && rule.backend_refs.is_empty() {
            continue;
        }

        if backends_ir.is_empty() && !rule.backend_refs.is_empty() {
            report.errors.push(udproute_obj_err(
                route,
                "Invalid: UDPRoute rule has no valid backends",
            ));
        }

        let meta_ts = route.creation_timestamp_rfc3339.clone().unwrap_or_default();

        for lk in &attach_keys {
            let Some(eff) = find_effective_for_key(snapshot, lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if normalize_protocol(&eff.protocol) != "UDP" {
                continue;
            }
            if report.udp_route_conflict_loser_at("UDPRoute", &route.namespace, &route.name, lk) {
                continue;
            }
            if !udp_route_listener_ok(route, eff) {
                continue;
            }

            let route_name = format!(
                "udp/{}/{}/{}:{}",
                route.namespace, route.name, rule_idx, lk.section_name
            );

            staged.push((
                RouteSortMeta {
                    ts: meta_ts.clone(),
                    route_ns: route.namespace.clone(),
                    route_name: route.name.clone(),
                    route_kind_order: 4,
                    rule_idx,
                    seq,
                },
                RouteRuleIr {
                    name: route_name,
                    route_family: RouteFamily::Udp,
                    hosts: vec![],
                    matches: vec![],
                    backends: backends_ir.clone(),
                    request_redirect: None,
                    request_header_modifier: None,
                    response_header_modifier: None,
                    url_rewrite: None,
                    request_timeout_ms: None,
                    backend_timeout_ms: None,
                    cors: None,
                    listener: Some(lk.clone()),
                },
            ));
            seq += 1;
        }
    }

    report.listener_route_attachment_counts =
        compute_listener_route_attachment_counts(snapshot, &attached_ls, &report);

    // GEP-1713 match precedence: per-rule specificity determines order, with
    // route metadata (creation timestamp, namespace/name) and rule index as
    // tie-breakers.  Specificity tuple: (path_type, path_len, method,
    // header_count, query_count) — method outranks header count per spec.
    type SpecTuple = (u8, usize, u8, usize, usize);
    let mut augmented: Vec<(SpecTuple, RouteSortMeta, RouteRuleIr)> = staged
        .into_iter()
        .map(|(meta, rule)| {
            let spec = rule_specificity(&rule.matches);
            (spec, meta, rule)
        })
        .collect();
    augmented.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let routes: Vec<RouteRuleIr> = augmented.into_iter().map(|(_, _, r)| r).collect();

    let mut backends: BTreeMap<String, Vec<BackendEndpointIr>> = BTreeMap::new();
    for r in &routes {
        for wb in &r.backends {
            if wb.backend.inference.is_some() {
                continue;
            }
            let key = crate::ir::backend_endpoints_map_key(
                &wb.backend.service_namespace,
                &wb.backend.service_name,
                wb.backend.port_number.unwrap_or(80),
            );
            let mut eps = collect_endpoints(
                snapshot,
                &wb.backend.service_namespace,
                &wb.backend.service_name,
            );
            if let Some(tp) = find_service(
                snapshot,
                &wb.backend.service_namespace,
                &wb.backend.service_name,
            )
            .and_then(|svc| {
                svc.ports.iter().find(|sp| {
                    Some(sp.port) == wb.backend.port_number
                        || wb.backend.port_name.as_deref() == Some(sp.name.as_str())
                })
            })
            .and_then(|sp| sp.target_port)
            {
                for e in &mut eps {
                    e.port = tp;
                }
            }
            if eps.is_empty()
                && !service_has_any_endpointslice(
                    snapshot,
                    &wb.backend.service_namespace,
                    &wb.backend.service_name,
                )
            {
                report.errors.push(ObjectRefErr {
                    group: "".into(),
                    version: "v1".into(),
                    kind: "Endpoints".into(),
                    namespace: wb.backend.service_namespace.clone(),
                    name: wb.backend.service_name.clone(),
                    reason: "NoReadyEndpoints".into(),
                });
            }
            backends.entry(key).or_default().extend(eps);
        }
    }
    for v in backends.values_mut() {
        v.sort();
        v.dedup();
    }

    let has_errors = !report.errors.is_empty();
    let snapshot_out = IrSnapshot {
        ir_version: IR_VERSION,
        revision: crate::ir::IrRevision::Sequence(0),
        scope,
        listeners,
        routes,
        backends,
        meta: SnapshotMeta {
            correlation_id,
            has_errors,
        },
        policy: PolicyOverlayIr::default(),
        inference: InferenceIr::default(),
    };
    (snapshot_out, report)
}

