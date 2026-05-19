//! Build [`crate::ir::IrSnapshot`] + [`crate::compile::CompileReport`] from a [`crate::snapshot::ClusterSnapshot`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use uuid::Uuid;

use crate::compile::{
    CompileReport, HttpGrpcConflictLoser, ObjectRefErr, grpc_route_accepted_condition_for_parent,
    http_route_accepted_condition_for_parent, tcp_route_accepted_condition_for_parent,
    tls_route_accepted_condition_for_parent, udp_route_accepted_condition_for_parent,
};
use crate::http_route_attachment::{
    attachment_keys_for_parent_ref, find_effective_for_key, grpc_attachment_keys_for_parent_ref,
    grpc_route_attachment_keys, grpc_route_listener_hostname_ok, intersect_route_listener_hosts,
    listener_attachment_count_key, reference_grant_permits_inference_pool_ref,
    reference_grant_permits_service_ref, route_attachment_keys, route_listener_hostname_ok,
    tcp_attachment_keys_for_parent_ref, tcp_route_attachment_keys, tcp_route_listener_ok,
    tls_attachment_keys_for_parent_ref, tls_route_attachment_keys, tls_route_listener_hostname_ok,
    udp_attachment_keys_for_parent_ref, udp_route_attachment_keys, udp_route_listener_ok,
};
use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, BackendIr, InferencePoolBackendIr, IrSnapshot, ListenerIr, NamedHeaderIr,
    RequestHeaderModifierIr, RequestRedirectIr, RouteFamily, RouteMatchIr, RouteRuleIr,
    SnapshotMeta, TlsMode, WeightedBackendIr, IR_VERSION,
};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, EffectiveListenerSnap, GrpcRouteMatchSnap, GrpcRouteRuleSnap, GrpcRouteSnap,
    HttpBackendRefSnap, HttpRequestMatchSnap, HttpRouteRuleSnap, HttpRouteSnap, InferencePoolSnap,
    ListenerParentSnap, ServiceSnap, TcpRouteRuleSnap, TcpRouteSnap, TlsRouteRuleSnap,
    TlsRouteSnap, UdpRouteRuleSnap, UdpRouteSnap,
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

/// Gateway API match specificity score (GEP-1713). Higher = more specific = should sort earlier.
/// Computed per match clause; for a rule with multiple OR'd matches, use the maximum.
fn match_specificity(m: &RouteMatchIr) -> (u8, usize, usize, usize, u8) {
    let path_type_score = if m.path_exact.is_some() {
        3u8
    } else if m.path_prefix.is_some() {
        2
    } else {
        1
    };
    let path_len = m
        .path_exact
        .as_ref()
        .or(m.path_prefix.as_ref())
        .map_or(0, |p| p.len());
    let header_count = m.headers_exact.len();
    let query_count = m.query_params_exact.len();
    let method_score = if m.method.is_some() { 1u8 } else { 0 };
    (path_type_score, path_len, header_count, query_count, method_score)
}

/// Max specificity across all match clauses in a rule (higher = more specific).
fn rule_specificity(matches: &[RouteMatchIr]) -> (u8, usize, usize, usize, u8) {
    matches
        .iter()
        .map(match_specificity)
        .max()
        .unwrap_or((1, 1, 0, 0, 0))
}

fn normalize_protocol(p: &str) -> String {
    let t = p.trim();
    if t.is_empty() {
        "HTTP".into()
    } else {
        t.to_ascii_uppercase()
    }
}

fn httproute_obj_err(route: &HttpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "HTTPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn grpcroute_obj_err(route: &GrpcRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "GRPCRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn tlsroute_obj_err(route: &TlsRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1".into(),
        kind: "TLSRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn tcproute_obj_err(route: &TcpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1alpha2".into(),
        kind: "TCPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn udproute_obj_err(route: &UdpRouteSnap, reason: impl Into<String>) -> ObjectRefErr {
    ObjectRefErr {
        group: "gateway.networking.k8s.io".into(),
        version: "v1alpha2".into(),
        kind: "UDPRoute".into(),
        namespace: route.namespace.clone(),
        name: route.name.clone(),
        reason: reason.into(),
    }
}

fn find_inference_pool<'a>(
    snapshot: &'a ClusterSnapshot,
    ns: &str,
    name: &str,
) -> Option<&'a InferencePoolSnap> {
    snapshot
        .inference_pools
        .iter()
        .find(|p| p.namespace == ns && p.name == name)
}

fn inference_pool_backend_tls_key(pool_ns: &str, pool_name: &str, port: u16) -> String {
    format!("inferencepool/{pool_ns}/{pool_name}:{port}")
}

fn resolve_inference_pool_target_port(
    pool: &InferencePoolSnap,
    br: &HttpBackendRefSnap,
    report: &mut CompileReport,
    err: &impl Fn(String) -> ObjectRefErr,
) -> Option<u16> {
    if br.port_name.is_some() {
        report.errors.push(err(
            "UnsupportedValue: InferencePool backendRef.portName not supported".into(),
        ));
        return None;
    }
    if let Some(n) = br.port {
        if pool.target_ports.iter().any(|tp| tp.number == n) {
            return Some(n);
        }
        report.errors.push(err(format!(
            "UnsupportedValue: InferencePool backendRef.port {n} not listed in pool targetPorts"
        )));
        return None;
    }
    match pool.target_ports.len() {
        0 => {
            report.errors.push(err(
                "UnsupportedValue: InferencePool has no targetPorts".into()
            ));
            None
        }
        1 => Some(pool.target_ports[0].number),
        _ => {
            report.errors.push(err(
                "UnsupportedValue: InferencePool backendRef.port required when multiple targetPorts exist"
                    .into(),
            ));
            None
        }
    }
}

fn inference_epp_dns_host(svc_name: &str, svc_namespace: &str) -> String {
    format!("{svc_name}.{svc_namespace}.svc.cluster.local")
}

fn resolve_inference_pool_backend(
    route_namespace: &str,
    route_kind: &str,
    br: &HttpBackendRefSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
    err: impl Fn(String) -> ObjectRefErr,
) -> Option<WeightedBackendIr> {
    let pool_ns = br.namespace.as_deref().unwrap_or(route_namespace);
    let pool_name = br.name.as_str();
    if !reference_grant_permits_inference_pool_ref(
        &snapshot.reference_grants,
        route_namespace,
        pool_ns,
        pool_name,
        route_kind,
    ) {
        report.errors.push(err("RefNotPermitted".into()));
        return None;
    }
    let Some(pool) = find_inference_pool(snapshot, pool_ns, pool_name) else {
        report.errors.push(ObjectRefErr {
            group: "inference.networking.k8s.io".into(),
            version: "v1".into(),
            kind: "InferencePool".into(),
            namespace: pool_ns.to_string(),
            name: pool_name.to_string(),
            reason: "BackendNotFound".into(),
        });
        return None;
    };
    let ep = &pool.endpoint_picker_ref;
    let pk = ep.kind.as_deref().unwrap_or("Service");
    if pk != "Service" {
        report.errors.push(err(format!(
            "InvalidExtensionRef: EndpointPickerRef kind {pk} not supported"
        )));
        return None;
    }
    let Some(port_snap) = ep.port.as_ref() else {
        report.errors.push(err(
            "InvalidExtensionRef: EndpointPickerRef.port required for Service".into(),
        ));
        return None;
    };
    let picker_port = port_snap.number;
    let svc_name = ep.name.as_str();
    if svc_name.is_empty() {
        report.errors.push(err(
            "InvalidExtensionRef: EndpointPickerRef.name required".into()
        ));
        return None;
    }
    let Some(svc) = find_service(snapshot, pool_ns, svc_name) else {
        report.errors.push(ObjectRefErr {
            group: "".into(),
            version: "v1".into(),
            kind: "Service".into(),
            namespace: pool_ns.to_string(),
            name: svc_name.to_string(),
            reason: "BackendNotFound".into(),
        });
        return None;
    };
    if !svc.ports.iter().any(|p| p.port == picker_port) {
        report.errors.push(err(format!(
            "InvalidExtensionRef: Service {svc_name} has no port number {picker_port}"
        )));
        return None;
    }
    let Some(target_port) = resolve_inference_pool_target_port(pool, br, report, &err) else {
        return None;
    };
    let tls_key = inference_pool_backend_tls_key(pool_ns, pool_name, target_port);
    let backend_tls = snapshot
        .backend_tls_effective
        .get(&tls_key)
        .map(|e| e.tls.clone());
    if backend_tls.as_ref().is_some_and(|t| t.invalid) {
        report.errors.push(err("InvalidCACertificateRef".into()));
    }
    let fail_open = ep
        .failure_mode
        .as_deref()
        .is_some_and(|m| m.eq_ignore_ascii_case("FailOpen"));
    let picker_host = inference_epp_dns_host(svc_name, pool_ns);
    Some(WeightedBackendIr {
        backend: BackendIr {
            service_namespace: String::new(),
            service_name: String::new(),
            port_name: None,
            port_number: None,
            backend_tls,
            inference: Some(InferencePoolBackendIr {
                pool_namespace: pool_ns.to_string(),
                pool_name: pool_name.to_string(),
                picker_host,
                picker_port,
                failure_mode_fail_open: fail_open,
            }),
            dial_port: None,
        },
        weight: br.weight.unwrap_or(1),
    })
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

fn validate_and_convert_matches(
    route: &HttpRouteSnap,
    rule_idx: usize,
    matches: &[HttpRequestMatchSnap],
    report: &mut CompileReport,
) -> Option<Vec<RouteMatchIr>> {
    let src: Vec<HttpRequestMatchSnap> = if matches.is_empty() {
        vec![HttpRequestMatchSnap::default()]
    } else {
        matches.to_vec()
    };
    let mut out = Vec::new();
    for (mi, m) in src.iter().enumerate() {
        let mut headers_exact = BTreeMap::new();
        for h in &m.headers {
            let mt = h.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: header match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            headers_exact.insert(h.name.to_ascii_lowercase(), h.value.clone());
        }

        let (path_prefix, path_exact) = match &m.path {
            None => (Some("/".into()), None),
            Some(p) => {
                let typ = p.path_type.as_deref().unwrap_or("PathPrefix");
                let val = p.value.clone().unwrap_or_else(|| "/".into());
                match typ {
                    "Exact" => (None, Some(val)),
                    "PathPrefix" => (Some(val), None),
                    other => {
                        report.errors.push(httproute_obj_err(
                            route,
                            format!("UnsupportedValue: path match type {other}"),
                        ));
                        return None;
                    }
                }
            }
        };
        let mut query_params_exact = BTreeMap::new();
        for qp in &m.query_params {
            let mt = qp.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: query param match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            query_params_exact.insert(qp.name.clone(), qp.value.clone());
        }

        out.push(RouteMatchIr {
            path_prefix,
            path_exact,
            headers_exact,
            method: m.method.clone(),
            query_params_exact,
            grpc_service: None,
            grpc_method: None,
        });
    }
    Some(out)
}

struct HttpRuleFilters {
    redirect: Option<RequestRedirectIr>,
    request_header_modifier: Option<RequestHeaderModifierIr>,
    response_header_modifier: Option<RequestHeaderModifierIr>,
    url_rewrite: Option<crate::ir::UrlRewriteIr>,
    cors: Option<crate::ir::CorsFilterIr>,
}

fn header_filter_snap_to_ir(hm: &crate::snapshot::HttpHeaderFilterSnap) -> RequestHeaderModifierIr {
    RequestHeaderModifierIr {
        set: hm
            .set
            .iter()
            .map(|x| NamedHeaderIr {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: hm
            .add
            .iter()
            .map(|x| NamedHeaderIr {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: hm.remove.clone(),
    }
}

fn extract_rule_filters(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    rule_idx: usize,
    report: &mut CompileReport,
) -> HttpRuleFilters {
    let mut redirect: Option<RequestRedirectIr> = None;
    let mut hdr_mod: Option<RequestHeaderModifierIr> = None;
    let mut resp_hdr: Option<RequestHeaderModifierIr> = None;
    let mut url_rewrite: Option<crate::ir::UrlRewriteIr> = None;
    let mut cors: Option<crate::ir::CorsFilterIr> = None;

    for (fi, f) in rule.filters.iter().enumerate() {
        match f.filter_type.as_str() {
            "RequestRedirect" => {
                let Some(rr) = f.request_redirect.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!("InvalidReference: RequestRedirect filter missing body (rule {rule_idx} filter {fi})"),
                    ));
                    continue;
                };
                let (path_replace_full, path_replace_prefix) = match &rr.path {
                    None => (None, None),
                    Some(pm) => match pm.modifier_type.as_str() {
                        "ReplaceFullPath" => (pm.replace_full_path.clone(), None),
                        "ReplacePrefixMatch" => (None, pm.replace_prefix_match.clone()),
                        _ => {
                            report.errors.push(httproute_obj_err(
                                route,
                                format!(
                                    "UnsupportedValue: redirect path modifier {} (rule {rule_idx})",
                                    pm.modifier_type
                                ),
                            ));
                            (None, None)
                        }
                    },
                };
                redirect = Some(RequestRedirectIr {
                    hostname: rr.hostname.clone(),
                    scheme: rr.scheme.clone(),
                    port: rr.port,
                    status_code: rr.status_code.map(|c| c.clamp(0, 599) as u32),
                    path_replace_full,
                    path_replace_prefix,
                });
            }
            "RequestHeaderModifier" => {
                let Some(hm) = f.request_header_modifier.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: RequestHeaderModifier missing body (rule {rule_idx})"
                        ),
                    ));
                    continue;
                };
                hdr_mod = Some(header_filter_snap_to_ir(hm));
            }
            "ResponseHeaderModifier" => {
                let Some(hm) = f.response_header_modifier.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: ResponseHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                resp_hdr = Some(header_filter_snap_to_ir(hm));
            }
            "URLRewrite" => {
                let Some(uw) = f.url_rewrite.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: URLRewrite filter missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                let (path_replace_full, path_replace_prefix) = match &uw.path {
                    None => (None, None),
                    Some(pm) => match pm.modifier_type.as_str() {
                        "ReplaceFullPath" => (pm.replace_full_path.clone(), None),
                        "ReplacePrefixMatch" => (None, pm.replace_prefix_match.clone()),
                        _ => {
                            report.errors.push(httproute_obj_err(
                                route,
                                format!(
                                    "UnsupportedValue: url rewrite path modifier {} (rule {rule_idx})",
                                    pm.modifier_type
                                ),
                            ));
                            (None, None)
                        }
                    },
                };
                url_rewrite = Some(crate::ir::UrlRewriteIr {
                    hostname: uw.hostname.clone(),
                    path_replace_full,
                    path_replace_prefix,
                });
            }
            "CORS" => {
                let Some(c) = f.cors.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!("InvalidReference: CORS filter missing body (rule {rule_idx} filter {fi})"),
                    ));
                    continue;
                };
                cors = Some(crate::ir::CorsFilterIr {
                    allow_origins: c.allow_origins.clone(),
                    allow_methods: c.allow_methods.clone(),
                    allow_headers: c.allow_headers.clone(),
                    expose_headers: c.expose_headers.clone(),
                    max_age: c.max_age.map(|v| v.max(0) as u32).unwrap_or(5),
                    allow_credentials: c.allow_credentials,
                });
            }
            "RequestMirror" | "ExtensionRef" => {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {} (rule {rule_idx} filter {fi})", f.filter_type),
                ));
            }
            other => {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {other} (rule {rule_idx})"),
                ));
            }
        }
    }
    HttpRuleFilters {
        redirect,
        request_header_modifier: hdr_mod,
        response_header_modifier: resp_hdr,
        url_rewrite,
        cors,
    }
}

fn resolve_service_port(svc: &ServiceSnap, br: &HttpBackendRefSnap) -> Result<u16, String> {
    if let Some(p) = br.port {
        return Ok(p);
    }
    let pname = br.port_name.as_deref().unwrap_or("");
    for sp in &svc.ports {
        if !pname.is_empty() && sp.name == pname {
            return Ok(sp.port);
        }
    }
    if br.port.is_none() && svc.ports.len() == 1 {
        return Ok(svc.ports[0].port);
    }
    Err("port not found on Service".into())
}

fn headless_dial_port(svc: &ServiceSnap, service_port: u16) -> Option<u16> {
    if svc.cluster_ip.as_deref() != Some("None") {
        return None;
    }
    svc.ports
        .iter()
        .find(|sp| sp.port == service_port)
        .and_then(|sp| sp.target_port)
}

fn find_service<'a>(sn: &'a ClusterSnapshot, ns: &str, name: &str) -> Option<&'a ServiceSnap> {
    sn.services
        .iter()
        .find(|s| s.namespace == ns && s.name == name)
}

fn collect_endpoints(sn: &ClusterSnapshot, svc_ns: &str, svc_name: &str) -> Vec<BackendEndpointIr> {
    let mut out = Vec::new();
    for sl in &sn.endpoint_slices {
        if sl.namespace != svc_ns {
            continue;
        }
        if sl
            .service_name
            .as_deref()
            .map(|n| n == svc_name)
            .unwrap_or(false)
        {
            for ep in &sl.endpoints {
                for addr in &ep.addresses {
                    for p in &ep.ports {
                        out.push(BackendEndpointIr {
                            address: addr.clone(),
                            port: p.port,
                        });
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn service_has_any_endpointslice(snapshot: &ClusterSnapshot, svc_ns: &str, svc_name: &str) -> bool {
    snapshot.endpoint_slices.iter().any(|sl| {
        sl.namespace == svc_ns && sl.service_name.as_deref() == Some(svc_name)
    })
}

fn record_listener_conflicts(snapshot: &ClusterSnapshot, report: &mut CompileReport) {
    let mut seen_gateway = false;
    let mut seen_ls: HashSet<(String, String)> = HashSet::new();
    for e in &snapshot.effective_listeners {
        if !e.conflicted {
            continue;
        }
        match &e.parent {
            ListenerParentSnap::InlineGateway => {
                if !seen_gateway {
                    seen_gateway = true;
                    report.errors.push(ObjectRefErr {
                        group: "gateway.networking.k8s.io".into(),
                        version: "v1".into(),
                        kind: "Gateway".into(),
                        namespace: snapshot.gateway_namespace.clone(),
                        name: snapshot.gateway_name.clone(),
                        reason: "ListenersConflicted".into(),
                    });
                }
            }
            ListenerParentSnap::ListenerSet { namespace, name } => {
                if seen_ls.insert((namespace.clone(), name.clone())) {
                    report.errors.push(ObjectRefErr {
                        group: "gateway.networking.k8s.io".into(),
                        version: "v1".into(),
                        kind: "ListenerSet".into(),
                        namespace: namespace.clone(),
                        name: name.clone(),
                        reason: "ListenersConflicted".into(),
                    });
                }
            }
        }
    }
}

fn validate_and_convert_grpc_matches(
    route: &GrpcRouteSnap,
    rule_idx: usize,
    matches: &[GrpcRouteMatchSnap],
    report: &mut CompileReport,
) -> Option<Vec<RouteMatchIr>> {
    let src: Vec<GrpcRouteMatchSnap> = if matches.is_empty() {
        vec![GrpcRouteMatchSnap::default()]
    } else {
        matches.to_vec()
    };
    let mut out = Vec::new();
    for (mi, m) in src.iter().enumerate() {
        let mut headers_exact = BTreeMap::new();
        for h in &m.headers {
            let mt = h.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: gRPC header match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            headers_exact.insert(h.name.to_ascii_lowercase(), h.value.clone());
        }

        let (grpc_service, grpc_method) = match &m.method {
            None => (None, None),
            Some(mm) => {
                let typ = mm.match_type.as_deref().unwrap_or("Exact");
                if typ == "RegularExpression" {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "UnsupportedValue: gRPC method RegularExpression (rule {rule_idx} match {mi})"
                        ),
                    ));
                    return None;
                }
                if typ != "Exact" && !typ.is_empty() {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "UnsupportedValue: gRPC method match type {typ} (rule {rule_idx} match {mi})"
                        ),
                    ));
                    return None;
                }
                (
                    mm.service.clone().filter(|s| !s.is_empty()),
                    mm.method.clone().filter(|s| !s.is_empty()),
                )
            }
        };

        out.push(RouteMatchIr {
            path_prefix: None,
            path_exact: None,
            headers_exact,
            method: None,
            query_params_exact: BTreeMap::new(),
            grpc_service,
            grpc_method,
        });
    }
    Some(out)
}

fn extract_grpc_rule_filters(
    route: &GrpcRouteSnap,
    rule: &GrpcRouteRuleSnap,
    rule_idx: usize,
    report: &mut CompileReport,
) -> (
    Option<RequestHeaderModifierIr>,
    Option<RequestHeaderModifierIr>,
) {
    let mut hdr_mod: Option<RequestHeaderModifierIr> = None;
    let mut resp_hdr: Option<RequestHeaderModifierIr> = None;

    for (fi, f) in rule.filters.iter().enumerate() {
        match f.filter_type.as_str() {
            "RequestHeaderModifier" => {
                let Some(hm) = f.request_header_modifier.as_ref() else {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "InvalidReference: RequestHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                hdr_mod = Some(RequestHeaderModifierIr {
                    set: hm
                        .set
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    add: hm
                        .add
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    remove: hm.remove.clone(),
                });
            }
            "ResponseHeaderModifier" => {
                let Some(hm) = f.response_header_modifier.as_ref() else {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "InvalidReference: ResponseHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                resp_hdr = Some(RequestHeaderModifierIr {
                    set: hm
                        .set
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    add: hm
                        .add
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    remove: hm.remove.clone(),
                });
            }
            "RequestMirror" | "ExtensionRef" => {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: filter type {} (rule {rule_idx} filter {fi})",
                        f.filter_type
                    ),
                ));
            }
            other => {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {other} (rule {rule_idx})"),
                ));
            }
        }
    }
    (hdr_mod, resp_hdr)
}

fn resolve_http_route_backends(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.is_inference_pool() {
            if let Some(wb) = resolve_inference_pool_backend(
                &route.namespace,
                "HTTPRoute",
                br,
                snapshot,
                report,
                |msg| httproute_obj_err(route, msg),
            ) {
                backends.push(wb);
            }
            continue;
        }
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidKind".into(),
            });
            continue;
        }
        let svc_ns = br.namespace.as_deref().unwrap_or(&route.namespace);
        let svc_name = &br.name;
        if !reference_grant_permits_service_ref(
            &snapshot.reference_grants,
            &route.namespace,
            svc_ns,
            svc_name,
            "HTTPRoute",
        ) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "RefNotPermitted".into(),
            });
            continue;
        }
        let Some(svc) = find_service(snapshot, svc_ns, svc_name) else {
            report.errors.push(ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: svc_ns.to_string(),
                name: svc_name.clone(),
                reason: "BackendNotFound".into(),
            });
            continue;
        };
        let port = match resolve_service_port(svc, br) {
            Ok(p) => p,
            Err(reason) => {
                report.errors.push(httproute_obj_err(route, reason));
                continue;
            }
        };
        let backend_tls = snapshot
            .backend_tls_effective
            .get(&crate::ir::backend_endpoints_map_key(svc_ns, svc_name, port))
            .map(|e| e.tls.clone());
        if backend_tls.as_ref().is_some_and(|t| t.invalid) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidCACertificateRef".into(),
            });
        }
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
                backend_tls,
                inference: None,
                dial_port: headless_dial_port(svc, port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
}

fn resolve_grpc_route_backends(
    route: &GrpcRouteSnap,
    rule: &GrpcRouteRuleSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.is_inference_pool() {
            if let Some(wb) = resolve_inference_pool_backend(
                &route.namespace,
                "GRPCRoute",
                br,
                snapshot,
                report,
                |msg| grpcroute_obj_err(route, msg),
            ) {
                backends.push(wb);
            }
            continue;
        }
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "GRPCRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidKind".into(),
            });
            continue;
        }
        let svc_ns = br.namespace.as_deref().unwrap_or(&route.namespace);
        let svc_name = &br.name;
        if !reference_grant_permits_service_ref(
            &snapshot.reference_grants,
            &route.namespace,
            svc_ns,
            svc_name,
            "GRPCRoute",
        ) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "GRPCRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "RefNotPermitted".into(),
            });
            continue;
        }
        let Some(svc) = find_service(snapshot, svc_ns, svc_name) else {
            report.errors.push(ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: svc_ns.to_string(),
                name: svc_name.clone(),
                reason: "BackendNotFound".into(),
            });
            continue;
        };
        let port = match resolve_service_port(svc, br) {
            Ok(p) => p,
            Err(reason) => {
                report.errors.push(grpcroute_obj_err(route, reason));
                continue;
            }
        };
        let backend_tls = snapshot
            .backend_tls_effective
            .get(&crate::ir::backend_endpoints_map_key(svc_ns, svc_name, port))
            .map(|e| e.tls.clone());
        if backend_tls.as_ref().is_some_and(|t| t.invalid) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "GRPCRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidCACertificateRef".into(),
            });
        }
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
                backend_tls,
                inference: None,
                dial_port: headless_dial_port(svc, port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
}

fn resolve_tls_route_backends(
    route: &TlsRouteSnap,
    rule: &TlsRouteRuleSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.is_inference_pool() {
            if let Some(wb) = resolve_inference_pool_backend(
                &route.namespace,
                "TLSRoute",
                br,
                snapshot,
                report,
                |msg| tlsroute_obj_err(route, msg),
            ) {
                backends.push(wb);
            }
            continue;
        }
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "TLSRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidKind".into(),
            });
            continue;
        }
        let svc_ns = br.namespace.as_deref().unwrap_or(&route.namespace);
        let svc_name = &br.name;
        if !reference_grant_permits_service_ref(
            &snapshot.reference_grants,
            &route.namespace,
            svc_ns,
            svc_name,
            "TLSRoute",
        ) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "TLSRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "RefNotPermitted".into(),
            });
            continue;
        }
        let Some(svc) = find_service(snapshot, svc_ns, svc_name) else {
            report.errors.push(ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: svc_ns.to_string(),
                name: svc_name.clone(),
                reason: "BackendNotFound".into(),
            });
            continue;
        };
        let port = match resolve_service_port(svc, br) {
            Ok(p) => p,
            Err(reason) => {
                report.errors.push(tlsroute_obj_err(route, reason));
                continue;
            }
        };
        let backend_tls = snapshot
            .backend_tls_effective
            .get(&crate::ir::backend_endpoints_map_key(svc_ns, svc_name, port))
            .map(|e| e.tls.clone());
        if backend_tls.as_ref().is_some_and(|t| t.invalid) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "TLSRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidCACertificateRef".into(),
            });
        }
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
                backend_tls,
                inference: None,
                dial_port: headless_dial_port(svc, port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
}

fn resolve_tcp_route_backends(
    route: &TcpRouteSnap,
    rule: &TcpRouteRuleSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.is_inference_pool() {
            if let Some(wb) = resolve_inference_pool_backend(
                &route.namespace,
                "TCPRoute",
                br,
                snapshot,
                report,
                |msg| tcproute_obj_err(route, msg),
            ) {
                backends.push(wb);
            }
            continue;
        }
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "TCPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidKind".into(),
            });
            continue;
        }
        let svc_ns = br.namespace.as_deref().unwrap_or(&route.namespace);
        let svc_name = &br.name;
        if !reference_grant_permits_service_ref(
            &snapshot.reference_grants,
            &route.namespace,
            svc_ns,
            svc_name,
            "TCPRoute",
        ) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "TCPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "RefNotPermitted".into(),
            });
            continue;
        }
        let Some(svc) = find_service(snapshot, svc_ns, svc_name) else {
            report.errors.push(ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: svc_ns.to_string(),
                name: svc_name.clone(),
                reason: "BackendNotFound".into(),
            });
            continue;
        };
        let port = match resolve_service_port(svc, br) {
            Ok(p) => p,
            Err(reason) => {
                report.errors.push(tcproute_obj_err(route, reason));
                continue;
            }
        };
        let backend_tls = snapshot
            .backend_tls_effective
            .get(&crate::ir::backend_endpoints_map_key(svc_ns, svc_name, port))
            .map(|e| e.tls.clone());
        if backend_tls.as_ref().is_some_and(|t| t.invalid) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "TCPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidCACertificateRef".into(),
            });
        }
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
                backend_tls,
                inference: None,
                dial_port: headless_dial_port(svc, port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
}

fn resolve_udp_route_backends(
    route: &UdpRouteSnap,
    rule: &UdpRouteRuleSnap,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.is_inference_pool() {
            if let Some(wb) = resolve_inference_pool_backend(
                &route.namespace,
                "UDPRoute",
                br,
                snapshot,
                report,
                |msg| udproute_obj_err(route, msg),
            ) {
                backends.push(wb);
            }
            continue;
        }
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "UDPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidKind".into(),
            });
            continue;
        }
        let svc_ns = br.namespace.as_deref().unwrap_or(&route.namespace);
        let svc_name = &br.name;
        if !reference_grant_permits_service_ref(
            &snapshot.reference_grants,
            &route.namespace,
            svc_ns,
            svc_name,
            "UDPRoute",
        ) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "UDPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "RefNotPermitted".into(),
            });
            continue;
        }
        let Some(svc) = find_service(snapshot, svc_ns, svc_name) else {
            report.errors.push(ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: svc_ns.to_string(),
                name: svc_name.clone(),
                reason: "BackendNotFound".into(),
            });
            continue;
        };
        let port = match resolve_service_port(svc, br) {
            Ok(p) => p,
            Err(reason) => {
                report.errors.push(udproute_obj_err(route, reason));
                continue;
            }
        };
        let backend_tls = snapshot
            .backend_tls_effective
            .get(&crate::ir::backend_endpoints_map_key(svc_ns, svc_name, port))
            .map(|e| e.tls.clone());
        if backend_tls.as_ref().is_some_and(|t| t.invalid) {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1alpha2".into(),
                kind: "UDPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "InvalidCACertificateRef".into(),
            });
        }
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
                backend_tls,
                inference: None,
                dial_port: headless_dial_port(svc, port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
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

fn hosts_for_grpc_route_on_listener(
    route: &GrpcRouteSnap,
    eff: &EffectiveListenerSnap,
) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

fn hosts_for_tls_route_on_listener(
    route: &TlsRouteSnap,
    eff: &EffectiveListenerSnap,
) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

fn host_sets_overlap(a: &[String], b: &[String]) -> bool {
    if a.is_empty() || b.is_empty() {
        return true;
    }
    a.iter().any(|x| b.iter().any(|y| x == y))
}

fn grpc_route_loses_http_conflict(h: &HttpRouteSnap, g: &GrpcRouteSnap) -> bool {
    match h
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(g.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (h.namespace.as_str(), h.name.as_str()) < (g.namespace.as_str(), g.name.as_str())
        }
    }
}

fn http_route_loses_grpc_conflict(h: &HttpRouteSnap, g: &GrpcRouteSnap) -> bool {
    match h
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(g.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => false,
        Ordering::Greater => true,
        Ordering::Equal => {
            (h.namespace.as_str(), h.name.as_str()) > (g.namespace.as_str(), g.name.as_str())
        }
    }
}

fn compute_http_grpc_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.http_routes {
        for k in route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }
    for r in &snapshot.grpc_routes {
        for k in grpc_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }

        let http_on: Vec<&HttpRouteSnap> = snapshot
            .http_routes
            .iter()
            .filter(|r| {
                route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && route_listener_hostname_ok(r, eff)
            })
            .collect();

        let grpc_on: Vec<&GrpcRouteSnap> = snapshot
            .grpc_routes
            .iter()
            .filter(|r| {
                grpc_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && grpc_route_listener_hostname_ok(r, eff)
            })
            .collect();

        for h in &http_on {
            for g in &grpc_on {
                let hh = hosts_for_route_on_listener(h, eff);
                let gh = hosts_for_grpc_route_on_listener(g, eff);
                if !host_sets_overlap(&hh, &gh) {
                    continue;
                }
                if grpc_route_loses_http_conflict(h, g) {
                    losers.push(HttpGrpcConflictLoser {
                        route_kind: "GRPCRoute".into(),
                        route_namespace: g.namespace.clone(),
                        route_name: g.name.clone(),
                        listener: lk.clone(),
                    });
                } else if http_route_loses_grpc_conflict(h, g) {
                    losers.push(HttpGrpcConflictLoser {
                        route_kind: "HTTPRoute".into(),
                        route_namespace: h.namespace.clone(),
                        route_name: h.name.clone(),
                        listener: lk.clone(),
                    });
                }
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

fn tls_route_hostname_specificity_score(host: &str) -> u64 {
    let h = host.trim();
    if let Some(rest) = h.strip_prefix("*.") {
        return 1_000 + rest.len() as u64;
    }
    10_000 + h.len() as u64
}

fn tls_route_max_score_on_listener(route: &TlsRouteSnap, eff: &EffectiveListenerSnap) -> u64 {
    let hosts = hosts_for_tls_route_on_listener(route, eff);
    hosts
        .iter()
        .map(|h| tls_route_hostname_specificity_score(h))
        .max()
        .unwrap_or(0)
}

/// Returns true if route `a` has precedence over route `b` on this listener (overlapping SNIs).
fn tls_route_beats(a: &TlsRouteSnap, b: &TlsRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    let sa = tls_route_max_score_on_listener(a, eff);
    let sb = tls_route_max_score_on_listener(b, eff);
    match sa.cmp(&sb) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => match a
            .creation_timestamp_rfc3339
            .as_deref()
            .unwrap_or("")
            .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
        {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
            }
        },
    }
}

fn compute_tls_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.tls_routes {
        for k in tls_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "TLS" {
            continue;
        }

        let routes_on: Vec<&TlsRouteSnap> = snapshot
            .tls_routes
            .iter()
            .filter(|r| {
                tls_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && tls_route_listener_hostname_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                let hi = hosts_for_tls_route_on_listener(ri, eff);
                let hj = hosts_for_tls_route_on_listener(rj, eff);
                if !host_sets_overlap(&hi, &hj) {
                    continue;
                }
                if tls_route_beats(rj, ri, eff) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "TLSRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

/// Returns true if TCPRoute `a` has precedence over `b` on the same TCP listener (older wins; tie-break name).
fn tcp_route_beats(a: &TcpRouteSnap, b: &TcpRouteSnap) -> bool {
    match a
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
        }
    }
}

fn compute_tcp_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.tcp_routes {
        for k in tcp_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "TCP" {
            continue;
        }

        let routes_on: Vec<&TcpRouteSnap> = snapshot
            .tcp_routes
            .iter()
            .filter(|r| {
                tcp_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && tcp_route_listener_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                if tcp_route_beats(rj, ri) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "TCPRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

/// Returns true if UDPRoute `a` has precedence over `b` on the same UDP listener (older wins; tie-break name).
fn udp_route_beats(a: &UdpRouteSnap, b: &UdpRouteSnap) -> bool {
    match a
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
        }
    }
}

fn compute_udp_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.udp_routes {
        for k in udp_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "UDP" {
            continue;
        }

        let routes_on: Vec<&UdpRouteSnap> = snapshot
            .udp_routes
            .iter()
            .filter(|r| {
                udp_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && udp_route_listener_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                if udp_route_beats(rj, ri) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "UDPRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

fn resolve_weighted_backends(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    _rule_idx: usize,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    resolve_http_route_backends(route, rule, snapshot, report)
}

fn hosts_for_route_on_listener(route: &HttpRouteSnap, eff: &EffectiveListenerSnap) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

fn bump_listener_route_count(counts: &mut BTreeMap<String, u32>, lk: &crate::ir::ListenerAttachmentIr) {
    *counts
        .entry(listener_attachment_count_key(lk))
        .or_insert(0) += 1;
}

fn http_route_attachment_accepted_for_listener(
    route: &HttpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) = http_route_accepted_condition_for_parent(
            route, parent, snapshot, attached_ls, report,
        );
        if st == "True" {
            return true;
        }
    }
    false
}

fn grpc_route_attachment_accepted_for_listener(
    route: &GrpcRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = grpc_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) = grpc_route_accepted_condition_for_parent(
            route, parent, snapshot, attached_ls, report,
        );
        if st == "True" {
            return true;
        }
    }
    false
}

fn tls_route_attachment_accepted_for_listener(
    route: &TlsRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = tls_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) = tls_route_accepted_condition_for_parent(
            route, parent, snapshot, attached_ls, report,
        );
        if st == "True" {
            return true;
        }
    }
    false
}

fn tcp_route_attachment_accepted_for_listener(
    route: &TcpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = tcp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) = tcp_route_accepted_condition_for_parent(
            route, parent, snapshot, attached_ls, report,
        );
        if st == "True" {
            return true;
        }
    }
    false
}

fn udp_route_attachment_accepted_for_listener(
    route: &UdpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = udp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) = udp_route_accepted_condition_for_parent(
            route, parent, snapshot, attached_ls, report,
        );
        if st == "True" {
            return true;
        }
    }
    false
}

/// Gateway API `attachedRoutes`: count each route resource once per listener only when it would be
/// **Accepted** there (hostname intersection + HTTP/GRPC/TLS/TCP/UDP precedence), not merely policy-attached.
fn compute_listener_route_attachment_counts(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();

    for route in &snapshot.http_routes {
        for lk in route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.http_grpc_conflict_loser_at("HTTPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !http_route_attachment_accepted_for_listener(route, &lk, snapshot, attached_ls, report)
            {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.grpc_routes {
        for lk in grpc_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !grpc_route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.http_grpc_conflict_loser_at("GRPCRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !grpc_route_attachment_accepted_for_listener(route, &lk, snapshot, attached_ls, report)
            {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.tls_routes {
        for lk in tls_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !tls_route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.tls_route_conflict_loser_at("TLSRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !tls_route_attachment_accepted_for_listener(route, &lk, snapshot, attached_ls, report)
            {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.tcp_routes {
        for lk in tcp_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted || !tcp_route_listener_ok(route, eff) {
                continue;
            }
            if report.tcp_route_conflict_loser_at("TCPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !tcp_route_attachment_accepted_for_listener(route, &lk, snapshot, attached_ls, report)
            {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.udp_routes {
        for lk in udp_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted || !udp_route_listener_ok(route, eff) {
                continue;
            }
            if report.udp_route_conflict_loser_at("UDPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !udp_route_attachment_accepted_for_listener(route, &lk, snapshot, attached_ls, report)
            {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    counts
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

    // Gateway API spec: within a single Route, rules are evaluated in declaration
    // order (first matching rule wins). Across Routes, the most specific match
    // wins. To honour both constraints in a flat list we sort by *route-level*
    // max specificity (so more-specific routes come first), then fall through to
    // RouteSortMeta which preserves rule_idx within each route.
    type SpecTuple = (u8, usize, usize, usize, u8);
    let route_max_spec: std::collections::HashMap<(String, String, String, u8), SpecTuple> = {
        let mut m = std::collections::HashMap::new();
        for (meta, rule) in &staged {
            let key = (
                meta.ts.clone(),
                meta.route_ns.clone(),
                meta.route_name.clone(),
                meta.route_kind_order,
            );
            let spec = rule_specificity(&rule.matches);
            m.entry(key)
                .and_modify(|e: &mut SpecTuple| {
                    if spec > *e {
                        *e = spec;
                    }
                })
                .or_insert(spec);
        }
        m
    };
    let mut augmented: Vec<(SpecTuple, RouteSortMeta, RouteRuleIr)> = staged
        .into_iter()
        .map(|(meta, rule)| {
            let key = (
                meta.ts.clone(),
                meta.route_ns.clone(),
                meta.route_name.clone(),
                meta.route_kind_order,
            );
            let rspec = route_max_spec[&key];
            (rspec, meta, rule)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::ir::{BackendTlsIr, RouteFamily};
    use crate::listener_merge::merge_effective_listeners;
    use crate::snapshot::{
        AllowedRoutesNamespacesSnap, AllowedRoutesSnap, BackendTlsEffectiveSnap, EndpointPortSnap,
        EndpointSliceSnap, EndpointSnap, GatewayListenerSnap, GatewayListenerTlsSnap,
        GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap, HttpPathMatchSnap, HttpRouteRuleSnap,
        ListenerSetSnap, ParentRefSnap, ReferenceGrantFromSnap, ReferenceGrantSnap,
        ReferenceGrantToSnap, RouteGroupKindSnap, ServicePortSnap, ServiceSnap,
        TcpRouteRuleSnap, TcpRouteSnap, TlsRouteRuleSnap, TlsRouteSnap, UdpRouteRuleSnap,
        UdpRouteSnap,
    };

    fn minimal_cluster() -> ClusterSnapshot {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: None,
            tls: None,
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners,
            effective_listeners,
            listener_sets: vec![],
            rejected_listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![HttpRouteSnap {
                namespace: "default".into(),
                name: "hr1".into(),
                generation: 1,
                creation_timestamp_rfc3339: None,
                parent_refs: vec![ParentRefSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                hostnames: vec!["example.com".into()],
                rules: vec![HttpRouteRuleSnap {
                    matches: vec![HttpRequestMatchSnap {
                        path: Some(HttpPathMatchSnap {
                            path_type: Some("PathPrefix".into()),
                            value: Some("/".into()),
                        }),
                        headers: vec![],
                        ..Default::default()
                    }],
                    filters: vec![],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: None,
                        name: "svc".into(),
                        port: Some(80),
                        port_name: None,
                        weight: None,
                    }],
                    ..Default::default()
                }],
            }],
            grpc_routes: vec![],
            tls_routes: vec![],
            tcp_routes: vec![],
            udp_routes: vec![],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "http".into(),
                    port: 80,
                    target_port: Some(8080),
                }],
                cluster_ip: None,
            }],
            endpoint_slices: vec![EndpointSliceSnap {
                namespace: "default".into(),
                name: "svc-1".into(),
                service_name: Some("svc".into()),
                endpoints: vec![EndpointSnap {
                    addresses: vec!["10.0.0.1".into()],
                    ports: vec![EndpointPortSnap {
                        name: Some("http".into()),
                        port: 8080,
                    }],
                }],
            }],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: BTreeMap::new(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: BTreeMap::new(),
            inference_pools: vec![],
        }
    }

    #[test]
    fn http_backend_attaches_backend_tls_from_snapshot() {
        let mut snap = minimal_cluster();
        snap.backend_tls_effective.insert(
            "default/svc:80".into(),
            BackendTlsEffectiveSnap {
                tls: BackendTlsIr {
                    ca_cert_pem: b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"
                        .to_vec(),
                    hostname: "svc.example.com".into(),
                    sans: vec!["alt.example.com".into()],
                    trust_system_ca: false,
                    client_cert_pem: vec![],
                    client_key_pem: vec![],
                    invalid: false,
                },
            },
        );
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let rule = ir
            .routes
            .iter()
            .find(|r| r.route_family == RouteFamily::Http)
            .expect("http route");
        let tls = rule.backends[0]
            .backend
            .backend_tls
            .as_ref()
            .expect("backend TLS");
        assert_eq!(tls.hostname, "svc.example.com");
        assert_eq!(tls.sans, vec!["alt.example.com".to_string()]);
        assert!(!tls.trust_system_ca);
        assert!(!tls.invalid);
        assert!(!tls.ca_cert_pem.is_empty());
    }

    #[test]
    fn http_backend_tls_system_trust_sets_flag_and_empty_ca() {
        let mut snap = minimal_cluster();
        snap.backend_tls_effective.insert(
            "default/svc:80".into(),
            BackendTlsEffectiveSnap {
                tls: BackendTlsIr {
                    ca_cert_pem: vec![],
                    hostname: "svc.example.com".into(),
                    sans: vec![],
                    trust_system_ca: true,
                    client_cert_pem: vec![],
                    client_key_pem: vec![],
                    invalid: false,
                },
            },
        );
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let tls = ir.routes[0].backends[0]
            .backend
            .backend_tls
            .as_ref()
            .unwrap();
        assert!(tls.trust_system_ca);
        assert!(tls.ca_cert_pem.is_empty());
    }

    #[test]
    fn http_backend_tls_invalid_records_invalid_ca_ref_error() {
        let mut snap = minimal_cluster();
        snap.backend_tls_effective.insert(
            "default/svc:80".into(),
            BackendTlsEffectiveSnap {
                tls: BackendTlsIr {
                    ca_cert_pem: vec![],
                    hostname: "svc.example.com".into(),
                    sans: vec![],
                    trust_system_ca: false,
                    client_cert_pem: vec![],
                    client_key_pem: vec![],
                    invalid: true,
                },
            },
        );
        let (_ir, rep) = ir_from_cluster(&snap);
        assert!(rep.has_errors());
        assert!(rep
            .errors
            .iter()
            .any(|e| e.reason == "InvalidCACertificateRef"));
    }

    #[test]
    fn backend_tls_effective_key_is_per_service_port_not_whole_service() {
        let mut snap = minimal_cluster();
        snap.services[0].ports.push(ServicePortSnap {
            name: "https".into(),
            port: 443,
            target_port: Some(8443),
        });
        snap.endpoint_slices[0].endpoints[0]
            .ports
            .push(EndpointPortSnap {
                name: Some("https".into()),
                port: 8443,
            });
        snap.backend_tls_effective.insert(
            "default/svc:443".into(),
            BackendTlsEffectiveSnap {
                tls: BackendTlsIr {
                    ca_cert_pem: vec![b'x'],
                    hostname: "secure.backend".into(),
                    sans: vec![],
                    trust_system_ca: false,
                    client_cert_pem: vec![],
                    client_key_pem: vec![],
                    invalid: false,
                },
            },
        );
        snap.http_routes[0].rules = vec![HttpRouteRuleSnap {
            matches: snap.http_routes[0].rules[0].matches.clone(),
            filters: vec![],
            backend_refs: vec![
                HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(80),
                    port_name: None,
                    weight: Some(1),
                },
                HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(443),
                    port_name: None,
                    weight: Some(1),
                },
            ],
            ..Default::default()
        }];
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let backends = &ir.routes[0].backends;
        assert_eq!(backends.len(), 2);
        assert!(
            backends[0].backend.backend_tls.is_none(),
            "port 80 must stay plaintext when only :443 has a policy row"
        );
        let t = backends[1].backend.backend_tls.as_ref().unwrap();
        assert_eq!(t.hostname, "secure.backend");
    }

    #[test]
    fn listenerset_parent_produces_listener_attachment_on_route() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: None,
            tls: None,
        }];
        let listener_sets = vec![ListenerSetSnap {
            namespace: "default".into(),
            name: "extra".into(),
            uid: "uid-ls".into(),
            creation_timestamp_rfc3339: None,
            parent_refs: vec![],
            listeners: vec![GatewayListenerSnap {
                name: "alt".into(),
                port: 443,
                protocol: "HTTP".into(),
                hostname: None,
                allowed_routes: None,
                tls: None,
            }],
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &listener_sets);
        let snap = ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners,
            effective_listeners,
            listener_sets,
            rejected_listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![HttpRouteSnap {
                namespace: "default".into(),
                name: "hr-ls".into(),
                generation: 1,
                creation_timestamp_rfc3339: None,
                parent_refs: vec![ParentRefSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("ListenerSet".into()),
                    namespace: Some("default".into()),
                    name: "extra".into(),
                    section_name: None,
                    port: None,
                }],
                hostnames: vec!["example.com".into()],
                rules: vec![HttpRouteRuleSnap {
                    matches: vec![HttpRequestMatchSnap {
                        path: Some(HttpPathMatchSnap {
                            path_type: Some("PathPrefix".into()),
                            value: Some("/".into()),
                        }),
                        headers: vec![],
                        ..Default::default()
                    }],
                    filters: vec![],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: None,
                        name: "svc".into(),
                        port: Some(80),
                        port_name: None,
                        weight: None,
                    }],
                    ..Default::default()
                }],
            }],
            grpc_routes: vec![],
            tls_routes: vec![],
            tcp_routes: vec![],
            udp_routes: vec![],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "http".into(),
                    port: 80,
                    target_port: Some(8080),
                }],
                cluster_ip: None,
            }],
            endpoint_slices: vec![EndpointSliceSnap {
                namespace: "default".into(),
                name: "svc-1".into(),
                service_name: Some("svc".into()),
                endpoints: vec![EndpointSnap {
                    addresses: vec!["10.0.0.1".into()],
                    ports: vec![EndpointPortSnap {
                        name: Some("http".into()),
                        port: 8080,
                    }],
                }],
            }],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: BTreeMap::new(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: BTreeMap::new(),
            inference_pools: vec![],
        };
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(
            ir.routes.iter().any(|r| r
                .listener
                .as_ref()
                .is_some_and(|l| l.section_name == "alt" && l.parent_kind == "ListenerSet")),
            "{:?}",
            ir.routes
        );
    }

    #[test]
    fn https_listener_errors_without_pem() {
        let listeners = vec![GatewayListenerSnap {
            name: "https".into(),
            port: 443,
            protocol: "HTTPS".into(),
            hostname: None,
            allowed_routes: None,
            tls: Some(crate::snapshot::GatewayListenerTlsSnap {
                mode: Some("Terminate".into()),
                certificate_refs: vec![],
            }),
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        let mut snap = minimal_cluster();
        snap.listeners = listeners;
        snap.effective_listeners = effective_listeners;
        let (_, rep) = ir_from_cluster(&snap);
        assert!(rep.has_errors());
    }

    #[test]
    fn https_listener_ok_with_resolved_pem() {
        let listeners = vec![GatewayListenerSnap {
            name: "https".into(),
            port: 443,
            protocol: "HTTPS".into(),
            hostname: None,
            allowed_routes: None,
            tls: Some(crate::snapshot::GatewayListenerTlsSnap {
                mode: Some("Terminate".into()),
                certificate_refs: vec![],
            }),
        }];
        let mut effective_listeners = merge_effective_listeners(&listeners, &[]);
        effective_listeners[0].tls_cert_pem = b"CERT".to_vec();
        effective_listeners[0].tls_key_pem = b"KEY".to_vec();
        let mut snap = minimal_cluster();
        snap.listeners = listeners;
        snap.effective_listeners = effective_listeners;
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir
            .listeners
            .iter()
            .any(|l| l.bind_port == 443 && !l.tls_cert_pem.is_empty()));
    }

    #[test]
    fn route_attachment_counts_recorded() {
        let (ir, rep) = ir_from_cluster(&minimal_cluster());
        assert!(!rep.listener_route_attachment_counts.is_empty());
        let lk = crate::ir::ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "http".into(),
            port: 80,
        };
        assert!(rep.attachment_count_for_listener_key(&lk) >= 1);
        assert!(!ir.routes.is_empty());
    }

    #[test]
    fn route_attachment_counts_with_selector_namespace() {
        let mut c = minimal_cluster();
        c.listeners[0].allowed_routes = Some(AllowedRoutesSnap {
            namespaces: Some(AllowedRoutesNamespacesSnap {
                from: Some("Selector".into()),
                selector_match_labels: BTreeMap::from([
                    ("kubernetes.io/metadata.name".into(), "default".into()),
                ]),
            }),
            kinds: vec![RouteGroupKindSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
            }],
        });
        c.namespace_labels = BTreeMap::from([(
            "default".into(),
            BTreeMap::from([
                ("kubernetes.io/metadata.name".into(), "default".into()),
            ]),
        )]);
        c.effective_listeners = merge_effective_listeners(&c.listeners, &[]);
        let (_ir, rep) = ir_from_cluster(&c);
        let lk = crate::ir::ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "http".into(),
            port: 80,
        };
        assert!(
            rep.attachment_count_for_listener_key(&lk) >= 1,
            "Selector-based namespace should produce attachment count, got {:?}",
            rep.listener_route_attachment_counts,
        );
    }

    #[test]
    fn exact_path_match_round_trips() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].matches[0].path = Some(HttpPathMatchSnap {
            path_type: Some("Exact".into()),
            value: Some("/foo".into()),
        });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir.routes.iter().any(|r| r
            .matches
            .iter()
            .any(|m| m.path_exact.as_deref() == Some("/foo"))));
    }

    #[test]
    fn weighted_backends_in_rule() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0]
            .backend_refs
            .push(HttpBackendRefSnap {
                group: None,
                kind: Some("Service".into()),
                namespace: None,
                name: "svc".into(),
                port: Some(80),
                port_name: None,
                weight: Some(3),
            });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let r0 = ir.routes.iter().find(|r| !r.backends.is_empty()).unwrap();
        assert_eq!(r0.backends.len(), 2);
        assert_eq!(r0.backends[0].weight, 1);
        assert_eq!(r0.backends[1].weight, 3);
    }

    #[test]
    fn cross_namespace_backend_requires_grant() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].backend_refs[0].namespace = Some("other".into());
        c.services.push(ServiceSnap {
            namespace: "other".into(),
            name: "svc".into(),
            ports: vec![ServicePortSnap {
                name: "http".into(),
                port: 80,
                target_port: Some(9090),
            }],
            cluster_ip: None,
        });
        c.endpoint_slices.push(EndpointSliceSnap {
            namespace: "other".into(),
            name: "svc-2".into(),
            service_name: Some("svc".into()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.9.9.9".into()],
                ports: vec![EndpointPortSnap {
                    name: Some("http".into()),
                    port: 9090,
                }],
            }],
        });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(rep.has_errors());
        assert!(ir.routes.is_empty() || rep.errors.iter().any(|e| e.reason == "RefNotPermitted"));

        c.reference_grants
            .push(crate::snapshot::ReferenceGrantSnap {
                namespace: "other".into(),
                name: "g".into(),
                from: vec![crate::snapshot::ReferenceGrantFromSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: "HTTPRoute".into(),
                    namespace: "default".into(),
                }],
                to: vec![crate::snapshot::ReferenceGrantToSnap {
                    group: Some("".into()),
                    kind: "Service".into(),
                    name: None,
                }],
            });
        let (ir2, rep2) = ir_from_cluster(&c);
        assert!(!rep2.has_errors(), "{:?}", rep2.errors);
        assert!(!ir2.routes.is_empty());
    }

    #[test]
    fn partially_invalid_reference_grant_keeps_valid_rule_marks_invalid() {
        let mut c = minimal_cluster();
        c.gateway_namespace = "infra".into();
        c.gateway_name = "gw".into();
        c.listeners[0].allowed_routes = Some(AllowedRoutesSnap {
            namespaces: Some(AllowedRoutesNamespacesSnap {
                from: Some("Same".into()),
                selector_match_labels: BTreeMap::new(),
            }),
            kinds: vec![],
        });
        c.effective_listeners =
            merge_effective_listeners(&c.listeners, &[]);

        c.services.push(ServiceSnap {
            namespace: "app-backend".into(),
            name: "v1".into(),
            ports: vec![ServicePortSnap { name: "http".into(), port: 8080, target_port: Some(3000) }],
            cluster_ip: None,
        });
        c.services.push(ServiceSnap {
            namespace: "app-backend".into(),
            name: "v2".into(),
            ports: vec![ServicePortSnap { name: "http".into(), port: 8080, target_port: Some(3000) }],
            cluster_ip: None,
        });
        c.endpoint_slices.push(EndpointSliceSnap {
            namespace: "app-backend".into(),
            name: "v1-eps".into(),
            service_name: Some("v1".into()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.0.1.1".into()],
                ports: vec![EndpointPortSnap { name: Some("http".into()), port: 3000 }],
            }],
        });
        c.endpoint_slices.push(EndpointSliceSnap {
            namespace: "app-backend".into(),
            name: "v2-eps".into(),
            service_name: Some("v2".into()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.0.1.2".into()],
                ports: vec![EndpointPortSnap { name: Some("http".into()), port: 3000 }],
            }],
        });

        c.reference_grants.push(crate::snapshot::ReferenceGrantSnap {
            namespace: "app-backend".into(),
            name: "g".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "infra".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("".into()),
                kind: "Service".into(),
                name: Some("v1".into()),
            }],
        });

        c.http_routes = vec![HttpRouteSnap {
            namespace: "infra".into(),
            name: "partial-grant".into(),
            generation: 1,
            creation_timestamp_rfc3339: None,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: None,
                port: None,
            }],
            hostnames: vec![],
            rules: vec![
                HttpRouteRuleSnap {
                    matches: vec![HttpRequestMatchSnap {
                        path: Some(HttpPathMatchSnap {
                            path_type: Some("PathPrefix".into()),
                            value: Some("/v2".into()),
                        }),
                        headers: vec![],
                        ..Default::default()
                    }],
                    filters: vec![],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: Some("app-backend".into()),
                        name: "v2".into(),
                        port: Some(8080),
                        port_name: None,
                        weight: None,
                    }],
                    ..Default::default()
                },
                HttpRouteRuleSnap {
                    matches: vec![],
                    filters: vec![],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: Some("app-backend".into()),
                        name: "v1".into(),
                        port: Some(8080),
                        port_name: None,
                        weight: None,
                    }],
                    ..Default::default()
                },
            ],
        }];

        let (ir, rep) = ir_from_cluster(&c);

        assert!(
            rep.errors.iter().any(|e| e.reason == "RefNotPermitted"),
            "expected RefNotPermitted error for v2, got: {:?}",
            rep.errors
        );

        assert_eq!(ir.routes.len(), 2, "both rules must appear in the IR");

        let v2_rule = ir.routes.iter().find(|r| {
            r.matches.iter().any(|m| m.path_prefix.as_deref() == Some("/v2"))
        });
        assert!(v2_rule.is_some(), "must have a /v2 rule in IR");
        assert!(
            v2_rule.unwrap().backends.is_empty(),
            "/v2 rule must have no backends (denied by grant)"
        );

        let catchall = ir.routes.iter().find(|r| {
            r.matches.iter().any(|m| m.path_prefix.as_deref() == Some("/"))
        });
        assert!(catchall.is_some(), "must have a catch-all rule in IR");
        assert!(
            !catchall.unwrap().backends.is_empty(),
            "catch-all rule must have backends (v1 allowed by grant)"
        );
        assert_eq!(
            catchall.unwrap().backends[0].backend.service_name, "v1",
            "catch-all backend must be v1"
        );

    }

    #[test]
    fn cross_namespace_route_parent_attaches_without_reference_grant_when_listener_allows() {
        let mut c = minimal_cluster();
        c.gateway_namespace = "infra".into();
        c.gateway_name = "gw".into();
        c.listeners[0].allowed_routes = Some(AllowedRoutesSnap {
            namespaces: Some(AllowedRoutesNamespacesSnap {
                from: Some("All".into()),
                selector_match_labels: BTreeMap::new(),
            }),
            kinds: vec![],
        });
        c.effective_listeners = merge_effective_listeners(&c.listeners, &[]);
        c.http_routes[0].namespace = "team-a".into();
        c.http_routes[0].parent_refs[0].namespace = Some("infra".into());
        c.http_routes[0].parent_refs[0].name = "gw".into();
        c.services[0].namespace = "team-a".into();
        c.endpoint_slices[0].namespace = "team-a".into();

        let (ir, rep) = ir_from_cluster(&c);
        assert!(
            !ir.routes.is_empty(),
            "expected cross-namespace Gateway parent attachment without ReferenceGrant: {:?}",
            rep.errors
        );

        c.reference_grants.push(ReferenceGrantSnap {
            namespace: "infra".into(),
            name: "rg".into(),
            from: vec![ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "team-a".into(),
            }],
            to: vec![ReferenceGrantToSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "Gateway".into(),
                name: Some("gw".into()),
            }],
        });
        let (ir2, rep2) = ir_from_cluster(&c);
        assert!(
            !ir2.routes.is_empty(),
            "expected routes with optional explicit grant: {:?}",
            rep2.errors
        );
    }

    #[test]
    fn redirect_rule_emits_no_backend_cluster() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].backend_refs.clear();
        c.http_routes[0].rules[0]
            .filters
            .push(crate::snapshot::HttpRouteFilterSnap {
                filter_type: "RequestRedirect".into(),
                request_redirect: Some(crate::snapshot::HttpRequestRedirectFilterSnap {
                    scheme: Some("https".into()),
                    hostname: Some("other.example".into()),
                    path: None,
                    port: None,
                    status_code: Some(302),
                }),
                request_header_modifier: None,
                response_header_modifier: None,
                url_rewrite: None,
                cors: None,
            });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir
            .routes
            .iter()
            .any(|r| r.request_redirect.is_some() && r.backends.is_empty()));
    }

    #[test]
    fn invalid_backend_kind_keeps_route_in_ir() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].backend_refs = vec![HttpBackendRefSnap {
            group: None,
            kind: Some("CronJob".into()),
            namespace: None,
            name: "cj".into(),
            port: Some(80),
            port_name: None,
            weight: None,
        }];
        let (ir, rep) = ir_from_cluster(&c);
        assert!(rep.has_errors());
        assert!(
            ir.routes.iter().any(|r| r.backends.is_empty()),
            "route with invalid backend kind must stay in IR with empty backends"
        );
    }

    #[test]
    fn non_existent_backend_keeps_route_in_ir() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].backend_refs = vec![HttpBackendRefSnap {
            group: None,
            kind: Some("Service".into()),
            namespace: None,
            name: "does-not-exist".into(),
            port: Some(80),
            port_name: None,
            weight: None,
        }];
        let (ir, rep) = ir_from_cluster(&c);
        assert!(rep.has_errors());
        assert!(
            ir.routes.iter().any(|r| r.backends.is_empty()),
            "route with missing backend must stay in IR with empty backends"
        );
    }

    #[test]
    fn headless_service_sets_dial_port() {
        let mut c = minimal_cluster();
        c.services[0].cluster_ip = Some("None".into());
        let (ir, _rep) = ir_from_cluster(&c);
        let backend = ir
            .routes
            .iter()
            .flat_map(|r| &r.backends)
            .next()
            .expect("should have a backend");
        assert_eq!(
            backend.backend.dial_port,
            Some(8080),
            "headless service should set dial_port to targetPort"
        );
    }

    #[test]
    fn clusterip_service_has_no_dial_port() {
        let mut c = minimal_cluster();
        c.services[0].cluster_ip = Some("10.96.0.1".into());
        let (ir, _rep) = ir_from_cluster(&c);
        let backend = ir
            .routes
            .iter()
            .flat_map(|r| &r.backends)
            .next()
            .expect("should have a backend");
        assert_eq!(
            backend.backend.dial_port, None,
            "ClusterIP service should not set dial_port"
        );
    }

    #[test]
    fn grpc_route_compiles_parallel_to_http() {
        let mut c = minimal_cluster();
        c.grpc_routes.push(GrpcRouteSnap {
            namespace: "default".into(),
            name: "gr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2022-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: None,
                port: None,
            }],
            hostnames: vec!["other.example.com".into()],
            rules: vec![GrpcRouteRuleSnap {
                matches: vec![],
                filters: vec![],
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(80),
                    port_name: None,
                    weight: None,
                }],
            }],
        });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir
            .routes
            .iter()
            .any(|r| r.route_family == RouteFamily::Grpc));
        assert!(ir
            .routes
            .iter()
            .any(|r| r.route_family == RouteFamily::Http));
    }

    #[test]
    fn http_grpc_hostname_conflict_drops_newer_grpc() {
        let mut c = minimal_cluster();
        c.http_routes[0].creation_timestamp_rfc3339 = Some("2020-01-01T00:00:00Z".into());
        c.grpc_routes.push(GrpcRouteSnap {
            namespace: "default".into(),
            name: "gr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2021-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: None,
                port: None,
            }],
            hostnames: vec!["example.com".into()],
            rules: vec![GrpcRouteRuleSnap {
                matches: vec![],
                filters: vec![],
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(80),
                    port_name: None,
                    weight: None,
                }],
            }],
        });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir
            .routes
            .iter()
            .all(|r| r.route_family != RouteFamily::Grpc));
        assert!(rep
            .http_grpc_conflict_losers
            .iter()
            .any(|l| { l.route_kind == "GRPCRoute" && l.route_name == "gr1" }));
    }

    fn tls_passthrough_cluster_one_route(tr: TlsRouteSnap) -> ClusterSnapshot {
        let listeners = vec![GatewayListenerSnap {
            name: "tls".into(),
            port: 443,
            protocol: "TLS".into(),
            hostname: None,
            allowed_routes: None,
            tls: Some(GatewayListenerTlsSnap {
                mode: Some("Passthrough".into()),
                certificate_refs: vec![],
            }),
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners,
            effective_listeners,
            listener_sets: vec![],
            rejected_listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![],
            grpc_routes: vec![],
            tls_routes: vec![tr],
            tcp_routes: vec![],
            udp_routes: vec![],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![
                    ServicePortSnap {
                        name: "http".into(),
                        port: 80,
                        target_port: Some(8080),
                    },
                    ServicePortSnap {
                        name: "https".into(),
                        port: 443,
                        target_port: Some(8443),
                    },
                ],
                cluster_ip: None,
            }],
            endpoint_slices: vec![EndpointSliceSnap {
                namespace: "default".into(),
                name: "svc-1".into(),
                service_name: Some("svc".into()),
                endpoints: vec![EndpointSnap {
                    addresses: vec!["10.0.0.1".into()],
                    ports: vec![
                        EndpointPortSnap {
                            name: Some("http".into()),
                            port: 8080,
                        },
                        EndpointPortSnap {
                            name: Some("https".into()),
                            port: 8443,
                        },
                    ],
                }],
            }],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: BTreeMap::new(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: BTreeMap::new(),
            inference_pools: vec![],
        }
    }

    #[test]
    fn tls_passthrough_listener_tls_route_compiles() {
        let tr = TlsRouteSnap {
            namespace: "default".into(),
            name: "tr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: None,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tls".into()),
                port: None,
            }],
            hostnames: vec!["svc.example.com".into()],
            rules: vec![TlsRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(443),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let snap = tls_passthrough_cluster_one_route(tr);
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let tls_listener = ir
            .listeners
            .iter()
            .find(|l| l.section_name == "tls")
            .expect("tls listener");
        assert_eq!(tls_listener.bind_port, 443);
        assert_eq!(tls_listener.tls, crate::ir::TlsMode::Passthrough);
        let r = ir
            .routes
            .iter()
            .find(|x| x.route_family == RouteFamily::Tls)
            .expect("tls route rule");
        assert_eq!(r.hosts, vec!["svc.example.com".to_string()]);
        assert!(r.matches.is_empty());
    }

    fn tcp_listener_cluster(routes: Vec<TcpRouteSnap>) -> ClusterSnapshot {
        let listeners = vec![GatewayListenerSnap {
            name: "tcp".into(),
            port: 9000,
            protocol: "TCP".into(),
            hostname: None,
            allowed_routes: None,
            tls: None,
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners,
            effective_listeners,
            listener_sets: vec![],
            rejected_listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![],
            grpc_routes: vec![],
            tls_routes: vec![],
            tcp_routes: routes,
            udp_routes: vec![],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "tcp".into(),
                    port: 9000,
                    target_port: Some(19000),
                }],
                cluster_ip: None,
            }],
            endpoint_slices: vec![EndpointSliceSnap {
                namespace: "default".into(),
                name: "svc-1".into(),
                service_name: Some("svc".into()),
                endpoints: vec![EndpointSnap {
                    addresses: vec!["10.0.0.1".into()],
                    ports: vec![EndpointPortSnap {
                        name: Some("tcp".into()),
                        port: 19000,
                    }],
                }],
            }],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: BTreeMap::new(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: BTreeMap::new(),
            inference_pools: vec![],
        }
    }

    #[test]
    fn tcp_listener_tcp_route_compiles() {
        let tr = TcpRouteSnap {
            namespace: "default".into(),
            name: "tcr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: None,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tcp".into()),
                port: None,
            }],
            rules: vec![TcpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9000),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let snap = tcp_listener_cluster(vec![tr]);
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let ln = ir
            .listeners
            .iter()
            .find(|l| l.section_name == "tcp")
            .expect("tcp listener");
        assert_eq!(ln.bind_port, 9000);
        assert_eq!(ln.gateway_protocol, "TCP");
        assert!(ln
            .allowed_route_group_kinds
            .iter()
            .any(|x| x.contains("TCPRoute")));
        let r = ir
            .routes
            .iter()
            .find(|x| x.route_family == RouteFamily::Tcp)
            .expect("tcp route rule");
        assert!(r.hosts.is_empty());
        assert!(r.matches.is_empty());
    }

    #[test]
    fn tcp_listener_two_tcproutes_conflict_drops_newer() {
        let older = TcpRouteSnap {
            namespace: "default".into(),
            name: "tcp-old".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2020-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tcp".into()),
                port: None,
            }],
            rules: vec![TcpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9000),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let newer = TcpRouteSnap {
            namespace: "default".into(),
            name: "tcp-new".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2021-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tcp".into()),
                port: None,
            }],
            rules: vec![TcpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9000),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let snap = tcp_listener_cluster(vec![older, newer]);
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(
            ir.routes
                .iter()
                .filter(|r| r.route_family == RouteFamily::Tcp)
                .count()
                == 1
        );
        assert!(rep
            .tcp_route_conflict_losers
            .iter()
            .any(|l| { l.route_kind == "TCPRoute" && l.route_name == "tcp-new" }));
    }

    fn udp_listener_cluster(routes: Vec<UdpRouteSnap>) -> ClusterSnapshot {
        let listeners = vec![GatewayListenerSnap {
            name: "udp".into(),
            port: 9500,
            protocol: "UDP".into(),
            hostname: None,
            allowed_routes: None,
            tls: None,
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners,
            effective_listeners,
            listener_sets: vec![],
            rejected_listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![],
            grpc_routes: vec![],
            tls_routes: vec![],
            tcp_routes: vec![],
            udp_routes: routes,
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "udp".into(),
                    port: 9500,
                    target_port: Some(19500),
                }],
                cluster_ip: None,
            }],
            endpoint_slices: vec![EndpointSliceSnap {
                namespace: "default".into(),
                name: "svc-1".into(),
                service_name: Some("svc".into()),
                endpoints: vec![EndpointSnap {
                    addresses: vec!["10.0.0.1".into()],
                    ports: vec![EndpointPortSnap {
                        name: Some("udp".into()),
                        port: 19500,
                    }],
                }],
            }],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: BTreeMap::new(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: BTreeMap::new(),
            inference_pools: vec![],
        }
    }

    #[test]
    fn udp_listener_udp_route_compiles() {
        let ur = UdpRouteSnap {
            namespace: "default".into(),
            name: "udpr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: None,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("udp".into()),
                port: None,
            }],
            rules: vec![UdpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9500),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let snap = udp_listener_cluster(vec![ur]);
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        let ln = ir
            .listeners
            .iter()
            .find(|l| l.section_name == "udp")
            .expect("udp listener");
        assert_eq!(ln.bind_port, 9500);
        assert_eq!(ln.gateway_protocol, "UDP");
        assert!(ln
            .allowed_route_group_kinds
            .iter()
            .any(|x| x.contains("UDPRoute")));
        let r = ir
            .routes
            .iter()
            .find(|x| x.route_family == RouteFamily::Udp)
            .expect("udp route rule");
        assert!(r.hosts.is_empty());
        assert!(r.matches.is_empty());
    }

    #[test]
    fn udp_listener_two_udproutes_conflict_drops_newer() {
        let older = UdpRouteSnap {
            namespace: "default".into(),
            name: "udp-old".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2020-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("udp".into()),
                port: None,
            }],
            rules: vec![UdpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9500),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let newer = UdpRouteSnap {
            namespace: "default".into(),
            name: "udp-new".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2021-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("udp".into()),
                port: None,
            }],
            rules: vec![UdpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(9500),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let snap = udp_listener_cluster(vec![older, newer]);
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(
            ir.routes
                .iter()
                .filter(|r| r.route_family == RouteFamily::Udp)
                .count()
                == 1
        );
        assert!(rep
            .udp_route_conflict_losers
            .iter()
            .any(|l| { l.route_kind == "UDPRoute" && l.route_name == "udp-new" }));
    }

    #[test]
    fn tls_route_cross_namespace_backend_requires_grant() {
        let tr = TlsRouteSnap {
            namespace: "default".into(),
            name: "tr1".into(),
            generation: 1,
            creation_timestamp_rfc3339: None,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tls".into()),
                port: None,
            }],
            hostnames: vec!["svc.example.com".into()],
            rules: vec![TlsRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: Some("other".into()),
                    name: "svc".into(),
                    port: Some(443),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let mut snap = tls_passthrough_cluster_one_route(tr);
        snap.services.push(ServiceSnap {
            namespace: "other".into(),
            name: "svc".into(),
            ports: vec![ServicePortSnap {
                name: "https".into(),
                port: 443,
                target_port: Some(8443),
            }],
            cluster_ip: None,
        });
        snap.endpoint_slices.push(EndpointSliceSnap {
            namespace: "other".into(),
            name: "svc-2".into(),
            service_name: Some("svc".into()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.9.9.9".into()],
                ports: vec![EndpointPortSnap {
                    name: Some("https".into()),
                    port: 8443,
                }],
            }],
        });
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(rep.has_errors());
        assert!(
            ir.routes.iter().all(|r| r.route_family != RouteFamily::Tls)
                || rep.errors.iter().any(|e| e.reason == "RefNotPermitted")
        );

        snap.reference_grants
            .push(crate::snapshot::ReferenceGrantSnap {
                namespace: "other".into(),
                name: "g".into(),
                from: vec![crate::snapshot::ReferenceGrantFromSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: "TLSRoute".into(),
                    namespace: "default".into(),
                }],
                to: vec![crate::snapshot::ReferenceGrantToSnap {
                    group: Some("".into()),
                    kind: "Service".into(),
                    name: None,
                }],
            });
        let (ir2, rep2) = ir_from_cluster(&snap);
        assert!(!rep2.has_errors(), "{:?}", rep2.errors);
        assert!(ir2
            .routes
            .iter()
            .any(|r| r.route_family == RouteFamily::Tls));
    }

    #[test]
    fn tls_route_hostname_conflict_drops_newer() {
        let tr_old = TlsRouteSnap {
            namespace: "default".into(),
            name: "tr-old".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2020-01-01T00:00:00Z".into()),
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: None,
                name: "gw".into(),
                section_name: Some("tls".into()),
                port: None,
            }],
            hostnames: vec!["svc.example.com".into()],
            rules: vec![TlsRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    group: None,
                    kind: Some("Service".into()),
                    namespace: None,
                    name: "svc".into(),
                    port: Some(443),
                    port_name: None,
                    weight: None,
                }],
            }],
        };
        let tr_new = TlsRouteSnap {
            namespace: "default".into(),
            name: "tr-new".into(),
            generation: 1,
            creation_timestamp_rfc3339: Some("2021-01-01T00:00:00Z".into()),
            parent_refs: tr_old.parent_refs.clone(),
            hostnames: vec!["svc.example.com".into()],
            rules: tr_old.rules.clone(),
        };
        let mut snap = tls_passthrough_cluster_one_route(tr_old.clone());
        snap.tls_routes.push(tr_new.clone());
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert_eq!(
            ir.routes
                .iter()
                .filter(|r| r.route_family == RouteFamily::Tls)
                .count(),
            1
        );
        assert!(ir
            .routes
            .iter()
            .any(|r| { r.route_family == RouteFamily::Tls && r.name.contains("tr-old") }));
        assert!(rep
            .tls_route_conflict_losers
            .iter()
            .any(|l| { l.route_kind == "TLSRoute" && l.route_name == "tr-new" }));
    }
}
