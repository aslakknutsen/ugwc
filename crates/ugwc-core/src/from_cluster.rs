//! Build [`crate::ir::IrSnapshot`] + [`crate::compile::CompileReport`] from a [`crate::snapshot::ClusterSnapshot`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use uuid::Uuid;

use crate::compile::{CompileReport, ObjectRefErr};
use crate::http_route_attachment::{
    find_effective_for_key, intersect_route_listener_hosts, reference_grant_permits_service_ref,
    route_attachment_keys,
};
use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, BackendIr, IrSnapshot, ListenerIr, NamedHeaderIr,
    RequestHeaderModifierIr, RequestRedirectIr, RouteMatchIr, RouteRuleIr, SnapshotMeta, TlsMode,
    WeightedBackendIr, IR_VERSION,
};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, EffectiveListenerSnap, HttpBackendRefSnap, HttpRequestMatchSnap,
    HttpRouteRuleSnap, HttpRouteSnap, ListenerParentSnap, ServiceSnap,
};

#[derive(Clone, Eq, PartialEq)]
struct RouteSortMeta {
    ts: String,
    route_ns: String,
    route_name: String,
    rule_idx: usize,
    seq: usize,
}

impl Ord for RouteSortMeta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.route_ns.cmp(&other.route_ns))
            .then_with(|| self.route_name.cmp(&other.route_name))
            .then_with(|| self.rule_idx.cmp(&other.rule_idx))
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for RouteSortMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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
            report.errors.push(gateway_obj_err(
                snapshot,
                "UnsupportedProtocol: TLS listener requires TLSRoute",
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
                    format!("UnsupportedValue: header match type {mt} (rule {rule_idx} match {mi})"),
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
        out.push(RouteMatchIr {
            path_prefix,
            path_exact,
            headers_exact,
        });
    }
    Some(out)
}

fn extract_rule_filters(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    rule_idx: usize,
    report: &mut CompileReport,
) -> (Option<RequestRedirectIr>, Option<RequestHeaderModifierIr>) {
    let mut redirect: Option<RequestRedirectIr> = None;
    let mut hdr_mod: Option<RequestHeaderModifierIr> = None;

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
            other => {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {other} (rule {rule_idx})"),
                ));
            }
        }
    }
    (redirect, hdr_mod)
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

fn find_service<'a>(sn: &'a ClusterSnapshot, ns: &str, name: &str) -> Option<&'a ServiceSnap> {
    sn.services
        .iter()
        .find(|s| s.namespace == ns && s.name == name)
}

fn backend_key(ns: &str, name: &str, port: u16) -> String {
    format!("{ns}/{name}:{port}")
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

fn resolve_weighted_backends(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    _rule_idx: usize,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    let mut backends = Vec::new();
    for br in &rule.backend_refs {
        if br.kind.as_deref().unwrap_or("Service") != "Service" {
            report.errors.push(ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: route.namespace.clone(),
                name: route.name.clone(),
                reason: "UnsupportedBackendKind".into(),
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
        backends.push(WeightedBackendIr {
            backend: BackendIr {
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.clone(),
                port_name: br.port_name.clone(),
                port_number: Some(port),
            },
            weight: br.weight.unwrap_or(1),
        });
    }
    backends
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

    let mut listeners = Vec::new();
    for e in &snapshot.effective_listeners {
        let (tls_mode, tls_cert_pem, tls_key_pem) = listener_tls_ir_fields(e, snapshot, &mut report);
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
            parent_kind: pk,
            parent_namespace: pns,
            parent_name: pn,
            conflicted: e.conflicted,
            hostname: e.hostname.clone(),
            tls_cert_pem,
            tls_key_pem,
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

            let (redirect_ir, hdr_mod_ir) =
                extract_rule_filters(route, rule, rule_idx, &mut report);

            let backends_ir = resolve_weighted_backends(route, rule, rule_idx, snapshot, &mut report);

            if redirect_ir.is_some() && !rule.backend_refs.is_empty() {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "Invalid: RequestRedirect cannot be combined with backendRefs (rule {rule_idx})"
                    ),
                ));
                continue;
            }

            if redirect_ir.is_none() && backends_ir.is_empty() && rule.filters.is_empty() {
                // No-op rule — skip
                continue;
            }

            if redirect_ir.is_none() && backends_ir.is_empty() {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("Invalid: rule {rule_idx} has no valid backends"),
                ));
                continue;
            }

            let meta_ts = route
                .creation_timestamp_rfc3339
                .clone()
                .unwrap_or_default();

            for lk in &attach_keys {
                let Some(eff) = find_effective_for_key(snapshot, lk) else {
                    continue;
                };
                if eff.conflicted {
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

                staged.push((
                    RouteSortMeta {
                        ts: meta_ts.clone(),
                        route_ns: route.namespace.clone(),
                        route_name: route.name.clone(),
                        rule_idx,
                        seq,
                    },
                    RouteRuleIr {
                        name: route_name,
                        hosts,
                        matches: matches_ir.clone(),
                        backends: backends_ir.clone(),
                        request_redirect: redirect_ir.clone(),
                        request_header_modifier: hdr_mod_ir.clone(),
                        listener: Some(lk.clone()),
                    },
                ));
                report.record_listener_route_attachment(lk);
                seq += 1;
            }
        }
    }

    staged.sort_by(|a, b| a.0.cmp(&b.0));
    let routes: Vec<RouteRuleIr> = staged.into_iter().map(|(_, r)| r).collect();

    let mut backends: BTreeMap<String, Vec<BackendEndpointIr>> = BTreeMap::new();
    for r in &routes {
        for wb in &r.backends {
            let key = backend_key(
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
            if eps.is_empty() {
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
    use crate::listener_merge::merge_effective_listeners;
    use crate::snapshot::{
        EndpointPortSnap, EndpointSliceSnap, EndpointSnap, GatewayListenerSnap, HttpBackendRefSnap,
        HttpPathMatchSnap, HttpRouteRuleSnap, ListenerSetSnap, ParentRefSnap, ServicePortSnap,
        ServiceSnap,
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
                }],
            }],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "http".into(),
                    port: 80,
                    target_port: Some(8080),
                }],
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
            namespace_labels: BTreeMap::new(),
        }
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
                }],
            }],
            services: vec![ServiceSnap {
                namespace: "default".into(),
                name: "svc".into(),
                ports: vec![ServicePortSnap {
                    name: "http".into(),
                    port: 80,
                    target_port: Some(8080),
                }],
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
            namespace_labels: BTreeMap::new(),
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
        c.http_routes[0].rules[0].backend_refs.push(HttpBackendRefSnap {
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

        c.reference_grants.push(crate::snapshot::ReferenceGrantSnap {
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
    fn redirect_rule_emits_no_backend_cluster() {
        let mut c = minimal_cluster();
        c.http_routes[0].rules[0].backend_refs.clear();
        c.http_routes[0].rules[0].filters.push(crate::snapshot::HttpRouteFilterSnap {
            filter_type: "RequestRedirect".into(),
            request_redirect: Some(crate::snapshot::HttpRequestRedirectFilterSnap {
                scheme: Some("https".into()),
                hostname: Some("other.example".into()),
                path: None,
                port: None,
                status_code: Some(302),
            }),
            request_header_modifier: None,
        });
        let (ir, rep) = ir_from_cluster(&c);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(ir.routes.iter().any(|r| r.request_redirect.is_some() && r.backends.is_empty()));
    }
}
