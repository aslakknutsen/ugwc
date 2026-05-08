//! Build [`crate::ir::IrSnapshot`] + [`crate::compile::CompileReport`] from a [`crate::snapshot::ClusterSnapshot`].

use std::collections::{BTreeMap, HashSet};

use crate::compile::{CompileReport, ObjectRefErr};
use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, BackendIr, IrSnapshot, ListenerAttachmentIr, ListenerIr, RouteRuleIr,
    SnapshotMeta, TlsMode, IR_VERSION,
};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, EffectiveListenerSnap, HttpBackendRefSnap, HttpRouteSnap, ListenerParentSnap,
    ParentRefSnap, ServiceSnap,
};
use uuid::Uuid;

fn gateway_api_group(g: &Option<String>) -> bool {
    g.as_deref().unwrap_or("gateway.networking.k8s.io") == "gateway.networking.k8s.io"
}

fn parent_namespace(p: &ParentRefSnap, route_ns: &str) -> String {
    p.namespace.clone().unwrap_or_else(|| route_ns.to_string())
}

fn normalize_protocol(p: &str) -> String {
    let t = p.trim();
    if t.is_empty() {
        "HTTP".into()
    } else {
        t.to_ascii_uppercase()
    }
}

fn effective_listener_matches_attachment(
    e: &EffectiveListenerSnap,
    k: &ListenerAttachmentIr,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    if e.conflicted || e.section_name != k.section_name || e.port != k.port {
        return false;
    }
    match k.parent_kind.as_str() {
        "Gateway" => {
            matches!(e.parent, ListenerParentSnap::InlineGateway)
                && k.parent_namespace == gw_ns
                && k.parent_name == gw_name
        }
        "ListenerSet" => {
            if let ListenerParentSnap::ListenerSet { namespace, name } = &e.parent {
                k.parent_namespace == *namespace && k.parent_name == *name
            } else {
                false
            }
        }
        _ => false,
    }
}

fn find_effective_for_key<'a>(
    snapshot: &'a ClusterSnapshot,
    k: &ListenerAttachmentIr,
) -> Option<&'a EffectiveListenerSnap> {
    snapshot.effective_listeners.iter().find(|e| {
        effective_listener_matches_attachment(
            e,
            k,
            &snapshot.gateway_namespace,
            &snapshot.gateway_name,
        )
    })
}

/// Collect attachment keys from one route `parentRef` relevant to this Gateway reconcile scope.
fn attachment_keys_for_parent_ref(
    route: &HttpRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<ListenerAttachmentIr> {
    let route_ns = &route.namespace;
    let mut keys = Vec::new();

    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");

    if gateway_api_group(&Some(g.to_string())) && k == "Gateway" {
        let pns = parent_namespace(p, route_ns);
        if pns != snapshot.gateway_namespace || p.name != snapshot.gateway_name {
            return keys;
        }
        for e in &snapshot.effective_listeners {
            if !matches!(e.parent, ListenerParentSnap::InlineGateway) {
                continue;
            }
            if e.conflicted {
                continue;
            }
            if let Some(ref sn) = p.section_name {
                if sn != &e.section_name {
                    continue;
                }
            }
            if let Some(port) = p.port {
                if port != e.port {
                    continue;
                }
            }
            keys.push(ListenerAttachmentIr {
                parent_kind: "Gateway".into(),
                parent_namespace: snapshot.gateway_namespace.clone(),
                parent_name: snapshot.gateway_name.clone(),
                section_name: e.section_name.clone(),
                port: e.port,
            });
        }
        return keys;
    }

    if gateway_api_group(&Some(g.to_string())) && k == "ListenerSet" {
        let ls_ns = parent_namespace(p, route_ns);
        if !attached_ls.contains(&(ls_ns.clone(), p.name.clone())) {
            return keys;
        }
        for e in &snapshot.effective_listeners {
            let ListenerParentSnap::ListenerSet {
                namespace: ns,
                name: ln,
            } = &e.parent
            else {
                continue;
            };
            if ns != &ls_ns || ln != &p.name {
                continue;
            }
            if e.conflicted {
                continue;
            }
            if let Some(ref sn) = p.section_name {
                if sn != &e.section_name {
                    continue;
                }
            }
            if let Some(port) = p.port {
                if port != e.port {
                    continue;
                }
            }
            keys.push(ListenerAttachmentIr {
                parent_kind: "ListenerSet".into(),
                parent_namespace: ls_ns.clone(),
                parent_name: p.name.clone(),
                section_name: e.section_name.clone(),
                port: e.port,
            });
        }
    }

    keys
}

fn route_attachment_keys(
    route: &HttpRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<ListenerAttachmentIr> {
    let mut keys: Vec<ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

fn hosts_match_route_listener(route_host: &str, listener_host: &str) -> bool {
    if route_host == listener_host {
        return true;
    }
    let Some(suffix) = listener_host.strip_prefix("*.") else {
        return false;
    };
    let rh = route_host.to_ascii_lowercase();
    let suf = suffix.to_ascii_lowercase();
    rh == suf || rh.ends_with(&format!(".{suf}"))
}

/// Hostnames after intersecting route `spec.hostnames` with listener hostname.
fn intersect_route_listener_hosts(
    route_hostnames: &[String],
    listener_hostname: &Option<String>,
) -> Option<Vec<String>> {
    let lh = listener_hostname
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if route_hostnames.is_empty() {
        return match lh {
            None => Some(Vec::new()),
            Some(h) => Some(vec![h.to_string()]),
        };
    }

    let Some(lh) = lh else {
        return Some(route_hostnames.to_vec());
    };

    let mut out = Vec::new();
    for rh in route_hostnames {
        let r = rh.trim();
        if r.is_empty() {
            continue;
        }
        if hosts_match_route_listener(r, lh) {
            out.push(r.to_string());
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
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

/// Build IR + report from cluster snapshot (pure).
#[must_use]
pub fn ir_from_cluster(snapshot: &ClusterSnapshot) -> (IrSnapshot, CompileReport) {
    let correlation_id = Uuid::nil();
    let scope = crate::ir::IrScopeId::new(&snapshot.gateway_namespace, &snapshot.gateway_name);
    let mut report = CompileReport::default();

    record_listener_conflicts(snapshot, &mut report);

    let attached_ls: HashSet<(String, String)> = snapshot
        .listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let mut listeners = Vec::new();
    for e in &snapshot.effective_listeners {
        let tls = if normalize_protocol(&e.protocol) == "HTTPS" {
            TlsMode::Terminate
        } else {
            TlsMode::Plaintext
        };
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
            tls,
            parent_kind: pk,
            parent_namespace: pns,
            parent_name: pn,
            conflicted: e.conflicted,
        });
    }

    let mut routes = Vec::new();
    for route in &snapshot.http_routes {
        let attach_keys = route_attachment_keys(route, snapshot, &attached_ls);
        if attach_keys.is_empty() {
            continue;
        }

        for (rule_idx, rule) in route.rules.iter().enumerate() {
            let matches: Vec<_> = if rule.matches.is_empty() {
                vec![crate::snapshot::HttpRequestMatchSnap::default()]
            } else {
                rule.matches.clone()
            };
            for m in &matches {
                let path_prefix = m.path.as_ref().and_then(|p| {
                    if p.path_type.as_deref() == Some("Exact") {
                        return None;
                    }
                    Some(p.value.clone().unwrap_or_else(|| "/".into()))
                });
                let host_from_match = m
                    .hostname
                    .clone()
                    .or_else(|| route.hostnames.first().cloned());
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
                            report.errors.push(ObjectRefErr {
                                group: "gateway.networking.k8s.io".into(),
                                version: "v1".into(),
                                kind: "HTTPRoute".into(),
                                namespace: route.namespace.clone(),
                                name: route.name.clone(),
                                reason,
                            });
                            continue;
                        }
                    };
                    let backend = BackendIr {
                        service_namespace: svc_ns.to_string(),
                        service_name: svc_name.clone(),
                        port_name: br.port_name.clone(),
                        port_number: Some(port),
                    };

                    for lk in &attach_keys {
                        let Some(eff) = find_effective_for_key(snapshot, lk) else {
                            continue;
                        };
                        if eff.conflicted {
                            continue;
                        }
                        let rh_src: &[String] = &route.hostnames;
                        let intersected = intersect_route_listener_hosts(rh_src, &eff.hostname);
                        let Some(intersected) = intersected else {
                            continue;
                        };
                        let hosts_out: Vec<Option<String>> = if intersected.is_empty() {
                            vec![host_from_match.clone()]
                        } else {
                            intersected.into_iter().map(Some).collect()
                        };
                        for (hi, hopt) in hosts_out.into_iter().enumerate() {
                            let host = hopt.or_else(|| host_from_match.clone());
                            let route_name = format!(
                                "{}/{}/{}:{}:{}",
                                route.namespace, route.name, rule_idx, lk.section_name, hi
                            );
                            routes.push(RouteRuleIr {
                                name: route_name,
                                host,
                                path_prefix: path_prefix.clone(),
                                backend: backend.clone(),
                                listener: Some(lk.clone()),
                            });
                        }
                    }
                }
            }
        }
    }

    let mut backends: BTreeMap<String, Vec<BackendEndpointIr>> = BTreeMap::new();
    for r in &routes {
        let key = backend_key(
            &r.backend.service_namespace,
            &r.backend.service_name,
            r.backend.port_number.unwrap_or(80),
        );
        let mut eps = collect_endpoints(
            snapshot,
            &r.backend.service_namespace,
            &r.backend.service_name,
        );
        if let Some(tp) = find_service(
            snapshot,
            &r.backend.service_namespace,
            &r.backend.service_name,
        )
        .and_then(|svc| {
            svc.ports.iter().find(|sp| {
                Some(sp.port) == r.backend.port_number
                    || r.backend.port_name.as_deref() == Some(sp.name.as_str())
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
                namespace: r.backend.service_namespace.clone(),
                name: r.backend.service_name.clone(),
                reason: "NoReadyEndpoints".into(),
            });
        }
        backends.insert(key, eps);
    }

    let has_errors = !report.errors.is_empty();
    let snapshot = IrSnapshot {
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
    (snapshot, report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listener_merge::merge_effective_listeners;
    use crate::snapshot::{
        EndpointPortSnap, EndpointSliceSnap, EndpointSnap, GatewayListenerSnap, HttpPathMatchSnap,
        HttpRequestMatchSnap, HttpRouteRuleSnap, ListenerSetSnap, ParentRefSnap, ServicePortSnap,
        ServiceSnap,
    };

    fn minimal_cluster() -> ClusterSnapshot {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &[]);
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            listeners,
            effective_listeners,
            listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![HttpRouteSnap {
                namespace: "default".into(),
                name: "hr1".into(),
                generation: 1,
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
                        hostname: None,
                    }],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: None,
                        name: "svc".into(),
                        port: Some(80),
                        port_name: None,
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
        }
    }

    #[test]
    fn listenerset_parent_produces_listener_attachment_on_route() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
        }];
        let listener_sets = vec![ListenerSetSnap {
            namespace: "default".into(),
            name: "extra".into(),
            uid: "uid-ls".into(),
            creation_timestamp_rfc3339: None,
            parent_refs: vec![],
            listeners: vec![GatewayListenerSnap {
                name: "https".into(),
                port: 443,
                protocol: "HTTPS".into(),
                hostname: None,
            }],
        }];
        let effective_listeners = merge_effective_listeners(&listeners, &listener_sets);
        let snap = ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            listeners,
            effective_listeners,
            listener_sets,
            allowed_listeners: None,
            http_routes: vec![HttpRouteSnap {
                namespace: "default".into(),
                name: "hr-ls".into(),
                generation: 1,
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
                        hostname: None,
                    }],
                    backend_refs: vec![HttpBackendRefSnap {
                        group: None,
                        kind: Some("Service".into()),
                        namespace: None,
                        name: "svc".into(),
                        port: Some(80),
                        port_name: None,
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
        };
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert!(
            ir.routes.iter().any(|r| r
                .listener
                .as_ref()
                .is_some_and(|l| l.section_name == "https" && l.parent_kind == "ListenerSet")),
            "{:?}",
            ir.routes
        );
    }
}
