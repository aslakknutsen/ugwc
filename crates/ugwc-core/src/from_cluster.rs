//! Build [`crate::ir::IrSnapshot`] + [`crate::compile::CompileReport`] from a [`ClusterSnapshot`].

use std::collections::BTreeMap;

use crate::compile::{CompileReport, ObjectRefErr};
use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, BackendIr, IrSnapshot, ListenerIr, RouteRuleIr, SnapshotMeta, TlsMode,
    IR_VERSION,
};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, HttpBackendRefSnap, HttpRouteSnap, ParentRefSnap, ServiceSnap,
};
use uuid::Uuid;

fn gateway_api_group(g: &Option<String>) -> bool {
    g.as_deref().unwrap_or("gateway.networking.k8s.io") == "gateway.networking.k8s.io"
}

fn is_gateway_parent(p: &ParentRefSnap) -> bool {
    gateway_api_group(&p.group) && p.kind.as_deref().unwrap_or("Gateway") == "Gateway"
}

fn parent_namespace(p: &ParentRefSnap, route_ns: &str) -> String {
    p.namespace.clone().unwrap_or_else(|| route_ns.to_string())
}

fn route_attaches_to_gateway(route: &HttpRouteSnap, gw_ns: &str, gw_name: &str) -> bool {
    route.parent_refs.iter().any(|p| {
        if !is_gateway_parent(p) {
            return false;
        }
        parent_namespace(p, &route.namespace) == gw_ns && p.name == gw_name
    })
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

/// Build IR + report from cluster snapshot (pure).
#[must_use]
pub fn ir_from_cluster(snapshot: &ClusterSnapshot) -> (IrSnapshot, CompileReport) {
    let correlation_id = Uuid::nil();
    let scope = crate::ir::IrScopeId::new(&snapshot.gateway_namespace, &snapshot.gateway_name);
    let mut report = CompileReport::default();
    let mut listeners = Vec::new();
    for l in &snapshot.listeners {
        let tls = if l.protocol.eq_ignore_ascii_case("HTTPS") {
            TlsMode::Terminate
        } else {
            TlsMode::Plaintext
        };
        listeners.push(ListenerIr {
            name: l.name.clone(),
            bind_port: l.port,
            tls,
        });
    }

    let mut routes = Vec::new();
    for route in &snapshot.http_routes {
        if !route_attaches_to_gateway(route, &snapshot.gateway_namespace, &snapshot.gateway_name) {
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
                let host = m
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
                    let route_name = format!("{}/{}/{}", route.namespace, route.name, rule_idx);
                    routes.push(RouteRuleIr {
                        name: route_name,
                        host: host.clone(),
                        path_prefix: path_prefix.clone(),
                        backend,
                    });
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
    use crate::snapshot::{
        EndpointPortSnap, EndpointSliceSnap, EndpointSnap, GatewayListenerSnap, HttpPathMatchSnap,
        HttpRequestMatchSnap, HttpRouteRuleSnap, ServicePortSnap, ServiceSnap,
    };

    fn minimal_cluster() -> ClusterSnapshot {
        ClusterSnapshot {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            listeners: vec![GatewayListenerSnap {
                name: "http".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: None,
            }],
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
    fn cluster_happy_path_populates_routes_and_backends() {
        let snap = minimal_cluster();
        let (ir, rep) = ir_from_cluster(&snap);
        assert!(!rep.has_errors(), "{:?}", rep.errors);
        assert_eq!(ir.listeners.len(), 1);
        assert_eq!(ir.routes.len(), 1);
        assert!(ir.backends.contains_key("default/svc:80"));
    }
}
