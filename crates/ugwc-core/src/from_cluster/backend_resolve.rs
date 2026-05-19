use super::{
    grpcroute_obj_err, httproute_obj_err, tcproute_obj_err, tlsroute_obj_err, udproute_obj_err,
};
use crate::compile::{CompileReport, ObjectRefErr};
use crate::http_route_attachment::{
    reference_grant_permits_inference_pool_ref, reference_grant_permits_service_ref,
};
use crate::ir::{
    BackendEndpointIr, BackendIr, InferencePoolBackendIr, WeightedBackendIr,
};
use crate::snapshot::{
    ClusterSnapshot, GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap, HttpRouteRuleSnap,
    HttpRouteSnap, InferencePoolSnap, ServiceSnap, TcpRouteRuleSnap, TcpRouteSnap,
    TlsRouteRuleSnap, TlsRouteSnap, UdpRouteRuleSnap, UdpRouteSnap,
};

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

pub(super) fn resolve_service_port(svc: &ServiceSnap, br: &HttpBackendRefSnap) -> Result<u16, String> {
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

pub(super) fn headless_dial_port(svc: &ServiceSnap, service_port: u16) -> Option<u16> {
    if svc.cluster_ip.as_deref() != Some("None") {
        return None;
    }
    svc.ports
        .iter()
        .find(|sp| sp.port == service_port)
        .and_then(|sp| sp.target_port)
}

pub(super) fn find_service<'a>(sn: &'a ClusterSnapshot, ns: &str, name: &str) -> Option<&'a ServiceSnap> {
    sn.services
        .iter()
        .find(|s| s.namespace == ns && s.name == name)
}

pub(super) fn collect_endpoints(sn: &ClusterSnapshot, svc_ns: &str, svc_name: &str) -> Vec<BackendEndpointIr> {
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

pub(super) fn service_has_any_endpointslice(snapshot: &ClusterSnapshot, svc_ns: &str, svc_name: &str) -> bool {
    snapshot.endpoint_slices.iter().any(|sl| {
        sl.namespace == svc_ns && sl.service_name.as_deref() == Some(svc_name)
    })
}

pub(super) fn resolve_http_route_backends(
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

pub(super) fn resolve_grpc_route_backends(
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

pub(super) fn resolve_tls_route_backends(
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

pub(super) fn resolve_tcp_route_backends(
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

pub(super) fn resolve_udp_route_backends(
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

pub(super) fn resolve_weighted_backends(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    _rule_idx: usize,
    snapshot: &ClusterSnapshot,
    report: &mut CompileReport,
) -> Vec<WeightedBackendIr> {
    resolve_http_route_backends(route, rule, snapshot, report)
}

