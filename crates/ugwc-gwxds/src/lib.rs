//! Encode [`ugwc_core::IrSnapshot`] into `istio.gwxds` protobuf for Praxis xDS clients.

#![allow(clippy::missing_errors_doc)]

pub mod istio {
    pub mod gwxds {
        include!(concat!(env!("OUT_DIR"), "/istio.gwxds.rs"));
    }
}

use istio::gwxds::{
    Backend, BackendRef, BackendTls, FailureMode, HeaderNameValue,
    InferencePool as InferencePoolPb, Listener, Protocol, RequestHeaderModifier, RequestRedirect,
    Resource, Route, RouteMatch, TlsConfig,
};
use ugwc_core::ir::{RouteFamily, TlsMode};
use ugwc_core::IrSnapshot;

/// ADS `type_url` for gwxds resources (matches Praxis `xds-client`).
pub const GW_TYPE_URL: &str = "type.googleapis.com/istio.gwxds.Resource";

fn parse_scope(scope: &ugwc_core::IrScopeId) -> (String, String) {
    let s = &scope.0;
    if let Some((ns, rest)) = s.split_once('/') {
        (ns.to_string(), rest.to_string())
    } else {
        ("default".into(), s.clone())
    }
}

fn qualify_host(service_ns: &str, host: &str) -> String {
    if host.parse::<std::net::IpAddr>().is_ok() || host.contains('.') {
        return host.to_owned();
    }
    format!("{host}.{service_ns}.svc.cluster.local")
}

fn redirect_proto(r: &ugwc_core::ir::RequestRedirectIr) -> RequestRedirect {
    let path = r
        .path_replace_full
        .clone()
        .or_else(|| r.path_replace_prefix.clone())
        .unwrap_or_default();
    RequestRedirect {
        hostname: r.hostname.clone().unwrap_or_default(),
        path,
        port: u32::from(r.port.unwrap_or(0)),
        scheme: r.scheme.clone().unwrap_or_default(),
        status_code: r.status_code.unwrap_or(302),
    }
}

fn header_modifier_proto(h: &ugwc_core::ir::RequestHeaderModifierIr) -> RequestHeaderModifier {
    RequestHeaderModifier {
        set: h
            .set
            .iter()
            .map(|x| HeaderNameValue {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .iter()
            .map(|x| HeaderNameValue {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone(),
    }
}

fn backend_tls_proto(t: &ugwc_core::ir::BackendTlsIr) -> BackendTls {
    BackendTls {
        ca_cert: t.ca_cert_pem.clone(),
        hostname: t.hostname.clone(),
        sans: t.sans.clone(),
        client_cert: vec![],
        client_key: vec![],
        invalid: t.invalid,
    }
}

fn route_matches_proto(
    family: RouteFamily,
    matches: &[ugwc_core::ir::RouteMatchIr],
) -> Vec<RouteMatch> {
    let src: Vec<ugwc_core::ir::RouteMatchIr> = if matches.is_empty() {
        match family {
            RouteFamily::Grpc => vec![ugwc_core::ir::RouteMatchIr {
                path_prefix: None,
                path_exact: None,
                headers_exact: Default::default(),
                grpc_service: None,
                grpc_method: None,
            }],
            RouteFamily::Http => vec![ugwc_core::ir::RouteMatchIr {
                path_prefix: Some("/".into()),
                path_exact: None,
                headers_exact: Default::default(),
                grpc_service: None,
                grpc_method: None,
            }],
            RouteFamily::Tls => vec![ugwc_core::ir::RouteMatchIr {
                path_prefix: None,
                path_exact: None,
                headers_exact: Default::default(),
                grpc_service: None,
                grpc_method: None,
            }],
            RouteFamily::Tcp => vec![ugwc_core::ir::RouteMatchIr {
                path_prefix: None,
                path_exact: None,
                headers_exact: Default::default(),
                grpc_service: None,
                grpc_method: None,
            }],
            RouteFamily::Udp => vec![ugwc_core::ir::RouteMatchIr {
                path_prefix: None,
                path_exact: None,
                headers_exact: Default::default(),
                grpc_service: None,
                grpc_method: None,
            }],
        }
    } else {
        matches.to_vec()
    };
    src.iter()
        .map(|m| RouteMatch {
            path_prefix: m.path_prefix.clone().unwrap_or_default(),
            path_exact: m.path_exact.clone().unwrap_or_default(),
            path_regex: String::new(),
            headers: m
                .headers_exact
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            methods: vec![],
            grpc_service: m.grpc_service.clone().unwrap_or_default(),
            grpc_method: m.grpc_method.clone().unwrap_or_default(),
        })
        .collect()
}

/// Encode IR into one [`Resource`] per listener (routes duplicated per listener for v0).
#[must_use]
pub fn encode_ir(ir: &IrSnapshot) -> Vec<Resource> {
    let (gw_ns, gw_name) = parse_scope(&ir.scope);
    let mut out = Vec::new();
    for ln in &ir.listeners {
        if ln.conflicted {
            continue;
        }
        let listener_key = format!(
            "{gw_ns}/{gw_name}/{}/{}/{}/{}/{}",
            ln.parent_kind, ln.parent_namespace, ln.parent_name, ln.section_name, ln.bind_port
        );
        let gp_norm = ln.gateway_protocol.trim().to_ascii_uppercase();
        let protocol = if gp_norm == "TCP" {
            Protocol::Tcp as i32
        } else if gp_norm == "UDP" {
            Protocol::Udp as i32
        } else {
            match ln.tls {
                TlsMode::Passthrough => Protocol::Tls as i32,
                TlsMode::Terminate => Protocol::Https as i32,
                TlsMode::Plaintext => Protocol::Http as i32,
            }
        };
        let hostname = ln
            .hostname
            .clone()
            .filter(|h| !h.trim().is_empty())
            .unwrap_or_else(|| "*".into());
        let tls = if matches!(ln.tls, TlsMode::Terminate)
            && !ln.tls_cert_pem.is_empty()
            && !ln.tls_key_pem.is_empty()
        {
            Some(TlsConfig {
                cert_pem: ln.tls_cert_pem.clone(),
                key_pem: ln.tls_key_pem.clone(),
                ca_pem: vec![],
                min_version: String::new(),
            })
        } else {
            None
        };
        let allowed_routes = if ln.allowed_route_group_kinds.is_empty() {
            if gp_norm == "TCP" {
                vec!["gateway.networking.k8s.io/TCPRoute".into()]
            } else if gp_norm == "UDP" {
                vec!["gateway.networking.k8s.io/UDPRoute".into()]
            } else if matches!(ln.tls, TlsMode::Passthrough) {
                vec!["gateway.networking.k8s.io/TLSRoute".into()]
            } else {
                vec![
                    "gateway.networking.k8s.io/HTTPRoute".into(),
                    "gateway.networking.k8s.io/GRPCRoute".into(),
                ]
            }
        } else {
            ln.allowed_route_group_kinds.clone()
        };
        let listener = Listener {
            key: listener_key.clone(),
            hostname,
            port: u32::from(ln.bind_port),
            protocol,
            tls,
            allowed_routes,
        };

        let mut routes = Vec::new();
        for (idx, r) in ir.routes.iter().enumerate() {
            let attach_ok = r.listener.as_ref().map_or(false, |att| {
                att.section_name == ln.section_name
                    && att.port == ln.bind_port
                    && att.parent_kind == ln.parent_kind
                    && att.parent_namespace == ln.parent_namespace
                    && att.parent_name == ln.parent_name
            });
            if !attach_ok {
                continue;
            }
            let route_key = format!("{}/{}/{}", gw_ns, r.name, idx);
            let hostnames: Vec<String> = if r.hosts.is_empty() {
                vec!["*".into()]
            } else {
                r.hosts.clone()
            };

            let matches_pb = route_matches_proto(r.route_family.clone(), &r.matches);

            let mut backends_pb = Vec::new();
            let mut invalid_backend_ref = false;
            if r.request_redirect.is_none() {
                for wb in &r.backends {
                    if let Some(ref inf) = wb.backend.inference {
                        let picker_host = inf.picker_host.clone();
                        let picker_port = u32::from(inf.picker_port);
                        backends_pb.push(Backend {
                            host: picker_host.clone(),
                            port: picker_port,
                            weight: wb.weight,
                            inference_pool: Some(InferencePoolPb {
                                endpoint_picker: Some(BackendRef {
                                    host: picker_host,
                                    port: picker_port,
                                }),
                                failure_mode: if inf.failure_mode_fail_open {
                                    FailureMode::FailOpen as i32
                                } else {
                                    FailureMode::FailClosed as i32
                                },
                            }),
                            tls: None,
                            dial_port: picker_port,
                        });
                        continue;
                    }
                    let key = format!(
                        "{}/{}/{}",
                        wb.backend.service_namespace,
                        wb.backend.service_name,
                        wb.backend.port_number.unwrap_or(80)
                    );
                    let eps = ir.backends.get(&key).cloned().unwrap_or_default();
                    let host =
                        qualify_host(&wb.backend.service_namespace, &wb.backend.service_name);
                    let port = wb.backend.port_number.unwrap_or(80) as u32;
                    let dial_port = eps.first().map(|e| u32::from(e.port)).unwrap_or(port);
                    let tls = wb.backend.backend_tls.as_ref().map(backend_tls_proto);
                    backends_pb.push(Backend {
                        host,
                        port,
                        weight: wb.weight,
                        inference_pool: None,
                        tls,
                        dial_port,
                    });
                }
                if backends_pb.is_empty() {
                    invalid_backend_ref = true;
                }
            }

            let request_redirect = r.request_redirect.as_ref().map(redirect_proto);
            let request_header_modifier = r
                .request_header_modifier
                .as_ref()
                .map(header_modifier_proto);
            let response_header_modifier = r
                .response_header_modifier
                .as_ref()
                .map(header_modifier_proto);
            let grpc_route = matches!(r.route_family, RouteFamily::Grpc);
            let tls_route = matches!(r.route_family, RouteFamily::Tls);
            let tcp_route = matches!(r.route_family, RouteFamily::Tcp);
            let udp_route = matches!(r.route_family, RouteFamily::Udp);

            routes.push(Route {
                key: route_key,
                listener_key: listener_key.clone(),
                hostnames,
                matches: matches_pb,
                backends: backends_pb,
                request_redirect,
                request_header_modifier,
                invalid_backend_ref,
                response_header_modifier,
                grpc_route,
                tls_route,
                tcp_route,
                udp_route,
            });
        }

        out.push(Resource {
            key: listener_key.clone(),
            listener: Some(listener),
            routes,
        });
    }
    if out.is_empty() {
        let listener_key = format!("{gw_ns}/{gw_name}/default");
        out.push(Resource {
            key: listener_key.clone(),
            listener: Some(Listener {
                key: listener_key,
                hostname: "*".into(),
                port: 80,
                protocol: Protocol::Http as i32,
                tls: None,
                allowed_routes: vec![],
            }),
            routes: vec![],
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::http_ir::HttpCompileFixture;
    use ugwc_core::ir::TlsMode;

    #[test]
    fn encode_minimal_fixture_non_empty() {
        let f = HttpCompileFixture::minimal_http();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        assert!(!r.is_empty());
        let rt = &r[0].routes[0];
        assert_eq!(rt.backends.len(), 1);
        assert_eq!(rt.backends[0].weight, 1);
        assert!(rt.matches.iter().any(|m| !m.path_prefix.is_empty()));
        let ln = r[0].listener.as_ref().expect("listener");
        assert!(ln.allowed_routes.iter().any(|x| x.contains("GRPCRoute")));
    }

    #[test]
    fn encode_exact_path_on_match() {
        let mut f = HttpCompileFixture::minimal_http();
        f.routes[0].matches[0].path_prefix = None;
        f.routes[0].matches[0].path_exact = Some("/foo".into());
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        let m = &r[0].routes[0].matches[0];
        assert_eq!(m.path_exact, "/foo");
        assert!(m.path_prefix.is_empty());
    }

    #[test]
    fn encode_grpc_route_family_sets_grpc_match_and_flag() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendEndpointIr, BackendIr, IrRevision, IrScopeId, ListenerAttachmentIr, ListenerIr,
            RouteFamily, RouteMatchIr, RouteRuleIr, SnapshotMeta, WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "https".into(),
            bind_port: 443,
            tls: TlsMode::Terminate,
            gateway_protocol: "HTTPS".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: Some("gw.example.com".into()),
            tls_cert_pem: b"C".to_vec(),
            tls_key_pem: b"K".to_vec(),
            allowed_route_group_kinds: vec![
                "gateway.networking.k8s.io/HTTPRoute".into(),
                "gateway.networking.k8s.io/GRPCRoute".into(),
            ],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "https".into(),
            port: 443,
        };
        let mut backends_map = BTreeMap::new();
        backends_map.insert(
            "default/svc:443".into(),
            vec![BackendEndpointIr {
                address: "10.0.0.2".into(),
                port: 8443,
            }],
        );
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "grpc/x".into(),
                route_family: RouteFamily::Grpc,
                hosts: vec!["svc.example.com".into()],
                matches: vec![RouteMatchIr {
                    path_prefix: None,
                    path_exact: None,
                    headers_exact: BTreeMap::new(),
                    grpc_service: Some("hello.Greeter".into()),
                    grpc_method: Some("SayHello".into()),
                }],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: None,
                        port_number: Some(443),
                        backend_tls: None,
                        inference: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: backends_map,
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let rt = &enc[0].routes[0];
        assert!(rt.grpc_route);
        let m = &rt.matches[0];
        assert_eq!(m.grpc_service, "hello.Greeter");
        assert_eq!(m.grpc_method, "SayHello");
        assert!(!rt.tls_route);
        assert!(!rt.tcp_route);
        assert!(!rt.udp_route);
    }

    #[test]
    fn encode_tls_passthrough_listener_and_tls_route_flag() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendEndpointIr, BackendIr, IrRevision, IrScopeId, ListenerAttachmentIr, ListenerIr,
            RouteFamily, RouteRuleIr, SnapshotMeta, WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "tls".into(),
            bind_port: 443,
            tls: TlsMode::Passthrough,
            gateway_protocol: "TLS".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: Some("*.example.com".into()),
            tls_cert_pem: vec![],
            tls_key_pem: vec![],
            allowed_route_group_kinds: vec![],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "tls".into(),
            port: 443,
        };
        let backends_map = BTreeMap::from([(
            "default/svc:443".into(),
            vec![BackendEndpointIr {
                address: "10.1.2.3".into(),
                port: 8443,
            }],
        )]);
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "tls/x".into(),
                route_family: RouteFamily::Tls,
                hosts: vec!["svc.example.com".into()],
                matches: vec![],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: None,
                        port_number: Some(443),
                        backend_tls: None,
                        inference: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: backends_map,
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let ln = enc[0].listener.as_ref().expect("listener");
        assert_eq!(ln.protocol, Protocol::Tls as i32);
        assert!(ln.allowed_routes.iter().any(|x| x.contains("TLSRoute")));
        assert!(ln.tls.is_none());
        let rt = &enc[0].routes[0];
        assert!(rt.tls_route);
        assert!(!rt.grpc_route);
        assert!(!rt.tcp_route);
        assert!(!rt.udp_route);
    }

    #[test]
    fn encode_tcp_listener_protocol_and_tcp_route_flag() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendEndpointIr, BackendIr, IrRevision, IrScopeId, ListenerAttachmentIr, ListenerIr,
            RouteFamily, RouteRuleIr, SnapshotMeta, TlsMode, WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "tcp".into(),
            bind_port: 9000,
            tls: TlsMode::Plaintext,
            gateway_protocol: "TCP".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: None,
            tls_cert_pem: vec![],
            tls_key_pem: vec![],
            allowed_route_group_kinds: vec![],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "tcp".into(),
            port: 9000,
        };
        let backends_map = BTreeMap::from([(
            "default/svc:9000".into(),
            vec![BackendEndpointIr {
                address: "10.1.2.3".into(),
                port: 19000,
            }],
        )]);
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "tcp/x".into(),
                route_family: RouteFamily::Tcp,
                hosts: vec![],
                matches: vec![],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: None,
                        port_number: Some(9000),
                        backend_tls: None,
                        inference: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: backends_map,
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let ln = enc[0].listener.as_ref().expect("listener");
        assert_eq!(ln.protocol, Protocol::Tcp as i32);
        assert!(ln.allowed_routes.iter().any(|x| x.contains("TCPRoute")));
        let rt = &enc[0].routes[0];
        assert!(rt.tcp_route);
        assert!(!rt.tls_route);
        assert!(!rt.grpc_route);
        assert!(!rt.udp_route);
    }

    #[test]
    fn encode_udp_listener_protocol_and_udp_route_flag() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendEndpointIr, BackendIr, IrRevision, IrScopeId, ListenerAttachmentIr, ListenerIr,
            RouteFamily, RouteRuleIr, SnapshotMeta, TlsMode, WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "udp".into(),
            bind_port: 9500,
            tls: TlsMode::Plaintext,
            gateway_protocol: "UDP".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: None,
            tls_cert_pem: vec![],
            tls_key_pem: vec![],
            allowed_route_group_kinds: vec![],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "udp".into(),
            port: 9500,
        };
        let backends_map = BTreeMap::from([(
            "default/svc:9500".into(),
            vec![BackendEndpointIr {
                address: "10.1.2.3".into(),
                port: 19500,
            }],
        )]);
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "udp/x".into(),
                route_family: RouteFamily::Udp,
                hosts: vec![],
                matches: vec![],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: None,
                        port_number: Some(9500),
                        backend_tls: None,
                        inference: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: backends_map,
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let ln = enc[0].listener.as_ref().expect("listener");
        assert_eq!(ln.protocol, Protocol::Udp as i32);
        assert!(ln.allowed_routes.iter().any(|x| x.contains("UDPRoute")));
        let rt = &enc[0].routes[0];
        assert!(rt.udp_route);
        assert!(!rt.tcp_route);
        assert!(!rt.tls_route);
        assert!(!rt.grpc_route);
    }

    #[test]
    fn encode_backend_tls_populates_backend_message() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendEndpointIr, BackendIr, BackendTlsIr, IrRevision, IrScopeId,
            ListenerAttachmentIr, ListenerIr, RouteFamily, RouteMatchIr, RouteRuleIr, SnapshotMeta,
            TlsMode, WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "http".into(),
            bind_port: 80,
            tls: TlsMode::Plaintext,
            gateway_protocol: "HTTP".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: None,
            tls_cert_pem: vec![],
            tls_key_pem: vec![],
            allowed_route_group_kinds: vec![],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "http".into(),
            port: 80,
        };
        let backends_map = BTreeMap::from([(
            "default/svc:80".into(),
            vec![BackendEndpointIr {
                address: "10.1.2.3".into(),
                port: 8080,
            }],
        )]);
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "hr/x".into(),
                route_family: RouteFamily::Http,
                hosts: vec![],
                matches: vec![RouteMatchIr {
                    path_prefix: Some("/".into()),
                    path_exact: None,
                    headers_exact: BTreeMap::new(),
                    grpc_service: None,
                    grpc_method: None,
                }],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: Some("http".into()),
                        port_number: Some(80),
                        backend_tls: Some(BackendTlsIr {
                            ca_cert_pem: b"BEGIN CA".to_vec(),
                            hostname: "svc.internal".into(),
                            sans: vec!["alt.internal".into()],
                            trust_system_ca: false,
                            invalid: false,
                        }),
                        inference: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: backends_map,
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let b = &enc[0].routes[0].backends[0];
        let tls = b.tls.as_ref().expect("backend tls");
        assert_eq!(tls.hostname, "svc.internal");
        assert_eq!(tls.ca_cert, b"BEGIN CA".as_slice());
        assert_eq!(tls.sans, vec!["alt.internal".to_string()]);
        assert!(!tls.invalid);
    }

    #[test]
    fn encode_inference_pool_sets_endpoint_picker_and_failure_mode() {
        use std::collections::BTreeMap;

        use ugwc_core::ir::{
            BackendIr, InferencePoolBackendIr, IrRevision, IrScopeId, ListenerAttachmentIr,
            ListenerIr, RouteFamily, RouteMatchIr, RouteRuleIr, SnapshotMeta, TlsMode,
            WeightedBackendIr, IR_VERSION,
        };
        use ugwc_core::IrSnapshot;

        let scope = IrScopeId::new("default", "gw");
        let listener = ListenerIr {
            section_name: "http".into(),
            bind_port: 80,
            tls: TlsMode::Plaintext,
            gateway_protocol: "HTTP".into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: None,
            tls_cert_pem: vec![],
            tls_key_pem: vec![],
            allowed_route_group_kinds: vec![],
        };
        let attach = ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "http".into(),
            port: 80,
        };
        let picker_host = "epp.ns.svc.cluster.local".to_string();
        let ir = IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![listener],
            routes: vec![RouteRuleIr {
                name: "hr/pool".into(),
                route_family: RouteFamily::Http,
                hosts: vec![],
                matches: vec![RouteMatchIr {
                    path_prefix: Some("/".into()),
                    path_exact: None,
                    headers_exact: BTreeMap::new(),
                    grpc_service: None,
                    grpc_method: None,
                }],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: String::new(),
                        service_name: String::new(),
                        port_name: None,
                        port_number: None,
                        backend_tls: None,
                        inference: Some(InferencePoolBackendIr {
                            pool_namespace: "inf".into(),
                            pool_name: "p1".into(),
                            picker_host: picker_host.clone(),
                            picker_port: 9000,
                            failure_mode_fail_open: false,
                        }),
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                listener: Some(attach),
            }],
            backends: BTreeMap::new(),
            meta: SnapshotMeta {
                correlation_id: uuid::Uuid::nil(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        };
        let enc = encode_ir(&ir);
        let b = &enc[0].routes[0].backends[0];
        assert_eq!(b.host, picker_host);
        assert_eq!(b.port, 9000);
        assert_eq!(b.dial_port, 9000);
        let ip = b.inference_pool.as_ref().expect("inference_pool");
        let ep = ip.endpoint_picker.as_ref().expect("endpoint_picker");
        assert_eq!(ep.host, picker_host);
        assert_eq!(ep.port, 9000);
        assert_eq!(ip.failure_mode, FailureMode::FailClosed as i32);

        let mut ir_open = ir.clone();
        ir_open.routes[0].backends[0]
            .backend
            .inference
            .as_mut()
            .expect("inference")
            .failure_mode_fail_open = true;
        let enc_open = encode_ir(&ir_open);
        let ip_open = enc_open[0].routes[0].backends[0]
            .inference_pool
            .as_ref()
            .expect("inference_pool");
        assert_eq!(ip_open.failure_mode, FailureMode::FailOpen as i32);
    }

    #[test]
    fn encode_https_tls_and_hostname() {
        let mut f = HttpCompileFixture::minimal_http();
        f.listeners[0].tls = TlsMode::Terminate;
        f.listeners[0].hostname = Some("gw.example.com".into());
        f.listeners[0].tls_cert_pem = b"BEGIN CERT".to_vec();
        f.listeners[0].tls_key_pem = b"BEGIN KEY".to_vec();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        let l = r[0].listener.as_ref().expect("listener");
        assert_eq!(l.hostname, "gw.example.com");
        assert!(
            l.tls.as_ref().is_some_and(|t| !t.cert_pem.is_empty()),
            "{l:?}"
        );
    }
}
