use super::*;
use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::ir::{BackendTlsIr, RouteFamily};
use crate::listener_merge::merge_effective_listeners;
use crate::snapshot::{
    AllowedRoutesNamespacesSnap, AllowedRoutesSnap, BackendTlsEffectiveSnap, EndpointPortSnap,
    EndpointSliceSnap, EndpointSnap, GatewayListenerSnap, GatewayListenerTlsSnap,
    GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap, HttpHeaderMatchSnap, HttpPathMatchSnap,
    HttpQueryParamMatchSnap, HttpRequestMatchSnap, HttpRouteRuleSnap, ListenerSetSnap,
    ParentRefSnap, ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap,
    RouteGroupKindSnap, ServicePortSnap, ServiceSnap, TcpRouteRuleSnap, TcpRouteSnap,
    TlsRouteRuleSnap, TlsRouteSnap, UdpRouteRuleSnap, UdpRouteSnap,
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
                client_key_pem: Zeroizing::new(vec![]),
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
                client_key_pem: Zeroizing::new(vec![]),
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
                client_key_pem: Zeroizing::new(vec![]),
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
                client_key_pem: Zeroizing::new(vec![]),
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
    effective_listeners[0].tls_key_pem = Zeroizing::new(b"KEY".to_vec());
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
            selector_match_labels: BTreeMap::from([(
                "kubernetes.io/metadata.name".into(),
                "default".into(),
            )]),
        }),
        kinds: vec![RouteGroupKindSnap {
            group: Some("gateway.networking.k8s.io".into()),
            kind: "HTTPRoute".into(),
        }],
    });
    c.namespace_labels = BTreeMap::from([(
        "default".into(),
        BTreeMap::from([("kubernetes.io/metadata.name".into(), "default".into())]),
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
    c.effective_listeners = merge_effective_listeners(&c.listeners, &[]);

    c.services.push(ServiceSnap {
        namespace: "app-backend".into(),
        name: "v1".into(),
        ports: vec![ServicePortSnap {
            name: "http".into(),
            port: 8080,
            target_port: Some(3000),
        }],
        cluster_ip: None,
    });
    c.services.push(ServiceSnap {
        namespace: "app-backend".into(),
        name: "v2".into(),
        ports: vec![ServicePortSnap {
            name: "http".into(),
            port: 8080,
            target_port: Some(3000),
        }],
        cluster_ip: None,
    });
    c.endpoint_slices.push(EndpointSliceSnap {
        namespace: "app-backend".into(),
        name: "v1-eps".into(),
        service_name: Some("v1".into()),
        endpoints: vec![EndpointSnap {
            addresses: vec!["10.0.1.1".into()],
            ports: vec![EndpointPortSnap {
                name: Some("http".into()),
                port: 3000,
            }],
        }],
    });
    c.endpoint_slices.push(EndpointSliceSnap {
        namespace: "app-backend".into(),
        name: "v2-eps".into(),
        service_name: Some("v2".into()),
        endpoints: vec![EndpointSnap {
            addresses: vec!["10.0.1.2".into()],
            ports: vec![EndpointPortSnap {
                name: Some("http".into()),
                port: 3000,
            }],
        }],
    });

    c.reference_grants
        .push(crate::snapshot::ReferenceGrantSnap {
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
        r.matches
            .iter()
            .any(|m| m.path_prefix.as_deref() == Some("/v2"))
    });
    assert!(v2_rule.is_some(), "must have a /v2 rule in IR");
    assert!(
        v2_rule.unwrap().backends.is_empty(),
        "/v2 rule must have no backends (denied by grant)"
    );

    let catchall = ir.routes.iter().find(|r| {
        r.matches
            .iter()
            .any(|m| m.path_prefix.as_deref() == Some("/"))
    });
    assert!(catchall.is_some(), "must have a catch-all rule in IR");
    assert!(
        !catchall.unwrap().backends.is_empty(),
        "catch-all rule must have backends (v1 allowed by grant)"
    );
    assert_eq!(
        catchall.unwrap().backends[0].backend.service_name,
        "v1",
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

#[test]
fn header_match_rule_sorts_before_query_param_rule_in_same_route() {
    let mut c = minimal_cluster();
    c.http_routes[0].rules = vec![
        HttpRouteRuleSnap {
            matches: vec![HttpRequestMatchSnap {
                query_params: vec![HttpQueryParamMatchSnap {
                    name: "animal".into(),
                    value: "hydra".into(),
                    match_type: Some("Exact".into()),
                }],
                ..Default::default()
            }],
            backend_refs: vec![HttpBackendRefSnap {
                group: None,
                kind: Some("Service".into()),
                namespace: None,
                name: "svc-qp".into(),
                port: Some(80),
                port_name: None,
                weight: None,
            }],
            ..Default::default()
        },
        HttpRouteRuleSnap {
            matches: vec![HttpRequestMatchSnap {
                headers: vec![HttpHeaderMatchSnap {
                    match_type: Some("Exact".into()),
                    name: "version".into(),
                    value: "four".into(),
                }],
                ..Default::default()
            }],
            backend_refs: vec![HttpBackendRefSnap {
                group: None,
                kind: Some("Service".into()),
                namespace: None,
                name: "svc-hdr".into(),
                port: Some(80),
                port_name: None,
                weight: None,
            }],
            ..Default::default()
        },
    ];
    for svc_name in &["svc-qp", "svc-hdr"] {
        c.services.push(ServiceSnap {
            namespace: "default".into(),
            name: svc_name.to_string(),
            ports: vec![ServicePortSnap {
                name: "http".into(),
                port: 80,
                target_port: Some(8080),
            }],
            cluster_ip: None,
        });
        c.endpoint_slices.push(EndpointSliceSnap {
            namespace: "default".into(),
            name: format!("{svc_name}-1"),
            service_name: Some(svc_name.to_string()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.0.0.2".into()],
                ports: vec![EndpointPortSnap {
                    name: Some("http".into()),
                    port: 8080,
                }],
            }],
        });
    }

    let (ir, rep) = ir_from_cluster(&c);
    assert!(!rep.has_errors(), "{:?}", rep.errors);
    let http_routes: Vec<&RouteRuleIr> = ir
        .routes
        .iter()
        .filter(|r| r.route_family == RouteFamily::Http)
        .collect();
    assert!(
        http_routes.len() >= 2,
        "expected at least 2 HTTP rules, got {}",
        http_routes.len()
    );
    assert!(
        http_routes[0]
            .backends
            .iter()
            .any(|b| b.backend.service_name == "svc-hdr"),
        "header-match rule should sort first (GEP-1713); got {:?}",
        http_routes[0].backends,
    );
}

#[test]
fn method_match_rule_sorts_before_header_match_rule_in_same_route() {
    let mut c = minimal_cluster();
    c.http_routes[0].rules = vec![
        HttpRouteRuleSnap {
            matches: vec![HttpRequestMatchSnap {
                method: Some("PATCH".into()),
                ..Default::default()
            }],
            backend_refs: vec![HttpBackendRefSnap {
                group: None,
                kind: Some("Service".into()),
                namespace: None,
                name: "svc-method".into(),
                port: Some(80),
                port_name: None,
                weight: None,
            }],
            ..Default::default()
        },
        HttpRouteRuleSnap {
            matches: vec![HttpRequestMatchSnap {
                headers: vec![HttpHeaderMatchSnap {
                    match_type: Some("Exact".into()),
                    name: "version".into(),
                    value: "four".into(),
                }],
                ..Default::default()
            }],
            backend_refs: vec![HttpBackendRefSnap {
                group: None,
                kind: Some("Service".into()),
                namespace: None,
                name: "svc-hdr".into(),
                port: Some(80),
                port_name: None,
                weight: None,
            }],
            ..Default::default()
        },
    ];
    for svc_name in &["svc-method", "svc-hdr"] {
        c.services.push(ServiceSnap {
            namespace: "default".into(),
            name: svc_name.to_string(),
            ports: vec![ServicePortSnap {
                name: "http".into(),
                port: 80,
                target_port: Some(8080),
            }],
            cluster_ip: None,
        });
        c.endpoint_slices.push(EndpointSliceSnap {
            namespace: "default".into(),
            name: format!("{svc_name}-1"),
            service_name: Some(svc_name.to_string()),
            endpoints: vec![EndpointSnap {
                addresses: vec!["10.0.0.2".into()],
                ports: vec![EndpointPortSnap {
                    name: Some("http".into()),
                    port: 8080,
                }],
            }],
        });
    }

    let (ir, rep) = ir_from_cluster(&c);
    assert!(!rep.has_errors(), "{:?}", rep.errors);
    let http_routes: Vec<&RouteRuleIr> = ir
        .routes
        .iter()
        .filter(|r| r.route_family == RouteFamily::Http)
        .collect();
    assert!(
        http_routes.len() >= 2,
        "expected at least 2 HTTP rules, got {}",
        http_routes.len()
    );
    assert!(
        http_routes[0]
            .backends
            .iter()
            .any(|b| b.backend.service_name == "svc-method"),
        "method-match rule should sort before header-match rule (spec: method > headers); got {:?}",
        http_routes[0].backends,
    );
}
