//! Load [`ugwc_core::ClusterSnapshot`] for a reconciled `Gateway`.

use std::collections::{BTreeMap, HashSet};

use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use thiserror::Error;
use tracing::warn;
use ugwc_core::http_route_attachment::reference_grant_permits_gateway_secret_ref;
use ugwc_core::listener_merge::merge_effective_listeners;
use ugwc_core::snapshot::{
    AllowedListenersSnap, AllowedRoutesNamespacesSnap, AllowedRoutesSnap, ClusterSnapshot,
    EffectiveListenerSnap, EndpointPickerRefSnap, EndpointPortSnap, EndpointSliceSnap,
    EndpointSnap, GatewayAddressSnap, GatewayClassSnap, GatewayInfrastructureSnap,
    GatewayListenerSnap, GatewayListenerTlsSnap, GrpcHeaderMatchSnap, GrpcMethodMatchSnap,
    GrpcRouteFilterSnap, GrpcRouteMatchSnap, GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap,
    HttpHeaderFilterSnap, HttpHeaderMatchSnap, HttpHeaderNameValueSnap, HttpPathMatchSnap,
    HttpPathModifierSnap, HttpRequestMatchSnap, HttpRequestRedirectFilterSnap, HttpRouteFilterSnap,
    HttpRouteRuleSnap, HttpRouteSnap, InferencePoolPortSnap, InferencePoolSelectorSnap,
    InferencePoolSnap, ListenerNamespacesSnap, ListenerSetSnap, ParentRefSnap,
    ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap, RouteGroupKindSnap,
    SecretObjectReferenceSnap, ServicePortSnap, ServiceSnap, TcpRouteRuleSnap, TcpRouteSnap,
    TlsRouteRuleSnap, TlsRouteSnap, UdpRouteRuleSnap, UdpRouteSnap,
};

use crate::resources::{
    GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet, ParentReferenceSpec,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
}

#[path = "backend_tls.rs"]
pub(crate) mod backend_tls;

/// Convert spec parentRef to snapshot form (HTTPRoute status merge).
#[must_use]
pub fn parent_reference_to_snap(p: &ParentReferenceSpec) -> ParentRefSnap {
    ParentRefSnap {
        group: p.group.clone(),
        kind: p.kind.clone(),
        namespace: p.namespace.clone(),
        name: p.name.clone(),
        section_name: p.section_name.clone(),
        port: p.port,
    }
}

fn parent_to_snap(p: &ParentReferenceSpec) -> ParentRefSnap {
    parent_reference_to_snap(p)
}

fn inference_pool_to_snap(ip: InferencePool) -> InferencePoolSnap {
    let namespace = ip.namespace().unwrap_or_default();
    let name = ip.name_any();
    let spec = ip.spec;
    InferencePoolSnap {
        namespace,
        name,
        selector: InferencePoolSelectorSnap {
            match_labels: spec.selector.match_labels,
        },
        target_ports: spec
            .target_ports
            .into_iter()
            .filter_map(|p| {
                let n = p.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            })
            .collect(),
        app_protocol: spec.app_protocol,
        endpoint_picker_ref: EndpointPickerRefSnap {
            group: spec.endpoint_picker_ref.group,
            kind: spec.endpoint_picker_ref.kind,
            name: spec.endpoint_picker_ref.name,
            port: spec.endpoint_picker_ref.port.and_then(|x| {
                let n = x.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            }),
            failure_mode: spec.endpoint_picker_ref.failure_mode,
        },
    }
}

/// When `allowedListeners` is unset or defaults to no attachment, ListenerSets are ignored.
fn allowed_snap_for_merge(gw: &Gateway) -> Option<AllowedListenersSnap> {
    let al = gw.spec.allowed_listeners.as_ref()?;
    let ns = al.namespaces.as_ref()?;
    let from = ns.from.as_deref().unwrap_or("None");
    if from == "None" || from.is_empty() {
        return None;
    }
    Some(AllowedListenersSnap {
        namespaces: Some(ListenerNamespacesSnap {
            from: Some(from.to_string()),
            selector_match_labels: ns
                .selector
                .as_ref()
                .map(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
    })
}

fn listenerset_parents_gateway(ls: &ListenerSet, gw_ns: &str, gw_name: &str) -> bool {
    let ls_ns_default = ls.metadata.namespace.as_deref().unwrap_or("");
    ls.spec.parent_refs.iter().any(|p| {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" || k != "Gateway" {
            return false;
        }
        let pns = p.namespace.as_deref().unwrap_or(ls_ns_default);
        pns == gw_ns && p.name == gw_name
    })
}

async fn namespace_labels(
    client: &Client,
    ns: &str,
) -> Result<BTreeMap<String, String>, kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    let n = api.get(ns).await?;
    Ok(n.metadata.labels.unwrap_or_default())
}

async fn listener_set_namespace_allowed(
    client: &Client,
    gateway_ns: &str,
    ls_ns: &str,
    allowed: &AllowedListenersSnap,
) -> Result<bool, kube::Error> {
    let Some(ref nses) = allowed.namespaces else {
        return Ok(false);
    };
    let from = nses.from.as_deref().unwrap_or("None");
    Ok(match from {
        "All" => true,
        "Same" => ls_ns == gateway_ns,
        "Selector" => {
            if nses.selector_match_labels.is_empty() {
                false
            } else {
                let labels = namespace_labels(client, ls_ns).await.unwrap_or_default();
                nses.selector_match_labels
                    .iter()
                    .all(|(k, v)| labels.get(k).map(|x| x == v).unwrap_or(false))
            }
        }
        _ => false,
    })
}

fn allowed_routes_snap(ar: &crate::resources::AllowedRoutesSpec) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar.namespaces.as_ref().map(|n| AllowedRoutesNamespacesSnap {
            from: n.from.clone(),
            selector_match_labels: n
                .selector
                .as_ref()
                .map(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
        kinds: ar
            .kinds
            .iter()
            .map(|k| RouteGroupKindSnap {
                group: k.group.clone(),
                kind: k.kind.clone(),
            })
            .collect(),
    }
}

fn gateway_listener_to_snap(l: &crate::resources::GatewayListenerSpec) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: l.port,
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l.allowed_routes.as_ref().map(allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_to_snap),
    }
}

fn listener_tls_to_snap(t: &crate::resources::ListenerTlsSpec) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: t.mode.clone(),
        certificate_refs: t
            .certificate_refs
            .iter()
            .map(|r| SecretObjectReferenceSnap {
                group: r.group.clone(),
                kind: r.kind.clone(),
                name: r.name.clone(),
                namespace: r.namespace.clone(),
            })
            .collect(),
    }
}

fn extract_tls_pem_from_secret(sec: &Secret) -> Result<(Vec<u8>, Vec<u8>), String> {
    let data = sec
        .data
        .as_ref()
        .ok_or_else(|| "Secret has no data".to_string())?;
    let crt = data
        .get("tls.crt")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.crt".to_string())?;
    let key = data
        .get("tls.key")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.key".to_string())?;
    Ok((crt, key))
}

/// Reject obviously invalid `kubernetes.io/tls` material before xDS reaches the dataplane.
///
/// [`extract_tls_pem_from_secret`] only checks keys exist. Junk or cert-only blobs in `tls.key`
/// would still be forwarded and fail in the proxy (e.g. Pingora: "no private key found in PEM file").
fn tls_k8s_secret_pem_looks_valid(cert_pem: &[u8], key_pem: &[u8]) -> bool {
    fn has_token(pem: &[u8], tok: &[u8]) -> bool {
        if tok.is_empty() {
            return false;
        }
        pem.windows(tok.len()).any(|w| w == tok)
    }
    has_token(key_pem, b"PRIVATE KEY") && has_token(cert_pem, b"CERTIFICATE")
}

/// Resolve `certificateRefs` into PEM material on HTTPS listeners (same-namespace or via ReferenceGrant).
async fn resolve_gateway_listener_tls(
    client: &Client,
    gateway_ns: &str,
    grants: &[ReferenceGrantSnap],
    effective: &mut [EffectiveListenerSnap],
) -> Result<(), LoaderError> {
    for e in effective.iter_mut() {
        if e.conflicted {
            continue;
        }
        let proto = e.protocol.trim().to_ascii_uppercase();
        if proto != "HTTPS" {
            continue;
        }
        let mode = e
            .tls
            .as_ref()
            .and_then(|t| t.mode.as_deref())
            .unwrap_or("Terminate");
        if mode.eq_ignore_ascii_case("Passthrough") {
            e.tls_resolve_error = Some("UnsupportedValue: HTTPS TLS Passthrough".into());
            continue;
        }
        let refs = e
            .tls
            .as_ref()
            .map(|t| &t.certificate_refs[..])
            .unwrap_or(&[]);
        if refs.is_empty() {
            e.tls_resolve_error =
                Some("InvalidCertificateRef: HTTPS listener requires certificateRefs".into());
            continue;
        }
        let r = &refs[0];
        let kind = r.kind.as_deref().unwrap_or("Secret");
        let group = r.group.as_deref().unwrap_or("");
        // Core `Secret` refs use an empty group (or, rarely, "core"); anything else is invalid.
        let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
        if !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok {
            e.tls_resolve_error = Some(format!(
                "InvalidCertificateRef: unsupported certificate ref {group}/{kind}"
            ));
            continue;
        }
        let secret_ns = r.namespace.as_deref().unwrap_or(gateway_ns);
        if !reference_grant_permits_gateway_secret_ref(grants, gateway_ns, secret_ns, &r.name) {
            e.tls_resolve_error = Some("RefNotPermitted".into());
            continue;
        }
        let api: Api<Secret> = Api::namespaced(client.clone(), secret_ns);
        match api.get(&r.name).await {
            Ok(sec) => match extract_tls_pem_from_secret(&sec) {
                Ok((crt, key)) => {
                    if !tls_k8s_secret_pem_looks_valid(&crt, &key) {
                        e.tls_resolve_error = Some(
                            "InvalidCertificateRef: tls.crt/tls.key must contain PEM certificate and private key"
                                .into(),
                        );
                        continue;
                    }
                    e.tls_cert_pem = crt;
                    e.tls_key_pem = key;
                }
                Err(msg) => {
                    e.tls_resolve_error = Some(format!("InvalidCertificateRef: {msg}"));
                }
            },
            Err(_) => {
                e.tls_resolve_error = Some("InvalidCertificateRef: Secret not found".into());
            }
        }
    }
    Ok(())
}

fn http_header_filter_snap(h: &crate::resources::HTTPHeaderFilterSpec) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone(),
    }
}

fn http_route_filter_snap(f: &crate::resources::HTTPRouteFilterSpec) -> HttpRouteFilterSnap {
    HttpRouteFilterSnap {
        filter_type: f.filter_type.clone(),
        request_redirect: f
            .request_redirect
            .as_ref()
            .map(|rr| HttpRequestRedirectFilterSnap {
                scheme: rr.scheme.clone(),
                hostname: rr.hostname.clone(),
                port: rr.port,
                status_code: rr.status_code,
                path: rr.path.as_ref().map(|pm| HttpPathModifierSnap {
                    modifier_type: pm.modifier_type.clone(),
                    replace_full_path: pm.replace_full_path.clone(),
                    replace_prefix_match: pm.replace_prefix_match.clone(),
                }),
            }),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
    }
}

fn grpc_route_filter_snap(f: &crate::resources::GRPCRouteFilterSpec) -> GrpcRouteFilterSnap {
    GrpcRouteFilterSnap {
        filter_type: f.filter_type.clone(),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
        response_header_modifier: f
            .response_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
    }
}

fn reference_grant_to_snap(rg: ReferenceGrant) -> ReferenceGrantSnap {
    ReferenceGrantSnap {
        namespace: rg.metadata.namespace.unwrap_or_default(),
        name: rg.metadata.name.unwrap_or_default(),
        from: rg
            .spec
            .from
            .iter()
            .map(|f| ReferenceGrantFromSnap {
                group: f.group.clone(),
                kind: f.kind.clone(),
                namespace: f.namespace.clone(),
            })
            .collect(),
        to: rg
            .spec
            .to
            .iter()
            .map(|t| ReferenceGrantToSnap {
                group: t.group.clone(),
                kind: t.kind.clone(),
                name: t.name.clone(),
            })
            .collect(),
    }
}

fn listenerset_to_snap(ls: ListenerSet) -> ListenerSetSnap {
    let ns = ls.metadata.namespace.unwrap_or_default();
    let name = ls.metadata.name.unwrap_or_default();
    let uid = ls.metadata.uid.unwrap_or_default();
    let creation_timestamp_rfc3339 = ls.metadata.creation_timestamp.map(|t| t.0.to_rfc3339());
    let parent_refs = ls.spec.parent_refs.iter().map(parent_to_snap).collect();
    let listeners = ls
        .spec
        .listeners
        .iter()
        .map(gateway_listener_to_snap)
        .collect();
    ListenerSetSnap {
        namespace: ns,
        name,
        uid,
        creation_timestamp_rfc3339,
        parent_refs,
        listeners,
    }
}

fn route_attaches_to_gateway_parent(
    p: &ParentReferenceSpec,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = p.namespace.as_deref().or(route_namespace).unwrap_or("");
    pns == gw_ns && p.name == gw_name
}

fn route_attaches_to_listener_set_parent(
    p: &ParentReferenceSpec,
    route_namespace: Option<&str>,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("ListenerSet");
    if g != "gateway.networking.k8s.io" || k != "ListenerSet" {
        return false;
    }
    let ls_ns = p
        .namespace
        .as_deref()
        .or(route_namespace)
        .unwrap_or_default()
        .to_string();
    attached_ls.contains(&(ls_ns, p.name.clone()))
}

fn route_relevant_to_gateway_refs(
    parent_refs: &[ParentReferenceSpec],
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    parent_refs.iter().any(|p| {
        route_attaches_to_gateway_parent(p, route_namespace, gw_ns, gw_name)
            || route_attaches_to_listener_set_parent(p, route_namespace, attached_ls)
    })
}

fn route_relevant_to_gateway(
    route: &HTTPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn grpc_route_relevant_to_gateway(
    route: &GRPCRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn tls_route_relevant_to_gateway(
    route: &TLSRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn tcp_route_relevant_to_gateway(
    route: &TCPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn udp_route_relevant_to_gateway(
    route: &UDPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn http_route_to_snap(route: &HTTPRoute) -> HttpRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<HttpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| HttpRouteRuleSnap {
            matches: r
                .matches
                .iter()
                .map(|m| HttpRequestMatchSnap {
                    path: m.path.as_ref().map(|p| HttpPathMatchSnap {
                        path_type: p.path_type.clone(),
                        value: p.value.clone(),
                    }),
                    headers: m
                        .headers
                        .iter()
                        .map(|h| HttpHeaderMatchSnap {
                            match_type: h.match_type.clone(),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            filters: r.filters.iter().map(http_route_filter_snap).collect(),
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    HttpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        hostnames: route.spec.hostnames.clone(),
        rules,
    }
}

fn grpc_route_to_snap(route: &GRPCRoute) -> GrpcRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<GrpcRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| GrpcRouteRuleSnap {
            matches: r
                .matches
                .iter()
                .map(|m| GrpcRouteMatchSnap {
                    method: m.method.as_ref().map(|mm| GrpcMethodMatchSnap {
                        match_type: mm.match_type.clone(),
                        service: mm.service.clone(),
                        method: mm.method.clone(),
                    }),
                    headers: m
                        .headers
                        .iter()
                        .map(|h| GrpcHeaderMatchSnap {
                            match_type: h.match_type.clone(),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            filters: r.filters.iter().map(grpc_route_filter_snap).collect(),
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    GrpcRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        hostnames: route.spec.hostnames.clone(),
        rules,
    }
}

fn tls_route_to_snap(route: &TLSRoute) -> TlsRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<TlsRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| TlsRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    TlsRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        hostnames: route.spec.hostnames.clone(),
        rules,
    }
}

fn tcp_route_to_snap(route: &TCPRoute) -> TcpRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<TcpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| TcpRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    TcpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        rules,
    }
}

fn udp_route_to_snap(route: &UDPRoute) -> UdpRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<UdpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| UdpRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    UdpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        rules,
    }
}

pub async fn load_cluster_snapshot(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
) -> Result<ClusterSnapshot, LoaderError> {
    let gw_api: Api<Gateway> = Api::namespaced(client.clone(), gateway_ns);
    let gw = gw_api.get(gateway_name).await?;

    let listeners: Vec<GatewayListenerSnap> = gw
        .spec
        .listeners
        .iter()
        .map(gateway_listener_to_snap)
        .collect();

    let allowed_listeners = allowed_snap_for_merge(&gw);

    let mut listener_sets = Vec::new();
    if let Some(ref allowed) = allowed_listeners {
        let ls_api: Api<ListenerSet> = Api::all(client.clone());
        match ls_api.list(&ListParams::default()).await {
            Ok(ls_list) => {
                for ls in ls_list.items {
                    if !listenerset_parents_gateway(&ls, gateway_ns, gateway_name) {
                        continue;
                    }
                    let ls_ns = ls.metadata.namespace.clone().unwrap_or_default();
                    if !listener_set_namespace_allowed(client, gateway_ns, &ls_ns, allowed).await? {
                        continue;
                    }
                    listener_sets.push(listenerset_to_snap(ls));
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "ListenerSet list failed — clusters without ListenerSet CRD skip merge"
                );
            }
        }
    }

    let attached_ls_keys: HashSet<(String, String)> = listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let mut effective_listeners = merge_effective_listeners(&listeners, &listener_sets);

    let route_api: Api<HTTPRoute> = Api::all(client.clone());
    let routes_list = route_api.list(&ListParams::default()).await?;
    let mut http_routes: Vec<HttpRouteSnap> = Vec::new();
    let mut grpc_routes: Vec<GrpcRouteSnap> = Vec::new();
    let mut tls_routes: Vec<TlsRouteSnap> = Vec::new();
    let mut tcp_routes: Vec<TcpRouteSnap> = Vec::new();
    let mut udp_routes: Vec<UdpRouteSnap> = Vec::new();
    let mut needed_services: Vec<(String, String)> = Vec::new();
    let mut needed_pools: Vec<(String, String)> = Vec::new();
    let mut ns_for_labels: HashSet<String> = HashSet::new();
    ns_for_labels.insert(gateway_ns.to_string());

    for item in routes_list.items {
        if !route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
            continue;
        }
        let rns = item.metadata.namespace.clone().unwrap_or_default();
        ns_for_labels.insert(rns.clone());

        let snap = http_route_to_snap(&item);
        for rule in &item.spec.rules {
            for (sns, sname) in crate::resources::collect_http_route_rule_service_refs(&rns, rule) {
                ns_for_labels.insert(sns.clone());
                needed_services.push((sns, sname));
            }
            for (pns, pname) in
                crate::resources::collect_http_route_rule_inference_pool_refs(&rns, rule)
            {
                ns_for_labels.insert(pns.clone());
                needed_pools.push((pns, pname));
            }
        }
        http_routes.push(snap);
    }

    let grpc_api: Api<GRPCRoute> = Api::all(client.clone());
    match grpc_api.list(&ListParams::default()).await {
        Ok(grpc_list) => {
            for item in grpc_list.items {
                if !grpc_route_relevant_to_gateway(
                    &item,
                    gateway_ns,
                    gateway_name,
                    &attached_ls_keys,
                ) {
                    continue;
                }
                let rns = item.metadata.namespace.clone().unwrap_or_default();
                ns_for_labels.insert(rns.clone());

                let snap = grpc_route_to_snap(&item);
                for rule in &item.spec.rules {
                    for (sns, sname) in
                        crate::resources::collect_grpc_route_rule_service_refs(&rns, rule)
                    {
                        ns_for_labels.insert(sns.clone());
                        needed_services.push((sns, sname));
                    }
                    for (pns, pname) in
                        crate::resources::collect_grpc_route_rule_inference_pool_refs(&rns, rule)
                    {
                        ns_for_labels.insert(pns.clone());
                        needed_pools.push((pns, pname));
                    }
                }
                grpc_routes.push(snap);
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "GRPCRoute list failed — clusters without GRPCRoute CRD skip gRPC routes"
            );
        }
    }

    let tls_api: Api<TLSRoute> = Api::all(client.clone());
    match tls_api.list(&ListParams::default()).await {
        Ok(tls_list) => {
            for item in tls_list.items {
                if !tls_route_relevant_to_gateway(
                    &item,
                    gateway_ns,
                    gateway_name,
                    &attached_ls_keys,
                ) {
                    continue;
                }
                let rns = item.metadata.namespace.clone().unwrap_or_default();
                ns_for_labels.insert(rns.clone());

                let snap = tls_route_to_snap(&item);
                for rule in &item.spec.rules {
                    for (sns, sname) in crate::resources::collect_backend_refs_service_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(sns.clone());
                        needed_services.push((sns, sname));
                    }
                    for (pns, pname) in crate::resources::collect_backend_refs_inference_pool_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(pns.clone());
                        needed_pools.push((pns, pname));
                    }
                }
                tls_routes.push(snap);
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "TLSRoute list failed — clusters without TLSRoute CRD skip TLS routes"
            );
        }
    }

    let tcp_api: Api<TCPRoute> = Api::all(client.clone());
    match tcp_api.list(&ListParams::default()).await {
        Ok(tcp_list) => {
            for item in tcp_list.items {
                if !tcp_route_relevant_to_gateway(
                    &item,
                    gateway_ns,
                    gateway_name,
                    &attached_ls_keys,
                ) {
                    continue;
                }
                let rns = item.metadata.namespace.clone().unwrap_or_default();
                ns_for_labels.insert(rns.clone());

                let snap = tcp_route_to_snap(&item);
                for rule in &item.spec.rules {
                    for (sns, sname) in crate::resources::collect_backend_refs_service_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(sns.clone());
                        needed_services.push((sns, sname));
                    }
                    for (pns, pname) in crate::resources::collect_backend_refs_inference_pool_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(pns.clone());
                        needed_pools.push((pns, pname));
                    }
                }
                tcp_routes.push(snap);
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "TCPRoute list failed — clusters without TCPRoute CRD skip TCP routes"
            );
        }
    }

    let udp_api: Api<UDPRoute> = Api::all(client.clone());
    match udp_api.list(&ListParams::default()).await {
        Ok(udp_list) => {
            for item in udp_list.items {
                if !udp_route_relevant_to_gateway(
                    &item,
                    gateway_ns,
                    gateway_name,
                    &attached_ls_keys,
                ) {
                    continue;
                }
                let rns = item.metadata.namespace.clone().unwrap_or_default();
                ns_for_labels.insert(rns.clone());

                let snap = udp_route_to_snap(&item);
                for rule in &item.spec.rules {
                    for (sns, sname) in crate::resources::collect_backend_refs_service_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(sns.clone());
                        needed_services.push((sns, sname));
                    }
                    for (pns, pname) in crate::resources::collect_backend_refs_inference_pool_refs(
                        &rns,
                        &rule.backend_refs,
                    ) {
                        ns_for_labels.insert(pns.clone());
                        needed_pools.push((pns, pname));
                    }
                }
                udp_routes.push(snap);
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "UDPRoute list failed — clusters without UDPRoute CRD skip UDP routes"
            );
        }
    }

    needed_pools.sort();
    needed_pools.dedup();

    let mut inference_pools: Vec<InferencePoolSnap> = Vec::new();
    for (pns, pname) in &needed_pools {
        let api = Api::<InferencePool>::namespaced(client.clone(), pns);
        match api.get(pname).await {
            Ok(ip) => {
                let picker = &ip.spec.endpoint_picker_ref;
                let pk = picker.kind.as_deref().unwrap_or("Service");
                if pk == "Service" && !picker.name.is_empty() {
                    needed_services.push((pns.clone(), picker.name.clone()));
                    ns_for_labels.insert(pns.clone());
                }
                inference_pools.push(inference_pool_to_snap(ip));
            }
            Err(e) => {
                warn!(
                    error = %e,
                    pool_ns = %pns,
                    pool_name = %pname,
                    "InferencePool get failed — missing pool or CRD not installed"
                );
            }
        }
    }

    needed_services.sort();
    needed_services.dedup();

    let mut services = Vec::new();
    for (sns, sname) in &needed_services {
        if let Ok(svc) = Api::<Service>::namespaced(client.clone(), sns)
            .get(sname)
            .await
        {
            let ports: Vec<ServicePortSnap> = svc
                .spec
                .as_ref()
                .map(|spec| {
                    spec.ports
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|p| ServicePortSnap {
                            name: p.name.unwrap_or_default(),
                            port: p.port as u16,
                            target_port: p.target_port.and_then(|tp| match tp {
                                IntOrString::Int(i) => Some(i as u16),
                                IntOrString::String(_) => None,
                            }),
                        })
                        .collect()
                })
                .unwrap_or_default();
            services.push(ServiceSnap {
                namespace: svc.metadata.namespace.unwrap_or_default(),
                name: svc.metadata.name.unwrap_or_default(),
                ports,
            });
        }
    }

    let mut endpoint_slices = Vec::new();
    let es_api: Api<EndpointSlice> = Api::all(client.clone());
    let all_slices = es_api.list(&ListParams::default()).await?;
    for sl in all_slices.items {
        let slice_ns = sl.metadata.namespace.clone().unwrap_or_default();
        let svc_name = sl
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name").cloned());
        let Some(ref sn) = svc_name else {
            continue;
        };
        if !needed_services
            .iter()
            .any(|(sns, n)| n == sn && sns == &slice_ns)
        {
            continue;
        }
        let mut endpoints = Vec::new();
        for ep in sl.endpoints {
            let mut addrs = Vec::new();
            addrs.extend(ep.addresses);
            let mut ports = Vec::new();
            if let Some(ports_slice) = sl.ports.clone() {
                for p in ports_slice {
                    if let Some(port) = p.port {
                        ports.push(EndpointPortSnap {
                            name: p.name.clone(),
                            port: port as u16,
                        });
                    }
                }
            }
            if !addrs.is_empty() {
                endpoints.push(EndpointSnap {
                    addresses: addrs,
                    ports,
                });
            }
        }
        endpoint_slices.push(EndpointSliceSnap {
            namespace: sl.metadata.namespace.unwrap_or_default(),
            name: sl.metadata.name.unwrap_or_default(),
            service_name: svc_name,
            endpoints,
        });
    }

    let mut ns_label_map: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for ns in &ns_for_labels {
        if let Ok(lbl) = namespace_labels(client, ns).await {
            ns_label_map.insert(ns.clone(), lbl);
        }
    }

    let mut reference_grants: Vec<ReferenceGrantSnap> = Vec::new();
    let rg_api: Api<ReferenceGrant> = Api::all(client.clone());
    match rg_api.list(&ListParams::default()).await {
        Ok(list) => {
            reference_grants = list
                .items
                .into_iter()
                .map(reference_grant_to_snap)
                .collect();
        }
        Err(e) => {
            warn!(
                error = %e,
                "ReferenceGrant list failed — clusters without CRD skip cross-namespace grants"
            );
        }
    }

    resolve_gateway_listener_tls(
        client,
        gateway_ns,
        &reference_grants,
        &mut effective_listeners,
    )
    .await?;

    let gateway_class = if gw.spec.gateway_class_name.is_empty() {
        None
    } else {
        let gc_api: Api<GatewayClass> = Api::all(client.clone());
        gc_api
            .get(&gw.spec.gateway_class_name)
            .await
            .ok()
            .map(|gc| GatewayClassSnap {
                name: gc.metadata.name.unwrap_or_default(),
                controller_name: gc.spec.controller_name.clone(),
            })
    };

    let (backend_tls_effective, backend_tls_policy_outcomes) =
        backend_tls::load_backend_tls_for_gateway(
            client,
            &needed_services,
            &services,
            &needed_pools,
            &inference_pools,
            &reference_grants,
        )
        .await?;

    Ok(ClusterSnapshot {
        gateway_namespace: gateway_ns.to_string(),
        gateway_name: gateway_name.to_string(),
        gateway_class_name: gw.spec.gateway_class_name.clone(),
        gateway_generation: gw.metadata.generation.filter(|&g| g > 0).unwrap_or(1),
        gateway_addresses: gw
            .spec
            .addresses
            .iter()
            .map(|a| GatewayAddressSnap {
                type_: a.type_.clone(),
                value: a.value.clone(),
            })
            .collect(),
        gateway_infrastructure: gw.spec.infrastructure.as_ref().map(|inf| {
            GatewayInfrastructureSnap {
                labels: inf.labels.clone(),
                annotations: inf.annotations.clone(),
            }
        }),
        listeners,
        effective_listeners,
        listener_sets,
        allowed_listeners,
        http_routes,
        grpc_routes,
        tls_routes,
        tcp_routes,
        udp_routes,
        services,
        endpoint_slices,
        gateway_class,
        reference_grants,
        backend_tls_effective,
        backend_tls_policy_outcomes,
        namespace_labels: ns_label_map,
        inference_pools,
    })
}

#[cfg(test)]
mod backend_tls_loader_tests {
    use super::backend_tls::backend_tls_section_applies;

    #[test]
    fn section_none_applies_to_all_named_service_ports() {
        assert!(backend_tls_section_applies(None, "http"));
        assert!(backend_tls_section_applies(None, "https"));
    }

    #[test]
    fn section_name_matches_only_that_service_port_name() {
        assert!(backend_tls_section_applies(Some("https"), "https"));
        assert!(!backend_tls_section_applies(Some("https"), "http"));
    }

    #[test]
    fn backend_tls_winner_sort_prefers_older_creation_timestamp() {
        let mut candidates = vec![
            ("2024-06-02T10:00:00Z", "newer"),
            ("2024-06-01T10:00:00Z", "older"),
        ];
        candidates.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(candidates[0].1, "older");
    }

    #[test]
    fn backend_tls_winner_sort_tie_breaker_lexicographic_policy_identity() {
        let mut candidates = vec![
            ("2024-06-01T10:00:00Z", "ns-a", "policy-z"),
            ("2024-06-01T10:00:00Z", "ns-a", "policy-a"),
        ];
        candidates.sort_by(|a, b| a.0.cmp(b.0).then_with(|| (a.1, a.2).cmp(&(b.1, b.2))));
        assert_eq!(candidates[0].2, "policy-a");
    }
}
