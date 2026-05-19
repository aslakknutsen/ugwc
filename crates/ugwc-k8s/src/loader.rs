//! Load [`ugwc_core::ClusterSnapshot`] for a reconciled `Gateway`.

use std::collections::{BTreeMap, HashSet};

use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use thiserror::Error;
use tracing::warn;
use ugwc_core::listener_merge::merge_effective_listeners;
use ugwc_core::snapshot::{
    AllowedListenersSnap, AllowedRoutesNamespacesSnap, AllowedRoutesSnap, ClusterSnapshot,
    EffectiveListenerSnap, EndpointPickerRefSnap, EndpointPortSnap, EndpointSliceSnap,
    EndpointSnap, GatewayAddressSnap, GatewayClassSnap, GatewayInfrastructureSnap,
    GatewayListenerSnap, GatewayListenerTlsSnap, GrpcHeaderMatchSnap, GrpcMethodMatchSnap,
    GrpcRouteFilterSnap, GrpcRouteMatchSnap, GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap,
    HttpCorsFilterSnap, HttpHeaderFilterSnap, HttpHeaderMatchSnap, HttpHeaderNameValueSnap,
    HttpPathMatchSnap, HttpPathModifierSnap, HttpQueryParamMatchSnap, HttpRequestMatchSnap,
    HttpRequestRedirectFilterSnap, HttpRouteFilterSnap, HttpRouteRuleSnap, HttpRouteSnap,
    HttpRouteTimeoutsSnap, HttpUrlRewriteFilterSnap, InferencePoolPortSnap,
    InferencePoolSelectorSnap, InferencePoolSnap, ListenerNamespacesSnap, ListenerParentSnap,
    ListenerSetSnap, ParentRefSnap, RejectedListenerSetSnap, ReferenceGrantFromSnap,
    ReferenceGrantSnap, ReferenceGrantToSnap, RouteGroupKindSnap, SecretObjectReferenceSnap,
    ServicePortSnap, ServiceSnap, TcpRouteRuleSnap, TcpRouteSnap, TlsRouteRuleSnap, TlsRouteSnap,
    UdpRouteRuleSnap, UdpRouteSnap,
};

use crate::crd_supervisor::{ActiveCrds, ManagedCrd};
use crate::resources::{
    Gateway, GatewayAllowedListenersNamespacesFrom, GatewayClass, GatewayListeners,
    GatewayListenersAllowedRoutes, GatewayListenersAllowedRoutesKinds,
    GatewayListenersAllowedRoutesNamespaces, GatewayListenersTls,
    GatewayListenersTlsCertificateRefs, GatewayListenersTlsMode, GRPCRoute, GrpcRouteParentRefs,
    GrpcRouteRules, GrpcRouteRulesBackendRefs, GrpcRouteRulesFilters,
    GrpcRouteRulesFiltersRequestHeaderModifier, GrpcRouteRulesFiltersResponseHeaderModifier,
    GrpcRouteRulesFiltersType, GrpcRouteRulesMatches, GrpcRouteRulesMatchesHeaders,
    GrpcRouteRulesMatchesHeadersType, GrpcRouteRulesMatchesMethodType, HTTPRoute,
    HttpRouteParentRefs, HttpRouteRules, HttpRouteRulesBackendRefs, HttpRouteRulesFilters,
    HttpRouteRulesFiltersRequestHeaderModifier, HttpRouteRulesFiltersRequestRedirectPath,
    HttpRouteRulesFiltersRequestRedirectPathType, HttpRouteRulesFiltersRequestRedirectScheme,
    HttpRouteRulesFiltersResponseHeaderModifier, HttpRouteRulesFiltersType,
    HttpRouteRulesFiltersUrlRewritePath,
    HttpRouteRulesFiltersUrlRewritePathType,
    HttpRouteRulesMatches, HttpRouteRulesMatchesHeaders, HttpRouteRulesMatchesHeadersType,
    HttpRouteRulesMatchesMethod, HttpRouteRulesMatchesPathType, HttpRouteRulesMatchesQueryParams,
    HttpRouteRulesTimeouts,
    InferencePool, ListenerSet, ListenerSetListeners,
    ListenerSetListenersAllowedRoutes, ListenerSetListenersAllowedRoutesKinds,
    ListenerSetListenersAllowedRoutesNamespaces, ListenerSetListenersTls,
    ListenerSetListenersTlsCertificateRefs, ListenerSetListenersTlsMode, ListenerSetParentRef,
    ReferenceGrant, ReferenceGrantFrom, ReferenceGrantTo, TCPRoute, TcpRouteParentRefs,
    TcpRouteRules, TcpRouteRulesBackendRefs, TLSRoute, TlsRouteParentRefs, TlsRouteRules,
    TlsRouteRulesBackendRefs, UDPRoute, UdpRouteParentRefs, UdpRouteRules,
    UdpRouteRulesBackendRefs,
};

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
}

#[path = "backend_tls.rs"]
pub(crate) mod backend_tls;

fn snap_u16_port(port: Option<i32>) -> Option<u16> {
    port.and_then(|n| u16::try_from(n).ok())
}

fn snap_nonnegative_weight(w: Option<i32>) -> Option<u32> {
    match w {
        Some(x) if x >= 0 => Some(x as u32),
        _ => None,
    }
}

#[must_use]
fn parent_ref_snap_from_route_fields(
    group: Option<String>,
    kind: Option<String>,
    namespace: Option<String>,
    name: String,
    section_name: Option<String>,
    port: Option<i32>,
) -> ParentRefSnap {
    ParentRefSnap {
        group,
        kind,
        namespace,
        name,
        section_name,
        port: snap_u16_port(port),
    }
}

/// Convert spec parentRef to snapshot form (HTTPRoute status merge).
#[must_use]
pub fn parent_reference_to_snap(p: &HttpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

fn tls_parent_ref_to_snap(p: &TlsRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

fn grpc_parent_ref_to_snap(p: &GrpcRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

fn tcp_parent_ref_to_snap(p: &TcpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

fn udp_parent_ref_to_snap(p: &UdpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

#[must_use]
fn listenerset_parent_ref_to_snap(p: &ListenerSetParentRef) -> ParentRefSnap {
    ParentRefSnap {
        group: p.group.clone(),
        kind: p.kind.clone(),
        namespace: p.namespace.clone(),
        name: p.name.clone(),
        section_name: None,
        port: None,
    }
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
    let from = ns
        .from
        .as_ref()
        .cloned()
        .unwrap_or(GatewayAllowedListenersNamespacesFrom::None);
    if matches!(from, GatewayAllowedListenersNamespacesFrom::None) {
        return None;
    }
    Some(AllowedListenersSnap {
        namespaces: Some(ListenerNamespacesSnap {
            from: Some(format!("{from:?}")),
            selector_match_labels: ns
                .selector
                .as_ref()
                .and_then(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
    })
}

fn listenerset_parents_gateway(ls: &ListenerSet, gw_ns: &str, gw_name: &str) -> bool {
    let p = &ls.spec.parent_ref;
    let ls_ns_default = ls.metadata.namespace.as_deref().unwrap_or("");
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = p.namespace.as_deref().unwrap_or(ls_ns_default);
    pns == gw_ns && p.name == gw_name
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

fn gateway_allowed_routes_namespaces_snap(
    n: &GatewayListenersAllowedRoutesNamespaces,
) -> AllowedRoutesNamespacesSnap {
    AllowedRoutesNamespacesSnap {
        from: n.from.as_ref().map(|f| format!("{f:?}")),
        selector_match_labels: n
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.clone())
            .unwrap_or_default(),
    }
}

fn gateway_allowed_routes_snap(ar: &GatewayListenersAllowedRoutes) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar.namespaces.as_ref().map(gateway_allowed_routes_namespaces_snap),
        kinds: ar
            .kinds
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|k: &GatewayListenersAllowedRoutesKinds| RouteGroupKindSnap {
                group: k.group.clone(),
                kind: k.kind.clone(),
            })
            .collect(),
    }
}

fn listenerset_allowed_routes_namespaces_snap(
    n: &ListenerSetListenersAllowedRoutesNamespaces,
) -> AllowedRoutesNamespacesSnap {
    AllowedRoutesNamespacesSnap {
        from: n.from.as_ref().map(|f| format!("{f:?}")),
        selector_match_labels: n
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.clone())
            .unwrap_or_default(),
    }
}

fn listenerset_allowed_routes_snap(ar: &ListenerSetListenersAllowedRoutes) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar
            .namespaces
            .as_ref()
            .map(listenerset_allowed_routes_namespaces_snap),
        kinds: ar
            .kinds
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|k: &ListenerSetListenersAllowedRoutesKinds| RouteGroupKindSnap {
                group: k.group.clone(),
                kind: k.kind.clone(),
            })
            .collect(),
    }
}

fn gateway_listener_to_snap(l: &GatewayListeners) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: snap_u16_port(Some(l.port)).unwrap_or(0),
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l.allowed_routes.as_ref().map(gateway_allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_gateway_to_snap),
    }
}

fn listenerset_listener_to_snap(l: &ListenerSetListeners) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: snap_u16_port(Some(l.port)).unwrap_or(0),
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l.allowed_routes.as_ref().map(listenerset_allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_listenerset_to_snap),
    }
}

fn tls_mode_gateway_str(mode: Option<GatewayListenersTlsMode>) -> Option<String> {
    mode.map(|m| match m {
        GatewayListenersTlsMode::Terminate => "Terminate".to_string(),
        GatewayListenersTlsMode::Passthrough => "Passthrough".to_string(),
    })
}

fn tls_mode_listenerset_str(mode: Option<ListenerSetListenersTlsMode>) -> Option<String> {
    mode.map(|m| match m {
        ListenerSetListenersTlsMode::Terminate => "Terminate".to_string(),
        ListenerSetListenersTlsMode::Passthrough => "Passthrough".to_string(),
    })
}

fn snap_from_gateway_cert_ref(r: &GatewayListenersTlsCertificateRefs) -> SecretObjectReferenceSnap {
    SecretObjectReferenceSnap {
        group: r.group.clone(),
        kind: r.kind.clone(),
        name: r.name.clone(),
        namespace: r.namespace.clone(),
    }
}

fn snap_from_listenerset_cert_ref(
    r: &ListenerSetListenersTlsCertificateRefs,
) -> SecretObjectReferenceSnap {
    SecretObjectReferenceSnap {
        group: r.group.clone(),
        kind: r.kind.clone(),
        name: r.name.clone(),
        namespace: r.namespace.clone(),
    }
}

fn listener_tls_gateway_to_snap(t: &GatewayListenersTls) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: tls_mode_gateway_str(t.mode.clone()),
        certificate_refs: t
            .certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(snap_from_gateway_cert_ref)
            .collect(),
    }
}

fn listener_tls_listenerset_to_snap(t: &ListenerSetListenersTls) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: tls_mode_listenerset_str(t.mode.clone()),
        certificate_refs: t
            .certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(snap_from_listenerset_cert_ref)
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
        let mut bad_ref: Option<String> = None;
        for r in refs.iter() {
            let kind = r.kind.as_deref().unwrap_or("Secret");
            let group = r.group.as_deref().unwrap_or("");
            let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
            if !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok {
                bad_ref = Some(format!(
                    "InvalidCertificateRef: unsupported certificate ref {group}/{kind}"
                ));
                break;
            }
        }
        if let Some(err) = bad_ref {
            e.tls_resolve_error = Some(err);
            continue;
        }
        let (from_ns, from_kind) = match &e.parent {
            ListenerParentSnap::InlineGateway => (gateway_ns.to_string(), "Gateway"),
            ListenerParentSnap::ListenerSet { namespace, .. } => {
                (namespace.clone(), "ListenerSet")
            }
        };
        let mut grant_blocked = false;
        for r in refs.iter() {
            let secret_ns = r.namespace.as_deref().unwrap_or(&from_ns);
            if !reference_grant_permits_listener_secret_ref(
                grants, &from_ns, from_kind, secret_ns, &r.name,
            ) {
                grant_blocked = true;
                break;
            }
        }
        if grant_blocked {
            e.tls_resolve_error = Some("RefNotPermitted".into());
            continue;
        }
        let r = &refs[0];
        let secret_ns = r.namespace.as_deref().unwrap_or(&from_ns);
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

/// Check if a ReferenceGrant allows a listener (Gateway or ListenerSet) to
/// reference a Secret in another namespace.
fn reference_grant_permits_listener_secret_ref(
    grants: &[ReferenceGrantSnap],
    from_namespace: &str,
    from_kind: &str,
    secret_namespace: &str,
    secret_name: &str,
) -> bool {
    if from_namespace == secret_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";

    for g in grants {
        if g.namespace != secret_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == from_kind && fr.namespace == from_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "Secret" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !tg.is_empty() {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == secret_name => return true,
                _ => {}
            }
        }
    }
    false
}

fn http_header_filter_snap(h: &HttpRouteRulesFiltersRequestHeaderModifier) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone().unwrap_or_default(),
    }
}

fn grpc_req_header_filter_snap(h: &GrpcRouteRulesFiltersRequestHeaderModifier) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone().unwrap_or_default(),
    }
}

fn grpc_resp_header_filter_snap(
    h: &GrpcRouteRulesFiltersResponseHeaderModifier,
) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone().unwrap_or_default(),
    }
}

fn http_route_filters_type_str(ty: &HttpRouteRulesFiltersType) -> &'static str {
    match ty {
        HttpRouteRulesFiltersType::RequestHeaderModifier => "RequestHeaderModifier",
        HttpRouteRulesFiltersType::ResponseHeaderModifier => "ResponseHeaderModifier",
        HttpRouteRulesFiltersType::RequestMirror => "RequestMirror",
        HttpRouteRulesFiltersType::RequestRedirect => "RequestRedirect",
        HttpRouteRulesFiltersType::UrlRewrite => "URLRewrite",
        HttpRouteRulesFiltersType::ExtensionRef => "ExtensionRef",
        HttpRouteRulesFiltersType::Cors => "CORS",
    }
}

fn redirect_scheme_snap(sch: Option<&HttpRouteRulesFiltersRequestRedirectScheme>) -> Option<String> {
    sch.map(|s| match s {
        HttpRouteRulesFiltersRequestRedirectScheme::Http => "http".to_string(),
        HttpRouteRulesFiltersRequestRedirectScheme::Https => "https".to_string(),
    })
}

fn http_path_modifier_snap(p: &HttpRouteRulesFiltersRequestRedirectPath) -> HttpPathModifierSnap {
    let modifier_type = match p.r#type {
        HttpRouteRulesFiltersRequestRedirectPathType::ReplaceFullPath => "ReplaceFullPath",
        HttpRouteRulesFiltersRequestRedirectPathType::ReplacePrefixMatch => "ReplacePrefixMatch",
    };
    HttpPathModifierSnap {
        modifier_type: modifier_type.to_string(),
        replace_full_path: p.replace_full_path.clone(),
        replace_prefix_match: p.replace_prefix_match.clone(),
    }
}

fn url_rewrite_path_modifier_snap(p: &HttpRouteRulesFiltersUrlRewritePath) -> HttpPathModifierSnap {
    let modifier_type = match p.r#type {
        HttpRouteRulesFiltersUrlRewritePathType::ReplaceFullPath => "ReplaceFullPath",
        HttpRouteRulesFiltersUrlRewritePathType::ReplacePrefixMatch => "ReplacePrefixMatch",
    };
    HttpPathModifierSnap {
        modifier_type: modifier_type.to_string(),
        replace_full_path: p.replace_full_path.clone(),
        replace_prefix_match: p.replace_prefix_match.clone(),
    }
}

fn resp_header_filter_snap(h: &HttpRouteRulesFiltersResponseHeaderModifier) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.as_deref().unwrap_or_default().to_vec(),
    }
}

fn http_method_str(m: &HttpRouteRulesMatchesMethod) -> String {
    match m {
        HttpRouteRulesMatchesMethod::Get => "GET",
        HttpRouteRulesMatchesMethod::Head => "HEAD",
        HttpRouteRulesMatchesMethod::Post => "POST",
        HttpRouteRulesMatchesMethod::Put => "PUT",
        HttpRouteRulesMatchesMethod::Delete => "DELETE",
        HttpRouteRulesMatchesMethod::Connect => "CONNECT",
        HttpRouteRulesMatchesMethod::Options => "OPTIONS",
        HttpRouteRulesMatchesMethod::Trace => "TRACE",
        HttpRouteRulesMatchesMethod::Patch => "PATCH",
    }
    .to_string()
}

fn http_route_filter_snap(f: &HttpRouteRulesFilters) -> HttpRouteFilterSnap {
    HttpRouteFilterSnap {
        filter_type: http_route_filters_type_str(&f.r#type).to_string(),
        request_redirect: f.request_redirect.as_ref().map(|rr| HttpRequestRedirectFilterSnap {
            scheme: redirect_scheme_snap(rr.scheme.as_ref()),
            hostname: rr.hostname.clone(),
            port: snap_u16_port(rr.port),
            status_code: rr
                .status_code
                .and_then(|c| i32::try_from(c).ok()),
            path: rr.path.as_ref().map(http_path_modifier_snap),
        }),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
        response_header_modifier: f
            .response_header_modifier
            .as_ref()
            .map(resp_header_filter_snap),
        url_rewrite: f.url_rewrite.as_ref().map(|uw| HttpUrlRewriteFilterSnap {
            hostname: uw.hostname.clone(),
            path: uw.path.as_ref().map(url_rewrite_path_modifier_snap),
        }),
        cors: f.cors.as_ref().map(|c| HttpCorsFilterSnap {
            allow_origins: c.allow_origins.clone().unwrap_or_default(),
            allow_methods: c.allow_methods.clone().unwrap_or_default(),
            allow_headers: c.allow_headers.clone().unwrap_or_default(),
            expose_headers: c.expose_headers.clone().unwrap_or_default(),
            max_age: c.max_age,
            allow_credentials: c.allow_credentials.unwrap_or(false),
        }),
    }
}

fn grpc_route_filters_type_str(ty: &GrpcRouteRulesFiltersType) -> &'static str {
    match ty {
        GrpcRouteRulesFiltersType::ResponseHeaderModifier => "ResponseHeaderModifier",
        GrpcRouteRulesFiltersType::RequestHeaderModifier => "RequestHeaderModifier",
        GrpcRouteRulesFiltersType::RequestMirror => "RequestMirror",
        GrpcRouteRulesFiltersType::ExtensionRef => "ExtensionRef",
    }
}

fn grpc_route_filter_snap(f: &GrpcRouteRulesFilters) -> GrpcRouteFilterSnap {
    GrpcRouteFilterSnap {
        filter_type: grpc_route_filters_type_str(&f.r#type).to_string(),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(grpc_req_header_filter_snap),
        response_header_modifier: f
            .response_header_modifier
            .as_ref()
            .map(grpc_resp_header_filter_snap),
    }
}

fn reference_grant_from_to_snap(f: ReferenceGrantFrom) -> ReferenceGrantFromSnap {
    ReferenceGrantFromSnap {
        group: if f.group.is_empty() {
            None
        } else {
            Some(f.group)
        },
        kind: f.kind,
        namespace: f.namespace,
    }
}

fn reference_grant_to_snap_one(t: ReferenceGrantTo) -> ReferenceGrantToSnap {
    ReferenceGrantToSnap {
        group: if t.group.is_empty() {
            None
        } else {
            Some(t.group)
        },
        kind: t.kind,
        name: t.name,
    }
}

fn reference_grant_to_snap(rg: ReferenceGrant) -> ReferenceGrantSnap {
    ReferenceGrantSnap {
        namespace: rg.metadata.namespace.unwrap_or_default(),
        name: rg.metadata.name.unwrap_or_default(),
        from: rg
            .spec
            .from
            .into_iter()
            .map(reference_grant_from_to_snap)
            .collect(),
        to: rg
            .spec
            .to
            .into_iter()
            .map(reference_grant_to_snap_one)
            .collect(),
    }
}

fn listenerset_to_snap(ls: ListenerSet) -> ListenerSetSnap {
    let ns = ls.metadata.namespace.unwrap_or_default();
    let name = ls.metadata.name.unwrap_or_default();
    let uid = ls.metadata.uid.unwrap_or_default();
    let creation_timestamp_rfc3339 = ls.metadata.creation_timestamp.map(|t| t.0.to_string());
    let parent_refs = vec![listenerset_parent_ref_to_snap(&ls.spec.parent_ref)];
    let mut seen_names = HashSet::new();
    let listeners = ls
        .spec
        .listeners
        .iter()
        .filter(|l| seen_names.insert(l.name.clone()))
        .map(listenerset_listener_to_snap)
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

fn route_attaches_to_gateway_parent_fields(
    group: Option<&str>,
    kind: Option<&str>,
    namespace: Option<&str>,
    name: &str,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    let g = group.unwrap_or("gateway.networking.k8s.io");
    let k = kind.unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = namespace.or(route_namespace).unwrap_or("");
    pns == gw_ns && name == gw_name
}

fn route_attaches_to_listener_set_parent_fields(
    group: Option<&str>,
    kind: Option<&str>,
    namespace: Option<&str>,
    name: &str,
    route_namespace: Option<&str>,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let g = group.unwrap_or("gateway.networking.k8s.io");
    let k = kind.unwrap_or("ListenerSet");
    if g != "gateway.networking.k8s.io" || k != "ListenerSet" {
        return false;
    }
    let ls_ns = namespace.or(route_namespace).unwrap_or_default().to_string();
    attached_ls.contains(&(ls_ns, name.to_string()))
}

fn route_parent_relevant_http(
    p: &HttpRouteParentRefs,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_attaches_to_gateway_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        gw_ns,
        gw_name,
    ) || route_attaches_to_listener_set_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        attached_ls,
    )
}

fn route_parent_relevant_tls(
    p: &TlsRouteParentRefs,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_attaches_to_gateway_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        gw_ns,
        gw_name,
    ) || route_attaches_to_listener_set_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        attached_ls,
    )
}

fn route_parent_relevant_grpc(
    p: &GrpcRouteParentRefs,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_attaches_to_gateway_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        gw_ns,
        gw_name,
    ) || route_attaches_to_listener_set_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        attached_ls,
    )
}

fn route_parent_relevant_tcp(
    p: &TcpRouteParentRefs,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_attaches_to_gateway_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        gw_ns,
        gw_name,
    ) || route_attaches_to_listener_set_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        attached_ls,
    )
}

fn route_parent_relevant_udp(
    p: &UdpRouteParentRefs,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_attaches_to_gateway_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        gw_ns,
        gw_name,
    ) || route_attaches_to_listener_set_parent_fields(
        p.group.as_deref(),
        p.kind.as_deref(),
        p.namespace.as_deref(),
        &p.name,
        route_namespace,
        attached_ls,
    )
}

fn route_relevant_to_gateway(
    route: &HTTPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route
        .spec
        .parent_refs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|p| {
            route_parent_relevant_http(
                p,
                route.metadata.namespace.as_deref(),
                gw_ns,
                gw_name,
                attached_ls,
            )
        })
}

fn grpc_route_relevant_to_gateway(
    route: &GRPCRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route
        .spec
        .parent_refs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|p| {
            route_parent_relevant_grpc(
                p,
                route.metadata.namespace.as_deref(),
                gw_ns,
                gw_name,
                attached_ls,
            )
        })
}

fn tls_route_relevant_to_gateway(
    route: &TLSRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route
        .spec
        .parent_refs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|p| {
            route_parent_relevant_tls(
                p,
                route.metadata.namespace.as_deref(),
                gw_ns,
                gw_name,
                attached_ls,
            )
        })
}

fn tcp_route_relevant_to_gateway(
    route: &TCPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route
        .spec
        .parent_refs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|p| {
            route_parent_relevant_tcp(
                p,
                route.metadata.namespace.as_deref(),
                gw_ns,
                gw_name,
                attached_ls,
            )
        })
}

fn udp_route_relevant_to_gateway(
    route: &UDPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route
        .spec
        .parent_refs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|p| {
            route_parent_relevant_udp(
                p,
                route.metadata.namespace.as_deref(),
                gw_ns,
                gw_name,
                attached_ls,
            )
        })
}

fn http_path_match_type_str(ty: Option<&HttpRouteRulesMatchesPathType>) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

fn http_header_match_type_str(ty: Option<&HttpRouteRulesMatchesHeadersType>) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

fn grpc_header_match_type_str(ty: Option<&GrpcRouteRulesMatchesHeadersType>) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

fn grpc_method_match_type_str(ty: Option<&GrpcRouteRulesMatchesMethodType>) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

fn http_backend_ref_to_snap(b: &HttpRouteRulesBackendRefs) -> HttpBackendRefSnap {
    HttpBackendRefSnap {
        group: b.group.clone(),
        kind: b.kind.clone(),
        namespace: b.namespace.clone(),
        name: b.name.clone(),
        port: snap_u16_port(b.port),
        port_name: None,
        weight: snap_nonnegative_weight(b.weight),
    }
}

fn grpc_backend_ref_to_snap(b: &GrpcRouteRulesBackendRefs) -> HttpBackendRefSnap {
    HttpBackendRefSnap {
        group: b.group.clone(),
        kind: b.kind.clone(),
        namespace: b.namespace.clone(),
        name: b.name.clone(),
        port: snap_u16_port(b.port),
        port_name: None,
        weight: snap_nonnegative_weight(b.weight),
    }
}

fn tls_backend_ref_to_snap(b: &TlsRouteRulesBackendRefs) -> HttpBackendRefSnap {
    HttpBackendRefSnap {
        group: b.group.clone(),
        kind: b.kind.clone(),
        namespace: b.namespace.clone(),
        name: b.name.clone(),
        port: snap_u16_port(b.port),
        port_name: None,
        weight: snap_nonnegative_weight(b.weight),
    }
}

fn tcp_backend_ref_to_snap(b: &TcpRouteRulesBackendRefs) -> HttpBackendRefSnap {
    HttpBackendRefSnap {
        group: b.group.clone(),
        kind: b.kind.clone(),
        namespace: b.namespace.clone(),
        name: b.name.clone(),
        port: snap_u16_port(b.port),
        port_name: None,
        weight: snap_nonnegative_weight(b.weight),
    }
}

fn udp_backend_ref_to_snap(b: &UdpRouteRulesBackendRefs) -> HttpBackendRefSnap {
    HttpBackendRefSnap {
        group: b.group.clone(),
        kind: b.kind.clone(),
        namespace: b.namespace.clone(),
        name: b.name.clone(),
        port: snap_u16_port(b.port),
        port_name: None,
        weight: snap_nonnegative_weight(b.weight),
    }
}

fn http_route_to_snap(route: &HTTPRoute) -> HttpRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string());
    let rules: Vec<HttpRouteRuleSnap> = route
        .spec
        .rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|r: &HttpRouteRules| HttpRouteRuleSnap {
            name: r.name.clone(),
            matches: r
                .matches
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|m: &HttpRouteRulesMatches| HttpRequestMatchSnap {
                    path: m.path.as_ref().map(|p| HttpPathMatchSnap {
                        path_type: http_path_match_type_str(p.r#type.as_ref()),
                        value: p.value.clone(),
                    }),
                    headers: m
                        .headers
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|h: &HttpRouteRulesMatchesHeaders| HttpHeaderMatchSnap {
                            match_type: http_header_match_type_str(h.r#type.as_ref()),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                    method: m.method.as_ref().map(http_method_str),
                    query_params: m
                        .query_params
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|qp: &HttpRouteRulesMatchesQueryParams| HttpQueryParamMatchSnap {
                            name: qp.name.clone(),
                            value: qp.value.clone(),
                            match_type: qp.r#type.as_ref().map(|t| format!("{t:?}")),
                        })
                        .collect(),
                })
                .collect(),
            filters: r
                .filters
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(http_route_filter_snap)
                .collect(),
            backend_refs: r
                .backend_refs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(http_backend_ref_to_snap)
                .collect(),
            timeouts: r.timeouts.as_ref().map(|t: &HttpRouteRulesTimeouts| HttpRouteTimeoutsSnap {
                request: t.request.clone(),
                backend_request: t.backend_request.clone(),
            }),
        })
        .collect();

    HttpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route
            .spec
            .parent_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(parent_reference_to_snap)
            .collect(),
        hostnames: route.spec.hostnames.clone().unwrap_or_default(),
        rules,
    }
}

fn grpc_route_to_snap(route: &GRPCRoute) -> GrpcRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string());
    let rules: Vec<GrpcRouteRuleSnap> = route
        .spec
        .rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|r: &GrpcRouteRules| GrpcRouteRuleSnap {
            matches: r
                .matches
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|m: &GrpcRouteRulesMatches| GrpcRouteMatchSnap {
                    method: m.method.as_ref().map(|mm| GrpcMethodMatchSnap {
                        match_type: grpc_method_match_type_str(mm.r#type.as_ref()),
                        service: mm.service.clone(),
                        method: mm.method.clone(),
                    }),
                    headers: m
                        .headers
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|h: &GrpcRouteRulesMatchesHeaders| GrpcHeaderMatchSnap {
                            match_type: grpc_header_match_type_str(h.r#type.as_ref()),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            filters: r
                .filters
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(grpc_route_filter_snap)
                .collect(),
            backend_refs: r
                .backend_refs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(grpc_backend_ref_to_snap)
                .collect(),
        })
        .collect();

    GrpcRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route
            .spec
            .parent_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(grpc_parent_ref_to_snap)
            .collect(),
        hostnames: route.spec.hostnames.clone().unwrap_or_default(),
        rules,
    }
}

fn tls_route_to_snap(route: &TLSRoute) -> TlsRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string());
    let rules: Vec<TlsRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r: &TlsRouteRules| TlsRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(tls_backend_ref_to_snap)
                .collect(),
        })
        .collect();

    TlsRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route
            .spec
            .parent_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(tls_parent_ref_to_snap)
            .collect(),
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
        .map(|t| t.0.to_string());
    let rules: Vec<TcpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r: &TcpRouteRules| TcpRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(tcp_backend_ref_to_snap)
                .collect(),
        })
        .collect();

    TcpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route
            .spec
            .parent_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(tcp_parent_ref_to_snap)
            .collect(),
        rules,
    }
}

fn udp_route_to_snap(route: &UDPRoute) -> UdpRouteSnap {
    let gen = route.metadata.generation.filter(|&g| g > 0).unwrap_or(1);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string());
    let rules: Vec<UdpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r: &UdpRouteRules| UdpRouteRuleSnap {
            backend_refs: r
                .backend_refs
                .iter()
                .map(udp_backend_ref_to_snap)
                .collect(),
        })
        .collect();

    UdpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route
            .spec
            .parent_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(udp_parent_ref_to_snap)
            .collect(),
        rules,
    }
}

fn crd_active(active: &ActiveCrds, m: ManagedCrd) -> bool {
    active.read().map_or(false, |set| set.contains(&m))
}

pub async fn load_cluster_snapshot(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    active_crds: &ActiveCrds,
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
    let mut rejected_listener_sets: Vec<RejectedListenerSetSnap> = Vec::new();
    if crd_active(active_crds, ManagedCrd::ListenerSet) {
        let ls_api: Api<ListenerSet> = Api::all(client.clone());
        match ls_api.list(&ListParams::default()).await {
            Ok(ls_list) => {
                for ls in ls_list.items {
                    if !listenerset_parents_gateway(&ls, gateway_ns, gateway_name) {
                        continue;
                    }
                    let ls_ns = ls.metadata.namespace.clone().unwrap_or_default();
                    let ls_name = ls.metadata.name.clone().unwrap_or_default();
                    if let Some(ref allowed) = allowed_listeners {
                        if !listener_set_namespace_allowed(client, gateway_ns, &ls_ns, allowed)
                            .await?
                        {
                            rejected_listener_sets.push(RejectedListenerSetSnap {
                                namespace: ls_ns,
                                name: ls_name,
                                reason: "NotAllowed".into(),
                            });
                            continue;
                        }
                        listener_sets.push(listenerset_to_snap(ls));
                    } else {
                        rejected_listener_sets.push(RejectedListenerSetSnap {
                            namespace: ls_ns,
                            name: ls_name,
                            reason: "NotAllowed".into(),
                        });
                    }
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
        for rule in item.spec.rules.as_deref().unwrap_or_default() {
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

    if crd_active(active_crds, ManagedCrd::GrpcRoute) {
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
                    for rule in item.spec.rules.as_deref().unwrap_or_default() {
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
    }

    if crd_active(active_crds, ManagedCrd::TlsRoute) {
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
                        for (sns, sname) in
                            crate::resources::collect_tls_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_tls_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
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
    }

    if crd_active(active_crds, ManagedCrd::TcpRoute) {
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
                        for (sns, sname) in
                            crate::resources::collect_tcp_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_tcp_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
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
    }

    if crd_active(active_crds, ManagedCrd::UdpRoute) {
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
                        for (sns, sname) in
                            crate::resources::collect_udp_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_udp_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
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
                cluster_ip: svc.spec.as_ref().and_then(|s| s.cluster_ip.clone()),
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
        match namespace_labels(client, ns).await {
            Ok(lbl) => {
                ns_label_map.insert(ns.clone(), lbl);
            }
            Err(e) => {
                warn!(error = %e, namespace = %ns, "failed to fetch namespace labels; selector-based allowedRoutes may not match");
            }
        }
    }

    let mut reference_grants: Vec<ReferenceGrantSnap> = Vec::new();
    if crd_active(active_crds, ManagedCrd::ReferenceGrant) {
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
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|a| GatewayAddressSnap {
                type_: a.r#type.clone(),
                value: a.value.clone().unwrap_or_default(),
            })
            .collect(),
        gateway_infrastructure: gw.spec.infrastructure.as_ref().map(|inf| {
            GatewayInfrastructureSnap {
                labels: inf.labels.clone().unwrap_or_default(),
                annotations: inf.annotations.clone().unwrap_or_default(),
            }
        }),
        listeners,
        effective_listeners,
        listener_sets,
        rejected_listener_sets,
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
