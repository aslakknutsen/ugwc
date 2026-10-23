use ugwc_core::snapshot::{
    GrpcHeaderMatchSnap, GrpcMethodMatchSnap, GrpcRouteFilterSnap, GrpcRouteMatchSnap,
    GrpcRouteRuleSnap, GrpcRouteSnap, HttpBackendRefSnap, HttpCorsFilterSnap, HttpHeaderFilterSnap,
    HttpHeaderMatchSnap, HttpHeaderNameValueSnap, HttpPathMatchSnap, HttpPathModifierSnap,
    HttpQueryParamMatchSnap, HttpRequestMatchSnap, HttpRequestRedirectFilterSnap,
    HttpRouteFilterSnap, HttpRouteRuleSnap, HttpRouteSnap, HttpRouteTimeoutsSnap,
    HttpUrlRewriteFilterSnap, TcpRouteRuleSnap, TcpRouteSnap, TlsRouteRuleSnap, TlsRouteSnap,
    UdpRouteRuleSnap, UdpRouteSnap,
};

use crate::resources::{
    GRPCRoute, GrpcRouteRules, GrpcRouteRulesBackendRefs, GrpcRouteRulesFilters,
    GrpcRouteRulesFiltersRequestHeaderModifier, GrpcRouteRulesFiltersResponseHeaderModifier,
    GrpcRouteRulesFiltersType, GrpcRouteRulesMatches, GrpcRouteRulesMatchesHeaders,
    GrpcRouteRulesMatchesHeadersType, GrpcRouteRulesMatchesMethodType, HTTPRoute, HttpRouteRules,
    HttpRouteRulesBackendRefs, HttpRouteRulesFilters, HttpRouteRulesFiltersRequestHeaderModifier,
    HttpRouteRulesFiltersRequestRedirectPath, HttpRouteRulesFiltersRequestRedirectPathType,
    HttpRouteRulesFiltersRequestRedirectScheme, HttpRouteRulesFiltersResponseHeaderModifier,
    HttpRouteRulesFiltersType, HttpRouteRulesFiltersUrlRewritePath,
    HttpRouteRulesFiltersUrlRewritePathType, HttpRouteRulesMatches, HttpRouteRulesMatchesHeaders,
    HttpRouteRulesMatchesHeadersType, HttpRouteRulesMatchesMethod, HttpRouteRulesMatchesPathType,
    HttpRouteRulesMatchesQueryParams, HttpRouteRulesTimeouts, TCPRoute, TLSRoute, TcpRouteRules,
    TcpRouteRulesBackendRefs, TlsRouteRules, TlsRouteRulesBackendRefs, UDPRoute, UdpRouteRules,
    UdpRouteRulesBackendRefs,
};

use super::parent_refs::{
    grpc_parent_ref_to_snap, parent_reference_to_snap, tcp_parent_ref_to_snap,
    tls_parent_ref_to_snap, udp_parent_ref_to_snap,
};
use super::{snap_nonnegative_weight, snap_u16_port};

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

fn grpc_req_header_filter_snap(
    h: &GrpcRouteRulesFiltersRequestHeaderModifier,
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

fn redirect_scheme_snap(
    sch: Option<&HttpRouteRulesFiltersRequestRedirectScheme>,
) -> Option<String> {
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

fn resp_header_filter_snap(
    h: &HttpRouteRulesFiltersResponseHeaderModifier,
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
        request_redirect: f
            .request_redirect
            .as_ref()
            .map(|rr| HttpRequestRedirectFilterSnap {
                scheme: redirect_scheme_snap(rr.scheme.as_ref()),
                hostname: rr.hostname.clone(),
                port: snap_u16_port(rr.port),
                status_code: rr.status_code.and_then(|c| i32::try_from(c).ok()),
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

pub(super) fn http_path_match_type_str(
    ty: Option<&HttpRouteRulesMatchesPathType>,
) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

pub(super) fn http_header_match_type_str(
    ty: Option<&HttpRouteRulesMatchesHeadersType>,
) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

pub(super) fn grpc_header_match_type_str(
    ty: Option<&GrpcRouteRulesMatchesHeadersType>,
) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

pub(super) fn grpc_method_match_type_str(
    ty: Option<&GrpcRouteRulesMatchesMethodType>,
) -> Option<String> {
    ty.map(|t| format!("{t:?}"))
}

pub(super) fn http_backend_ref_to_snap(b: &HttpRouteRulesBackendRefs) -> HttpBackendRefSnap {
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

pub(super) fn grpc_backend_ref_to_snap(b: &GrpcRouteRulesBackendRefs) -> HttpBackendRefSnap {
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

pub(super) fn tls_backend_ref_to_snap(b: &TlsRouteRulesBackendRefs) -> HttpBackendRefSnap {
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

pub(super) fn tcp_backend_ref_to_snap(b: &TcpRouteRulesBackendRefs) -> HttpBackendRefSnap {
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

pub(super) fn udp_backend_ref_to_snap(b: &UdpRouteRulesBackendRefs) -> HttpBackendRefSnap {
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

pub(super) fn http_route_to_snap(route: &HTTPRoute) -> HttpRouteSnap {
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
                        .map(
                            |qp: &HttpRouteRulesMatchesQueryParams| HttpQueryParamMatchSnap {
                                name: qp.name.clone(),
                                value: qp.value.clone(),
                                match_type: qp.r#type.as_ref().map(|t| format!("{t:?}")),
                            },
                        )
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
            timeouts: r
                .timeouts
                .as_ref()
                .map(|t: &HttpRouteRulesTimeouts| HttpRouteTimeoutsSnap {
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

pub(super) fn grpc_route_to_snap(route: &GRPCRoute) -> GrpcRouteSnap {
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

pub(super) fn tls_route_to_snap(route: &TLSRoute) -> TlsRouteSnap {
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
            backend_refs: r.backend_refs.iter().map(tls_backend_ref_to_snap).collect(),
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

pub(super) fn tcp_route_to_snap(route: &TCPRoute) -> TcpRouteSnap {
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
            backend_refs: r.backend_refs.iter().map(tcp_backend_ref_to_snap).collect(),
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

pub(super) fn udp_route_to_snap(route: &UDPRoute) -> UdpRouteSnap {
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
            backend_refs: r.backend_refs.iter().map(udp_backend_ref_to_snap).collect(),
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
