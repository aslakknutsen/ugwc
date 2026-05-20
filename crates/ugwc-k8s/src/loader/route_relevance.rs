use std::collections::HashSet;

use crate::resources::{
    GRPCRoute, GrpcRouteParentRefs, HTTPRoute, HttpRouteParentRefs, TCPRoute, TcpRouteParentRefs,
    TLSRoute, TlsRouteParentRefs, UDPRoute, UdpRouteParentRefs,
};

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

pub(super) fn route_parent_relevant_http(
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

pub(super) fn route_relevant_to_gateway(
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

pub(super) fn grpc_route_relevant_to_gateway(
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

pub(super) fn tls_route_relevant_to_gateway(
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

pub(super) fn tcp_route_relevant_to_gateway(
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

pub(super) fn udp_route_relevant_to_gateway(
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
