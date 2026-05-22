//! Store-backed gateway target resolution for reverse-index fan-out fallbacks.

use std::collections::HashSet;

use kube::ResourceExt;

use crate::resources::{BackendTLSPolicy, GRPCRoute, HTTPRoute, TCPRoute, TLSRoute, UDPRoute};
use crate::stores::SharedStores;
use crate::watch_helpers::{
    grpc_route_references_backend_tls_policy_target, http_route_references_backend_tls_policy_target,
    tcp_route_references_backend_tls_policy_target, tls_route_references_backend_tls_policy_target,
    udp_route_references_backend_tls_policy_target,
};

pub(crate) fn gateway_targets_for_route_from_stores(
    stores: &SharedStores,
    route: &HTTPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs_store(
        stores,
        route.metadata.namespace.clone(),
        route.spec.parent_refs.as_deref().unwrap_or(&[]),
    )
}

pub(crate) fn gateway_targets_for_grpc_route_from_stores(
    stores: &SharedStores,
    route: &GRPCRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs_store(
        stores,
        route.metadata.namespace.clone(),
        route.spec.parent_refs.as_deref().unwrap_or(&[]),
    )
}

pub(crate) fn gateway_targets_for_tls_route_from_stores(
    stores: &SharedStores,
    route: &TLSRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs_store(
        stores,
        route.metadata.namespace.clone(),
        route.spec.parent_refs.as_deref().unwrap_or(&[]),
    )
}

pub(crate) fn gateway_targets_for_tcp_route_from_stores(
    stores: &SharedStores,
    route: &TCPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs_store(
        stores,
        route.metadata.namespace.clone(),
        route.spec.parent_refs.as_deref().unwrap_or(&[]),
    )
}

pub(crate) fn gateway_targets_for_udp_route_from_stores(
    stores: &SharedStores,
    route: &UDPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs_store(
        stores,
        route.metadata.namespace.clone(),
        route.spec.parent_refs.as_deref().unwrap_or(&[]),
    )
}

pub(crate) fn gateway_targets_for_backend_tls_policy_from_stores(
    stores: &SharedStores,
    pol: &BackendTLSPolicy,
) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    for route in stores.list_http_routes() {
        if http_route_references_backend_tls_policy_target(pol, &route) {
            out.extend(gateway_targets_for_route_from_stores(stores, &route));
        }
    }
    for route in stores.list_grpc_routes() {
        if grpc_route_references_backend_tls_policy_target(pol, &route) {
            out.extend(gateway_targets_for_grpc_route_from_stores(stores, &route));
        }
    }
    for route in stores.list_tls_routes() {
        if tls_route_references_backend_tls_policy_target(pol, &route) {
            out.extend(gateway_targets_for_tls_route_from_stores(stores, &route));
        }
    }
    for route in stores.list_tcp_routes() {
        if tcp_route_references_backend_tls_policy_target(pol, &route) {
            out.extend(gateway_targets_for_tcp_route_from_stores(stores, &route));
        }
    }
    for route in stores.list_udp_routes() {
        if udp_route_references_backend_tls_policy_target(pol, &route) {
            out.extend(gateway_targets_for_udp_route_from_stores(stores, &route));
        }
    }
    out
}

pub(crate) fn gateway_targets_from_parent_refs_store<P: crate::watch_helpers::ParentRefLike>(
    stores: &SharedStores,
    route_namespace: Option<String>,
    parent_refs: &[P],
) -> HashSet<(String, String)> {
    let mut targets = HashSet::new();
    for p in parent_refs {
        if crate::watch_helpers::is_gateway_parent(p) {
            let pns = p
                .namespace()
                .map(str::to_owned)
                .or_else(|| route_namespace.clone())
                .unwrap_or_default();
            targets.insert((pns, p.name().to_owned()));
            continue;
        }
        if !crate::watch_helpers::is_listenerset_parent(p) {
            continue;
        }
        let ls_ns = p
            .namespace()
            .map(str::to_owned)
            .or_else(|| route_namespace.clone())
            .unwrap_or_default();
        let ls_name = p.name();
        let Some(ls) = stores
            .list_listener_sets()
            .into_iter()
            .find(|ls| ls.namespace().unwrap_or_default() == ls_ns && ls.name_any() == ls_name)
        else {
            continue;
        };
        let pr = &ls.spec.parent_ref;
        if crate::watch_helpers::is_gateway_parent(pr) {
            let gwns = pr
                .namespace
                .clone()
                .or(ls.metadata.namespace.clone())
                .unwrap_or_default();
            targets.insert((gwns, pr.name.clone()));
        }
    }
    targets
}
