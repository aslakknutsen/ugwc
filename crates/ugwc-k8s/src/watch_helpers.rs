//! Shared graph helpers for gateway controller watchers (`controller`, `gateway_watchers`).

use std::collections::HashSet;

use kube::{Api, Client, ResourceExt};
use kube::api::ListParams;

use crate::resources::{
    backend_tls_policy_targets_inference_pool, backend_tls_policy_targets_service, BackendTLSPolicy,
    GRPCRoute, HTTPRoute, ListenerSet, ParentReferenceSpec, TCPRoute, TLSRoute, UDPRoute,
};

pub(crate) fn is_gateway_parent(p: &crate::resources::ParentReferenceSpec) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    g == "gateway.networking.k8s.io" && k == "Gateway"
}

pub(crate) fn is_listenerset_parent(p: &crate::resources::ParentReferenceSpec) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("ListenerSet");
    g == "gateway.networking.k8s.io" && k == "ListenerSet"
}

pub(crate) async fn gateway_targets_from_parent_refs(
    client: &Client,
    route_namespace: Option<String>,
    route_log_name: &str,
    parent_refs: &[ParentReferenceSpec],
) -> HashSet<(String, String)> {
    let mut targets = HashSet::new();
    for p in parent_refs {
        if is_gateway_parent(p) {
            let pns = p
                .namespace
                .clone()
                .or_else(|| route_namespace.clone())
                .unwrap_or_default();
            targets.insert((pns, p.name.clone()));
            continue;
        }
        if !is_listenerset_parent(p) {
            continue;
        }
        let ls_ns = p
            .namespace
            .clone()
            .or_else(|| route_namespace.clone())
            .unwrap_or_default();
        let ls_api: Api<ListenerSet> = Api::namespaced(client.clone(), &ls_ns);
        match ls_api.get(&p.name).await {
            Ok(ls) => {
                for pr in &ls.spec.parent_refs {
                    let pg = pr.group.as_deref().unwrap_or("gateway.networking.k8s.io");
                    let pk = pr.kind.as_deref().unwrap_or("Gateway");
                    if pg != "gateway.networking.k8s.io" || pk != "Gateway" {
                        continue;
                    }
                    let gwns = pr
                        .namespace
                        .clone()
                        .or(ls.metadata.namespace.clone())
                        .unwrap_or_default();
                    targets.insert((gwns, pr.name.clone()));
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    route = %route_log_name,
                    "route listenerset parent resolve"
                );
            }
        }
    }
    targets
}

pub(crate) async fn gateway_targets_for_route(
    client: &Client,
    route: &HTTPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

pub(crate) async fn gateway_targets_for_grpc_route(
    client: &Client,
    route: &GRPCRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

pub(crate) async fn gateway_targets_for_tls_route(
    client: &Client,
    route: &TLSRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

pub(crate) async fn gateway_targets_for_tcp_route(
    client: &Client,
    route: &TCPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

pub(crate) async fn gateway_targets_for_udp_route(
    client: &Client,
    route: &UDPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

fn http_route_references_inference_pool(route: &HTTPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn grpc_route_references_inference_pool(route: &GRPCRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tls_route_references_inference_pool(route: &TLSRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tcp_route_references_inference_pool(route: &TCPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn udp_route_references_inference_pool(route: &UDPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

pub(crate) async fn gateway_targets_for_inference_pool(
    client: &Client,
    pool_ns: &str,
    pool_name: &str,
) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    let hr_api: Api<HTTPRoute> = Api::all(client.clone());
    match hr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if http_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_route(client, &route).await);
                }
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list httproute after inferencepool change"
        ),
    }
    let gr_api: Api<GRPCRoute> = Api::all(client.clone());
    match gr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if grpc_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_grpc_route(client, &route).await);
                }
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list grpcroute after inferencepool change"
        ),
    }
    let tr_api: Api<TLSRoute> = Api::all(client.clone());
    match tr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if tls_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_tls_route(client, &route).await);
                }
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list tlsroute after inferencepool change"
        ),
    }
    let tcp_api: Api<TCPRoute> = Api::all(client.clone());
    match tcp_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if tcp_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_tcp_route(client, &route).await);
                }
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list tcproute after inferencepool change"
        ),
    }
    let udp_api: Api<UDPRoute> = Api::all(client.clone());
    match udp_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if udp_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_udp_route(client, &route).await);
                }
            }
        }
        Err(e) => tracing::warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list udproute after inferencepool change"
        ),
    }
    out
}

fn http_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &HTTPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for (sns, sname) in crate::resources::collect_http_route_rule_service_refs(rns, rule) {
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, &sns, &sname) {
                return true;
            }
        }
        for (pns, pname) in crate::resources::collect_http_route_rule_inference_pool_refs(rns, rule)
        {
            if backend_tls_policy_targets_inference_pool(&pol.spec, pol_ns, &pns, &pname) {
                return true;
            }
        }
    }
    false
}

fn grpc_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &GRPCRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for (sns, sname) in crate::resources::collect_grpc_route_rule_service_refs(rns, rule) {
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, &sns, &sname) {
                return true;
            }
        }
        for (pns, pname) in crate::resources::collect_grpc_route_rule_inference_pool_refs(rns, rule)
        {
            if backend_tls_policy_targets_inference_pool(&pol.spec, pol_ns, &pns, &pname) {
                return true;
            }
        }
    }
    false
}

fn tls_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &TLSRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for (sns, sname) in
            crate::resources::collect_backend_refs_service_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, &sns, &sname) {
                return true;
            }
        }
        for (pns, pname) in
            crate::resources::collect_backend_refs_inference_pool_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_inference_pool(&pol.spec, pol_ns, &pns, &pname) {
                return true;
            }
        }
    }
    false
}

fn tcp_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &TCPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for (sns, sname) in
            crate::resources::collect_backend_refs_service_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, &sns, &sname) {
                return true;
            }
        }
        for (pns, pname) in
            crate::resources::collect_backend_refs_inference_pool_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_inference_pool(&pol.spec, pol_ns, &pns, &pname) {
                return true;
            }
        }
    }
    false
}

fn udp_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &UDPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for (sns, sname) in
            crate::resources::collect_backend_refs_service_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, &sns, &sname) {
                return true;
            }
        }
        for (pns, pname) in
            crate::resources::collect_backend_refs_inference_pool_refs(rns, &rule.backend_refs)
        {
            if backend_tls_policy_targets_inference_pool(&pol.spec, pol_ns, &pns, &pname) {
                return true;
            }
        }
    }
    false
}

pub(crate) async fn gateway_targets_for_backend_tls_policy(
    client: &Client,
    pol: &BackendTLSPolicy,
) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    let hr_api: Api<HTTPRoute> = Api::all(client.clone());
    if let Ok(list) = hr_api.list(&ListParams::default()).await {
        for route in list.items {
            if http_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_route(client, &route).await);
            }
        }
    }
    let gr_api: Api<GRPCRoute> = Api::all(client.clone());
    if let Ok(list) = gr_api.list(&ListParams::default()).await {
        for route in list.items {
            if grpc_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_grpc_route(client, &route).await);
            }
        }
    }
    let tr_api: Api<TLSRoute> = Api::all(client.clone());
    if let Ok(list) = tr_api.list(&ListParams::default()).await {
        for route in list.items {
            if tls_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_tls_route(client, &route).await);
            }
        }
    }
    let tcp_api: Api<TCPRoute> = Api::all(client.clone());
    if let Ok(list) = tcp_api.list(&ListParams::default()).await {
        for route in list.items {
            if tcp_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_tcp_route(client, &route).await);
            }
        }
    }
    let udp_api: Api<UDPRoute> = Api::all(client.clone());
    if let Ok(list) = udp_api.list(&ListParams::default()).await {
        for route in list.items {
            if udp_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_udp_route(client, &route).await);
            }
        }
    }
    out
}
