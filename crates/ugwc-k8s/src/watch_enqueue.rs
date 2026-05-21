//! Shared gateway reconcile enqueue helpers for watch handlers (apply and delete).

use std::collections::HashSet;

use k8s_openapi::api::core::v1::Service;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use tracing::warn;

use crate::reconcile_queue::ReconcileSender;
use crate::resources::{
    backend_tls_policy_spec_references_configmap, backend_tls_policy_spec_references_secret,
    gateway_spec_references_secret, listenerset_spec_references_secret, BackendTLSPolicy, Gateway,
    GRPCRoute, HTTPRoute, ListenerSet, ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};
use crate::watch_helpers::{
    gateway_targets_for_backend_tls_policy, gateway_targets_for_grpc_route,
    gateway_targets_for_inference_pool, gateway_targets_for_route, gateway_targets_for_tcp_route,
    gateway_targets_for_tls_route, gateway_targets_for_udp_route,
};

pub async fn gateway_keys_for_http_route(
    client: &Client,
    route: &HTTPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_for_route(client, route).await
}

pub async fn gateway_keys_for_grpc_route(
    client: &Client,
    route: &GRPCRoute,
) -> HashSet<(String, String)> {
    gateway_targets_for_grpc_route(client, route).await
}

pub async fn gateway_keys_for_tls_route(
    client: &Client,
    route: &TLSRoute,
) -> HashSet<(String, String)> {
    gateway_targets_for_tls_route(client, route).await
}

pub async fn gateway_keys_for_tcp_route(
    client: &Client,
    route: &TCPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_for_tcp_route(client, route).await
}

pub async fn gateway_keys_for_udp_route(
    client: &Client,
    route: &UDPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_for_udp_route(client, route).await
}

/// Gateway keys to reconcile when a ReferenceGrant is applied or deleted.
pub async fn gateway_keys_for_reference_grant(
    client: &Client,
    rg: &ReferenceGrant,
) -> HashSet<(String, String)> {
    let mut keys: HashSet<(String, String)> = HashSet::new();
    let grant_ns = rg.metadata.namespace.clone().unwrap_or_default();
    if !grant_ns.is_empty() {
        let gw_api: Api<Gateway> = Api::namespaced(client.clone(), &grant_ns);
        match gw_api.list(&ListParams::default()).await {
            Ok(list) => {
                for gw in list.items {
                    keys.insert((grant_ns.clone(), gw.name_any()));
                }
            }
            Err(e) => warn!(
                error = %e,
                grant_ns = %grant_ns,
                "list gateways in ReferenceGrant namespace after grant change"
            ),
        }
    }

    let mut route_ns_list: Vec<String> = rg
        .spec
        .from
        .iter()
        .map(|f| f.namespace.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    route_ns_list.sort();
    route_ns_list.dedup();

    if route_ns_list.is_empty() {
        let hr_api: Api<HTTPRoute> = Api::all(client.clone());
        match hr_api.list(&ListParams::default()).await {
            Ok(list) => {
                for route in list.items {
                    keys.extend(gateway_targets_for_route(client, &route).await);
                }
            }
            Err(e) => warn!(
                error = %e,
                "list httproute after referencegrant change (fallback)"
            ),
        }
        let gr_api: Api<GRPCRoute> = Api::all(client.clone());
        match gr_api.list(&ListParams::default()).await {
            Ok(list) => {
                for route in list.items {
                    keys.extend(gateway_targets_for_grpc_route(client, &route).await);
                }
            }
            Err(e) => warn!(
                error = %e,
                "list grpcroute after referencegrant change (fallback)"
            ),
        }
        let tls_api: Api<TLSRoute> = Api::all(client.clone());
        match tls_api.list(&ListParams::default()).await {
            Ok(list) => {
                for route in list.items {
                    keys.extend(gateway_targets_for_tls_route(client, &route).await);
                }
            }
            Err(e) => warn!(
                error = %e,
                "list tlsroute after referencegrant change (fallback)"
            ),
        }
        let tcp_api: Api<TCPRoute> = Api::all(client.clone());
        match tcp_api.list(&ListParams::default()).await {
            Ok(list) => {
                for route in list.items {
                    keys.extend(gateway_targets_for_tcp_route(client, &route).await);
                }
            }
            Err(e) => warn!(
                error = %e,
                "list tcproute after referencegrant change (fallback)"
            ),
        }
        let udp_api: Api<UDPRoute> = Api::all(client.clone());
        match udp_api.list(&ListParams::default()).await {
            Ok(list) => {
                for route in list.items {
                    keys.extend(gateway_targets_for_udp_route(client, &route).await);
                }
            }
            Err(e) => warn!(
                error = %e,
                "list udproute after referencegrant change (fallback)"
            ),
        }
    } else {
        for route_ns in route_ns_list {
            let hr_api: Api<HTTPRoute> = Api::namespaced(client.clone(), &route_ns);
            match hr_api.list(&ListParams::default()).await {
                Ok(list) => {
                    for route in list.items {
                        keys.extend(gateway_targets_for_route(client, &route).await);
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    route_ns = %route_ns,
                    "list httproute after referencegrant change"
                ),
            }
            let gr_api: Api<GRPCRoute> = Api::namespaced(client.clone(), &route_ns);
            match gr_api.list(&ListParams::default()).await {
                Ok(list) => {
                    for route in list.items {
                        keys.extend(gateway_targets_for_grpc_route(client, &route).await);
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    route_ns = %route_ns,
                    "list grpcroute after referencegrant change"
                ),
            }
            let tls_api: Api<TLSRoute> = Api::namespaced(client.clone(), &route_ns);
            match tls_api.list(&ListParams::default()).await {
                Ok(list) => {
                    for route in list.items {
                        keys.extend(gateway_targets_for_tls_route(client, &route).await);
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    route_ns = %route_ns,
                    "list tlsroute after referencegrant change"
                ),
            }
            let tcp_api: Api<TCPRoute> = Api::namespaced(client.clone(), &route_ns);
            match tcp_api.list(&ListParams::default()).await {
                Ok(list) => {
                    for route in list.items {
                        keys.extend(gateway_targets_for_tcp_route(client, &route).await);
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    route_ns = %route_ns,
                    "list tcproute after referencegrant change"
                ),
            }
            let udp_api: Api<UDPRoute> = Api::namespaced(client.clone(), &route_ns);
            match udp_api.list(&ListParams::default()).await {
                Ok(list) => {
                    for route in list.items {
                        keys.extend(gateway_targets_for_udp_route(client, &route).await);
                    }
                }
                Err(e) => warn!(
                    error = %e,
                    route_ns = %route_ns,
                    "list udproute after referencegrant change"
                ),
            }
        }
    }
    keys
}

/// Parent gateway for a ListenerSet (if parentRef targets Gateway).
pub fn gateway_key_for_listener_set(ls: &ListenerSet) -> Option<(String, String)> {
    let pr = &ls.spec.parent_ref;
    let pg = pr.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let pk = pr.kind.as_deref().unwrap_or("Gateway");
    if pg == "gateway.networking.k8s.io" && pk == "Gateway" {
        let gwns = pr
            .namespace
            .clone()
            .or(ls.metadata.namespace.clone())
            .unwrap_or_default();
        Some((gwns, pr.name.clone()))
    } else {
        None
    }
}

pub async fn gateway_keys_for_backend_tls_policy(
    client: &Client,
    pol: &BackendTLSPolicy,
) -> HashSet<(String, String)> {
    gateway_targets_for_backend_tls_policy(client, pol).await
}

pub async fn gateway_keys_for_inference_pool(
    client: &Client,
    pool_ns: &str,
    pool_name: &str,
) -> HashSet<(String, String)> {
    gateway_targets_for_inference_pool(client, pool_ns, pool_name).await
}

/// Gateways referencing a Secret (TLS cert, BackendTLS, etc.).
pub async fn gateway_keys_for_secret(
    client: &Client,
    secret_ns: &str,
    secret_name: &str,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    let gw_api: Api<Gateway> = Api::all(client.clone());
    match gw_api.list(&ListParams::default()).await {
        Ok(list) => {
            for gw in list.items {
                let gwns = gw.namespace().unwrap_or_default();
                if gateway_spec_references_secret(&gw.spec, &gwns, secret_ns, secret_name) {
                    keys.insert((gwns, gw.name_any()));
                }
            }
        }
        Err(e) => warn!(error = %e, "list gateways after secret change"),
    }
    let ls_api: Api<ListenerSet> = Api::all(client.clone());
    match ls_api.list(&ListParams::default()).await {
        Ok(ls_list) => {
            for ls in ls_list.items {
                let ls_ns = ls.namespace().unwrap_or_default();
                if !listenerset_spec_references_secret(&ls.spec, &ls_ns, secret_ns, secret_name) {
                    continue;
                }
                if let Some(k) = gateway_key_for_listener_set(&ls) {
                    keys.insert(k);
                }
            }
        }
        Err(e) => warn!(error = %e, "list listenersets after secret change"),
    }
    let pol_api: Api<BackendTLSPolicy> = Api::all(client.clone());
    match pol_api.list(&ListParams::default()).await {
        Ok(pol_list) => {
            for pol in pol_list.items {
                let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
                if backend_tls_policy_spec_references_secret(
                    &pol.spec,
                    pol_ns,
                    secret_ns,
                    secret_name,
                ) {
                    keys.extend(gateway_targets_for_backend_tls_policy(client, &pol).await);
                }
            }
        }
        Err(e) => warn!(error = %e, "list BackendTLSPolicy after secret change"),
    }
    keys
}

/// Gateways affected by a BackendTLS CA ConfigMap change.
pub async fn gateway_keys_for_configmap(
    client: &Client,
    cm_ns: &str,
    cm_name: &str,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    let pol_api: Api<BackendTLSPolicy> = Api::all(client.clone());
    let policies = match pol_api.list(&ListParams::default()).await {
        Ok(l) => l.items,
        Err(e) => {
            warn!(error = %e, "list BackendTLSPolicy after ConfigMap change");
            return keys;
        }
    };
    for pol in policies {
        let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
        if !backend_tls_policy_spec_references_configmap(&pol.spec, pol_ns, cm_ns, cm_name) {
            continue;
        }
        keys.extend(gateway_targets_for_backend_tls_policy(client, &pol).await);
    }
    keys
}

/// Owning gateway for a ugwc-managed dataplane Service (from labels).
pub fn gateway_key_for_managed_service(svc: &Service) -> Option<(String, String)> {
    let labels = svc.metadata.labels.as_ref()?;
    let gw_ns = labels
        .get("gateway.ugwc.io/gateway-namespace")?
        .clone();
    let gw_name = labels
        .get("gateway.ugwc.io/gateway-name")?
        .clone();
    if gw_ns.is_empty() || gw_name.is_empty() {
        None
    } else {
        Some((gw_ns, gw_name))
    }
}

/// All gateways in the cluster.
pub async fn gateway_keys_all(client: &Client) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    let gw_api: Api<Gateway> = Api::all(client.clone());
    match gw_api.list(&ListParams::default()).await {
        Ok(list) => {
            for gw in list.items {
                keys.insert((gw.namespace().unwrap_or_default(), gw.name_any()));
            }
        }
        Err(e) => warn!(error = %e, "list all gateways"),
    }
    keys
}

/// Gateways using a given GatewayClass name.
pub async fn gateway_keys_for_gateway_class(
    client: &Client,
    class_name: &str,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    let gw_api: Api<Gateway> = Api::all(client.clone());
    match gw_api.list(&ListParams::default()).await {
        Ok(list) => {
            for gw in list.items {
                if gw.spec.gateway_class_name == class_name {
                    keys.insert((gw.namespace().unwrap_or_default(), gw.name_any()));
                }
            }
        }
        Err(e) => warn!(error = %e, class = %class_name, "list gateways for gatewayclass"),
    }
    keys
}

/// Send all keys; returns false if the queue closed.
pub async fn drain_gateway_keys(
    keys: HashSet<(String, String)>,
    tx: &ReconcileSender,
) -> bool {
    for (pns, pname) in keys {
        if tx.send(pns, pname).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use crate::resources::ReferenceGrantFrom;

    fn route_namespace_hints(from: &[ReferenceGrantFrom]) -> Vec<String> {
        let mut route_ns_list: Vec<String> = from
            .iter()
            .map(|f| f.namespace.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        route_ns_list.sort();
        route_ns_list.dedup();
        route_ns_list
    }

    #[test]
    fn route_namespace_hints_sort_dedup() {
        let from = vec![
            ReferenceGrantFrom {
                namespace: "b".into(),
                kind: "HTTPRoute".into(),
                ..Default::default()
            },
            ReferenceGrantFrom {
                namespace: "a".into(),
                kind: "HTTPRoute".into(),
                ..Default::default()
            },
            ReferenceGrantFrom {
                namespace: "b".into(),
                kind: "GRPCRoute".into(),
                ..Default::default()
            },
        ];
        assert_eq!(
            route_namespace_hints(&from),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn route_namespace_hints_skips_blank() {
        let from = vec![ReferenceGrantFrom {
            namespace: "  ".into(),
            kind: "HTTPRoute".into(),
            ..Default::default()
        }];
        assert!(route_namespace_hints(&from).is_empty());
    }
}
