//! Shared gateway reconcile enqueue helpers for watch handlers (apply and delete).

use std::collections::HashSet;

use k8s_openapi::api::core::v1::Service;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use tracing::warn;

use crate::reconcile_queue::ReconcileSender;
use crate::resources::{
    backend_tls_policy_spec_references_configmap, backend_tls_policy_spec_references_secret,
    gateway_spec_references_secret, listenerset_spec_references_secret, BackendTLSPolicy,
    GRPCRoute, Gateway, HTTPRoute, ListenerSet, ReferenceGrant, ReferenceGrantFrom,
    ReferenceGrantTo, TCPRoute, TLSRoute, UDPRoute,
};
use crate::stores::SharedStores;
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
    let mut keys = gateway_keys_for_reference_grant_listeners(client, rg).await;
    keys.extend(gateway_keys_for_reference_grant_routes(client, rg).await);
    keys
}

const ROUTE_REFERENCE_GRANT_KINDS: &[&str] =
    &["HTTPRoute", "GRPCRoute", "TLSRoute", "TCPRoute", "UDPRoute"];

fn reference_grant_from_matches(from: &ReferenceGrantFrom, from_kind: &str, from_ns: &str) -> bool {
    const G_FROM: &str = "gateway.networking.k8s.io";
    let fg = if from.group.is_empty() {
        G_FROM
    } else {
        from.group.as_str()
    };
    fg == G_FROM && from.kind == from_kind && from.namespace == from_ns
}

fn reference_grant_to_permits_secret(to: &ReferenceGrantTo, secret_name: &str) -> bool {
    if to.kind != "Secret" {
        return false;
    }
    if !to.group.is_empty() {
        return false;
    }
    match &to.name {
        None => true,
        Some(n) if n.is_empty() => true,
        Some(n) if n == secret_name => true,
        Some(_) => false,
    }
}

fn reference_grant_permits_listener_secret_from_spec(
    rg: &ReferenceGrant,
    from_kind: &str,
    from_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    if from_ns == secret_ns {
        return true;
    }
    let grant_ns = rg.metadata.namespace.as_deref().unwrap_or("");
    if grant_ns != secret_ns {
        return false;
    }
    let from_ok = rg
        .spec
        .from
        .iter()
        .any(|fr| reference_grant_from_matches(fr, from_kind, from_ns));
    if !from_ok {
        return false;
    }
    rg.spec
        .to
        .iter()
        .any(|to| reference_grant_to_permits_secret(to, secret_name))
}

fn gateway_spec_has_grantable_secret_ref(
    rg: &ReferenceGrant,
    from_kind: &str,
    from_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    secret_ns != from_ns
        && reference_grant_permits_listener_secret_from_spec(
            rg,
            from_kind,
            from_ns,
            secret_ns,
            secret_name,
        )
}

fn gateway_spec_references_grant_secrets(
    spec: &gateway_api::gateways::GatewaySpec,
    gw_ns: &str,
    rg: &ReferenceGrant,
) -> bool {
    spec.listeners.iter().any(|l| {
        let Some(ref tls) = l.tls else {
            return false;
        };
        tls.certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|r| {
                let secret_ns = r.namespace.as_deref().unwrap_or(gw_ns);
                gateway_spec_has_grantable_secret_ref(rg, "Gateway", gw_ns, secret_ns, &r.name)
            })
    })
}

fn listenerset_spec_references_grant_secrets(
    spec: &gateway_api::listenersets::ListenerSetSpec,
    ls_ns: &str,
    rg: &ReferenceGrant,
) -> bool {
    spec.listeners.iter().any(|l| {
        let Some(ref tls) = l.tls else {
            return false;
        };
        tls.certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|r| {
                let secret_ns = r.namespace.as_deref().unwrap_or(ls_ns);
                gateway_spec_has_grantable_secret_ref(rg, "ListenerSet", ls_ns, secret_ns, &r.name)
            })
    })
}

/// Store-backed fan-out for Gateway / ListenerSet Secret certificateRef grants.
#[must_use]
pub fn gateway_keys_for_reference_grant_from_stores(
    stores: &SharedStores,
    rg: &ReferenceGrant,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for fr in &rg.spec.from {
        let from_ns = fr.namespace.trim();
        if from_ns.is_empty() {
            continue;
        }
        match fr.kind.as_str() {
            "Gateway" => {
                for gw in stores.list_gateways() {
                    let gwns = gw.namespace().unwrap_or_default();
                    if gwns != from_ns {
                        continue;
                    }
                    if gateway_spec_references_grant_secrets(&gw.spec, from_ns, rg) {
                        keys.insert((gwns, gw.name_any()));
                    }
                }
            }
            "ListenerSet" => {
                for ls in stores.list_listener_sets() {
                    let ls_ns = ls.namespace().unwrap_or_default();
                    if ls_ns != from_ns {
                        continue;
                    }
                    if listenerset_spec_references_grant_secrets(&ls.spec, from_ns, rg) {
                        if let Some(k) = gateway_key_for_listener_set(&ls) {
                            keys.insert(k);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    keys
}

async fn gateway_keys_for_reference_grant_listeners(
    client: &Client,
    rg: &ReferenceGrant,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for fr in &rg.spec.from {
        let from_ns = fr.namespace.trim();
        if from_ns.is_empty() {
            continue;
        }
        match fr.kind.as_str() {
            "Gateway" => {
                let api: Api<Gateway> = Api::namespaced(client.clone(), from_ns);
                match api.list(&ListParams::default()).await {
                    Ok(list) => {
                        for gw in list.items {
                            let gwns = gw.namespace().unwrap_or_default();
                            if gateway_spec_references_grant_secrets(&gw.spec, from_ns, rg) {
                                keys.insert((gwns, gw.name_any()));
                            }
                        }
                    }
                    Err(e) => warn!(
                        error = %e,
                        namespace = %from_ns,
                        "list gateways after referencegrant change"
                    ),
                }
            }
            "ListenerSet" => {
                let api: Api<ListenerSet> = Api::namespaced(client.clone(), from_ns);
                match api.list(&ListParams::default()).await {
                    Ok(list) => {
                        for ls in list.items {
                            if listenerset_spec_references_grant_secrets(&ls.spec, from_ns, rg) {
                                if let Some(k) = gateway_key_for_listener_set(&ls) {
                                    keys.insert(k);
                                }
                            }
                        }
                    }
                    Err(e) => warn!(
                        error = %e,
                        namespace = %from_ns,
                        "list listenersets after referencegrant change"
                    ),
                }
            }
            _ => {}
        }
    }
    keys
}

async fn gateway_keys_for_reference_grant_routes(
    client: &Client,
    rg: &ReferenceGrant,
) -> HashSet<(String, String)> {
    let mut keys: HashSet<(String, String)> = HashSet::new();
    let mut route_ns_list: Vec<String> = rg
        .spec
        .from
        .iter()
        .filter(|f| ROUTE_REFERENCE_GRANT_KINDS.contains(&f.kind.as_str()))
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

/// Owning gateway for a ugwc-managed dataplane Service (from labels).
pub fn gateway_key_for_managed_service(svc: &Service) -> Option<(String, String)> {
    let labels = svc.metadata.labels.as_ref()?;
    let gw_ns = labels.get("gateway.ugwc.io/gateway-namespace")?.clone();
    let gw_name = labels.get("gateway.ugwc.io/gateway-name")?.clone();
    if gw_ns.is_empty() || gw_name.is_empty() {
        None
    } else {
        Some((gw_ns, gw_name))
    }
}

/// All gateways in the cluster (store-backed).
#[must_use]
pub fn all_gateway_keys_from_stores(stores: &SharedStores) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for gw in stores.list_gateways() {
        keys.insert((gw.namespace().unwrap_or_default(), gw.name_any()));
    }
    keys
}

/// Gateways referencing a Secret (TLS cert, BackendTLS, etc.) — store-backed.
#[must_use]
pub fn gateway_keys_for_secret_from_stores(
    stores: &SharedStores,
    secret_ns: &str,
    secret_name: &str,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for gw in stores.list_gateways() {
        let gwns = gw.namespace().unwrap_or_default();
        if gateway_spec_references_secret(&gw.spec, &gwns, secret_ns, secret_name) {
            keys.insert((gwns, gw.name_any()));
        }
    }
    for ls in stores.list_listener_sets() {
        let ls_ns = ls.namespace().unwrap_or_default();
        if !listenerset_spec_references_secret(&ls.spec, &ls_ns, secret_ns, secret_name) {
            continue;
        }
        if let Some(k) = gateway_key_for_listener_set(&ls) {
            keys.insert(k);
        }
    }
    for pol in stores.list_backend_tls_policies() {
        let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
        if backend_tls_policy_spec_references_secret(&pol.spec, pol_ns, secret_ns, secret_name) {
            keys.extend(
                crate::watch_helpers_store::gateway_targets_for_backend_tls_policy_from_stores(
                    stores, &pol,
                ),
            );
        }
    }
    keys
}

/// Gateways affected by a BackendTLS CA ConfigMap change — store-backed.
#[must_use]
pub fn gateway_keys_for_configmap_from_stores(
    stores: &SharedStores,
    cm_ns: &str,
    cm_name: &str,
) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for pol in stores.list_backend_tls_policies() {
        let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
        if !backend_tls_policy_spec_references_configmap(&pol.spec, pol_ns, cm_ns, cm_name) {
            continue;
        }
        keys.extend(
            crate::watch_helpers_store::gateway_targets_for_backend_tls_policy_from_stores(
                stores, &pol,
            ),
        );
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
pub async fn drain_gateway_keys(keys: HashSet<(String, String)>, tx: &ReconcileSender) -> bool {
    for (pns, pname) in keys {
        if tx.send(pns, pname).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use crate::resources::{
        ReferenceGrant, ReferenceGrantFrom, ReferenceGrantSpec, ReferenceGrantTo,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn route_namespace_hints(from: &[ReferenceGrantFrom]) -> Vec<String> {
        let mut route_ns_list: Vec<String> = from
            .iter()
            .filter(|f| super::ROUTE_REFERENCE_GRANT_KINDS.contains(&f.kind.as_str()))
            .map(|f| f.namespace.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        route_ns_list.sort();
        route_ns_list.dedup();
        route_ns_list
    }

    fn conformance_all_in_namespace_grant() -> ReferenceGrant {
        ReferenceGrant {
            metadata: ObjectMeta {
                name: Some("reference-grant-all-in-namespace".into()),
                namespace: Some("gateway-conformance-web-backend".into()),
                ..Default::default()
            },
            spec: ReferenceGrantSpec {
                from: vec![ReferenceGrantFrom {
                    group: "gateway.networking.k8s.io".into(),
                    kind: "Gateway".into(),
                    namespace: "gateway-conformance-infra".into(),
                }],
                to: vec![ReferenceGrantTo {
                    group: "".into(),
                    kind: "Secret".into(),
                    name: None,
                }],
            },
            ..Default::default()
        }
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

    #[test]
    fn route_namespace_hints_skips_gateway_from() {
        let from = vec![
            ReferenceGrantFrom {
                namespace: "infra".into(),
                kind: "Gateway".into(),
                ..Default::default()
            },
            ReferenceGrantFrom {
                namespace: "routes".into(),
                kind: "HTTPRoute".into(),
                ..Default::default()
            },
        ];
        assert_eq!(route_namespace_hints(&from), vec!["routes".to_string()]);
    }

    #[test]
    fn reference_grant_permits_cross_namespace_gateway_secret() {
        let rg = conformance_all_in_namespace_grant();
        assert!(super::reference_grant_permits_listener_secret_from_spec(
            &rg,
            "Gateway",
            "gateway-conformance-infra",
            "gateway-conformance-web-backend",
            "certificate",
        ));
    }

    #[test]
    fn reference_grant_to_permits_named_secret_only() {
        let rg = ReferenceGrant {
            metadata: ObjectMeta {
                namespace: Some("sec-ns".into()),
                ..Default::default()
            },
            spec: ReferenceGrantSpec {
                from: vec![ReferenceGrantFrom {
                    kind: "Gateway".into(),
                    namespace: "gw-ns".into(),
                    ..Default::default()
                }],
                to: vec![ReferenceGrantTo {
                    kind: "Secret".into(),
                    name: Some("tls-secret".into()),
                    ..Default::default()
                }],
            },
            ..Default::default()
        };
        assert!(super::reference_grant_permits_listener_secret_from_spec(
            &rg,
            "Gateway",
            "gw-ns",
            "sec-ns",
            "tls-secret",
        ));
        assert!(!super::reference_grant_permits_listener_secret_from_spec(
            &rg,
            "Gateway",
            "gw-ns",
            "sec-ns",
            "other-secret",
        ));
    }

    #[test]
    fn gateway_spec_has_grantable_secret_ref_requires_cross_namespace() {
        let rg = conformance_all_in_namespace_grant();
        assert!(!super::gateway_spec_has_grantable_secret_ref(
            &rg,
            "Gateway",
            "gateway-conformance-infra",
            "gateway-conformance-infra",
            "local-secret",
        ));
    }
}
