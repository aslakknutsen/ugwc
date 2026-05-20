//! CRD-backed resource watchers for the gateway reconcile graph (started/stopped by `crd_supervisor`).

use std::collections::HashSet;

use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::resources::{
    GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet, ReferenceGrant,
    TCPRoute, TLSRoute, UDPRoute,
};
use crate::watch_helpers::{
    gateway_targets_for_backend_tls_policy, gateway_targets_for_grpc_route,
    gateway_targets_for_inference_pool, gateway_targets_for_route, gateway_targets_for_tcp_route,
    gateway_targets_for_tls_route, gateway_targets_for_udp_route,
};
use crate::watch_stream::{self, run_stream_loop, watcher_config};

pub(crate) fn spawn_gateway_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<Gateway> = Api::all(client);
        let stream = watch_stream::meta_applied_stream(api, watcher_config());
        run_stream_loop(token.clone(), stream, "gateway", |meta| {
            let tx = tx.clone();
            async move {
                let ns = meta.namespace().unwrap_or_default();
                let name = meta.name_any();
                tx.send(ns, name).await.is_ok()
            }
        })
        .await;
        info!("gateway watcher shutdown");
    });
}

macro_rules! spawn_meta_route_watcher {
    ($spawn_fn:ident, $type:ty, $log_label:literal, $targets_fn:ident) => {
        pub(crate) fn $spawn_fn(
            client: Client,
            tx: crate::reconcile_queue::ReconcileSender,
            token: CancellationToken,
        ) {
            crate::spawn::spawn_with_trace_context(async move {
                let api: Api<$type> = Api::all(client.clone());
                let stream = watch_stream::meta_applied_stream(api, watcher_config());
                run_stream_loop(token.clone(), stream, $log_label, move |meta| {
                    let client = client.clone();
                    let tx = tx.clone();
                    async move {
                        let ns = meta.namespace().unwrap_or_default();
                        let name = meta.name_any();
                        if ns.is_empty() || name.is_empty() {
                            return true;
                        }
                        let obj_api: Api<$type> = Api::namespaced(client.clone(), &ns);
                        let obj = match obj_api.get(&name).await {
                            Ok(o) => o,
                            Err(e) => {
                                warn!(error = %e, %ns, %name, route = $log_label, "get after metadata watch");
                                return true;
                            }
                        };
                        for (pns, pname) in $targets_fn(&client, &obj).await {
                            if tx.send(pns, pname).await.is_err() {
                                return false;
                            }
                        }
                        true
                    }
                })
                .await;
                info!(stream = $log_label, "route watcher shutdown");
            });
        }
    };
}

spawn_meta_route_watcher!(spawn_http_route_watcher, HTTPRoute, "httproute", gateway_targets_for_route);
spawn_meta_route_watcher!(spawn_grpc_route_watcher, GRPCRoute, "grpcroute", gateway_targets_for_grpc_route);
spawn_meta_route_watcher!(spawn_tls_route_watcher, TLSRoute, "tlsroute", gateway_targets_for_tls_route);
spawn_meta_route_watcher!(spawn_tcp_route_watcher, TCPRoute, "tcproute", gateway_targets_for_tcp_route);
spawn_meta_route_watcher!(spawn_udp_route_watcher, UDPRoute, "udproute", gateway_targets_for_udp_route);

pub(crate) fn spawn_inference_pool_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<InferencePool> = Api::all(client.clone());
        let stream = watch_stream::meta_applied_stream(api, watcher_config());
        run_stream_loop(token.clone(), stream, "inferencepool", move |meta| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let pns = meta.namespace().unwrap_or_default();
                let pname = meta.name_any();
                if pns.is_empty() || pname.is_empty() {
                    return true;
                }
                let targets = gateway_targets_for_inference_pool(&client, &pns, &pname).await;
                for (gwns, gwname) in targets {
                    if tx.send(gwns, gwname).await.is_err() {
                        return false;
                    }
                }
                true
            }
        })
        .await;
        info!("inferencepool watcher shutdown");
    });
}

pub(crate) fn spawn_reference_grant_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<ReferenceGrant> = Api::all(client.clone());
        let stream = watch_stream::meta_applied_stream(api, watcher_config());
        run_stream_loop(token.clone(), stream, "referencegrant", move |meta| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let ns = meta.namespace().unwrap_or_default();
                let name = meta.name_any();
                if ns.is_empty() || name.is_empty() {
                    return true;
                }
                let rg_api: Api<ReferenceGrant> = Api::namespaced(client.clone(), &ns);
                let rg = match rg_api.get(&name).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(error = %e, %ns, %name, "get referencegrant after metadata watch");
                        return true;
                    }
                };
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
                                let targets = gateway_targets_for_route(&client, &route).await;
                                for t in targets {
                                    keys.insert(t);
                                }
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
                                let targets =
                                    gateway_targets_for_grpc_route(&client, &route).await;
                                for t in targets {
                                    keys.insert(t);
                                }
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
                                let targets =
                                    gateway_targets_for_tls_route(&client, &route).await;
                                for t in targets {
                                    keys.insert(t);
                                }
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
                                let targets =
                                    gateway_targets_for_tcp_route(&client, &route).await;
                                for t in targets {
                                    keys.insert(t);
                                }
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
                                let targets =
                                    gateway_targets_for_udp_route(&client, &route).await;
                                for t in targets {
                                    keys.insert(t);
                                }
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
                                    let targets =
                                        gateway_targets_for_route(&client, &route).await;
                                    for t in targets {
                                        keys.insert(t);
                                    }
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
                                    let targets =
                                        gateway_targets_for_grpc_route(&client, &route).await;
                                    for t in targets {
                                        keys.insert(t);
                                    }
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
                                    let targets =
                                        gateway_targets_for_tls_route(&client, &route).await;
                                    for t in targets {
                                        keys.insert(t);
                                    }
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
                                    let targets =
                                        gateway_targets_for_tcp_route(&client, &route).await;
                                    for t in targets {
                                        keys.insert(t);
                                    }
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
                                    let targets =
                                        gateway_targets_for_udp_route(&client, &route).await;
                                    for t in targets {
                                        keys.insert(t);
                                    }
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

                for (pns, pname) in keys {
                    if tx.send(pns, pname).await.is_err() {
                        return false;
                    }
                }
                true
            }
        })
        .await;
        info!("referencegrant watcher shutdown");
    });
}

pub(crate) fn spawn_listener_set_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<ListenerSet> = Api::all(client.clone());
        let stream = watch_stream::meta_applied_stream(api, watcher_config());
        run_stream_loop(token.clone(), stream, "listenerset", move |meta| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let ns = meta.namespace().unwrap_or_default();
                let name = meta.name_any();
                if ns.is_empty() || name.is_empty() {
                    return true;
                }
                let ls_api: Api<ListenerSet> = Api::namespaced(client, &ns);
                let ls = match ls_api.get(&name).await {
                    Ok(l) => l,
                    Err(e) => {
                        warn!(error = %e, %ns, %name, "get listenerset after metadata watch");
                        return true;
                    }
                };
                let pr = &ls.spec.parent_ref;
                let pg = pr.group.as_deref().unwrap_or("gateway.networking.k8s.io");
                let pk = pr.kind.as_deref().unwrap_or("Gateway");
                if pg == "gateway.networking.k8s.io" && pk == "Gateway" {
                    let gwns = pr
                        .namespace
                        .clone()
                        .or(ls.metadata.namespace.clone())
                        .unwrap_or_default();
                    return tx.send(gwns, pr.name.clone()).await.is_ok();
                }
                true
            }
        })
        .await;
        info!("listenerset watcher shutdown");
    });
}

pub(crate) fn spawn_gateway_class_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    owned_controller_names: Vec<String>,
) {
    let owned: HashSet<String> = owned_controller_names.iter().cloned().collect();
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<GatewayClass> = Api::all(client.clone());
        let stream = watch_stream::meta_applied_stream(api, watcher_config());
        run_stream_loop(token.clone(), stream, "gatewayclass", move |meta| {
            let client = client.clone();
            let tx = tx.clone();
            let owned = owned.clone();
            async move {
                let name = meta.name_any();
                if name.is_empty() {
                    return true;
                }
                let gc_api: Api<GatewayClass> = Api::all(client.clone());
                let gc = match gc_api.get(&name).await {
                    Ok(g) => g,
                    Err(e) => {
                        warn!(error = %e, %name, "get gatewayclass after metadata watch");
                        return true;
                    }
                };
                if !owned.contains(&gc.spec.controller_name) {
                    return true;
                }
                if let Err(e) =
                    crate::status::apply_gateway_class_accepted_stub(&client, &gc).await
                {
                    warn!(error = %e, "gatewayclass status patch");
                }
                let class_name = gc.name_any();
                let gw_api: Api<Gateway> = Api::all(client.clone());
                match gw_api.list(&ListParams::default()).await {
                    Ok(list) => {
                        for gw in list.items {
                            if gw.spec.gateway_class_name != class_name {
                                continue;
                            }
                            let gw_name = gw.name_any();
                            let ns = gw.namespace().unwrap_or_default();
                            if tx.send(ns, gw_name).await.is_err() {
                                return false;
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "list gateways after gatewayclass change"),
                }
                true
            }
        })
        .await;
        info!("gatewayclass watcher shutdown");
    });
}

spawn_meta_route_watcher!(
    spawn_backend_tls_policy_watcher,
    crate::resources::BackendTLSPolicy,
    "backendtlspolicy",
    gateway_targets_for_backend_tls_policy
);
