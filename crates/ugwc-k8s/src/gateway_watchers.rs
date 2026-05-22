//! CRD-backed resource watchers for the gateway reconcile graph (started/stopped by `crd_supervisor`).

use std::collections::HashSet;

use kube::runtime::reflector::{self, Store};
use kube::{Api, Client, ResourceExt};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::resources::{
    BackendTLSPolicy, GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};
use crate::watch_enqueue::{
    drain_gateway_keys, gateway_key_for_listener_set, gateway_keys_for_backend_tls_policy,
    gateway_keys_for_gateway_class, gateway_keys_for_grpc_route, gateway_keys_for_http_route,
    gateway_keys_for_inference_pool, gateway_keys_for_reference_grant,
    gateway_keys_for_tcp_route, gateway_keys_for_tls_route, gateway_keys_for_udp_route,
};
use crate::watch_stream::{self, run_watch_loop, watcher_config, WatchItem};

pub(crate) fn spawn_gateway_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    writer: reflector::store::Writer<Gateway>,
) -> Store<Gateway> {
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<Gateway>::all(client),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "gateway", move |item| {
            let tx = tx.clone();
            async move {
                let gw = match item {
                    WatchItem::Applied(g) | WatchItem::Deleted(g) => g,
                };
                let ns = gw.namespace().unwrap_or_default();
                let name = gw.name_any();
                tx.send(ns, name).await.is_ok()
            }
        })
        .await;
        info!("gateway watcher shutdown");
    });
    store
}

macro_rules! spawn_reflector_route_watcher {
    ($spawn_fn:ident, $type:ty, $log_label:literal, $keys_fn:ident) => {
        pub(crate) fn $spawn_fn(
            client: Client,
            tx: crate::reconcile_queue::ReconcileSender,
            token: CancellationToken,
            writer: reflector::store::Writer<$type>,
        ) -> Store<$type> {
            let (store, stream) = watch_stream::reflector_full_watch_stream(
                Api::<$type>::all(client.clone()),
                watcher_config(),
                writer,
            );
            crate::spawn::spawn_with_trace_context(async move {
                run_watch_loop(token.clone(), stream, $log_label, move |item| {
                    let client = client.clone();
                    let tx = tx.clone();
                    async move {
                        let route = match item {
                            WatchItem::Applied(r) | WatchItem::Deleted(r) => r,
                        };
                        let keys = $keys_fn(&client, &route).await;
                        drain_gateway_keys(keys, &tx).await
                    }
                })
                .await;
                info!(stream = $log_label, "route watcher shutdown");
            });
            store
        }
    };
}

spawn_reflector_route_watcher!(
    spawn_http_route_watcher,
    HTTPRoute,
    "httproute",
    gateway_keys_for_http_route
);
spawn_reflector_route_watcher!(
    spawn_grpc_route_watcher,
    GRPCRoute,
    "grpcroute",
    gateway_keys_for_grpc_route
);
spawn_reflector_route_watcher!(
    spawn_tls_route_watcher,
    TLSRoute,
    "tlsroute",
    gateway_keys_for_tls_route
);
spawn_reflector_route_watcher!(
    spawn_tcp_route_watcher,
    TCPRoute,
    "tcproute",
    gateway_keys_for_tcp_route
);
spawn_reflector_route_watcher!(
    spawn_udp_route_watcher,
    UDPRoute,
    "udproute",
    gateway_keys_for_udp_route
);

pub(crate) fn spawn_inference_pool_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    writer: reflector::store::Writer<InferencePool>,
) -> Store<InferencePool> {
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<InferencePool>::all(client.clone()),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "inferencepool", move |item| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let ip = match item {
                    WatchItem::Applied(p) | WatchItem::Deleted(p) => p,
                };
                let pns = ip.namespace().unwrap_or_default();
                let pname = ip.name_any();
                if pns.is_empty() || pname.is_empty() {
                    return true;
                }
                let keys = gateway_keys_for_inference_pool(&client, &pns, &pname).await;
                drain_gateway_keys(keys, &tx).await
            }
        })
        .await;
        info!("inferencepool watcher shutdown");
    });
    store
}

pub(crate) fn spawn_reference_grant_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    writer: reflector::store::Writer<ReferenceGrant>,
) -> Store<ReferenceGrant> {
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<ReferenceGrant>::all(client.clone()),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "referencegrant", move |item| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let rg = match item {
                    WatchItem::Applied(r) | WatchItem::Deleted(r) => r,
                };
                let keys = gateway_keys_for_reference_grant(&client, &rg).await;
                drain_gateway_keys(keys, &tx).await
            }
        })
        .await;
        info!("referencegrant watcher shutdown");
    });
    store
}

pub(crate) fn spawn_listener_set_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    writer: reflector::store::Writer<ListenerSet>,
) -> Store<ListenerSet> {
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<ListenerSet>::all(client),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "listenerset", move |item| {
            let tx = tx.clone();
            async move {
                let ls = match item {
                    WatchItem::Applied(l) | WatchItem::Deleted(l) => l,
                };
                match gateway_key_for_listener_set(&ls) {
                    Some((gwns, gwname)) => tx.send(gwns, gwname).await.is_ok(),
                    None => true,
                }
            }
        })
        .await;
        info!("listenerset watcher shutdown");
    });
    store
}

pub(crate) fn spawn_gateway_class_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    owned_controller_names: Vec<String>,
    writer: reflector::store::Writer<GatewayClass>,
) -> Store<GatewayClass> {
    let owned: HashSet<String> = owned_controller_names.iter().cloned().collect();
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<GatewayClass>::all(client.clone()),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "gatewayclass", move |item| {
            let client = client.clone();
            let tx = tx.clone();
            let owned = owned.clone();
            async move {
                let class_name = match &item {
                    WatchItem::Applied(gc) | WatchItem::Deleted(gc) => gc.name_any(),
                };
                if class_name.is_empty() {
                    return true;
                }
                if let WatchItem::Applied(ref gc) = item {
                    if owned.contains(&gc.spec.controller_name) {
                        if let Err(e) =
                            crate::status::apply_gateway_class_accepted_stub(&client, gc).await
                        {
                            warn!(error = %e, "gatewayclass status patch");
                        }
                    }
                }
                let keys = gateway_keys_for_gateway_class(&client, &class_name).await;
                drain_gateway_keys(keys, &tx).await
            }
        })
        .await;
        info!("gatewayclass watcher shutdown");
    });
    store
}

pub(crate) fn spawn_backend_tls_policy_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
    writer: reflector::store::Writer<BackendTLSPolicy>,
) -> Store<BackendTLSPolicy> {
    let (store, stream) = watch_stream::reflector_full_watch_stream(
        Api::<BackendTLSPolicy>::all(client.clone()),
        watcher_config(),
        writer,
    );
    crate::spawn::spawn_with_trace_context(async move {
        run_watch_loop(token.clone(), stream, "backendtlspolicy", move |item| {
            let client = client.clone();
            let tx = tx.clone();
            async move {
                let pol = match item {
                    WatchItem::Applied(p) | WatchItem::Deleted(p) => p,
                };
                let keys = gateway_keys_for_backend_tls_policy(&client, &pol).await;
                drain_gateway_keys(keys, &tx).await
            }
        })
        .await;
        info!("backendtlspolicy watcher shutdown");
    });
    store
}
