//! CRD-backed resource watchers for the gateway reconcile graph (started/stopped by `crd_supervisor`).

use std::collections::HashSet;
use std::time::Duration;

use futures::StreamExt;
use kube::api::ListParams;
use kube::runtime::watcher::{watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, ResourceExt};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::resources::{
    BackendTLSPolicy, GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};
use crate::watch_helpers::{
    gateway_targets_for_backend_tls_policy, gateway_targets_for_grpc_route,
    gateway_targets_for_inference_pool, gateway_targets_for_route, gateway_targets_for_tcp_route,
    gateway_targets_for_tls_route, gateway_targets_for_udp_route,
};

const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct StreamBackoff {
    next: Duration,
}

impl Default for StreamBackoff {
    fn default() -> Self {
        Self { next: BACKOFF_MIN }
    }
}

impl StreamBackoff {
    fn reset(&mut self) {
        self.next = BACKOFF_MIN;
    }

    fn bump(&mut self) {
        self.next = (self.next.saturating_mul(2)).min(BACKOFF_MAX);
    }

    async fn sleep_on_err(&mut self, token: &CancellationToken) {
        let dur = self.next;
        tokio::select! {
            _ = token.cancelled() => {}
            _ = tokio::time::sleep(dur) => {}
        }
        self.bump();
    }
}

pub(crate) fn spawn_gateway_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<Gateway> = Api::all(client);
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(gw)) => {
                            backoff.reset();
                            let ns = gw.namespace().unwrap_or_default();
                            let name = gw.name_any();
                            if tx.send(ns, name).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "gateway watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("gateway watcher shutdown");
    });
}

pub(crate) fn spawn_http_route_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<HTTPRoute> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_route(&client, &route).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "httproute watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("httproute watcher shutdown");
    });
}

pub(crate) fn spawn_grpc_route_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<GRPCRoute> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_grpc_route(&client, &route).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "grpcroute watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("grpcroute watcher shutdown");
    });
}

pub(crate) fn spawn_tls_route_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<TLSRoute> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_tls_route(&client, &route).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "tlsroute watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("tlsroute watcher shutdown");
    });
}

pub(crate) fn spawn_tcp_route_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<TCPRoute> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_tcp_route(&client, &route).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "tcproute watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("tcproute watcher shutdown");
    });
}

pub(crate) fn spawn_udp_route_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<UDPRoute> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_udp_route(&client, &route).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "udproute watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("udproute watcher shutdown");
    });
}

pub(crate) fn spawn_inference_pool_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<InferencePool> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(pool)) => {
                            backoff.reset();
                            let pns = pool.namespace().unwrap_or_default();
                            let pname = pool.name_any();
                            let targets =
                                gateway_targets_for_inference_pool(&client, &pns, &pname).await;
                            for (gwns, gwname) in targets {
                                if tx.send(gwns, gwname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "inferencepool watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("inferencepool watcher shutdown");
    });
}

pub(crate) fn spawn_reference_grant_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<ReferenceGrant> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(rg)) => {
                            backoff.reset();
                            let mut keys: HashSet<(String, String)> = HashSet::new();
                            let grant_ns = rg.metadata.namespace.clone().unwrap_or_default();
                            if !grant_ns.is_empty() {
                                let gw_api: Api<Gateway> =
                                    Api::namespaced(client.clone(), &grant_ns);
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
                                            let targets =
                                                gateway_targets_for_route(&client, &route).await;
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
                                            let targets = gateway_targets_for_grpc_route(
                                                &client,
                                                &route,
                                            )
                                            .await;
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
                                            let targets = gateway_targets_for_tls_route(
                                                &client,
                                                &route,
                                            )
                                            .await;
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
                                            let targets = gateway_targets_for_tcp_route(
                                                &client,
                                                &route,
                                            )
                                            .await;
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
                                            let targets = gateway_targets_for_udp_route(
                                                &client,
                                                &route,
                                            )
                                            .await;
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
                                for ns in route_ns_list {
                                    let hr_api: Api<HTTPRoute> =
                                        Api::namespaced(client.clone(), &ns);
                                    match hr_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_route(&client, &route)
                                                        .await;
                                                for t in targets {
                                                    keys.insert(t);
                                                }
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            route_ns = %ns,
                                            "list httproute after referencegrant change"
                                        ),
                                    }
                                    let gr_api: Api<GRPCRoute> =
                                        Api::namespaced(client.clone(), &ns);
                                    match gr_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_grpc_route(&client, &route)
                                                        .await;
                                                for t in targets {
                                                    keys.insert(t);
                                                }
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            route_ns = %ns,
                                            "list grpcroute after referencegrant change"
                                        ),
                                    }
                                    let tls_api: Api<TLSRoute> =
                                        Api::namespaced(client.clone(), &ns);
                                    match tls_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_tls_route(&client, &route)
                                                        .await;
                                                for t in targets {
                                                    keys.insert(t);
                                                }
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            route_ns = %ns,
                                            "list tlsroute after referencegrant change"
                                        ),
                                    }
                                    let tcp_api: Api<TCPRoute> =
                                        Api::namespaced(client.clone(), &ns);
                                    match tcp_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_tcp_route(&client, &route)
                                                        .await;
                                                for t in targets {
                                                    keys.insert(t);
                                                }
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            route_ns = %ns,
                                            "list tcproute after referencegrant change"
                                        ),
                                    }
                                    let udp_api: Api<UDPRoute> =
                                        Api::namespaced(client.clone(), &ns);
                                    match udp_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_udp_route(&client, &route)
                                                        .await;
                                                for t in targets {
                                                    keys.insert(t);
                                                }
                                            }
                                        }
                                        Err(e) => warn!(
                                            error = %e,
                                            route_ns = %ns,
                                            "list udproute after referencegrant change"
                                        ),
                                    }
                                }
                            }

                            for (pns, pname) in keys {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "referencegrant watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("referencegrant watcher shutdown");
    });
}

pub(crate) fn spawn_listener_set_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<ListenerSet> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(ls)) => {
                            backoff.reset();
                            let pr = &ls.spec.parent_ref;
                            let pg = pr.group.as_deref().unwrap_or("gateway.networking.k8s.io");
                            let pk = pr.kind.as_deref().unwrap_or("Gateway");
                            if pg == "gateway.networking.k8s.io" && pk == "Gateway" {
                                let gwns = pr
                                    .namespace
                                    .clone()
                                    .or(ls.metadata.namespace.clone())
                                    .unwrap_or_default();
                                if tx.send(gwns, pr.name.clone()).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "listenerset watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
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
    tokio::spawn(async move {
        let api: Api<GatewayClass> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(gc)) => {
                            backoff.reset();
                            if !owned.contains(&gc.spec.controller_name) {
                                continue;
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
                                        let name = gw.name_any();
                                        let ns = gw.namespace().unwrap_or_default();
                                        if tx.send(ns, name).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list gateways after gatewayclass change"),
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "gatewayclass watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("gatewayclass watcher shutdown");
    });
}

pub(crate) fn spawn_backend_tls_policy_watcher(
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        let api: Api<BackendTLSPolicy> = Api::all(client.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        let mut backoff = StreamBackoff::default();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(pol)) => {
                            backoff.reset();
                            let targets = gateway_targets_for_backend_tls_policy(&client, &pol).await;
                            for (pns, pname) in targets {
                                if tx.send(pns, pname).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "backendtlspolicy watch stream");
                            backoff.sleep_on_err(&token).await;
                        }
                        None => break,
                    }
                }
            }
        }
        info!("backendtlspolicy watcher shutdown");
    });
}
