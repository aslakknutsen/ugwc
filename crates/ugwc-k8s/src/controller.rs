//! Multi-watch controller graph: `Gateway`, `HTTPRoute`, `EndpointSlice` → reconcile key.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use futures::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::core::v1::Secret;
use kube::api::ListParams;
use kube::runtime::watcher::{watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, ResourceExt};
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::resources::{
    gateway_spec_references_secret, listenerset_spec_references_secret, Gateway, GatewayClass,
    HTTPRoute, ListenerSet, ReferenceGrant,
};

pub type ReconcileFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type ReconcileFn = Arc<dyn Fn(String, String) -> ReconcileFuture + Send + Sync>;

fn is_gateway_parent(p: &crate::resources::ParentReferenceSpec) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    g == "gateway.networking.k8s.io" && k == "Gateway"
}

fn is_listenerset_parent(p: &crate::resources::ParentReferenceSpec) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("ListenerSet");
    g == "gateway.networking.k8s.io" && k == "ListenerSet"
}

async fn gateway_targets_for_route(client: &Client, route: &HTTPRoute) -> HashSet<(String, String)> {
    let mut targets = HashSet::new();
    for p in &route.spec.parent_refs {
        if is_gateway_parent(p) {
            let pns = p
                .namespace
                .clone()
                .or(route.metadata.namespace.clone())
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
            .or(route.metadata.namespace.clone())
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
                warn!(
                    error = %e,
                    route = %route.name_any(),
                    "httproute listenerset parent resolve"
                );
            }
        }
    }
    targets
}

/// Observe `Gateway`, `HTTPRoute`, `ListenerSet`, `ReferenceGrant`, `EndpointSlice`, and `GatewayClass` (owned classes) cluster-wide until cancelled.
#[instrument(skip(client, shutdown, reconcile))]
pub async fn run_gateway_controller(
    client: Client,
    shutdown: tokio_util::sync::CancellationToken,
    endpoint_debounce: Duration,
    reconcile: ReconcileFn,
    owned_controller_names: Vec<String>,
) {
    let (tx, mut rx) = mpsc::channel::<(String, String)>(256);

    let sd_gw = shutdown.clone();
    let c_gw = client.clone();
    let tx_gw = tx.clone();
    tokio::spawn(async move {
        let api: Api<Gateway> = Api::all(c_gw);
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_gw.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(gw)) => {
                            let ns = gw.namespace().unwrap_or_default();
                            let name = gw.name_any();
                            if tx_gw.send((ns, name)).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "gateway watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("gateway watcher shutdown");
    });

    let sd_hr = shutdown.clone();
    let c_hr = client.clone();
    let tx_hr = tx.clone();
    tokio::spawn(async move {
        let api: Api<HTTPRoute> = Api::all(c_hr.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_hr.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            let targets = gateway_targets_for_route(&c_hr, &route).await;
                            for (pns, pname) in targets {
                                if tx_hr.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "httproute watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("httproute watcher shutdown");
    });

    let sd_rg = shutdown.clone();
    let c_rg = client.clone();
    let tx_rg = tx.clone();
    tokio::spawn(async move {
        let api: Api<ReferenceGrant> = Api::all(c_rg.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_rg.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(_)) => {
                            let hr_api: Api<HTTPRoute> = Api::all(c_rg.clone());
                            match hr_api.list(&ListParams::default()).await {
                                Ok(list) => {
                                    for route in list.items {
                                        let targets = gateway_targets_for_route(&c_rg, &route).await;
                                        for (pns, pname) in targets {
                                            if tx_rg.send((pns, pname)).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list httproute after referencegrant change"),
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "referencegrant watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("referencegrant watcher shutdown");
    });

    let sd_ls = shutdown.clone();
    let c_ls = client.clone();
    let tx_ls = tx.clone();
    tokio::spawn(async move {
        let api: Api<ListenerSet> = Api::all(c_ls);
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_ls.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(ls)) => {
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
                                if tx_ls.send((gwns, pr.name.clone())).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "listenerset watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("listenerset watcher shutdown");
    });

    let sd_ep = shutdown.clone();
    let c_ep = client.clone();
    let tx_ep = tx.clone();
    tokio::spawn(async move {
        let api: Api<EndpointSlice> = Api::all(c_ep.clone());
        let gw_api: Api<Gateway> = Api::all(c_ep);
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_ep.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "endpointslice watch stream"),
                        Some(Ok(_)) => {
                            tokio::time::sleep(endpoint_debounce).await;
                            if sd_ep.is_cancelled() {
                                break;
                            }
                            match gw_api.list(&ListParams::default()).await {
                                Ok(list) => {
                                    for gw in list.items {
                                        let name = gw.name_any();
                                        let ns = gw.namespace().unwrap_or_default();
                                        if tx_ep.send((ns, name)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list gateways after endpointslice change"),
                            }
                        }
                    }
                }
            }
        }
        info!("endpointslice watcher shutdown");
    });

    let owned: HashSet<String> = owned_controller_names.iter().cloned().collect();

    let sd_gc = shutdown.clone();
    let c_gc = client.clone();
    let tx_gc = tx.clone();
    tokio::spawn(async move {
        let api: Api<GatewayClass> = Api::all(c_gc.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_gc.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(gc)) => {
                            if !owned.contains(&gc.spec.controller_name) {
                                continue;
                            }
                            if let Err(e) =
                                crate::status::apply_gateway_class_accepted_stub(&c_gc, &gc).await
                            {
                                warn!(error = %e, "gatewayclass status patch");
                            }
                            let class_name = gc.name_any();
                            let gw_api: Api<Gateway> = Api::all(c_gc.clone());
                            match gw_api.list(&ListParams::default()).await {
                                Ok(list) => {
                                    for gw in list.items {
                                        if gw.spec.gateway_class_name != class_name {
                                            continue;
                                        }
                                        let name = gw.name_any();
                                        let ns = gw.namespace().unwrap_or_default();
                                        if tx_gc.send((ns, name)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list gateways after gatewayclass change"),
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "gatewayclass watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("gatewayclass watcher shutdown");
    });

    let sd_sec = shutdown.clone();
    let c_sec = client.clone();
    let tx_sec = tx.clone();
    tokio::spawn(async move {
        let api: Api<Secret> = Api::all(c_sec.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_sec.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(sec)) => {
                            let sns = sec.namespace().unwrap_or_default();
                            let sn = sec.name_any();
                            let gw_api: Api<Gateway> = Api::all(c_sec.clone());
                            match gw_api.list(&ListParams::default()).await {
                                Ok(list) => {
                                    for gw in list.items {
                                        let gwns = gw.namespace().unwrap_or_default();
                                        if gateway_spec_references_secret(&gw.spec, &gwns, &sns, &sn) {
                                            if tx_sec.send((gwns, gw.name_any())).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list gateways after secret change"),
                            }
                            let ls_api: Api<ListenerSet> = Api::all(c_sec.clone());
                            match ls_api.list(&ListParams::default()).await {
                                Ok(ls_list) => {
                                    for ls in ls_list.items {
                                        let ls_ns = ls.namespace().unwrap_or_default();
                                        if !listenerset_spec_references_secret(&ls.spec, &ls_ns, &sns, &sn) {
                                            continue;
                                        }
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
                                            if tx_sec.send((gwns, pr.name.clone())).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list listenersets after secret change"),
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "secret watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("secret watcher shutdown");
    });

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("graph controller shutdown");
                break;
            }
            pair = rx.recv() => {
                let Some((ns, name)) = pair else { break };
                if let Err(e) = reconcile(ns, name).await {
                    warn!(error = %e, "reconcile");
                }
            }
        }
    }
}
