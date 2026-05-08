//! Multi-watch controller graph: `Gateway`, `HTTPRoute`, `EndpointSlice` → reconcile key.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use futures::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ListParams;
use kube::runtime::watcher::{watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, ResourceExt};
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::resources::{Gateway, GatewayClass, HTTPRoute};

pub type ReconcileFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type ReconcileFn = Arc<dyn Fn(String, String) -> ReconcileFuture + Send + Sync>;

fn is_gateway_parent(p: &crate::resources::ParentReferenceSpec) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    g == "gateway.networking.k8s.io" && k == "Gateway"
}

/// Observe `Gateway`, `HTTPRoute`, `EndpointSlice`, and `GatewayClass` (owned classes) cluster-wide until cancelled.
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
        let api: Api<HTTPRoute> = Api::all(c_hr);
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_hr.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            for p in &route.spec.parent_refs {
                                if !is_gateway_parent(p) {
                                    continue;
                                }
                                let pns = p
                                    .namespace
                                    .clone()
                                    .or(route.metadata.namespace.clone())
                                    .unwrap_or_default();
                                if tx_hr.send((pns, p.name.clone())).await.is_err() {
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
