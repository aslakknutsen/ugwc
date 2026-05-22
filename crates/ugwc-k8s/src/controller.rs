//! Multi-watch controller graph: CRD-gated `Gateway`, routes, policies, classes, pools (via
//! [`crate::crd_supervisor`]); always-on `EndpointSlice`, `Secret`, `ConfigMap` (BackendTLS CA),
//! and managed `Service` (for LoadBalancer IP propagation to `Gateway.status.addresses`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use tracing::{info, instrument, warn};

use crate::crd_supervisor::{run_crd_supervisor, ActiveCrds};
use crate::initial_sync::InitialSyncGate;
use crate::reconcile_queue::{ReconcileQueue, ReconcileSender};
use crate::resources::Gateway;
use crate::watch_enqueue::{
    drain_gateway_keys, gateway_key_for_managed_service, gateway_keys_all,
    gateway_keys_for_configmap, gateway_keys_for_secret,
};
use crate::watch_stream::{self, watcher_config, WatchItem};

pub type ReconcileFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type ReconcileFn = Arc<dyn Fn(String, String) -> ReconcileFuture + Send + Sync>;

/// Observe gateways (CRD-gated via [`run_crd_supervisor`]), routes, grants, backend TLS policies,
/// CA ConfigMaps, endpoints, secrets, classes, and inference pools.
#[instrument(skip(client, shutdown, reconcile, work_queue, reconcile_tx, sync_gate))]
pub async fn run_gateway_controller(
    client: Client,
    shutdown: tokio_util::sync::CancellationToken,
    endpoint_debounce: Duration,
    reconcile_tx: ReconcileSender,
    mut work_queue: ReconcileQueue,
    reconcile: ReconcileFn,
    owned_controller_names: Vec<String>,
    active_crds: ActiveCrds,
    sync_gate: Option<Arc<InitialSyncGate>>,
) {
    let tx = reconcile_tx;

    if let Some(gate) = sync_gate.clone() {
        let c_list = client.clone();
        let sd_list = shutdown.clone();
        crate::spawn::spawn_with_trace_context(async move {
            let gw_api: Api<Gateway> = Api::all(c_list.clone());
            match gw_api.list(&ListParams::default()).await {
                Ok(_) => gate.mark_gateway_list(),
                Err(e) if !sd_list.is_cancelled() => {
                    warn!(error = %e, "initial gateway list for readiness");
                }
                Err(_) => {}
            }
            if sd_list.is_cancelled() {
                return;
            }
            let ep_api: Api<EndpointSlice> = Api::all(c_list);
            match ep_api.list(&ListParams::default()).await {
                Ok(_) => gate.mark_endpointslice_list(),
                Err(e) if !sd_list.is_cancelled() => {
                    warn!(error = %e, "initial endpointslice list for readiness");
                }
                Err(_) => {}
            }
        });
    }

    let c_sup = client.clone();
    let sd_sup = shutdown.clone();
    let tx_sup = tx.clone();
    let owned_sup = owned_controller_names.clone();
    let active_sup = active_crds.clone();
    let gate_sup = sync_gate.clone();
    crate::spawn::spawn_with_trace_context(async move {
        run_crd_supervisor(c_sup, sd_sup, tx_sup, owned_sup, active_sup, gate_sup).await;
    });

    let sd_ep = shutdown.clone();
    let c_ep = client.clone();
    let tx_ep = tx.clone();
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<EndpointSlice> = Api::all(c_ep.clone());
        let mut stream = watch_stream::full_watch_stream(api, watcher_config());
        loop {
            tokio::select! {
                _ = sd_ep.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "endpointslice watch stream"),
                        Some(Ok(WatchItem::Applied(_) | WatchItem::Deleted(_))) => {
                            tokio::time::sleep(endpoint_debounce).await;
                            if sd_ep.is_cancelled() {
                                break;
                            }
                            let keys = gateway_keys_all(&c_ep).await;
                            if !drain_gateway_keys(keys, &tx_ep).await {
                                return;
                            }
                        }
                    }
                }
            }
        }
        info!("endpointslice watcher shutdown");
    });

    let sd_svc = shutdown.clone();
    let tx_svc = tx.clone();
    let c_svc = client.clone();
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<Service> = Api::all(c_svc);
        let cfg = watcher_config().labels("gateway.ugwc.io/managed=ugwc-per-gateway");
        let mut stream = watch_stream::full_watch_stream(api, cfg);
        loop {
            tokio::select! {
                _ = sd_svc.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "service watch stream"),
                        Some(Ok(WatchItem::Applied(svc) | WatchItem::Deleted(svc))) => {
                            if let Some((gw_ns, gw_name)) = gateway_key_for_managed_service(&svc) {
                                if tx_svc.send(gw_ns, gw_name).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
        info!("service watcher shutdown");
    });

    let sd_sec = shutdown.clone();
    let c_sec = client.clone();
    let tx_sec = tx.clone();
    let secret_debounce = endpoint_debounce;
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<Secret> = Api::all(c_sec.clone());
        let mut stream = watch_stream::full_watch_stream(api, watcher_config());
        loop {
            tokio::select! {
                _ = sd_sec.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(WatchItem::Applied(sec) | WatchItem::Deleted(sec))) => {
                            let sns = sec.namespace().unwrap_or_default();
                            let sn = sec.name_any();
                            tokio::time::sleep(secret_debounce).await;
                            if sd_sec.is_cancelled() {
                                break;
                            }
                            let keys = gateway_keys_for_secret(&c_sec, &sns, &sn).await;
                            if !drain_gateway_keys(keys, &tx_sec).await {
                                return;
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

    let sd_cm = shutdown.clone();
    let c_cm = client.clone();
    let tx_cm = tx.clone();
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<ConfigMap> = Api::all(c_cm.clone());
        let mut stream = watch_stream::full_watch_stream(api, watcher_config());
        loop {
            tokio::select! {
                _ = sd_cm.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(WatchItem::Applied(cm) | WatchItem::Deleted(cm))) => {
                            let cm_ns = cm.namespace().unwrap_or_default();
                            let cm_name = cm.name_any();
                            let keys = gateway_keys_for_configmap(&c_cm, &cm_ns, &cm_name).await;
                            if !drain_gateway_keys(keys, &tx_cm).await {
                                return;
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "configmap watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("configmap watcher shutdown");
    });

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("graph controller shutdown");
                break;
            }
            pair = work_queue.recv() => {
                let Some((ns, name)) = pair else { break };
                let proc_started = work_queue.begin_reconcile();
                let result = reconcile(ns, name).await;
                work_queue.end_reconcile(proc_started);
                if let Err(e) = result {
                    work_queue.record_retry();
                    warn!(error = %e, "reconcile");
                }
            }
        }
    }
}
