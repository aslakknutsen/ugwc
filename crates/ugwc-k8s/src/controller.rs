//! Multi-watch controller graph: CRD-gated `Gateway`, routes, policies, classes, pools (via
//! [`crate::crd_supervisor`]); always-on `EndpointSlice`, `Secret`, `ConfigMap` (BackendTLS CA),
//! Namespace, and all `Service` objects (route backend resolution plus managed dataplane
//! LoadBalancer IP propagation to `Gateway.status.addresses`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::reflector;
use kube::{Api, Client, ResourceExt};
use tracing::{debug, info, instrument, warn};

use crate::crd_supervisor::{run_crd_supervisor, ActiveCrds};
use crate::gateway_index::GatewayIndex;
use crate::initial_sync::InitialSyncGate;
use crate::reconcile_queue::{ReconcileQueue, ReconcileSender};
use crate::stores::SharedStores;
use crate::watch_enqueue::{drain_gateway_keys, gateway_key_for_managed_service};
use crate::watch_stream::{self, watcher_config, WatchItem};

pub type ReconcileFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type ReconcileFn = Arc<dyn Fn(String, String) -> ReconcileFuture + Send + Sync>;

/// Create reflector writers and build [`SharedStores`] from their readers.
/// Returns the stores (for sharing with `ReconcileFn`) and the writers (for the controller).
pub fn build_shared_stores() -> (Arc<SharedStores>, AlwaysOnWriters) {
    let ep_writer = reflector::store::Writer::<EndpointSlice>::default();
    let ep_store = ep_writer.as_reader();

    let svc_writer = reflector::store::Writer::<Service>::default();
    let svc_store = svc_writer.as_reader();

    let sec_writer = reflector::store::Writer::<Secret>::default();
    let sec_store = sec_writer.as_reader();

    let cm_writer = reflector::store::Writer::<ConfigMap>::default();
    let cm_store = cm_writer.as_reader();

    let ns_writer = reflector::store::Writer::<Namespace>::default();
    let ns_store = ns_writer.as_reader();

    let stores = Arc::new(SharedStores::new(
        ep_store, svc_store, sec_store, cm_store, ns_store,
    ));
    let writers = AlwaysOnWriters {
        endpoint_slices: ep_writer,
        services: svc_writer,
        secrets: sec_writer,
        config_maps: cm_writer,
        namespaces: ns_writer,
    };
    (stores, writers)
}

/// Writers for always-on reflector streams, consumed by [`run_gateway_controller`].
pub struct AlwaysOnWriters {
    pub endpoint_slices: reflector::store::Writer<EndpointSlice>,
    pub services: reflector::store::Writer<Service>,
    pub secrets: reflector::store::Writer<Secret>,
    pub config_maps: reflector::store::Writer<ConfigMap>,
    pub namespaces: reflector::store::Writer<Namespace>,
}

/// Observe gateways (CRD-gated via [`run_crd_supervisor`]), routes, grants, backend TLS policies,
/// CA ConfigMaps, endpoints, secrets, classes, and inference pools.
#[instrument(skip(
    client,
    shutdown,
    reconcile,
    work_queue,
    reconcile_tx,
    sync_gate,
    stores,
    writers,
    gateway_index
))]
pub async fn run_gateway_controller(
    client: Client,
    shutdown: tokio_util::sync::CancellationToken,
    endpoint_debounce: Duration,
    reconcile_tx: ReconcileSender,
    work_queue: ReconcileQueue,
    reconcile: ReconcileFn,
    reconcile_concurrency: usize,
    owned_controller_names: Vec<String>,
    active_crds: ActiveCrds,
    sync_gate: Option<Arc<InitialSyncGate>>,
    stores: Arc<SharedStores>,
    writers: AlwaysOnWriters,
    gateway_index: Arc<GatewayIndex>,
) {
    let tx = reconcile_tx;

    // ── Initial sync via store readiness ─────────────────────────────────
    if let Some(gate) = sync_gate.clone() {
        let stores_gate = stores.clone();
        crate::spawn::spawn_with_trace_context(async move {
            let _ = stores_gate.endpoint_slices.wait_until_ready().await;
            gate.mark_endpointslice_list();
        });
    }

    // ── CRD supervisor (populates CRD-gated stores) ─────────────────────
    let c_sup = client.clone();
    let sd_sup = shutdown.clone();
    let tx_sup = tx.clone();
    let owned_sup = owned_controller_names.clone();
    let active_sup = active_crds.clone();
    let gate_sup = sync_gate.clone();
    let stores_sup = stores.clone();
    crate::spawn::spawn_with_trace_context(async move {
        run_crd_supervisor(
            c_sup, sd_sup, tx_sup, owned_sup, active_sup, gate_sup, stores_sup,
        )
        .await;
    });

    // ── EndpointSlice watcher (reflector-backed) ────────────────────────
    let sd_ep = shutdown.clone();
    let tx_ep = tx.clone();
    let stores_ep = stores.clone();
    let index_ep = gateway_index.clone();
    let (_, mut ep_stream) = watch_stream::reflector_full_watch_stream(
        Api::<EndpointSlice>::all(client.clone()),
        watcher_config(),
        writers.endpoint_slices,
    );
    crate::spawn::spawn_with_trace_context(async move {
        loop {
            tokio::select! {
                _ = sd_ep.cancelled() => break,
                ev = ep_stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "endpointslice watch stream"),
                        Some(Ok(WatchItem::Applied(es) | WatchItem::Deleted(es))) => {
                            tokio::time::sleep(endpoint_debounce).await;
                            if sd_ep.is_cancelled() {
                                break;
                            }
                            let svc_ns = es.namespace().unwrap_or_default();
                            let Some(svc_name) = es
                                .metadata
                                .labels
                                .as_ref()
                                .and_then(|l| l.get("kubernetes.io/service-name").cloned())
                            else {
                                debug!(endpointslice_ns = %svc_ns, "endpointslice event without service-name label");
                                continue;
                            };
                            let keys = index_ep.resolve_service_fanout(&stores_ep, &svc_ns, &svc_name);
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

    // ── Service watcher (reflector-backed, all Services for backend resolution) ─
    let sd_svc = shutdown.clone();
    let tx_svc = tx.clone();
    let stores_svc = stores.clone();
    let index_svc = gateway_index.clone();
    let (_, mut svc_stream) = watch_stream::reflector_full_watch_stream(
        Api::<Service>::all(client.clone()),
        watcher_config(),
        writers.services,
    );
    crate::spawn::spawn_with_trace_context(async move {
        loop {
            tokio::select! {
                _ = sd_svc.cancelled() => break,
                ev = svc_stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "service watch stream"),
                        Some(Ok(WatchItem::Applied(svc) | WatchItem::Deleted(svc))) => {
                            let svc_ns = svc.namespace().unwrap_or_default();
                            let svc_name = svc.name_any();
                            let keys = index_svc.resolve_service_fanout(&stores_svc, &svc_ns, &svc_name);
                            if !drain_gateway_keys(keys, &tx_svc).await {
                                return;
                            }
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

    // ── Secret watcher (reflector-backed) ───────────────────────────────
    let sd_sec = shutdown.clone();
    let tx_sec = tx.clone();
    let stores_sec = stores.clone();
    let index_sec = gateway_index.clone();
    let client_sec = client.clone();
    let controller_name_sec = owned_controller_names
        .first()
        .cloned()
        .unwrap_or_else(|| "ugwc.io/default-controller".into());
    let secret_debounce = endpoint_debounce;
    let (_, mut sec_stream) = watch_stream::reflector_secret_watch_stream(
        Api::<Secret>::all(client.clone()),
        watcher_config(),
        writers.secrets,
    );
    crate::spawn::spawn_with_trace_context(async move {
        loop {
            tokio::select! {
                _ = sd_sec.cancelled() => break,
                ev = sec_stream.next() => {
                    match ev {
                        Some(Ok(WatchItem::Applied(sec) | WatchItem::Deleted(sec))) => {
                            let sns = sec.namespace().unwrap_or_default();
                            let sn = sec.name_any();
                            tokio::time::sleep(secret_debounce).await;
                            if sd_sec.is_cancelled() {
                                break;
                            }
                            crate::status::apply_backend_tls_status_for_ca_ref_change(
                                &client_sec,
                                &stores_sec,
                                &controller_name_sec,
                                &sns,
                                &sn,
                                false,
                            )
                            .await;
                            let keys = index_sec.resolve_secret_fanout(&stores_sec, &sns, &sn);
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

    // ── ConfigMap watcher (reflector-backed) ────────────────────────────
    let sd_cm = shutdown.clone();
    let tx_cm = tx.clone();
    let stores_cm = stores.clone();
    let index_cm = gateway_index.clone();
    let client_cm = client.clone();
    let controller_name_cm = owned_controller_names
        .first()
        .cloned()
        .unwrap_or_else(|| "ugwc.io/default-controller".into());
    let (_, mut cm_stream) = watch_stream::reflector_configmap_watch_stream(
        Api::<ConfigMap>::all(client.clone()),
        watcher_config(),
        writers.config_maps,
    );
    crate::spawn::spawn_with_trace_context(async move {
        loop {
            tokio::select! {
                _ = sd_cm.cancelled() => break,
                ev = cm_stream.next() => {
                    match ev {
                        Some(Ok(WatchItem::Applied(cm) | WatchItem::Deleted(cm))) => {
                            let cm_ns = cm.namespace().unwrap_or_default();
                            let cm_name = cm.name_any();
                            crate::status::apply_backend_tls_status_for_ca_ref_change(
                                &client_cm,
                                &stores_cm,
                                &controller_name_cm,
                                &cm_ns,
                                &cm_name,
                                true,
                            )
                            .await;
                            let keys = index_cm.resolve_configmap_fanout(&stores_cm, &cm_ns, &cm_name);
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

    // ── Namespace watcher (reflector-backed, no enqueue) ────────────────
    let sd_ns = shutdown.clone();
    let (_, mut ns_stream) = watch_stream::reflector_full_watch_stream(
        Api::<Namespace>::all(client.clone()),
        watcher_config(),
        writers.namespaces,
    );
    crate::spawn::spawn_with_trace_context(async move {
        loop {
            tokio::select! {
                _ = sd_ns.cancelled() => break,
                ev = ns_stream.next() => {
                    match ev {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => warn!(error = %e, "namespace watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("namespace watcher shutdown");
    });

    // ── Reconcile workers (concurrent, dedup queue per-key lock) ─────────
    let mut workers = tokio::task::JoinSet::new();
    for worker_id in 0..reconcile_concurrency {
        let wq = work_queue.clone();
        let reconcile = reconcile.clone();
        let sd = shutdown.clone();
        workers.spawn(async move {
            reconcile_worker(worker_id, sd, wq, reconcile).await;
        });
    }

    while workers.join_next().await.is_some() {}
    info!("reconcile workers shutdown");
}

async fn reconcile_worker(
    worker_id: usize,
    shutdown: tokio_util::sync::CancellationToken,
    work_queue: ReconcileQueue,
    reconcile: ReconcileFn,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            pair = work_queue.recv() => {
                let Some((ns, name)) = pair else { break };
                let proc_started = work_queue.begin_reconcile();
                let result = reconcile(ns.clone(), name.clone()).await;
                work_queue.end_reconcile(proc_started);
                work_queue.done(&ns, &name);
                if let Err(e) = result {
                    work_queue.record_retry();
                    warn!(error = %e, worker_id, "reconcile");
                }
            }
        }
    }
}
