//! Multi-watch controller graph: CRD-gated `Gateway`, routes, policies, classes, pools (via
//! [`crate::crd_supervisor`]); always-on `EndpointSlice`, `Secret`, `ConfigMap` (BackendTLS CA),
//! and managed `Service` (for LoadBalancer IP propagation to `Gateway.status.addresses`).

use std::collections::HashSet;
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
use crate::reconcile_queue::{record_workqueue_retry, ReconcileQueue, ReconcileSender, GATEWAY_RECONCILE_QUEUE};
use crate::resources::{
    backend_tls_policy_spec_references_configmap, backend_tls_policy_spec_references_secret,
    gateway_spec_references_secret, listenerset_spec_references_secret, BackendTLSPolicy, Gateway,
    ListenerSet,
};
use crate::watch_helpers::gateway_targets_for_backend_tls_policy;
use crate::watch_stream::{self, watcher_config};

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
        let gw_api: Api<Gateway> = Api::all(c_ep);
        let mut stream = watch_stream::full_applied_stream(api, watcher_config());
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
                                        if tx_ep.send(ns, name).await.is_err() {
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

    // Watch managed dataplane Services so that when a LoadBalancer IP is assigned,
    // the owning Gateway gets re-reconciled to propagate the address to status.
    let sd_svc = shutdown.clone();
    let tx_svc = tx.clone();
    let c_svc = client.clone();
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<Service> = Api::all(c_svc);
        let cfg = watcher_config().labels("gateway.ugwc.io/managed=ugwc-per-gateway");
        let mut stream = watch_stream::full_applied_stream(api, cfg);
        loop {
            tokio::select! {
                _ = sd_svc.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => warn!(error = %e, "service watch stream"),
                        Some(Ok(svc)) => {
                            let labels = svc.metadata.labels.as_ref();
                            let gw_ns = labels
                                .and_then(|l| l.get("gateway.ugwc.io/gateway-namespace"))
                                .cloned()
                                .unwrap_or_default();
                            let gw_name = labels
                                .and_then(|l| l.get("gateway.ugwc.io/gateway-name"))
                                .cloned()
                                .unwrap_or_default();
                            if !gw_ns.is_empty() && !gw_name.is_empty() {
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
    crate::spawn::spawn_with_trace_context(async move {
        let api: Api<Secret> = Api::all(c_sec.clone());
        let mut stream = watch_stream::full_applied_stream(api, watcher_config());
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
                                        if gateway_spec_references_secret(&gw.spec, &gwns, &sns, &sn)
                                            && tx_sec.send(gwns, gw.name_any()).await.is_err()
                                        {
                                            return;
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
                                        let pr = &ls.spec.parent_ref;
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
                                        if tx_sec.send(gwns, pr.name.clone()).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list listenersets after secret change"),
                            }
                            let pol_api: Api<BackendTLSPolicy> = Api::all(c_sec.clone());
                            match pol_api.list(&ListParams::default()).await {
                                Ok(pol_list) => {
                                    let mut gateways = HashSet::new();
                                    for pol in pol_list.items {
                                        let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
                                        if backend_tls_policy_spec_references_secret(
                                            &pol.spec,
                                            pol_ns,
                                            &sns,
                                            &sn,
                                        ) {
                                            gateways.extend(
                                                gateway_targets_for_backend_tls_policy(&c_sec, &pol).await,
                                            );
                                        }
                                    }
                                    for (pns, pname) in gateways {
                                        if tx_sec.send(pns, pname).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                Err(e) => warn!(error = %e, "list BackendTLSPolicy after secret change"),
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
        let mut stream = watch_stream::full_applied_stream(api, watcher_config());
        loop {
            tokio::select! {
                _ = sd_cm.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(cm)) => {
                            let cm_ns = cm.namespace().unwrap_or_default();
                            let cm_name = cm.name_any();
                            let pol_api: Api<BackendTLSPolicy> = Api::all(c_cm.clone());
                            let policies = match pol_api.list(&ListParams::default()).await {
                                Ok(l) => l.items,
                                Err(e) => {
                                    warn!(error = %e, "list BackendTLSPolicy after ConfigMap change");
                                    continue;
                                }
                            };
                            let mut gateways = HashSet::new();
                            for pol in policies {
                                let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
                                if !backend_tls_policy_spec_references_configmap(
                                    &pol.spec,
                                    pol_ns,
                                    &cm_ns,
                                    &cm_name,
                                ) {
                                    continue;
                                }
                                gateways.extend(gateway_targets_for_backend_tls_policy(&c_cm, &pol).await);
                            }
                            for (pns, pname) in gateways {
                                if tx_cm.send(pns, pname).await.is_err() {
                                    return;
                                }
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
                    record_workqueue_retry(GATEWAY_RECONCILE_QUEUE);
                    warn!(error = %e, "reconcile");
                }
            }
        }
    }
}

#[cfg(test)]
mod reference_grant_route_ns_tests {
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
