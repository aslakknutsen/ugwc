//! Multi-watch controller graph: `Gateway`, `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `TCPRoute`, `UDPRoute`, `ListenerSet`,
//! `ReferenceGrant`, `BackendTLSPolicy`, `ConfigMap` (BackendTLS CA), `EndpointSlice`, `Secret`,
//! `GatewayClass`, `InferencePool` (fan-out via routes that reference a pool).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ListParams;
use kube::runtime::watcher::{watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, ResourceExt};
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::resources::{
    backend_tls_policy_spec_references_configmap, backend_tls_policy_targets_service,
    gateway_spec_references_secret, listenerset_spec_references_secret, BackendTLSPolicy,
    GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet, ParentReferenceSpec,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
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

async fn gateway_targets_from_parent_refs(
    client: &Client,
    route_namespace: Option<String>,
    route_log_name: &str,
    parent_refs: &[ParentReferenceSpec],
) -> HashSet<(String, String)> {
    let mut targets = HashSet::new();
    for p in parent_refs {
        if is_gateway_parent(p) {
            let pns = p
                .namespace
                .clone()
                .or_else(|| route_namespace.clone())
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
            .or_else(|| route_namespace.clone())
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
                    route = %route_log_name,
                    "route listenerset parent resolve"
                );
            }
        }
    }
    targets
}

async fn gateway_targets_for_route(
    client: &Client,
    route: &HTTPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

async fn gateway_targets_for_grpc_route(
    client: &Client,
    route: &GRPCRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

async fn gateway_targets_for_tls_route(
    client: &Client,
    route: &TLSRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

async fn gateway_targets_for_tcp_route(
    client: &Client,
    route: &TCPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

async fn gateway_targets_for_udp_route(
    client: &Client,
    route: &UDPRoute,
) -> HashSet<(String, String)> {
    gateway_targets_from_parent_refs(
        client,
        route.metadata.namespace.clone(),
        route.name_any().as_ref(),
        &route.spec.parent_refs,
    )
    .await
}

fn http_route_references_inference_pool(route: &HTTPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn grpc_route_references_inference_pool(route: &GRPCRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tls_route_references_inference_pool(route: &TLSRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tcp_route_references_inference_pool(route: &TCPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn udp_route_references_inference_pool(route: &UDPRoute, pool_ns: &str, pool_name: &str) -> bool {
    let rns = route.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if !crate::resources::is_inference_pool_backend_ref(br) {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(rns);
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

async fn gateway_targets_for_inference_pool(
    client: &Client,
    pool_ns: &str,
    pool_name: &str,
) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    let hr_api: Api<HTTPRoute> = Api::all(client.clone());
    match hr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if http_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_route(client, &route).await);
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list httproute after inferencepool change"
        ),
    }
    let gr_api: Api<GRPCRoute> = Api::all(client.clone());
    match gr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if grpc_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_grpc_route(client, &route).await);
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list grpcroute after inferencepool change"
        ),
    }
    let tr_api: Api<TLSRoute> = Api::all(client.clone());
    match tr_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if tls_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_tls_route(client, &route).await);
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list tlsroute after inferencepool change"
        ),
    }
    let tcp_api: Api<TCPRoute> = Api::all(client.clone());
    match tcp_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if tcp_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_tcp_route(client, &route).await);
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list tcproute after inferencepool change"
        ),
    }
    let udp_api: Api<UDPRoute> = Api::all(client.clone());
    match udp_api.list(&ListParams::default()).await {
        Ok(list) => {
            for route in list.items {
                if udp_route_references_inference_pool(&route, pool_ns, pool_name) {
                    out.extend(gateway_targets_for_udp_route(client, &route).await);
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "list udproute after inferencepool change"
        ),
    }
    out
}

fn http_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &HTTPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let sns = br
                .namespace
                .as_deref()
                .or(route.metadata.namespace.as_deref())
                .unwrap_or("");
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, sns, &br.name) {
                return true;
            }
        }
    }
    false
}

fn grpc_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &GRPCRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let sns = br
                .namespace
                .as_deref()
                .or(route.metadata.namespace.as_deref())
                .unwrap_or("");
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, sns, &br.name) {
                return true;
            }
        }
    }
    false
}

fn tls_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &TLSRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let sns = br
                .namespace
                .as_deref()
                .or(route.metadata.namespace.as_deref())
                .unwrap_or("");
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, sns, &br.name) {
                return true;
            }
        }
    }
    false
}

fn tcp_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &TCPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let sns = br
                .namespace
                .as_deref()
                .or(route.metadata.namespace.as_deref())
                .unwrap_or("");
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, sns, &br.name) {
                return true;
            }
        }
    }
    false
}

fn udp_route_references_backend_tls_policy_target(
    pol: &BackendTLSPolicy,
    route: &UDPRoute,
) -> bool {
    let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
    for rule in &route.spec.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let sns = br
                .namespace
                .as_deref()
                .or(route.metadata.namespace.as_deref())
                .unwrap_or("");
            if backend_tls_policy_targets_service(&pol.spec, pol_ns, sns, &br.name) {
                return true;
            }
        }
    }
    false
}

async fn gateway_targets_for_backend_tls_policy(
    client: &Client,
    pol: &BackendTLSPolicy,
) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    let hr_api: Api<HTTPRoute> = Api::all(client.clone());
    if let Ok(list) = hr_api.list(&ListParams::default()).await {
        for route in list.items {
            if http_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_route(client, &route).await);
            }
        }
    }
    let gr_api: Api<GRPCRoute> = Api::all(client.clone());
    if let Ok(list) = gr_api.list(&ListParams::default()).await {
        for route in list.items {
            if grpc_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_grpc_route(client, &route).await);
            }
        }
    }
    let tr_api: Api<TLSRoute> = Api::all(client.clone());
    if let Ok(list) = tr_api.list(&ListParams::default()).await {
        for route in list.items {
            if tls_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_tls_route(client, &route).await);
            }
        }
    }
    let tcp_api: Api<TCPRoute> = Api::all(client.clone());
    if let Ok(list) = tcp_api.list(&ListParams::default()).await {
        for route in list.items {
            if tcp_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_tcp_route(client, &route).await);
            }
        }
    }
    let udp_api: Api<UDPRoute> = Api::all(client.clone());
    if let Ok(list) = udp_api.list(&ListParams::default()).await {
        for route in list.items {
            if udp_route_references_backend_tls_policy_target(pol, &route) {
                out.extend(gateway_targets_for_udp_route(client, &route).await);
            }
        }
    }
    out
}

/// Observe gateways, routes, grants, backend TLS policies, CA ConfigMaps, endpoints, secrets,
/// classes, and inference pools (mapped via routes → gateways).
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

    let sd_gr = shutdown.clone();
    let c_gr = client.clone();
    let tx_gr = tx.clone();
    tokio::spawn(async move {
        let api: Api<GRPCRoute> = Api::all(c_gr.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_gr.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            let targets = gateway_targets_for_grpc_route(&c_gr, &route).await;
                            for (pns, pname) in targets {
                                if tx_gr.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "grpcroute watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("grpcroute watcher shutdown");
    });

    let sd_tr = shutdown.clone();
    let c_tr = client.clone();
    let tx_tr = tx.clone();
    tokio::spawn(async move {
        let api: Api<TLSRoute> = Api::all(c_tr.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_tr.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            let targets = gateway_targets_for_tls_route(&c_tr, &route).await;
                            for (pns, pname) in targets {
                                if tx_tr.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "tlsroute watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("tlsroute watcher shutdown");
    });

    let sd_tcp = shutdown.clone();
    let c_tcp = client.clone();
    let tx_tcp = tx.clone();
    tokio::spawn(async move {
        let api: Api<TCPRoute> = Api::all(c_tcp.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_tcp.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            let targets = gateway_targets_for_tcp_route(&c_tcp, &route).await;
                            for (pns, pname) in targets {
                                if tx_tcp.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "tcproute watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("tcproute watcher shutdown");
    });

    let sd_udp = shutdown.clone();
    let c_udp = client.clone();
    let tx_udp = tx.clone();
    tokio::spawn(async move {
        let api: Api<UDPRoute> = Api::all(c_udp.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_udp.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(route)) => {
                            let targets = gateway_targets_for_udp_route(&c_udp, &route).await;
                            for (pns, pname) in targets {
                                if tx_udp.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "udproute watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("udproute watcher shutdown");
    });

    let sd_ipool = shutdown.clone();
    let c_ipool = client.clone();
    let tx_ipool = tx.clone();
    tokio::spawn(async move {
        let api: Api<InferencePool> = Api::all(c_ipool.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_ipool.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(pool)) => {
                            let pns = pool.namespace().unwrap_or_default();
                            let pname = pool.name_any();
                            let targets =
                                gateway_targets_for_inference_pool(&c_ipool, &pns, &pname).await;
                            for (gwns, gwname) in targets {
                                if tx_ipool.send((gwns, gwname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "inferencepool watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("inferencepool watcher shutdown");
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
                        Some(Ok(rg)) => {
                            let mut keys: HashSet<(String, String)> = HashSet::new();
                            let grant_ns = rg.metadata.namespace.clone().unwrap_or_default();
                            if !grant_ns.is_empty() {
                                let gw_api: Api<Gateway> =
                                    Api::namespaced(c_rg.clone(), &grant_ns);
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
                                let hr_api: Api<HTTPRoute> = Api::all(c_rg.clone());
                                match hr_api.list(&ListParams::default()).await {
                                    Ok(list) => {
                                        for route in list.items {
                                            let targets =
                                                gateway_targets_for_route(&c_rg, &route).await;
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
                                let gr_api: Api<GRPCRoute> = Api::all(c_rg.clone());
                                match gr_api.list(&ListParams::default()).await {
                                    Ok(list) => {
                                        for route in list.items {
                                            let targets = gateway_targets_for_grpc_route(
                                                &c_rg,
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
                                let tls_api: Api<TLSRoute> = Api::all(c_rg.clone());
                                match tls_api.list(&ListParams::default()).await {
                                    Ok(list) => {
                                        for route in list.items {
                                            let targets = gateway_targets_for_tls_route(
                                                &c_rg,
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
                                let tcp_api: Api<TCPRoute> = Api::all(c_rg.clone());
                                match tcp_api.list(&ListParams::default()).await {
                                    Ok(list) => {
                                        for route in list.items {
                                            let targets = gateway_targets_for_tcp_route(
                                                &c_rg,
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
                                let udp_api: Api<UDPRoute> = Api::all(c_rg.clone());
                                match udp_api.list(&ListParams::default()).await {
                                    Ok(list) => {
                                        for route in list.items {
                                            let targets = gateway_targets_for_udp_route(
                                                &c_rg,
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
                                        Api::namespaced(c_rg.clone(), &ns);
                                    match hr_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_route(&c_rg, &route)
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
                                        Api::namespaced(c_rg.clone(), &ns);
                                    match gr_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_grpc_route(&c_rg, &route)
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
                                        Api::namespaced(c_rg.clone(), &ns);
                                    match tls_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_tls_route(&c_rg, &route)
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
                                        Api::namespaced(c_rg.clone(), &ns);
                                    match tcp_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_tcp_route(&c_rg, &route)
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
                                        Api::namespaced(c_rg.clone(), &ns);
                                    match udp_api.list(&ListParams::default()).await {
                                        Ok(list) => {
                                            for route in list.items {
                                                let targets =
                                                    gateway_targets_for_udp_route(&c_rg, &route)
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
                                if tx_rg.send((pns, pname)).await.is_err() {
                                    return;
                                }
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

    let sd_btp = shutdown.clone();
    let c_btp = client.clone();
    let tx_btp = tx.clone();
    tokio::spawn(async move {
        let api: Api<BackendTLSPolicy> = Api::all(c_btp.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_btp.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(pol)) => {
                            let targets = gateway_targets_for_backend_tls_policy(&c_btp, &pol).await;
                            for (pns, pname) in targets {
                                if tx_btp.send((pns, pname)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => warn!(error = %e, "backendtlspolicy watch stream"),
                        None => break,
                    }
                }
            }
        }
        info!("backendtlspolicy watcher shutdown");
    });

    let sd_cm = shutdown.clone();
    let c_cm = client.clone();
    let tx_cm = tx.clone();
    tokio::spawn(async move {
        let api: Api<ConfigMap> = Api::all(c_cm.clone());
        let cfg = WatcherConfig::default().any_semantic();
        let mut stream = Box::pin(watcher(api, cfg).applied_objects());
        loop {
            tokio::select! {
                _ = sd_cm.cancelled() => break,
                ev = stream.next() => {
                    match ev {
                        Some(Ok(cm)) => {
                            let cm_ns = cm.namespace().unwrap_or_default();
                            let cm_name = cm.name_any();
                            let pol_api: Api<BackendTLSPolicy> = Api::namespaced(c_cm.clone(), &cm_ns);
                            let policies = match pol_api.list(&ListParams::default()).await {
                                Ok(l) => l.items,
                                Err(e) => {
                                    warn!(error = %e, namespace = %cm_ns, "list BackendTLSPolicy after ConfigMap change");
                                    continue;
                                }
                            };
                            let mut gateways = HashSet::new();
                            for pol in policies {
                                if !backend_tls_policy_spec_references_configmap(
                                    &pol.spec,
                                    &cm_ns,
                                    &cm_ns,
                                    &cm_name,
                                ) {
                                    continue;
                                }
                                gateways.extend(gateway_targets_for_backend_tls_policy(&c_cm, &pol).await);
                            }
                            for (pns, pname) in gateways {
                                if tx_cm.send((pns, pname)).await.is_err() {
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
            pair = rx.recv() => {
                let Some((ns, name)) = pair else { break };
                if let Err(e) = reconcile(ns, name).await {
                    warn!(error = %e, "reconcile");
                }
            }
        }
    }
}

#[cfg(test)]
mod reference_grant_route_ns_tests {
    use crate::resources::ReferenceGrantFromSpec;

    fn route_namespace_hints(from: &[ReferenceGrantFromSpec]) -> Vec<String> {
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
            ReferenceGrantFromSpec {
                namespace: "b".into(),
                kind: "HTTPRoute".into(),
                ..Default::default()
            },
            ReferenceGrantFromSpec {
                namespace: "a".into(),
                kind: "HTTPRoute".into(),
                ..Default::default()
            },
            ReferenceGrantFromSpec {
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
        let from = vec![ReferenceGrantFromSpec {
            namespace: "  ".into(),
            kind: "HTTPRoute".into(),
            ..Default::default()
        }];
        assert!(route_namespace_hints(&from).is_empty());
    }
}
