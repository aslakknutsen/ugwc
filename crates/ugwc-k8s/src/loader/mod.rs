//! Load [`ugwc_core::ClusterSnapshot`] for a reconciled `Gateway`.

pub(crate) mod backend_tls;
mod gateway_snap;
mod parent_refs;
mod route_relevance;
mod route_snaps;
mod tests;
mod tls_resolve;

pub use parent_refs::parent_reference_to_snap;

use std::collections::{BTreeMap, HashSet};

use k8s_openapi::api::core::v1::{Namespace, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use thiserror::Error;
use tracing::warn;
use ugwc_core::listener_merge::merge_effective_listeners;
use ugwc_core::snapshot::{
    AllowedListenersSnap, ClusterSnapshot,
    EndpointPortSnap, EndpointSliceSnap,
    EndpointSnap, EndpointPickerRefSnap, GatewayAddressSnap, GatewayClassSnap,
    GatewayInfrastructureSnap, GatewayListenerSnap,
    InferencePoolPortSnap,
    InferencePoolSelectorSnap, InferencePoolSnap, ListenerNamespacesSnap,
    ListenerSetSnap, RejectedListenerSetSnap,
    ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap,
    ServicePortSnap, ServiceSnap,
};

use crate::crd_supervisor::{ActiveCrds, ManagedCrd};
use crate::resources::{
    Gateway, GatewayAllowedListenersNamespacesFrom, GatewayClass,
    GRPCRoute,
    HTTPRoute,
    InferencePool, ListenerSet,
    ReferenceGrant, ReferenceGrantFrom, ReferenceGrantTo, TCPRoute,
    TLSRoute,
    UDPRoute,
};

use gateway_snap::{gateway_listener_to_snap, listenerset_listener_to_snap};
use parent_refs::listenerset_parent_ref_to_snap;
use route_relevance::{
    grpc_route_relevant_to_gateway, route_relevant_to_gateway, tcp_route_relevant_to_gateway,
    tls_route_relevant_to_gateway, udp_route_relevant_to_gateway,
};
use route_snaps::{
    grpc_route_to_snap, http_route_to_snap, tcp_route_to_snap, tls_route_to_snap,
    udp_route_to_snap,
};
use tls_resolve::resolve_gateway_listener_tls;

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
}

fn snap_u16_port(port: Option<i32>) -> Option<u16> {
    port.and_then(|n| u16::try_from(n).ok())
}

fn snap_nonnegative_weight(w: Option<i32>) -> Option<u32> {
    match w {
        Some(x) if x >= 0 => Some(x as u32),
        _ => None,
    }
}

fn inference_pool_to_snap(ip: InferencePool) -> InferencePoolSnap {
    let namespace = ip.namespace().unwrap_or_default();
    let name = ip.name_any();
    let spec = ip.spec;
    InferencePoolSnap {
        namespace,
        name,
        selector: InferencePoolSelectorSnap {
            match_labels: spec.selector.match_labels,
        },
        target_ports: spec
            .target_ports
            .into_iter()
            .filter_map(|p| {
                let n = p.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            })
            .collect(),
        app_protocol: spec.app_protocol,
        endpoint_picker_ref: EndpointPickerRefSnap {
            group: spec.endpoint_picker_ref.group,
            kind: spec.endpoint_picker_ref.kind,
            name: spec.endpoint_picker_ref.name,
            port: spec.endpoint_picker_ref.port.and_then(|x| {
                let n = x.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            }),
            failure_mode: spec.endpoint_picker_ref.failure_mode,
        },
    }
}

/// When `allowedListeners` is unset or defaults to no attachment, ListenerSets are ignored.
fn allowed_snap_for_merge(gw: &Gateway) -> Option<AllowedListenersSnap> {
    let al = gw.spec.allowed_listeners.as_ref()?;
    let ns = al.namespaces.as_ref()?;
    let from = ns
        .from
        .as_ref()
        .cloned()
        .unwrap_or(GatewayAllowedListenersNamespacesFrom::None);
    if matches!(from, GatewayAllowedListenersNamespacesFrom::None) {
        return None;
    }
    Some(AllowedListenersSnap {
        namespaces: Some(ListenerNamespacesSnap {
            from: Some(format!("{from:?}")),
            selector_match_labels: ns
                .selector
                .as_ref()
                .and_then(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
    })
}

fn listenerset_parents_gateway(ls: &ListenerSet, gw_ns: &str, gw_name: &str) -> bool {
    let p = &ls.spec.parent_ref;
    let ls_ns_default = ls.metadata.namespace.as_deref().unwrap_or("");
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = p.namespace.as_deref().unwrap_or(ls_ns_default);
    pns == gw_ns && p.name == gw_name
}

async fn namespace_labels(
    client: &Client,
    ns: &str,
) -> Result<BTreeMap<String, String>, kube::Error> {
    let api: Api<Namespace> = Api::all(client.clone());
    let n = api.get(ns).await?;
    Ok(n.metadata.labels.unwrap_or_default())
}

async fn listener_set_namespace_allowed(
    client: &Client,
    gateway_ns: &str,
    ls_ns: &str,
    allowed: &AllowedListenersSnap,
) -> Result<bool, kube::Error> {
    let Some(ref nses) = allowed.namespaces else {
        return Ok(false);
    };
    let from = nses.from.as_deref().unwrap_or("None");
    Ok(match from {
        "All" => true,
        "Same" => ls_ns == gateway_ns,
        "Selector" => {
            if nses.selector_match_labels.is_empty() {
                false
            } else {
                let labels = namespace_labels(client, ls_ns).await.unwrap_or_default();
                nses.selector_match_labels
                    .iter()
                    .all(|(k, v)| labels.get(k).map(|x| x == v).unwrap_or(false))
            }
        }
        _ => false,
    })
}

fn reference_grant_from_to_snap(f: ReferenceGrantFrom) -> ReferenceGrantFromSnap {
    ReferenceGrantFromSnap {
        group: if f.group.is_empty() {
            None
        } else {
            Some(f.group)
        },
        kind: f.kind,
        namespace: f.namespace,
    }
}

fn reference_grant_to_snap_one(t: ReferenceGrantTo) -> ReferenceGrantToSnap {
    ReferenceGrantToSnap {
        group: if t.group.is_empty() {
            None
        } else {
            Some(t.group)
        },
        kind: t.kind,
        name: t.name,
    }
}

fn reference_grant_to_snap(rg: ReferenceGrant) -> ReferenceGrantSnap {
    ReferenceGrantSnap {
        namespace: rg.metadata.namespace.unwrap_or_default(),
        name: rg.metadata.name.unwrap_or_default(),
        from: rg
            .spec
            .from
            .into_iter()
            .map(reference_grant_from_to_snap)
            .collect(),
        to: rg
            .spec
            .to
            .into_iter()
            .map(reference_grant_to_snap_one)
            .collect(),
    }
}

fn listenerset_to_snap(ls: ListenerSet) -> ListenerSetSnap {
    let ns = ls.metadata.namespace.unwrap_or_default();
    let name = ls.metadata.name.unwrap_or_default();
    let uid = ls.metadata.uid.unwrap_or_default();
    let creation_timestamp_rfc3339 = ls.metadata.creation_timestamp.map(|t| t.0.to_string());
    let parent_refs = vec![listenerset_parent_ref_to_snap(&ls.spec.parent_ref)];
    let mut seen_names = HashSet::new();
    let listeners = ls
        .spec
        .listeners
        .iter()
        .filter(|l| seen_names.insert(l.name.clone()))
        .map(listenerset_listener_to_snap)
        .collect();
    ListenerSetSnap {
        namespace: ns,
        name,
        uid,
        creation_timestamp_rfc3339,
        parent_refs,
        listeners,
    }
}

fn crd_active(active: &ActiveCrds, m: ManagedCrd) -> bool {
    active.read().map_or(false, |set| set.contains(&m))
}

pub async fn load_cluster_snapshot(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    active_crds: &ActiveCrds,
) -> Result<ClusterSnapshot, LoaderError> {
    let gw_api: Api<Gateway> = Api::namespaced(client.clone(), gateway_ns);
    let gw = gw_api.get(gateway_name).await?;

    let listeners: Vec<GatewayListenerSnap> = gw
        .spec
        .listeners
        .iter()
        .map(gateway_listener_to_snap)
        .collect();

    let allowed_listeners = allowed_snap_for_merge(&gw);

    let mut listener_sets = Vec::new();
    let mut rejected_listener_sets: Vec<RejectedListenerSetSnap> = Vec::new();
    if crd_active(active_crds, ManagedCrd::ListenerSet) {
        let ls_api: Api<ListenerSet> = Api::all(client.clone());
        match ls_api.list(&ListParams::default()).await {
            Ok(ls_list) => {
                for ls in ls_list.items {
                    if !listenerset_parents_gateway(&ls, gateway_ns, gateway_name) {
                        continue;
                    }
                    let ls_ns = ls.metadata.namespace.clone().unwrap_or_default();
                    let ls_name = ls.metadata.name.clone().unwrap_or_default();
                    if let Some(ref allowed) = allowed_listeners {
                        if !listener_set_namespace_allowed(client, gateway_ns, &ls_ns, allowed)
                            .await?
                        {
                            rejected_listener_sets.push(RejectedListenerSetSnap {
                                namespace: ls_ns,
                                name: ls_name,
                                reason: "NotAllowed".into(),
                            });
                            continue;
                        }
                        listener_sets.push(listenerset_to_snap(ls));
                    } else {
                        rejected_listener_sets.push(RejectedListenerSetSnap {
                            namespace: ls_ns,
                            name: ls_name,
                            reason: "NotAllowed".into(),
                        });
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "ListenerSet list failed — clusters without ListenerSet CRD skip merge"
                );
            }
        }
    }

    let attached_ls_keys: HashSet<(String, String)> = listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let mut effective_listeners = merge_effective_listeners(&listeners, &listener_sets);

    let route_api: Api<HTTPRoute> = Api::all(client.clone());
    let routes_list = route_api.list(&ListParams::default()).await?;
    let mut http_routes = Vec::new();
    let mut grpc_routes = Vec::new();
    let mut tls_routes = Vec::new();
    let mut tcp_routes = Vec::new();
    let mut udp_routes = Vec::new();
    let mut needed_services: Vec<(String, String)> = Vec::new();
    let mut needed_pools: Vec<(String, String)> = Vec::new();
    let mut ns_for_labels: HashSet<String> = HashSet::new();
    ns_for_labels.insert(gateway_ns.to_string());

    for item in routes_list.items {
        if !route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
            continue;
        }
        let rns = item.metadata.namespace.clone().unwrap_or_default();
        ns_for_labels.insert(rns.clone());

        let snap = http_route_to_snap(&item);
        for rule in item.spec.rules.as_deref().unwrap_or_default() {
            for (sns, sname) in crate::resources::collect_http_route_rule_service_refs(&rns, rule) {
                ns_for_labels.insert(sns.clone());
                needed_services.push((sns, sname));
            }
            for (pns, pname) in
                crate::resources::collect_http_route_rule_inference_pool_refs(&rns, rule)
            {
                ns_for_labels.insert(pns.clone());
                needed_pools.push((pns, pname));
            }
        }
        http_routes.push(snap);
    }

    if crd_active(active_crds, ManagedCrd::GrpcRoute) {
        let grpc_api: Api<GRPCRoute> = Api::all(client.clone());
        match grpc_api.list(&ListParams::default()).await {
            Ok(grpc_list) => {
                for item in grpc_list.items {
                    if !grpc_route_relevant_to_gateway(
                        &item,
                        gateway_ns,
                        gateway_name,
                        &attached_ls_keys,
                    ) {
                        continue;
                    }
                    let rns = item.metadata.namespace.clone().unwrap_or_default();
                    ns_for_labels.insert(rns.clone());

                    let snap = grpc_route_to_snap(&item);
                    for rule in item.spec.rules.as_deref().unwrap_or_default() {
                        for (sns, sname) in
                            crate::resources::collect_grpc_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_grpc_route_rule_inference_pool_refs(&rns, rule)
                        {
                            ns_for_labels.insert(pns.clone());
                            needed_pools.push((pns, pname));
                        }
                    }
                    grpc_routes.push(snap);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "GRPCRoute list failed — clusters without GRPCRoute CRD skip gRPC routes"
                );
            }
        }
    }

    if crd_active(active_crds, ManagedCrd::TlsRoute) {
        let tls_api: Api<TLSRoute> = Api::all(client.clone());
        match tls_api.list(&ListParams::default()).await {
            Ok(tls_list) => {
                for item in tls_list.items {
                    if !tls_route_relevant_to_gateway(
                        &item,
                        gateway_ns,
                        gateway_name,
                        &attached_ls_keys,
                    ) {
                        continue;
                    }
                    let rns = item.metadata.namespace.clone().unwrap_or_default();
                    ns_for_labels.insert(rns.clone());

                    let snap = tls_route_to_snap(&item);
                    for rule in &item.spec.rules {
                        for (sns, sname) in
                            crate::resources::collect_tls_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_tls_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
                            ns_for_labels.insert(pns.clone());
                            needed_pools.push((pns, pname));
                        }
                    }
                    tls_routes.push(snap);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "TLSRoute list failed — clusters without TLSRoute CRD skip TLS routes"
                );
            }
        }
    }

    if crd_active(active_crds, ManagedCrd::TcpRoute) {
        let tcp_api: Api<TCPRoute> = Api::all(client.clone());
        match tcp_api.list(&ListParams::default()).await {
            Ok(tcp_list) => {
                for item in tcp_list.items {
                    if !tcp_route_relevant_to_gateway(
                        &item,
                        gateway_ns,
                        gateway_name,
                        &attached_ls_keys,
                    ) {
                        continue;
                    }
                    let rns = item.metadata.namespace.clone().unwrap_or_default();
                    ns_for_labels.insert(rns.clone());

                    let snap = tcp_route_to_snap(&item);
                    for rule in &item.spec.rules {
                        for (sns, sname) in
                            crate::resources::collect_tcp_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_tcp_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
                            ns_for_labels.insert(pns.clone());
                            needed_pools.push((pns, pname));
                        }
                    }
                    tcp_routes.push(snap);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "TCPRoute list failed — clusters without TCPRoute CRD skip TCP routes"
                );
            }
        }
    }

    if crd_active(active_crds, ManagedCrd::UdpRoute) {
        let udp_api: Api<UDPRoute> = Api::all(client.clone());
        match udp_api.list(&ListParams::default()).await {
            Ok(udp_list) => {
                for item in udp_list.items {
                    if !udp_route_relevant_to_gateway(
                        &item,
                        gateway_ns,
                        gateway_name,
                        &attached_ls_keys,
                    ) {
                        continue;
                    }
                    let rns = item.metadata.namespace.clone().unwrap_or_default();
                    ns_for_labels.insert(rns.clone());

                    let snap = udp_route_to_snap(&item);
                    for rule in &item.spec.rules {
                        for (sns, sname) in
                            crate::resources::collect_udp_route_rule_service_refs(&rns, rule)
                        {
                            ns_for_labels.insert(sns.clone());
                            needed_services.push((sns, sname));
                        }
                        for (pns, pname) in
                            crate::resources::collect_udp_route_rule_inference_pool_refs(
                                &rns, rule,
                            )
                        {
                            ns_for_labels.insert(pns.clone());
                            needed_pools.push((pns, pname));
                        }
                    }
                    udp_routes.push(snap);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "UDPRoute list failed — clusters without UDPRoute CRD skip UDP routes"
                );
            }
        }
    }

    needed_pools.sort();
    needed_pools.dedup();

    let mut inference_pools: Vec<InferencePoolSnap> = Vec::new();
    for (pns, pname) in &needed_pools {
        let api = Api::<InferencePool>::namespaced(client.clone(), pns);
        match api.get(pname).await {
            Ok(ip) => {
                let picker = &ip.spec.endpoint_picker_ref;
                let pk = picker.kind.as_deref().unwrap_or("Service");
                if pk == "Service" && !picker.name.is_empty() {
                    needed_services.push((pns.clone(), picker.name.clone()));
                    ns_for_labels.insert(pns.clone());
                }
                inference_pools.push(inference_pool_to_snap(ip));
            }
            Err(e) => {
                warn!(
                    error = %e,
                    pool_ns = %pns,
                    pool_name = %pname,
                    "InferencePool get failed — missing pool or CRD not installed"
                );
            }
        }
    }

    needed_services.sort();
    needed_services.dedup();

    let mut services = Vec::new();
    for (sns, sname) in &needed_services {
        if let Ok(svc) = Api::<Service>::namespaced(client.clone(), sns)
            .get(sname)
            .await
        {
            let ports: Vec<ServicePortSnap> = svc
                .spec
                .as_ref()
                .map(|spec| {
                    spec.ports
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|p| ServicePortSnap {
                            name: p.name.unwrap_or_default(),
                            port: p.port as u16,
                            target_port: p.target_port.and_then(|tp| match tp {
                                IntOrString::Int(i) => Some(i as u16),
                                IntOrString::String(_) => None,
                            }),
                        })
                        .collect()
                })
                .unwrap_or_default();
            services.push(ServiceSnap {
                namespace: svc.metadata.namespace.unwrap_or_default(),
                name: svc.metadata.name.unwrap_or_default(),
                ports,
                cluster_ip: svc.spec.as_ref().and_then(|s| s.cluster_ip.clone()),
            });
        }
    }

    let mut endpoint_slices = Vec::new();
    let es_api: Api<EndpointSlice> = Api::all(client.clone());
    let all_slices = es_api.list(&ListParams::default()).await?;
    for sl in all_slices.items {
        let slice_ns = sl.metadata.namespace.clone().unwrap_or_default();
        let svc_name = sl
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name").cloned());
        let Some(ref sn) = svc_name else {
            continue;
        };
        if !needed_services
            .iter()
            .any(|(sns, n)| n == sn && sns == &slice_ns)
        {
            continue;
        }
        let mut endpoints = Vec::new();
        for ep in sl.endpoints {
            let mut addrs = Vec::new();
            addrs.extend(ep.addresses);
            let mut ports = Vec::new();
            if let Some(ports_slice) = sl.ports.clone() {
                for p in ports_slice {
                    if let Some(port) = p.port {
                        ports.push(EndpointPortSnap {
                            name: p.name.clone(),
                            port: port as u16,
                        });
                    }
                }
            }
            if !addrs.is_empty() {
                endpoints.push(EndpointSnap {
                    addresses: addrs,
                    ports,
                });
            }
        }
        endpoint_slices.push(EndpointSliceSnap {
            namespace: sl.metadata.namespace.unwrap_or_default(),
            name: sl.metadata.name.unwrap_or_default(),
            service_name: svc_name,
            endpoints,
        });
    }

    let mut ns_label_map: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for ns in &ns_for_labels {
        match namespace_labels(client, ns).await {
            Ok(lbl) => {
                ns_label_map.insert(ns.clone(), lbl);
            }
            Err(e) => {
                warn!(error = %e, namespace = %ns, "failed to fetch namespace labels; selector-based allowedRoutes may not match");
            }
        }
    }

    let mut reference_grants: Vec<ReferenceGrantSnap> = Vec::new();
    if crd_active(active_crds, ManagedCrd::ReferenceGrant) {
        let rg_api: Api<ReferenceGrant> = Api::all(client.clone());
        match rg_api.list(&ListParams::default()).await {
            Ok(list) => {
                reference_grants = list
                    .items
                    .into_iter()
                    .map(reference_grant_to_snap)
                    .collect();
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "ReferenceGrant list failed — clusters without CRD skip cross-namespace grants"
                );
            }
        }
    }

    resolve_gateway_listener_tls(
        client,
        gateway_ns,
        &reference_grants,
        &mut effective_listeners,
    )
    .await?;

    let gateway_class = if gw.spec.gateway_class_name.is_empty() {
        None
    } else {
        let gc_api: Api<GatewayClass> = Api::all(client.clone());
        gc_api
            .get(&gw.spec.gateway_class_name)
            .await
            .ok()
            .map(|gc| GatewayClassSnap {
                name: gc.metadata.name.unwrap_or_default(),
                controller_name: gc.spec.controller_name.clone(),
            })
    };

    let (backend_tls_effective, backend_tls_policy_outcomes) =
        backend_tls::load_backend_tls_for_gateway(
            client,
            &needed_services,
            &services,
            &needed_pools,
            &inference_pools,
            &reference_grants,
        )
        .await?;

    Ok(ClusterSnapshot {
        gateway_namespace: gateway_ns.to_string(),
        gateway_name: gateway_name.to_string(),
        gateway_class_name: gw.spec.gateway_class_name.clone(),
        gateway_generation: gw.metadata.generation.filter(|&g| g > 0).unwrap_or(1),
        gateway_addresses: gw
            .spec
            .addresses
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|a| GatewayAddressSnap {
                type_: a.r#type.clone(),
                value: a.value.clone().unwrap_or_default(),
            })
            .collect(),
        gateway_infrastructure: gw.spec.infrastructure.as_ref().map(|inf| {
            GatewayInfrastructureSnap {
                labels: inf.labels.clone().unwrap_or_default(),
                annotations: inf.annotations.clone().unwrap_or_default(),
            }
        }),
        listeners,
        effective_listeners,
        listener_sets,
        rejected_listener_sets,
        allowed_listeners,
        http_routes,
        grpc_routes,
        tls_routes,
        tcp_routes,
        udp_routes,
        services,
        endpoint_slices,
        gateway_class,
        reference_grants,
        backend_tls_effective,
        backend_tls_policy_outcomes,
        namespace_labels: ns_label_map,
        inference_pools,
    })
}
