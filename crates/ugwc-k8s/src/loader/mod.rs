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

use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use tracing::warn;
use ugwc_core::listener_merge::merge_effective_listeners;
use ugwc_core::snapshot::{
    AllowedListenersSnap, ClusterSnapshot, EndpointPickerRefSnap, EndpointPortSnap,
    EndpointSliceSnap, EndpointSnap, GatewayAddressSnap, GatewayClassSnap,
    GatewayInfrastructureSnap, GatewayListenerSnap, InferencePoolPortSnap,
    InferencePoolSelectorSnap, InferencePoolSnap, ListenerNamespacesSnap, ListenerSetSnap,
    ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap, RejectedListenerSetSnap,
    ServicePortSnap, ServiceSnap,
};

use crate::crd_supervisor::{ActiveCrds, ManagedCrd};
use crate::resources::{
    Gateway, GatewayAllowedListenersNamespacesFrom, InferencePool, ListenerSet, ReferenceGrant,
    ReferenceGrantFrom, ReferenceGrantTo,
};
use crate::stores::SharedStores;

use gateway_snap::{gateway_listener_to_snap, listenerset_listener_to_snap};
use parent_refs::listenerset_parent_ref_to_snap;
use route_relevance::{
    grpc_route_relevant_to_gateway, route_relevant_to_gateway, tcp_route_relevant_to_gateway,
    tls_route_relevant_to_gateway, udp_route_relevant_to_gateway,
};
use route_snaps::{
    grpc_route_to_snap, http_route_to_snap, tcp_route_to_snap, tls_route_to_snap, udp_route_to_snap,
};
use tls_resolve::resolve_gateway_listener_tls;

#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("gateway not found: {0}/{1}")]
    GatewayNotFound(String, String),
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

fn inference_pool_to_snap(ip: &InferencePool) -> InferencePoolSnap {
    let namespace = ip.namespace().unwrap_or_default();
    let name = ip.name_any();
    let spec = &ip.spec;
    InferencePoolSnap {
        namespace,
        name,
        selector: InferencePoolSelectorSnap {
            match_labels: spec.selector.match_labels.clone(),
        },
        target_ports: spec
            .target_ports
            .iter()
            .filter_map(|p| {
                let n = p.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            })
            .collect(),
        app_protocol: spec.app_protocol.clone(),
        endpoint_picker_ref: EndpointPickerRefSnap {
            group: spec.endpoint_picker_ref.group.clone(),
            kind: spec.endpoint_picker_ref.kind.clone(),
            name: spec.endpoint_picker_ref.name.clone(),
            port: spec.endpoint_picker_ref.port.as_ref().and_then(|x| {
                let n = x.number;
                if (1..=65535).contains(&n) {
                    Some(InferencePoolPortSnap { number: n as u16 })
                } else {
                    None
                }
            }),
            failure_mode: spec.endpoint_picker_ref.failure_mode.clone(),
        },
    }
}

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

fn namespace_labels(stores: &SharedStores, ns: &str) -> BTreeMap<String, String> {
    stores
        .get_namespace(ns)
        .map(|n| n.metadata.labels.clone().unwrap_or_default())
        .unwrap_or_default()
}

fn listener_set_namespace_allowed(
    stores: &SharedStores,
    gateway_ns: &str,
    ls_ns: &str,
    allowed: &AllowedListenersSnap,
) -> bool {
    let Some(ref nses) = allowed.namespaces else {
        return false;
    };
    let from = nses.from.as_deref().unwrap_or("None");
    match from {
        "All" => true,
        "Same" => ls_ns == gateway_ns,
        "Selector" => {
            if nses.selector_match_labels.is_empty() {
                false
            } else {
                let labels = namespace_labels(stores, ls_ns);
                nses.selector_match_labels
                    .iter()
                    .all(|(k, v)| labels.get(k).map(|x| x == v).unwrap_or(false))
            }
        }
        _ => false,
    }
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

/// ReferenceGrant rows from the reflector store (for status refresh outside full snapshot load).
#[must_use]
pub(crate) fn reference_grant_snaps_from_stores(stores: &SharedStores) -> Vec<ReferenceGrantSnap> {
    stores
        .list_reference_grants()
        .into_iter()
        .map(|rg| reference_grant_to_snap(&rg))
        .collect()
}

fn reference_grant_to_snap(rg: &ReferenceGrant) -> ReferenceGrantSnap {
    ReferenceGrantSnap {
        namespace: rg.metadata.namespace.clone().unwrap_or_default(),
        name: rg.metadata.name.clone().unwrap_or_default(),
        from: rg
            .spec
            .from
            .iter()
            .cloned()
            .map(reference_grant_from_to_snap)
            .collect(),
        to: rg
            .spec
            .to
            .iter()
            .cloned()
            .map(reference_grant_to_snap_one)
            .collect(),
    }
}

fn listenerset_to_snap(ls: &ListenerSet) -> ListenerSetSnap {
    let ns = ls.metadata.namespace.clone().unwrap_or_default();
    let name = ls.metadata.name.clone().unwrap_or_default();
    let uid = ls.metadata.uid.clone().unwrap_or_default();
    let creation_timestamp_rfc3339 = ls
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_string());
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

pub fn load_cluster_snapshot(
    stores: &SharedStores,
    gateway_ns: &str,
    gateway_name: &str,
    active_crds: &ActiveCrds,
) -> Result<ClusterSnapshot, LoaderError> {
    let gw = stores
        .get_gateway(gateway_ns, gateway_name)
        .ok_or_else(|| LoaderError::GatewayNotFound(gateway_ns.into(), gateway_name.into()))?;

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
        for ls in stores.list_listener_sets() {
            if !listenerset_parents_gateway(&ls, gateway_ns, gateway_name) {
                continue;
            }
            let ls_ns = ls.metadata.namespace.clone().unwrap_or_default();
            let ls_name = ls.metadata.name.clone().unwrap_or_default();
            if let Some(ref allowed) = allowed_listeners {
                if !listener_set_namespace_allowed(stores, gateway_ns, &ls_ns, allowed) {
                    rejected_listener_sets.push(RejectedListenerSetSnap {
                        namespace: ls_ns,
                        name: ls_name,
                        reason: "NotAllowed".into(),
                    });
                    continue;
                }
                listener_sets.push(listenerset_to_snap(&ls));
            } else {
                rejected_listener_sets.push(RejectedListenerSetSnap {
                    namespace: ls_ns,
                    name: ls_name,
                    reason: "NotAllowed".into(),
                });
            }
        }
    }

    let attached_ls_keys: HashSet<(String, String)> = listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let mut effective_listeners = merge_effective_listeners(&listeners, &listener_sets);

    let all_http_routes = stores.list_http_routes();
    let mut http_routes = Vec::new();
    let mut grpc_routes = Vec::new();
    let mut tls_routes = Vec::new();
    let mut tcp_routes = Vec::new();
    let mut udp_routes = Vec::new();
    let mut needed_services: Vec<(String, String)> = Vec::new();
    let mut needed_pools: Vec<(String, String)> = Vec::new();
    let mut ns_for_labels: HashSet<String> = HashSet::new();
    ns_for_labels.insert(gateway_ns.to_string());

    for item in &all_http_routes {
        if !route_relevant_to_gateway(item, gateway_ns, gateway_name, &attached_ls_keys) {
            continue;
        }
        let rns = item.metadata.namespace.clone().unwrap_or_default();
        ns_for_labels.insert(rns.clone());

        let snap = http_route_to_snap(item);
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
        for item in stores.list_grpc_routes() {
            if !grpc_route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
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

    if crd_active(active_crds, ManagedCrd::TlsRoute) {
        for item in stores.list_tls_routes() {
            if !tls_route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
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
                    crate::resources::collect_tls_route_rule_inference_pool_refs(&rns, rule)
                {
                    ns_for_labels.insert(pns.clone());
                    needed_pools.push((pns, pname));
                }
            }
            tls_routes.push(snap);
        }
    }

    if crd_active(active_crds, ManagedCrd::TcpRoute) {
        for item in stores.list_tcp_routes() {
            if !tcp_route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
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
                    crate::resources::collect_tcp_route_rule_inference_pool_refs(&rns, rule)
                {
                    ns_for_labels.insert(pns.clone());
                    needed_pools.push((pns, pname));
                }
            }
            tcp_routes.push(snap);
        }
    }

    if crd_active(active_crds, ManagedCrd::UdpRoute) {
        for item in stores.list_udp_routes() {
            if !udp_route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
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
                    crate::resources::collect_udp_route_rule_inference_pool_refs(&rns, rule)
                {
                    ns_for_labels.insert(pns.clone());
                    needed_pools.push((pns, pname));
                }
            }
            udp_routes.push(snap);
        }
    }

    needed_pools.sort();
    needed_pools.dedup();

    let mut inference_pools: Vec<InferencePoolSnap> = Vec::new();
    for (pns, pname) in &needed_pools {
        match stores.get_inference_pool(pns, pname) {
            Some(ip) => {
                let picker = &ip.spec.endpoint_picker_ref;
                let pk = picker.kind.as_deref().unwrap_or("Service");
                if pk == "Service" && !picker.name.is_empty() {
                    needed_services.push((pns.clone(), picker.name.clone()));
                    ns_for_labels.insert(pns.clone());
                }
                inference_pools.push(inference_pool_to_snap(&ip));
            }
            None => {
                warn!(
                    pool_ns = %pns,
                    pool_name = %pname,
                    "InferencePool not found in store — missing pool or CRD not installed"
                );
            }
        }
    }

    needed_services.sort();
    needed_services.dedup();

    let mut services = Vec::new();
    for (sns, sname) in &needed_services {
        if let Some(svc) = stores.get_service(sns, sname) {
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
                namespace: svc.metadata.namespace.clone().unwrap_or_default(),
                name: svc.metadata.name.clone().unwrap_or_default(),
                ports,
                cluster_ip: svc.spec.as_ref().and_then(|s| s.cluster_ip.clone()),
            });
        }
    }

    let mut endpoint_slices = Vec::new();
    for sl in stores.list_endpoint_slices() {
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
        for ep in &sl.endpoints {
            let mut addrs = Vec::new();
            addrs.extend(ep.addresses.iter().cloned());
            let mut ports = Vec::new();
            if let Some(ref ports_slice) = sl.ports {
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
            namespace: sl.metadata.namespace.clone().unwrap_or_default(),
            name: sl.metadata.name.clone().unwrap_or_default(),
            service_name: svc_name,
            endpoints,
        });
    }

    let mut ns_label_map: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for ns in &ns_for_labels {
        let lbl = namespace_labels(stores, ns);
        if !lbl.is_empty() {
            ns_label_map.insert(ns.clone(), lbl);
        }
    }

    let mut reference_grants: Vec<ReferenceGrantSnap> = Vec::new();
    if crd_active(active_crds, ManagedCrd::ReferenceGrant) {
        reference_grants = stores
            .list_reference_grants()
            .iter()
            .map(|rg| reference_grant_to_snap(rg))
            .collect();
    }

    resolve_gateway_listener_tls(
        stores,
        gateway_ns,
        &reference_grants,
        &mut effective_listeners,
    );

    let gateway_class = if gw.spec.gateway_class_name.is_empty() {
        None
    } else {
        stores
            .get_gateway_class(&gw.spec.gateway_class_name)
            .map(|gc| GatewayClassSnap {
                name: gc.metadata.name.clone().unwrap_or_default(),
                controller_name: gc.spec.controller_name.clone(),
            })
    };

    let (backend_tls_effective, backend_tls_policy_outcomes) =
        backend_tls::load_backend_tls_for_gateway(
            stores,
            &needed_services,
            &services,
            &needed_pools,
            &inference_pools,
            &reference_grants,
        );

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
