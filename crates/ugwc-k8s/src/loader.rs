//! Load [`ugwc_core::ClusterSnapshot`] for a reconciled `Gateway`.

use std::collections::{BTreeMap, HashSet};

use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ListParams;
use kube::{Api, Client};
use thiserror::Error;
use tracing::warn;
use ugwc_core::http_route_attachment::reference_grant_permits_gateway_secret_ref;
use ugwc_core::listener_merge::merge_effective_listeners;
use ugwc_core::snapshot::{
    AllowedListenersSnap, AllowedRoutesNamespacesSnap, AllowedRoutesSnap, ClusterSnapshot,
    EffectiveListenerSnap, EndpointPortSnap, EndpointSliceSnap, EndpointSnap, GatewayAddressSnap,
    GatewayClassSnap, GatewayInfrastructureSnap, GatewayListenerSnap, GatewayListenerTlsSnap,
    GrpcHeaderMatchSnap, GrpcMethodMatchSnap, GrpcRouteFilterSnap, GrpcRouteMatchSnap,
    GrpcRouteRuleSnap, GrpcRouteSnap,
    HttpBackendRefSnap, HttpHeaderFilterSnap, HttpHeaderMatchSnap, HttpHeaderNameValueSnap,
    HttpPathMatchSnap, HttpPathModifierSnap, HttpRequestMatchSnap, HttpRequestRedirectFilterSnap,
    HttpRouteFilterSnap, HttpRouteRuleSnap, HttpRouteSnap, ListenerNamespacesSnap, ListenerSetSnap,
    ParentRefSnap, ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap,
    RouteGroupKindSnap, SecretObjectReferenceSnap, ServicePortSnap, ServiceSnap,
};

use crate::resources::{
    Gateway, GatewayClass, GRPCRoute, HTTPRoute, ListenerSet, ParentReferenceSpec, ReferenceGrant,
};

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
}

/// Convert spec parentRef to snapshot form (HTTPRoute status merge).
#[must_use]
pub fn parent_reference_to_snap(p: &ParentReferenceSpec) -> ParentRefSnap {
    ParentRefSnap {
        group: p.group.clone(),
        kind: p.kind.clone(),
        namespace: p.namespace.clone(),
        name: p.name.clone(),
        section_name: p.section_name.clone(),
        port: p.port,
    }
}

fn parent_to_snap(p: &ParentReferenceSpec) -> ParentRefSnap {
    parent_reference_to_snap(p)
}

/// When `allowedListeners` is unset or defaults to no attachment, ListenerSets are ignored.
fn allowed_snap_for_merge(gw: &Gateway) -> Option<AllowedListenersSnap> {
    let al = gw.spec.allowed_listeners.as_ref()?;
    let ns = al.namespaces.as_ref()?;
    let from = ns.from.as_deref().unwrap_or("None");
    if from == "None" || from.is_empty() {
        return None;
    }
    Some(AllowedListenersSnap {
        namespaces: Some(ListenerNamespacesSnap {
            from: Some(from.to_string()),
            selector_match_labels: ns
                .selector
                .as_ref()
                .map(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
    })
}

fn listenerset_parents_gateway(ls: &ListenerSet, gw_ns: &str, gw_name: &str) -> bool {
    let ls_ns_default = ls.metadata.namespace.as_deref().unwrap_or("");
    ls.spec.parent_refs.iter().any(|p| {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" || k != "Gateway" {
            return false;
        }
        let pns = p.namespace.as_deref().unwrap_or(ls_ns_default);
        pns == gw_ns && p.name == gw_name
    })
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

fn allowed_routes_snap(ar: &crate::resources::AllowedRoutesSpec) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar.namespaces.as_ref().map(|n| AllowedRoutesNamespacesSnap {
            from: n.from.clone(),
            selector_match_labels: n
                .selector
                .as_ref()
                .map(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }),
        kinds: ar
            .kinds
            .iter()
            .map(|k| RouteGroupKindSnap {
                group: k.group.clone(),
                kind: k.kind.clone(),
            })
            .collect(),
    }
}

fn gateway_listener_to_snap(l: &crate::resources::GatewayListenerSpec) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: l.port,
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l.allowed_routes.as_ref().map(allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_to_snap),
    }
}

fn listener_tls_to_snap(t: &crate::resources::ListenerTlsSpec) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: t.mode.clone(),
        certificate_refs: t
            .certificate_refs
            .iter()
            .map(|r| SecretObjectReferenceSnap {
                group: r.group.clone(),
                kind: r.kind.clone(),
                name: r.name.clone(),
                namespace: r.namespace.clone(),
            })
            .collect(),
    }
}

fn extract_tls_pem_from_secret(sec: &Secret) -> Result<(Vec<u8>, Vec<u8>), String> {
    let data = sec
        .data
        .as_ref()
        .ok_or_else(|| "Secret has no data".to_string())?;
    let crt = data
        .get("tls.crt")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.crt".to_string())?;
    let key = data
        .get("tls.key")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.key".to_string())?;
    Ok((crt, key))
}

/// Resolve `certificateRefs` into PEM material on HTTPS listeners (same-namespace or via ReferenceGrant).
async fn resolve_gateway_listener_tls(
    client: &Client,
    gateway_ns: &str,
    grants: &[ReferenceGrantSnap],
    effective: &mut [EffectiveListenerSnap],
) -> Result<(), LoaderError> {
    for e in effective.iter_mut() {
        if e.conflicted {
            continue;
        }
        let proto = e.protocol.trim().to_ascii_uppercase();
        if proto != "HTTPS" {
            continue;
        }
        let mode = e
            .tls
            .as_ref()
            .and_then(|t| t.mode.as_deref())
            .unwrap_or("Terminate");
        if mode.eq_ignore_ascii_case("Passthrough") {
            e.tls_resolve_error = Some("UnsupportedValue: HTTPS TLS Passthrough".into());
            continue;
        }
        let refs = e.tls.as_ref().map(|t| &t.certificate_refs[..]).unwrap_or(&[]);
        if refs.is_empty() {
            e.tls_resolve_error =
                Some("InvalidCertificateRef: HTTPS listener requires certificateRefs".into());
            continue;
        }
        let r = &refs[0];
        let kind = r.kind.as_deref().unwrap_or("Secret");
        let group = r.group.as_deref().unwrap_or("");
        if kind != "Secret" || (!group.is_empty() && group != "") {
            e.tls_resolve_error = Some(format!(
                "InvalidCertificateRef: unsupported certificate ref {group}/{kind}"
            ));
            continue;
        }
        let secret_ns = r.namespace.as_deref().unwrap_or(gateway_ns);
        if !reference_grant_permits_gateway_secret_ref(grants, gateway_ns, secret_ns, &r.name) {
            e.tls_resolve_error = Some("RefNotPermitted".into());
            continue;
        }
        let api: Api<Secret> = Api::namespaced(client.clone(), secret_ns);
        match api.get(&r.name).await {
            Ok(sec) => match extract_tls_pem_from_secret(&sec) {
                Ok((crt, key)) => {
                    e.tls_cert_pem = crt;
                    e.tls_key_pem = key;
                }
                Err(msg) => {
                    e.tls_resolve_error = Some(format!("InvalidCertificateRef: {msg}"));
                }
            },
            Err(_) => {
                e.tls_resolve_error = Some("InvalidCertificateRef: Secret not found".into());
            }
        }
    }
    Ok(())
}

fn http_header_filter_snap(h: &crate::resources::HTTPHeaderFilterSpec) -> HttpHeaderFilterSnap {
    HttpHeaderFilterSnap {
        set: h
            .set
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .iter()
            .map(|x| HttpHeaderNameValueSnap {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone(),
    }
}

fn http_route_filter_snap(f: &crate::resources::HTTPRouteFilterSpec) -> HttpRouteFilterSnap {
    HttpRouteFilterSnap {
        filter_type: f.filter_type.clone(),
        request_redirect: f.request_redirect.as_ref().map(|rr| HttpRequestRedirectFilterSnap {
            scheme: rr.scheme.clone(),
            hostname: rr.hostname.clone(),
            port: rr.port,
            status_code: rr.status_code,
            path: rr.path.as_ref().map(|pm| HttpPathModifierSnap {
                modifier_type: pm.modifier_type.clone(),
                replace_full_path: pm.replace_full_path.clone(),
                replace_prefix_match: pm.replace_prefix_match.clone(),
            }),
        }),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
    }
}

fn grpc_route_filter_snap(f: &crate::resources::GRPCRouteFilterSpec) -> GrpcRouteFilterSnap {
    GrpcRouteFilterSnap {
        filter_type: f.filter_type.clone(),
        request_header_modifier: f
            .request_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
        response_header_modifier: f
            .response_header_modifier
            .as_ref()
            .map(http_header_filter_snap),
    }
}

fn reference_grant_to_snap(rg: ReferenceGrant) -> ReferenceGrantSnap {
    ReferenceGrantSnap {
        namespace: rg.metadata.namespace.unwrap_or_default(),
        name: rg.metadata.name.unwrap_or_default(),
        from: rg
            .spec
            .from
            .iter()
            .map(|f| ReferenceGrantFromSnap {
                group: f.group.clone(),
                kind: f.kind.clone(),
                namespace: f.namespace.clone(),
            })
            .collect(),
        to: rg
            .spec
            .to
            .iter()
            .map(|t| ReferenceGrantToSnap {
                group: t.group.clone(),
                kind: t.kind.clone(),
                name: t.name.clone(),
            })
            .collect(),
    }
}

fn listenerset_to_snap(ls: ListenerSet) -> ListenerSetSnap {
    let ns = ls.metadata.namespace.unwrap_or_default();
    let name = ls.metadata.name.unwrap_or_default();
    let uid = ls.metadata.uid.unwrap_or_default();
    let creation_timestamp_rfc3339 = ls.metadata.creation_timestamp.map(|t| t.0.to_rfc3339());
    let parent_refs = ls.spec.parent_refs.iter().map(parent_to_snap).collect();
    let listeners = ls
        .spec
        .listeners
        .iter()
        .map(gateway_listener_to_snap)
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

fn route_attaches_to_gateway_parent(
    p: &ParentReferenceSpec,
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = p
        .namespace
        .as_deref()
        .or(route_namespace)
        .unwrap_or("");
    pns == gw_ns && p.name == gw_name
}

fn route_attaches_to_listener_set_parent(
    p: &ParentReferenceSpec,
    route_namespace: Option<&str>,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("ListenerSet");
    if g != "gateway.networking.k8s.io" || k != "ListenerSet" {
        return false;
    }
    let ls_ns = p
        .namespace
        .as_deref()
        .or(route_namespace)
        .unwrap_or_default()
        .to_string();
    attached_ls.contains(&(ls_ns, p.name.clone()))
}

fn route_relevant_to_gateway_refs(
    parent_refs: &[ParentReferenceSpec],
    route_namespace: Option<&str>,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    parent_refs.iter().any(|p| {
        route_attaches_to_gateway_parent(p, route_namespace, gw_ns, gw_name)
            || route_attaches_to_listener_set_parent(p, route_namespace, attached_ls)
    })
}

fn route_relevant_to_gateway(
    route: &HTTPRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn grpc_route_relevant_to_gateway(
    route: &GRPCRoute,
    gw_ns: &str,
    gw_name: &str,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    route_relevant_to_gateway_refs(
        &route.spec.parent_refs,
        route.metadata.namespace.as_deref(),
        gw_ns,
        gw_name,
        attached_ls,
    )
}

fn http_route_to_snap(route: &HTTPRoute) -> HttpRouteSnap {
    let gen = route.metadata.generation.unwrap_or(0);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<HttpRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| HttpRouteRuleSnap {
            matches: r
                .matches
                .iter()
                .map(|m| HttpRequestMatchSnap {
                    path: m.path.as_ref().map(|p| HttpPathMatchSnap {
                        path_type: p.path_type.clone(),
                        value: p.value.clone(),
                    }),
                    headers: m
                        .headers
                        .iter()
                        .map(|h| HttpHeaderMatchSnap {
                            match_type: h.match_type.clone(),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            filters: r.filters.iter().map(http_route_filter_snap).collect(),
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    HttpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        hostnames: route.spec.hostnames.clone(),
        rules,
    }
}

fn grpc_route_to_snap(route: &GRPCRoute) -> GrpcRouteSnap {
    let gen = route.metadata.generation.unwrap_or(0);
    let creation_timestamp_rfc3339 = route
        .metadata
        .creation_timestamp
        .as_ref()
        .map(|t| t.0.to_rfc3339());
    let rules: Vec<GrpcRouteRuleSnap> = route
        .spec
        .rules
        .iter()
        .map(|r| GrpcRouteRuleSnap {
            matches: r
                .matches
                .iter()
                .map(|m| GrpcRouteMatchSnap {
                    method: m.method.as_ref().map(|mm| GrpcMethodMatchSnap {
                        match_type: mm.match_type.clone(),
                        service: mm.service.clone(),
                        method: mm.method.clone(),
                    }),
                    headers: m
                        .headers
                        .iter()
                        .map(|h| GrpcHeaderMatchSnap {
                            match_type: h.match_type.clone(),
                            name: h.name.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            filters: r.filters.iter().map(grpc_route_filter_snap).collect(),
            backend_refs: r
                .backend_refs
                .iter()
                .map(|b| HttpBackendRefSnap {
                    group: b.group.clone(),
                    kind: b.kind.clone(),
                    namespace: b.namespace.clone(),
                    name: b.name.clone(),
                    port: b.port,
                    port_name: b.port_name.clone(),
                    weight: b.weight,
                })
                .collect(),
        })
        .collect();

    GrpcRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
        creation_timestamp_rfc3339,
        parent_refs: route.spec.parent_refs.iter().map(parent_to_snap).collect(),
        hostnames: route.spec.hostnames.clone(),
        rules,
    }
}

/// Build a [`ClusterSnapshot`] for `gateway_ns`/`gateway_name`.
pub async fn load_cluster_snapshot(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
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
    if let Some(ref allowed) = allowed_listeners {
        let ls_api: Api<ListenerSet> = Api::all(client.clone());
        match ls_api.list(&ListParams::default()).await {
            Ok(ls_list) => {
                for ls in ls_list.items {
                    if !listenerset_parents_gateway(&ls, gateway_ns, gateway_name) {
                        continue;
                    }
                    let ls_ns = ls.metadata.namespace.clone().unwrap_or_default();
                    if !listener_set_namespace_allowed(client, gateway_ns, &ls_ns, allowed).await? {
                        continue;
                    }
                    listener_sets.push(listenerset_to_snap(ls));
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
    let mut http_routes: Vec<HttpRouteSnap> = Vec::new();
    let mut grpc_routes: Vec<GrpcRouteSnap> = Vec::new();
    let mut needed_services: Vec<(String, String)> = Vec::new();
    let mut ns_for_labels: HashSet<String> = HashSet::new();
    ns_for_labels.insert(gateway_ns.to_string());

    for item in routes_list.items {
        if !route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys) {
            continue;
        }
        let rns = item.metadata.namespace.clone().unwrap_or_default();
        ns_for_labels.insert(rns.clone());

        let snap = http_route_to_snap(&item);
        for rule in &item.spec.rules {
            for br in &rule.backend_refs {
                if br.kind.as_deref().unwrap_or("Service") != "Service" {
                    continue;
                }
                let sns = br
                    .namespace
                    .clone()
                    .or(item.metadata.namespace.clone())
                    .unwrap_or_default();
                ns_for_labels.insert(sns.clone());
                needed_services.push((sns, br.name.clone()));
            }
        }
        http_routes.push(snap);
    }

    let grpc_api: Api<GRPCRoute> = Api::all(client.clone());
    match grpc_api.list(&ListParams::default()).await {
        Ok(grpc_list) => {
            for item in grpc_list.items {
                if !grpc_route_relevant_to_gateway(&item, gateway_ns, gateway_name, &attached_ls_keys)
                {
                    continue;
                }
                let rns = item.metadata.namespace.clone().unwrap_or_default();
                ns_for_labels.insert(rns.clone());

                let snap = grpc_route_to_snap(&item);
                for rule in &item.spec.rules {
                    for br in &rule.backend_refs {
                        if br.kind.as_deref().unwrap_or("Service") != "Service" {
                            continue;
                        }
                        let sns = br
                            .namespace
                            .clone()
                            .or(item.metadata.namespace.clone())
                            .unwrap_or_default();
                        ns_for_labels.insert(sns.clone());
                        needed_services.push((sns, br.name.clone()));
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
        if let Ok(lbl) = namespace_labels(client, ns).await {
            ns_label_map.insert(ns.clone(), lbl);
        }
    }

    let mut reference_grants: Vec<ReferenceGrantSnap> = Vec::new();
    let rg_api: Api<ReferenceGrant> = Api::all(client.clone());
    match rg_api.list(&ListParams::default()).await {
        Ok(list) => {
            reference_grants = list.items.into_iter().map(reference_grant_to_snap).collect();
        }
        Err(e) => {
            warn!(
                error = %e,
                "ReferenceGrant list failed — clusters without CRD skip cross-namespace grants"
            );
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

    Ok(ClusterSnapshot {
        gateway_namespace: gateway_ns.to_string(),
        gateway_name: gateway_name.to_string(),
        gateway_class_name: gw.spec.gateway_class_name.clone(),
        gateway_generation: gw.metadata.generation.unwrap_or(0),
        gateway_addresses: gw
            .spec
            .addresses
            .iter()
            .map(|a| GatewayAddressSnap {
                type_: a.type_.clone(),
                value: a.value.clone(),
            })
            .collect(),
        gateway_infrastructure: gw.spec.infrastructure.as_ref().map(|inf| GatewayInfrastructureSnap {
            labels: inf.labels.clone(),
            annotations: inf.annotations.clone(),
        }),
        listeners,
        effective_listeners,
        listener_sets,
        allowed_listeners,
        http_routes,
        grpc_routes,
        services,
        endpoint_slices,
        gateway_class,
        reference_grants,
        namespace_labels: ns_label_map,
    })
}
