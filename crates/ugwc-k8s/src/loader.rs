//! Load [`ugwc_core::ClusterSnapshot`] for a reconciled `Gateway`.

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ListParams;
use kube::{Api, Client};
use thiserror::Error;
use ugwc_core::snapshot::{
    ClusterSnapshot, EndpointPortSnap, EndpointSliceSnap, EndpointSnap, GatewayClassSnap,
    GatewayListenerSnap, HttpBackendRefSnap, HttpPathMatchSnap, HttpRequestMatchSnap,
    HttpRouteRuleSnap, HttpRouteSnap, ParentRefSnap, ServicePortSnap, ServiceSnap,
};

use crate::resources::{Gateway, GatewayClass, HTTPRoute, ParentReferenceSpec};

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
}

fn parent_to_snap(p: &ParentReferenceSpec) -> ParentRefSnap {
    ParentRefSnap {
        group: p.group.clone(),
        kind: p.kind.clone(),
        namespace: p.namespace.clone(),
        name: p.name.clone(),
        section_name: p.section_name.clone(),
        port: p.port,
    }
}

fn route_attaches_to_gateway(route: &HTTPRoute, gw_ns: &str, gw_name: &str) -> bool {
    route.spec.parent_refs.iter().any(|p| {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" || k != "Gateway" {
            return false;
        }
        let pns = p
            .namespace
            .as_deref()
            .or(route.metadata.namespace.as_deref())
            .unwrap_or("");
        pns == gw_ns && p.name == gw_name
    })
}

fn http_route_to_snap(route: &HTTPRoute) -> HttpRouteSnap {
    let gen = route.metadata.generation.unwrap_or(0);
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
                    hostname: None,
                })
                .collect(),
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
                })
                .collect(),
        })
        .collect();

    HttpRouteSnap {
        namespace: route.metadata.namespace.clone().unwrap_or_default(),
        name: route.metadata.name.clone().unwrap_or_default(),
        generation: gen,
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
        .map(|l| GatewayListenerSnap {
            name: l.name.clone(),
            port: l.port,
            protocol: if l.protocol.is_empty() {
                "HTTP".into()
            } else {
                l.protocol.clone()
            },
            hostname: l.hostname.clone(),
        })
        .collect();

    let route_api: Api<HTTPRoute> = Api::all(client.clone());
    let routes_list = route_api.list(&ListParams::default()).await?;
    let mut http_routes: Vec<HttpRouteSnap> = Vec::new();
    let mut needed_services: Vec<(String, String)> = Vec::new();

    for item in routes_list.items {
        if !route_attaches_to_gateway(&item, gateway_ns, gateway_name) {
            continue;
        }
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
                needed_services.push((sns, br.name.clone()));
            }
        }
        http_routes.push(snap);
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
        listeners,
        http_routes,
        services,
        endpoint_slices,
        gateway_class,
    })
}
