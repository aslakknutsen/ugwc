//! Gateway-sized reconcile keys (Phase 3 — pure mapper helpers).

use std::sync::Arc;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::resources::{GRPCRoute, Gateway as GwcGateway, GatewayClass, HTTPRoute, TCPRoute, TLSRoute, UDPRoute};

/// Namespace/name pair used as a controller queue key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GatewayKey {
    pub namespace: String,
    pub name: String,
}

/// Distinguish why a reconcile was enqueued (for logs/metrics).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchEventCause {
    SpecChange,
    RelatedResource { kind: &'static str },
}

/// Pure mapping: which Gateway should reconcile for a given object.
pub fn mapped_gateway_key(
    obj: Arc<dyn std::any::Any + Send + Sync>,
    cause: WatchEventCause,
) -> Option<GatewayKey> {
    let _ = cause;
    if let Some(g) = obj.downcast_ref::<GwcGateway>() {
        return Some(GatewayKey {
            namespace: g.metadata.namespace.clone().unwrap_or_default(),
            name: g.metadata.name.clone().unwrap_or_default(),
        });
    }
    if let Some(gc) = obj.downcast_ref::<GatewayClass>() {
        return Some(GatewayKey {
            namespace: "".into(),
            name: gc.metadata.name.clone().unwrap_or_default(),
        });
    }
    if let Some(r) = obj.downcast_ref::<HTTPRoute>() {
        for p in &r.spec.parent_refs {
            let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
            let k = p.kind.as_deref().unwrap_or("Gateway");
            if g != "gateway.networking.k8s.io" || k != "Gateway" {
                continue;
            }
            let ns = p
                .namespace
                .clone()
                .or(r.metadata.namespace.clone())
                .unwrap_or_default();
            return Some(GatewayKey {
                namespace: ns,
                name: p.name.clone(),
            });
        }
        return None;
    }
    if let Some(r) = obj.downcast_ref::<GRPCRoute>() {
        for p in &r.spec.parent_refs {
            let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
            let k = p.kind.as_deref().unwrap_or("Gateway");
            if g != "gateway.networking.k8s.io" || k != "Gateway" {
                continue;
            }
            let ns = p
                .namespace
                .clone()
                .or(r.metadata.namespace.clone())
                .unwrap_or_default();
            return Some(GatewayKey {
                namespace: ns,
                name: p.name.clone(),
            });
        }
        return None;
    }
    if let Some(r) = obj.downcast_ref::<TLSRoute>() {
        for p in &r.spec.parent_refs {
            let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
            let k = p.kind.as_deref().unwrap_or("Gateway");
            if g != "gateway.networking.k8s.io" || k != "Gateway" {
                continue;
            }
            let ns = p
                .namespace
                .clone()
                .or(r.metadata.namespace.clone())
                .unwrap_or_default();
            return Some(GatewayKey {
                namespace: ns,
                name: p.name.clone(),
            });
        }
        return None;
    }
    if let Some(r) = obj.downcast_ref::<TCPRoute>() {
        for p in &r.spec.parent_refs {
            let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
            let k = p.kind.as_deref().unwrap_or("Gateway");
            if g != "gateway.networking.k8s.io" || k != "Gateway" {
                continue;
            }
            let ns = p
                .namespace
                .clone()
                .or(r.metadata.namespace.clone())
                .unwrap_or_default();
            return Some(GatewayKey {
                namespace: ns,
                name: p.name.clone(),
            });
        }
        return None;
    }
    if let Some(r) = obj.downcast_ref::<UDPRoute>() {
        for p in &r.spec.parent_refs {
            let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
            let k = p.kind.as_deref().unwrap_or("Gateway");
            if g != "gateway.networking.k8s.io" || k != "Gateway" {
                continue;
            }
            let ns = p
                .namespace
                .clone()
                .or(r.metadata.namespace.clone())
                .unwrap_or_default();
            return Some(GatewayKey {
                namespace: ns,
                name: p.name.clone(),
            });
        }
        return None;
    }
    if let Some(_s) = obj.downcast_ref::<Service>() {
        return None;
    }
    if let Some(_e) = obj.downcast_ref::<EndpointSlice>() {
        return None;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{GRPCRoute, HTTPRoute, TCPRoute, TLSRoute, UDPRoute};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    #[test]
    fn gateway_maps_to_self() {
        let g = GwcGateway {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("gw".into()),
                ..Default::default()
            },
            spec: crate::resources::GatewaySpec {
                gateway_class_name: "cls".into(),
                listeners: vec![],
                allowed_listeners: None,
                addresses: vec![],
                infrastructure: None,
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(g), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }

    #[test]
    fn httproute_maps_to_gateway_parent() {
        let r = HTTPRoute {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("hr".into()),
                ..Default::default()
            },
            spec: crate::resources::HTTPRouteSpec {
                hostnames: vec![],
                parent_refs: vec![crate::resources::ParentReferenceSpec {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                rules: vec![],
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(r), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }

    #[test]
    fn grpcroute_maps_to_gateway_parent() {
        let r = GRPCRoute {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("gr".into()),
                ..Default::default()
            },
            spec: crate::resources::GRPCRouteSpec {
                hostnames: vec![],
                parent_refs: vec![crate::resources::ParentReferenceSpec {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                rules: vec![],
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(r), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }

    #[test]
    fn tlsroute_maps_to_gateway_parent() {
        let r = TLSRoute {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("tr".into()),
                ..Default::default()
            },
            spec: crate::resources::TLSRouteSpec {
                hostnames: vec![],
                parent_refs: vec![crate::resources::ParentReferenceSpec {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                rules: vec![],
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(r), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }

    #[test]
    fn tcproute_maps_to_gateway_parent() {
        let r = TCPRoute {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("tcp1".into()),
                ..Default::default()
            },
            spec: crate::resources::TCPRouteSpec {
                parent_refs: vec![crate::resources::ParentReferenceSpec {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                rules: vec![],
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(r), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }

    #[test]
    fn udproute_maps_to_gateway_parent() {
        let r = UDPRoute {
            metadata: ObjectMeta {
                namespace: Some("ns1".into()),
                name: Some("udp1".into()),
                ..Default::default()
            },
            spec: crate::resources::UDPRouteSpec {
                parent_refs: vec![crate::resources::ParentReferenceSpec {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: Some("Gateway".into()),
                    namespace: None,
                    name: "gw".into(),
                    section_name: None,
                    port: None,
                }],
                rules: vec![],
            },
            status: Default::default(),
        };
        let key = mapped_gateway_key(Arc::new(r), WatchEventCause::SpecChange);
        assert_eq!(
            key,
            Some(GatewayKey {
                namespace: "ns1".into(),
                name: "gw".into(),
            })
        );
    }
}
