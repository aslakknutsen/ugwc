//! Gateway-sized reconcile keys (Phase 3 — pure mapper helpers).

use std::sync::Arc;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::resources::{Gateway as GwcGateway, GatewayClass, HTTPRoute};

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
    if let Some(_r) = obj.downcast_ref::<HTTPRoute>() {
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
}
