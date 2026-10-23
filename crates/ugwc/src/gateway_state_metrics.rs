//! Gateway API state gauges (`gatewayapi_*`) from reconciled cluster snapshots.
//!
//! Each reconcile updates only the Prometheus series for the gateway being
//! reconciled, so concurrent workers do not clobber each other's metrics.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use ugwc_core::snapshot::{ClusterSnapshot, ParentRefSnap};

const SOURCE: &str = "ugwc";

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GatewayLabels {
    pub namespace: String,
    pub name: String,
    pub source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GatewayStatusLabels {
    pub namespace: String,
    pub name: String,
    pub status: String,
    pub source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabels {
    pub namespace: String,
    pub name: String,
    pub source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteRulesLabels {
    pub namespace: String,
    pub name: String,
    pub kind: String,
    pub source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RefGrantLabels {
    pub namespace: String,
    pub name: String,
    pub validated: String,
    pub source: String,
}

/// Holds all `gatewayapi_*` gauge families. Registered once on the global registry.
#[derive(Clone)]
pub struct GatewayStateMetrics {
    pub gateway_created: Family<GatewayLabels, Gauge>,
    pub gateway_status: Family<GatewayStatusLabels, Gauge>,
    pub httproute_attached: Family<RouteLabels, Gauge>,
    pub grpcroute_attached: Family<RouteLabels, Gauge>,
    pub tlsroute_attached: Family<RouteLabels, Gauge>,
    pub tcproute_attached: Family<RouteLabels, Gauge>,
    pub udproute_attached: Family<RouteLabels, Gauge>,
    pub route_rules: Family<RouteRulesLabels, Gauge>,
    pub referencegrant_status: Family<RefGrantLabels, Gauge>,
    pub inferencepool_status: Family<GatewayLabels, Gauge>,
}

impl GatewayStateMetrics {
    /// Register all gatewayapi_* families on the registry.
    pub fn register(registry: &mut Registry) -> Self {
        let gateway_created = Family::<GatewayLabels, Gauge>::default();
        registry.register(
            "gatewayapi_gateway_created",
            "Gateway exists",
            gateway_created.clone(),
        );

        let gateway_status = Family::<GatewayStatusLabels, Gauge>::default();
        registry.register(
            "gatewayapi_gateway_status",
            "Gateway programmed status",
            gateway_status.clone(),
        );

        let httproute_attached = Family::<RouteLabels, Gauge>::default();
        registry.register(
            "gatewayapi_httproute_attached_gateways",
            "HTTPRoute attached gateway count",
            httproute_attached.clone(),
        );

        let grpcroute_attached = Family::<RouteLabels, Gauge>::default();
        registry.register(
            "gatewayapi_grpcroute_attached_gateways",
            "GRPCRoute attached gateway count",
            grpcroute_attached.clone(),
        );

        let tlsroute_attached = Family::<RouteLabels, Gauge>::default();
        registry.register(
            "gatewayapi_tlsroute_attached_gateways",
            "TLSRoute attached gateway count",
            tlsroute_attached.clone(),
        );

        let tcproute_attached = Family::<RouteLabels, Gauge>::default();
        registry.register(
            "gatewayapi_tcproute_attached_gateways",
            "TCPRoute attached gateway count",
            tcproute_attached.clone(),
        );

        let udproute_attached = Family::<RouteLabels, Gauge>::default();
        registry.register(
            "gatewayapi_udproute_attached_gateways",
            "UDPRoute attached gateway count",
            udproute_attached.clone(),
        );

        let route_rules = Family::<RouteRulesLabels, Gauge>::default();
        registry.register(
            "gatewayapi_route_rules_total",
            "Route rule count",
            route_rules.clone(),
        );

        let referencegrant_status = Family::<RefGrantLabels, Gauge>::default();
        registry.register(
            "gatewayapi_referencegrant_status",
            "ReferenceGrant presence",
            referencegrant_status.clone(),
        );

        let inferencepool_status = Family::<GatewayLabels, Gauge>::default();
        registry.register(
            "gatewayapi_inferencepool_status",
            "InferencePool presence",
            inferencepool_status.clone(),
        );

        Self {
            gateway_created,
            gateway_status,
            httproute_attached,
            grpcroute_attached,
            tlsroute_attached,
            tcproute_attached,
            udproute_attached,
            route_rules,
            referencegrant_status,
            inferencepool_status,
        }
    }

    /// Update metrics for one gateway reconcile; only touches this gateway's label set.
    pub fn record_from_reconcile(&self, snap: &ClusterSnapshot, programmed_ok: bool) {
        self.clear_gateway_scope(snap);

        self.gateway_created
            .get_or_create(&GatewayLabels {
                namespace: snap.gateway_namespace.clone(),
                name: snap.gateway_name.clone(),
                source: SOURCE.to_string(),
            })
            .set(1);

        let gw_status = if programmed_ok {
            "Programmed"
        } else {
            "NotProgrammed"
        };
        self.gateway_status
            .get_or_create(&GatewayStatusLabels {
                namespace: snap.gateway_namespace.clone(),
                name: snap.gateway_name.clone(),
                status: gw_status.to_string(),
                source: SOURCE.to_string(),
            })
            .set(1);

        for r in &snap.http_routes {
            self.record_route(
                "HTTPRoute",
                &r.namespace,
                &r.name,
                &r.parent_refs,
                r.rules.len(),
                &snap.gateway_namespace,
                &snap.gateway_name,
            );
        }
        for r in &snap.grpc_routes {
            self.record_route(
                "GRPCRoute",
                &r.namespace,
                &r.name,
                &r.parent_refs,
                r.rules.len(),
                &snap.gateway_namespace,
                &snap.gateway_name,
            );
        }
        for r in &snap.tls_routes {
            self.record_route(
                "TLSRoute",
                &r.namespace,
                &r.name,
                &r.parent_refs,
                r.rules.len(),
                &snap.gateway_namespace,
                &snap.gateway_name,
            );
        }
        for r in &snap.tcp_routes {
            self.record_route(
                "TCPRoute",
                &r.namespace,
                &r.name,
                &r.parent_refs,
                r.rules.len(),
                &snap.gateway_namespace,
                &snap.gateway_name,
            );
        }
        for r in &snap.udp_routes {
            self.record_route(
                "UDPRoute",
                &r.namespace,
                &r.name,
                &r.parent_refs,
                r.rules.len(),
                &snap.gateway_namespace,
                &snap.gateway_name,
            );
        }

        for g in &snap.reference_grants {
            self.referencegrant_status
                .get_or_create(&RefGrantLabels {
                    namespace: g.namespace.clone(),
                    name: g.name.clone(),
                    validated: "present".to_string(),
                    source: SOURCE.to_string(),
                })
                .set(1);
        }

        for pool in &snap.inference_pools {
            self.inferencepool_status
                .get_or_create(&GatewayLabels {
                    namespace: pool.namespace.clone(),
                    name: pool.name.clone(),
                    source: SOURCE.to_string(),
                })
                .set(1);
        }
    }

    /// Remove all series for a deleted gateway (gateway-level labels only).
    pub fn clear_gateway(&self, ns: &str, name: &str) {
        self.remove_gateway_labels(ns, name);
    }

    /// Remove series for one gateway's current snapshot scope before re-emitting.
    fn clear_gateway_scope(&self, snap: &ClusterSnapshot) {
        self.remove_gateway_labels(&snap.gateway_namespace, &snap.gateway_name);

        for r in &snap.http_routes {
            self.remove_route_labels("HTTPRoute", &r.namespace, &r.name);
        }
        for r in &snap.grpc_routes {
            self.remove_route_labels("GRPCRoute", &r.namespace, &r.name);
        }
        for r in &snap.tls_routes {
            self.remove_route_labels("TLSRoute", &r.namespace, &r.name);
        }
        for r in &snap.tcp_routes {
            self.remove_route_labels("TCPRoute", &r.namespace, &r.name);
        }
        for r in &snap.udp_routes {
            self.remove_route_labels("UDPRoute", &r.namespace, &r.name);
        }
        for g in &snap.reference_grants {
            self.referencegrant_status.remove(&RefGrantLabels {
                namespace: g.namespace.clone(),
                name: g.name.clone(),
                validated: "present".to_string(),
                source: SOURCE.to_string(),
            });
        }
        for pool in &snap.inference_pools {
            self.inferencepool_status.remove(&GatewayLabels {
                namespace: pool.namespace.clone(),
                name: pool.name.clone(),
                source: SOURCE.to_string(),
            });
        }
    }

    fn remove_gateway_labels(&self, ns: &str, name: &str) {
        self.gateway_created.remove(&GatewayLabels {
            namespace: ns.to_string(),
            name: name.to_string(),
            source: SOURCE.to_string(),
        });
        self.gateway_status.remove(&GatewayStatusLabels {
            namespace: ns.to_string(),
            name: name.to_string(),
            status: "Programmed".to_string(),
            source: SOURCE.to_string(),
        });
        self.gateway_status.remove(&GatewayStatusLabels {
            namespace: ns.to_string(),
            name: name.to_string(),
            status: "NotProgrammed".to_string(),
            source: SOURCE.to_string(),
        });
    }

    fn remove_route_labels(&self, kind: &str, route_ns: &str, route_name: &str) {
        let labels = RouteLabels {
            namespace: route_ns.to_string(),
            name: route_name.to_string(),
            source: SOURCE.to_string(),
        };
        match kind {
            "HTTPRoute" => self.httproute_attached.remove(&labels),
            "GRPCRoute" => self.grpcroute_attached.remove(&labels),
            "TLSRoute" => self.tlsroute_attached.remove(&labels),
            "TCPRoute" => self.tcproute_attached.remove(&labels),
            "UDPRoute" => self.udproute_attached.remove(&labels),
            _ => self.httproute_attached.remove(&labels),
        };
        self.route_rules.remove(&RouteRulesLabels {
            namespace: route_ns.to_string(),
            name: route_name.to_string(),
            kind: kind.to_string(),
            source: SOURCE.to_string(),
        });
    }

    fn record_route(
        &self,
        kind: &str,
        route_ns: &str,
        route_name: &str,
        parent_refs: &[ParentRefSnap],
        rule_count: usize,
        gw_ns: &str,
        gw_name: &str,
    ) {
        let attached = parent_refs
            .iter()
            .filter(|p| parent_attaches_to_gateway(p, route_ns, gw_ns, gw_name))
            .count();

        let labels = RouteLabels {
            namespace: route_ns.to_string(),
            name: route_name.to_string(),
            source: SOURCE.to_string(),
        };

        let family = match kind {
            "HTTPRoute" => &self.httproute_attached,
            "GRPCRoute" => &self.grpcroute_attached,
            "TLSRoute" => &self.tlsroute_attached,
            "TCPRoute" => &self.tcproute_attached,
            "UDPRoute" => &self.udproute_attached,
            _ => &self.httproute_attached,
        };
        family.get_or_create(&labels).set(attached as i64);

        self.route_rules
            .get_or_create(&RouteRulesLabels {
                namespace: route_ns.to_string(),
                name: route_name.to_string(),
                kind: kind.to_string(),
                source: SOURCE.to_string(),
            })
            .set(rule_count as i64);
    }
}

fn parent_attaches_to_gateway(
    parent: &ParentRefSnap,
    route_ns: &str,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    let g = parent
        .group
        .as_deref()
        .unwrap_or("gateway.networking.k8s.io");
    let k = parent.kind.as_deref().unwrap_or("Gateway");
    if g != "gateway.networking.k8s.io" || k != "Gateway" {
        return false;
    }
    let pns = parent.namespace.as_deref().unwrap_or(route_ns);
    pns == gw_ns && parent.name == gw_name
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::snapshot::ClusterSnapshot;

    fn snap_for_gateway(ns: &str, name: &str) -> ClusterSnapshot {
        ClusterSnapshot {
            gateway_namespace: ns.into(),
            gateway_name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn concurrent_gateways_keep_distinct_series() {
        let mut registry = Registry::default();
        let metrics = GatewayStateMetrics::register(&mut registry);

        metrics.record_from_reconcile(&snap_for_gateway("ns-a", "gw-a"), true);
        metrics.record_from_reconcile(&snap_for_gateway("ns-b", "gw-b"), true);

        let a = metrics.gateway_created.get_or_create(&GatewayLabels {
            namespace: "ns-a".into(),
            name: "gw-a".into(),
            source: SOURCE.to_string(),
        });
        let b = metrics.gateway_created.get_or_create(&GatewayLabels {
            namespace: "ns-b".into(),
            name: "gw-b".into(),
            source: SOURCE.to_string(),
        });
        assert_eq!(a.get(), 1);
        assert_eq!(b.get(), 1);
    }
}
