//! Reverse index mapping Services, Secrets, and ConfigMaps to Gateways for targeted fan-out.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use ugwc_core::ClusterSnapshot;

use crate::stores::SharedStores;
use crate::watch_enqueue::{
    all_gateway_keys_from_stores, gateway_keys_for_configmap_from_stores,
    gateway_keys_for_secret_from_stores,
};

pub type ObjKey = (String, String);
pub type GwKey = (String, String);

/// References extracted from a gateway-scoped cluster snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GatewayRefs {
    pub services: HashSet<ObjKey>,
    pub secrets: HashSet<ObjKey>,
    pub configmaps: HashSet<ObjKey>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct IndexEventKindLabels {
    pub event_kind: String,
}

/// Prometheus handles for reverse-index fan-out metrics.
#[derive(Clone)]
pub struct GatewayIndexMetrics {
    pub fanout_size: Family<IndexEventKindLabels, Histogram>,
    pub gateways_indexed: Gauge,
}

impl GatewayIndexMetrics {
    #[must_use]
    pub fn noop() -> Self {
        Self {
            fanout_size: Family::new_with_constructor(|| {
                Histogram::new([0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0].into_iter())
            }),
            gateways_indexed: Gauge::default(),
        }
    }
}

#[derive(Debug, Default)]
struct GatewayIndexInner {
    service_to_gateways: HashMap<ObjKey, HashSet<GwKey>>,
    secret_to_gateways: HashMap<ObjKey, HashSet<GwKey>>,
    configmap_to_gateways: HashMap<ObjKey, HashSet<GwKey>>,
    gateway_to_refs: HashMap<GwKey, GatewayRefs>,
}

/// Thread-safe reverse index populated incrementally after each gateway reconcile.
pub struct GatewayIndex {
    inner: Mutex<GatewayIndexInner>,
    metrics: GatewayIndexMetrics,
}

impl GatewayIndex {
    #[must_use]
    pub fn new(metrics: GatewayIndexMetrics) -> Self {
        Self {
            inner: Mutex::new(GatewayIndexInner::default()),
            metrics,
        }
    }

    pub fn update_gateway(&self, gw_ns: &str, gw_name: &str, refs: GatewayRefs) {
        let gw_key = (gw_ns.to_string(), gw_name.to_string());
        let mut inner = self.inner.lock().expect("gateway index lock");
        if let Some(old) = inner.gateway_to_refs.remove(&gw_key) {
            remove_gateway_refs(&mut inner, &gw_key, &old);
        }
        add_gateway_refs(&mut inner, &gw_key, &refs);
        inner.gateway_to_refs.insert(gw_key, refs);
        self.metrics
            .gateways_indexed
            .set(inner.gateway_to_refs.len() as i64);
    }

    pub fn remove_gateway(&self, gw_ns: &str, gw_name: &str) {
        let gw_key = (gw_ns.to_string(), gw_name.to_string());
        let mut inner = self.inner.lock().expect("gateway index lock");
        if let Some(old) = inner.gateway_to_refs.remove(&gw_key) {
            remove_gateway_refs(&mut inner, &gw_key, &old);
        }
        self.metrics
            .gateways_indexed
            .set(inner.gateway_to_refs.len() as i64);
    }

    #[must_use]
    pub fn gateways_for_service(&self, svc_ns: &str, svc_name: &str) -> HashSet<GwKey> {
        let inner = self.inner.lock().expect("gateway index lock");
        inner
            .service_to_gateways
            .get(&(svc_ns.to_string(), svc_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn gateways_for_secret(&self, secret_ns: &str, secret_name: &str) -> HashSet<GwKey> {
        let inner = self.inner.lock().expect("gateway index lock");
        inner
            .secret_to_gateways
            .get(&(secret_ns.to_string(), secret_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn gateways_for_configmap(&self, cm_ns: &str, cm_name: &str) -> HashSet<GwKey> {
        let inner = self.inner.lock().expect("gateway index lock");
        inner
            .configmap_to_gateways
            .get(&(cm_ns.to_string(), cm_name.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn is_fully_indexed(&self, stores: &SharedStores) -> bool {
        let inner = self.inner.lock().expect("gateway index lock");
        inner.gateway_to_refs.len() >= stores.list_gateways().len()
    }

    #[must_use]
    pub fn resolve_service_fanout(
        &self,
        stores: &SharedStores,
        svc_ns: &str,
        svc_name: &str,
    ) -> HashSet<GwKey> {
        let targeted = self.gateways_for_service(svc_ns, svc_name);
        let keys = if !targeted.is_empty() {
            targeted
        } else if self.is_fully_indexed(stores) {
            HashSet::new()
        } else {
            all_gateway_keys_from_stores(stores)
        };
        self.observe_fanout("endpointslice", keys.len());
        keys
    }

    #[must_use]
    pub fn resolve_secret_fanout(
        &self,
        stores: &SharedStores,
        secret_ns: &str,
        secret_name: &str,
    ) -> HashSet<GwKey> {
        let targeted = self.gateways_for_secret(secret_ns, secret_name);
        let keys = if !targeted.is_empty() {
            targeted
        } else {
            gateway_keys_for_secret_from_stores(stores, secret_ns, secret_name)
        };
        self.observe_fanout("secret", keys.len());
        keys
    }

    #[must_use]
    pub fn resolve_configmap_fanout(
        &self,
        stores: &SharedStores,
        cm_ns: &str,
        cm_name: &str,
    ) -> HashSet<GwKey> {
        let targeted = self.gateways_for_configmap(cm_ns, cm_name);
        let keys = if !targeted.is_empty() {
            targeted
        } else {
            gateway_keys_for_configmap_from_stores(stores, cm_ns, cm_name)
        };
        self.observe_fanout("configmap", keys.len());
        keys
    }

    fn observe_fanout(&self, event_kind: &str, size: usize) {
        self.metrics
            .fanout_size
            .get_or_create(&IndexEventKindLabels {
                event_kind: event_kind.to_string(),
            })
            .observe(size as f64);
    }
}

fn add_gateway_refs(inner: &mut GatewayIndexInner, gw_key: &GwKey, refs: &GatewayRefs) {
    for obj in &refs.services {
        inner
            .service_to_gateways
            .entry(obj.clone())
            .or_default()
            .insert(gw_key.clone());
    }
    for obj in &refs.secrets {
        inner
            .secret_to_gateways
            .entry(obj.clone())
            .or_default()
            .insert(gw_key.clone());
    }
    for obj in &refs.configmaps {
        inner
            .configmap_to_gateways
            .entry(obj.clone())
            .or_default()
            .insert(gw_key.clone());
    }
}

fn remove_gateway_refs(inner: &mut GatewayIndexInner, gw_key: &GwKey, refs: &GatewayRefs) {
    for obj in &refs.services {
        if let Some(set) = inner.service_to_gateways.get_mut(obj) {
            set.remove(gw_key);
            if set.is_empty() {
                inner.service_to_gateways.remove(obj);
            }
        }
    }
    for obj in &refs.secrets {
        if let Some(set) = inner.secret_to_gateways.get_mut(obj) {
            set.remove(gw_key);
            if set.is_empty() {
                inner.secret_to_gateways.remove(obj);
            }
        }
    }
    for obj in &refs.configmaps {
        if let Some(set) = inner.configmap_to_gateways.get_mut(obj) {
            set.remove(gw_key);
            if set.is_empty() {
                inner.configmap_to_gateways.remove(obj);
            }
        }
    }
}

/// Build reverse-index entries from a gateway reconcile snapshot.
#[must_use]
pub fn refs_from_snapshot(stores: &SharedStores, snap: &ClusterSnapshot) -> GatewayRefs {
    let mut refs = GatewayRefs::default();
    for (ns, name) in snap.referenced_services() {
        refs.services.insert((ns, name));
    }
    for (ns, name) in snap.referenced_tls_secrets(&snap.gateway_namespace) {
        refs.secrets.insert((ns, name));
    }
    let (policy_secrets, policy_configmaps) = backend_tls_policy_index_refs(stores, snap);
    refs.secrets.extend(policy_secrets);
    refs.configmaps.extend(policy_configmaps);
    refs
}

fn backend_tls_policy_index_refs(
    stores: &SharedStores,
    snap: &ClusterSnapshot,
) -> (HashSet<ObjKey>, HashSet<ObjKey>) {
    let mut secrets = HashSet::new();
    let mut configmaps = HashSet::new();
    for outcome in &snap.backend_tls_policy_outcomes {
        let Some(pol) = stores.get_backend_tls_policy(&outcome.namespace, &outcome.name) else {
            continue;
        };
        let policy_ns = pol.metadata.namespace.as_deref().unwrap_or("");
        let v = &pol.spec.validation;
        for ca in v.ca_certificate_refs.as_deref().unwrap_or_default() {
            let kind = ca.kind.as_str();
            let kind = if kind.is_empty() { "ConfigMap" } else { kind };
            if kind.eq_ignore_ascii_case("ConfigMap") {
                configmaps.insert((policy_ns.to_string(), ca.name.clone()));
            } else if kind.eq_ignore_ascii_case("Secret") {
                secrets.insert((policy_ns.to_string(), ca.name.clone()));
            }
        }
    }
    (secrets, configmaps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::snapshot::{
        GatewayListenerSnap, GatewayListenerTlsSnap, HttpBackendRefSnap, HttpRouteRuleSnap,
        HttpRouteSnap, SecretObjectReferenceSnap, ServiceSnap,
    };

    fn test_index() -> GatewayIndex {
        GatewayIndex::new(GatewayIndexMetrics::noop())
    }

    fn empty_stores() -> SharedStores {
        let ep = kube::runtime::reflector::store::Writer::<k8s_openapi::api::discovery::v1::EndpointSlice>::default();
        let svc = kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::Service>::default();
        let sec = kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::Secret>::default();
        let cm = kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::ConfigMap>::default();
        let ns = kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::Namespace>::default();
        SharedStores::new(ep.as_reader(), svc.as_reader(), sec.as_reader(), cm.as_reader(), ns.as_reader())
    }

    #[test]
    fn update_replaces_stale_service_mapping() {
        let index = test_index();
        index.update_gateway(
            "ns",
            "gw",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc-a".into())]),
                ..Default::default()
            },
        );
        assert_eq!(
            index.gateways_for_service("ns", "svc-a"),
            HashSet::from([("ns".into(), "gw".into())])
        );
        index.update_gateway(
            "ns",
            "gw",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc-b".into())]),
                ..Default::default()
            },
        );
        assert!(index.gateways_for_service("ns", "svc-a").is_empty());
        assert_eq!(
            index.gateways_for_service("ns", "svc-b"),
            HashSet::from([("ns".into(), "gw".into())])
        );
    }

    #[test]
    fn remove_purges_reverse_entries() {
        let index = test_index();
        index.update_gateway(
            "ns",
            "gw",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc".into())]),
                secrets: HashSet::from([("ns".into(), "sec".into())]),
                configmaps: HashSet::from([("ns".into(), "cm".into())]),
            },
        );
        index.remove_gateway("ns", "gw");
        assert!(index.gateways_for_service("ns", "svc").is_empty());
        assert!(index.gateways_for_secret("ns", "sec").is_empty());
        assert!(index.gateways_for_configmap("ns", "cm").is_empty());
    }

    #[test]
    fn two_gateways_share_service() {
        let index = test_index();
        index.update_gateway(
            "ns",
            "gw-a",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc".into())]),
                ..Default::default()
            },
        );
        index.update_gateway(
            "ns",
            "gw-b",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc".into())]),
                ..Default::default()
            },
        );
        assert_eq!(
            index.gateways_for_service("ns", "svc"),
            HashSet::from([("ns".into(), "gw-a".into()), ("ns".into(), "gw-b".into())])
        );
    }

    #[test]
    fn resolve_service_fanout_empty_before_full_index_uses_broad() {
        let index = test_index();
        let stores = empty_stores();
        let keys = index.resolve_service_fanout(&stores, "ns", "missing");
        assert!(keys.is_empty());
    }

    #[test]
    fn resolve_service_fanout_targeted_when_indexed() {
        let index = test_index();
        let stores = empty_stores();
        index.update_gateway(
            "ns",
            "gw",
            GatewayRefs {
                services: HashSet::from([("ns".into(), "svc".into())]),
                ..Default::default()
            },
        );
        let keys = index.resolve_service_fanout(&stores, "ns", "svc");
        assert_eq!(keys, HashSet::from([("ns".into(), "gw".into())]));
    }

    #[test]
    fn referenced_services_from_route_backend_refs_without_service_rows() {
        let snap = ClusterSnapshot {
            gateway_namespace: "gw-ns".into(),
            http_routes: vec![HttpRouteSnap {
                namespace: "ns".into(),
                name: "rt".into(),
                rules: vec![HttpRouteRuleSnap {
                    backend_refs: vec![HttpBackendRefSnap {
                        name: "svc-0".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let refs = refs_from_snapshot(&empty_stores(), &snap);
        assert!(refs.services.contains(&("ns".into(), "svc-0".into())));
    }

    #[test]
    fn referenced_tls_secrets_from_snapshot() {
        let snap = ClusterSnapshot {
            gateway_namespace: "gw-ns".into(),
            listeners: vec![GatewayListenerSnap {
                name: "https".into(),
                port: 443,
                protocol: "HTTPS".into(),
                tls: Some(GatewayListenerTlsSnap {
                    certificate_refs: vec![SecretObjectReferenceSnap {
                        name: "tls-secret".into(),
                        namespace: Some("sec-ns".into()),
                        group: None,
                        kind: None,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            services: vec![ServiceSnap {
                namespace: "ns".into(),
                name: "svc".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let refs = refs_from_snapshot(&empty_stores(), &snap);
        assert!(refs.services.contains(&("ns".into(), "svc".into())));
        assert!(refs.secrets.contains(&("sec-ns".into(), "tls-secret".into())));
    }

    #[test]
    fn resolve_configmap_fanout_does_not_drop_when_index_full_and_key_missing() {
        let index = test_index();
        let stores = empty_stores();
        index.update_gateway(
            "ns",
            "gw",
            GatewayRefs {
                configmaps: HashSet::from([("ns".into(), "indexed-cm".into())]),
                ..Default::default()
            },
        );
        let keys = index.resolve_configmap_fanout(&stores, "ns", "missing-cm");
        assert!(keys.is_empty());
    }
}
