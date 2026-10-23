//! Watches `CustomResourceDefinition` objects and starts/stops per-kind gateway graph watchers.
//!
//! **RBAC:** the controller ServiceAccount needs cluster scope on `customresourcedefinitions`:
//! `get`, `list`, and `watch`. Without it, this supervisor logs errors and **no** CRD-gated
//! watchers start; core watches (`EndpointSlice`, `Secret`, `ConfigMap`) still run.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::ListParams;
use kube::runtime::reflector;
use kube::runtime::watcher::{watcher, Config as WatcherConfig, Event};
use kube::{Api, Client, Resource};
use std::borrow::Cow;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Live view of which CRDs the supervisor currently considers active.
pub type ActiveCrds = Arc<RwLock<HashSet<ManagedCrd>>>;

use crate::gateway_watchers::{
    spawn_backend_tls_policy_watcher, spawn_gateway_class_watcher, spawn_gateway_watcher,
    spawn_grpc_route_watcher, spawn_http_route_watcher, spawn_inference_pool_watcher,
    spawn_listener_set_watcher, spawn_reference_grant_watcher, spawn_tcp_route_watcher,
    spawn_tls_route_watcher, spawn_udp_route_watcher,
};
use crate::initial_sync::InitialSyncGate;
use crate::resources::{
    BackendTLSPolicy, GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};
use crate::stores::SharedStores;

/// Gateway API + inference CRDs whose watches are gated on CRD presence (`spec.group` + `spec.names.kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManagedCrd {
    Gateway,
    GatewayClass,
    HttpRoute,
    GrpcRoute,
    TlsRoute,
    TcpRoute,
    UdpRoute,
    ListenerSet,
    ReferenceGrant,
    BackendTlsPolicy,
    InferencePool,
}

impl ManagedCrd {
    pub fn from_group_kind(group: &str, kind: &str) -> Option<Self> {
        match (group, kind) {
            ("gateway.networking.k8s.io", "Gateway") => Some(Self::Gateway),
            ("gateway.networking.k8s.io", "GatewayClass") => Some(Self::GatewayClass),
            ("gateway.networking.k8s.io", "HTTPRoute") => Some(Self::HttpRoute),
            ("gateway.networking.k8s.io", "GRPCRoute") => Some(Self::GrpcRoute),
            ("gateway.networking.k8s.io", "TLSRoute") => Some(Self::TlsRoute),
            ("gateway.networking.k8s.io", "TCPRoute") => Some(Self::TcpRoute),
            ("gateway.networking.k8s.io", "UDPRoute") => Some(Self::UdpRoute),
            ("gateway.networking.k8s.io", "ListenerSet") => Some(Self::ListenerSet),
            ("gateway.networking.k8s.io", "ReferenceGrant") => Some(Self::ReferenceGrant),
            ("gateway.networking.k8s.io", "BackendTLSPolicy") => Some(Self::BackendTlsPolicy),
            ("inference.networking.k8s.io", "InferencePool") => Some(Self::InferencePool),
            _ => None,
        }
    }

    /// CRD `spec.versions[].name` (e.g. `"v1"`), not `apiVersion` (`group/version`).
    fn typed_version(&self) -> Cow<'static, str> {
        match self {
            Self::Gateway => <Gateway as Resource>::version(&()),
            Self::GatewayClass => <GatewayClass as Resource>::version(&()),
            Self::HttpRoute => <HTTPRoute as Resource>::version(&()),
            Self::GrpcRoute => <GRPCRoute as Resource>::version(&()),
            Self::TlsRoute => <TLSRoute as Resource>::version(&()),
            Self::TcpRoute => <TCPRoute as Resource>::version(&()),
            Self::UdpRoute => <UDPRoute as Resource>::version(&()),
            Self::ListenerSet => <ListenerSet as Resource>::version(&()),
            Self::ReferenceGrant => <ReferenceGrant as Resource>::version(&()),
            Self::BackendTlsPolicy => <BackendTLSPolicy as Resource>::version(&()),
            Self::InferencePool => <InferencePool as Resource>::version(&()),
        }
    }
}

fn managed_for_crd(crd: &CustomResourceDefinition) -> Option<ManagedCrd> {
    let kind = &crd.spec.names.kind;
    ManagedCrd::from_group_kind(&crd.spec.group, kind)
}

fn crd_eligible(crd: &CustomResourceDefinition) -> bool {
    crd.metadata.deletion_timestamp.is_none()
}

fn served_version_names(crd: &CustomResourceDefinition) -> Vec<String> {
    crd.spec
        .versions
        .iter()
        .filter(|v| v.served)
        .map(|v| v.name.clone())
        .collect()
}

fn warn_version_mismatch_if_needed(
    m: ManagedCrd,
    crd: &CustomResourceDefinition,
    last_warned_rv: &mut HashMap<ManagedCrd, String>,
) {
    let expected = m.typed_version();
    let served = served_version_names(crd);
    if served.iter().any(|v| v == expected.as_ref()) {
        return;
    }
    let rv = crd.metadata.resource_version.clone().unwrap_or_default();
    if last_warned_rv.get(&m) == Some(&rv) {
        return;
    }
    last_warned_rv.insert(m, rv);
    warn!(
        crd_name = %crd.metadata.name.as_deref().unwrap_or(""),
        group = %crd.spec.group,
        kind = %crd.spec.names.kind,
        expected_version = %expected.as_ref(),
        served_versions = ?served,
        "CRD served versions do not include ugwc typed version; watches may fail until the CRD is updated"
    );
}

fn stop_managed(
    m: ManagedCrd,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    active: &ActiveCrds,
    stores: &SharedStores,
) {
    if let Some(t) = running.remove(&m) {
        t.cancel();
        if let Ok(mut set) = active.write() {
            set.remove(&m);
        }
        clear_crd_store(m, stores);
    }
}

fn clear_crd_store(m: ManagedCrd, stores: &SharedStores) {
    match m {
        ManagedCrd::Gateway => stores.clear_gateway_store(),
        ManagedCrd::GatewayClass => stores.clear_gateway_class_store(),
        ManagedCrd::HttpRoute => stores.clear_http_route_store(),
        ManagedCrd::GrpcRoute => stores.clear_grpc_route_store(),
        ManagedCrd::TlsRoute => stores.clear_tls_route_store(),
        ManagedCrd::TcpRoute => stores.clear_tcp_route_store(),
        ManagedCrd::UdpRoute => stores.clear_udp_route_store(),
        ManagedCrd::ListenerSet => stores.clear_listener_set_store(),
        ManagedCrd::ReferenceGrant => stores.clear_reference_grant_store(),
        ManagedCrd::BackendTlsPolicy => stores.clear_backend_tls_policy_store(),
        ManagedCrd::InferencePool => stores.clear_inference_pool_store(),
    }
}

fn stop_for_crd(
    crd: &CustomResourceDefinition,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    active: &ActiveCrds,
    stores: &SharedStores,
) {
    if let Some(m) = managed_for_crd(crd) {
        stop_managed(m, running, active, stores);
    }
}

fn dispatch_spawn(
    m: ManagedCrd,
    client: Client,
    tx: crate::reconcile_queue::ReconcileSender,
    child: CancellationToken,
    owned_controller_names: Vec<String>,
    stores: &SharedStores,
) {
    match m {
        ManagedCrd::Gateway => {
            let w = reflector::store::Writer::default();
            let s = spawn_gateway_watcher(client, tx, child, w);
            stores.set_gateway_store(s);
        }
        ManagedCrd::GatewayClass => {
            let w = reflector::store::Writer::default();
            let s = spawn_gateway_class_watcher(client, tx, child, owned_controller_names, w);
            stores.set_gateway_class_store(s);
        }
        ManagedCrd::HttpRoute => {
            let w = reflector::store::Writer::default();
            let s = spawn_http_route_watcher(client, tx, child, w);
            stores.set_http_route_store(s);
        }
        ManagedCrd::GrpcRoute => {
            let w = reflector::store::Writer::default();
            let s = spawn_grpc_route_watcher(client, tx, child, w);
            stores.set_grpc_route_store(s);
        }
        ManagedCrd::TlsRoute => {
            let w = reflector::store::Writer::default();
            let s = spawn_tls_route_watcher(client, tx, child, w);
            stores.set_tls_route_store(s);
        }
        ManagedCrd::TcpRoute => {
            let w = reflector::store::Writer::default();
            let s = spawn_tcp_route_watcher(client, tx, child, w);
            stores.set_tcp_route_store(s);
        }
        ManagedCrd::UdpRoute => {
            let w = reflector::store::Writer::default();
            let s = spawn_udp_route_watcher(client, tx, child, w);
            stores.set_udp_route_store(s);
        }
        ManagedCrd::InferencePool => {
            let w = reflector::store::Writer::default();
            let s = spawn_inference_pool_watcher(client, tx, child, w);
            stores.set_inference_pool_store(s);
        }
        ManagedCrd::ReferenceGrant => {
            let w = reflector::store::Writer::default();
            let s = spawn_reference_grant_watcher(client, tx, child, w);
            stores.set_reference_grant_store(s);
        }
        ManagedCrd::ListenerSet => {
            let w = reflector::store::Writer::default();
            let s = spawn_listener_set_watcher(client, tx, child, w);
            stores.set_listener_set_store(s);
        }
        ManagedCrd::BackendTlsPolicy => {
            let w = reflector::store::Writer::default();
            let s = spawn_backend_tls_policy_watcher(client, tx, child, w);
            stores.set_backend_tls_policy_store(s);
        }
    }
}

pub(crate) async fn run_crd_supervisor(
    client: Client,
    shutdown: CancellationToken,
    tx: crate::reconcile_queue::ReconcileSender,
    owned_controller_names: Vec<String>,
    active: ActiveCrds,
    sync_gate: Option<Arc<InitialSyncGate>>,
    stores: Arc<SharedStores>,
) {
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let mut running: HashMap<ManagedCrd, CancellationToken> = HashMap::new();
    let mut last_warned_rv: HashMap<ManagedCrd, String> = HashMap::new();

    match api.list(&ListParams::default()).await {
        Ok(list) => {
            for crd in list.items {
                apply_crd(
                    &crd,
                    &mut running,
                    &mut last_warned_rv,
                    &client,
                    &shutdown,
                    &tx,
                    &owned_controller_names,
                    &active,
                    &stores,
                )
                .await;
            }
            if let Some(gate) = &sync_gate {
                gate.mark_crd_list();
            }
        }
        Err(e) => warn!(
            error = %e,
            "crd supervisor initial list failed; waiting for CRD watch. Gated watchers will not start until list/watch succeeds"
        ),
    }

    let cfg = WatcherConfig::default().any_semantic();
    let mut stream = Box::pin(watcher(api, cfg));
    let mut backoff_ms: u64 = 200;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            ev = stream.next() => {
                match ev {
                    Some(Ok(Event::Init)) => {
                        for (_, t) in running.drain() {
                            t.cancel();
                        }
                        if let Ok(mut set) = active.write() {
                            set.clear();
                        }
                        last_warned_rv.clear();
                    }
                    Some(Ok(Event::Apply(crd))) | Some(Ok(Event::InitApply(crd))) => {
                        backoff_ms = 200;
                        apply_crd(
                            &crd,
                            &mut running,
                            &mut last_warned_rv,
                            &client,
                            &shutdown,
                            &tx,
                            &owned_controller_names,
                            &active,
                            &stores,
                        )
                        .await;
                    }
                    Some(Ok(Event::Delete(crd))) => {
                        stop_for_crd(&crd, &mut running, &active, &stores);
                    }
                    Some(Ok(Event::InitDone)) => {}
                    Some(Err(e)) => {
                        warn!(error = %e, "customresourcedefinition watch stream");
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                        }
                        backoff_ms = (backoff_ms.saturating_mul(2)).min(30_000);
                    }
                    None => {
                        warn!("customresourcedefinition watch stream ended");
                        break;
                    }
                }
            }
        }
    }

    for (_, t) in running.drain() {
        t.cancel();
    }
    if let Ok(mut set) = active.write() {
        set.clear();
    }
    info!("crd supervisor shutdown");
}

#[allow(clippy::too_many_arguments)]
async fn apply_crd(
    crd: &CustomResourceDefinition,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    last_warned_rv: &mut HashMap<ManagedCrd, String>,
    client: &Client,
    shutdown: &CancellationToken,
    tx: &crate::reconcile_queue::ReconcileSender,
    owned_controller_names: &[String],
    active: &ActiveCrds,
    stores: &SharedStores,
) {
    let Some(m) = managed_for_crd(crd) else {
        return;
    };
    if !crd_eligible(crd) {
        stop_managed(m, running, active, stores);
        return;
    }
    warn_version_mismatch_if_needed(m, crd, last_warned_rv);
    if running.contains_key(&m) {
        return;
    }
    let child = shutdown.child_token();
    running.insert(m, child.clone());
    if let Ok(mut set) = active.write() {
        set.insert(m);
    }
    dispatch_spawn(
        m,
        client.clone(),
        tx.clone(),
        child,
        owned_controller_names.to_vec(),
        stores,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn crd(
        group: &str,
        kind: &str,
        versions: &[(&str, bool)],
        deletion: bool,
    ) -> CustomResourceDefinition {
        CustomResourceDefinition {
            metadata: ObjectMeta {
                name: Some(format!("things.{group}")),
                resource_version: Some("1".to_string()),
                deletion_timestamp: if deletion {
                    Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                        k8s_openapi::jiff::Timestamp::now(),
                    ))
                } else {
                    None
                },
                ..Default::default()
            },
            spec: k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionSpec {
                group: group.to_string(),
                names: k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionNames {
                    kind: kind.to_string(),
                    plural: "things".to_string(),
                    ..Default::default()
                },
                scope: "Namespaced".to_string(),
                versions: versions
                    .iter()
                    .map(|(name, served)| {
                        k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinitionVersion {
                            name: name.to_string(),
                            served: *served,
                            storage: true,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn managed_from_group_kind() {
        assert_eq!(
            ManagedCrd::from_group_kind("gateway.networking.k8s.io", "UDPRoute"),
            Some(ManagedCrd::UdpRoute)
        );
        assert_eq!(ManagedCrd::from_group_kind("foo.bar", "Baz"), None);
    }

    #[test]
    fn eligible_respects_deletion_timestamp() {
        let c = crd(
            "gateway.networking.k8s.io",
            "Gateway",
            &[("v1", true)],
            true,
        );
        assert!(!crd_eligible(&c));
        let c2 = crd(
            "gateway.networking.k8s.io",
            "Gateway",
            &[("v1", true)],
            false,
        );
        assert!(crd_eligible(&c2));
    }

    #[test]
    fn served_versions_filters_unserved() {
        let c = crd(
            "gateway.networking.k8s.io",
            "TCPRoute",
            &[("v1alpha2", true), ("v1beta1", false)],
            false,
        );
        assert_eq!(served_version_names(&c), vec!["v1alpha2".to_string()]);
    }

    #[test]
    fn tlsroute_served_v1_matches_typed_version() {
        let mut map = HashMap::new();
        let c = crd(
            "gateway.networking.k8s.io",
            "TLSRoute",
            &[("v1", true)],
            false,
        );
        warn_version_mismatch_if_needed(ManagedCrd::TlsRoute, &c, &mut map);
        assert!(
            map.is_empty(),
            "served v1 should match typed version, not apiVersion group/v1"
        );
    }

    #[test]
    fn version_warn_throttles_by_resource_version() {
        let mut map = HashMap::new();
        let c = crd(
            "gateway.networking.k8s.io",
            "UDPRoute",
            &[("v1", true)],
            false,
        );
        warn_version_mismatch_if_needed(ManagedCrd::UdpRoute, &c, &mut map);
        assert!(map.contains_key(&ManagedCrd::UdpRoute));
        warn_version_mismatch_if_needed(ManagedCrd::UdpRoute, &c, &mut map);
        assert_eq!(map.len(), 1);
    }
}
