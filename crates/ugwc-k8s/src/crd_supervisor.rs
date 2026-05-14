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
use kube::runtime::watcher::{watcher, Config as WatcherConfig, Event};
use kube::{Api, Client, Resource};
use std::borrow::Cow;
use tokio::sync::mpsc;
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
use crate::resources::{
    BackendTLSPolicy, Gateway, GatewayClass, GRPCRoute, HTTPRoute, InferencePool, ListenerSet,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};

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

    /// API version used by ugwc's typed `kube::CustomResource` clients (for mismatch warnings).
    fn typed_api_version(self) -> Cow<'static, str> {
        match self {
            Self::Gateway => Gateway::version(&()),
            Self::GatewayClass => GatewayClass::version(&()),
            Self::HttpRoute => HTTPRoute::version(&()),
            Self::GrpcRoute => GRPCRoute::version(&()),
            Self::TlsRoute => TLSRoute::version(&()),
            Self::TcpRoute => TCPRoute::version(&()),
            Self::UdpRoute => UDPRoute::version(&()),
            Self::ListenerSet => ListenerSet::version(&()),
            Self::ReferenceGrant => ReferenceGrant::version(&()),
            Self::BackendTlsPolicy => BackendTLSPolicy::version(&()),
            Self::InferencePool => InferencePool::version(&()),
        }
    }
}

fn crd_eligible(crd: &CustomResourceDefinition) -> bool {
    crd.metadata.deletion_timestamp.is_none()
}

fn managed_for_crd(crd: &CustomResourceDefinition) -> Option<ManagedCrd> {
    ManagedCrd::from_group_kind(&crd.spec.group, &crd.spec.names.kind)
}

fn served_version_names(crd: &CustomResourceDefinition) -> Vec<String> {
    crd.spec
        .versions
        .iter()
        .filter(|v| v.served)
        .map(|v| v.name.clone())
        .collect()
}

/// Warn once per `resourceVersion` when the typed client version is not among served CRD versions.
fn warn_version_mismatch_if_needed(
    m: ManagedCrd,
    crd: &CustomResourceDefinition,
    last_warned_rv: &mut HashMap<ManagedCrd, String>,
) {
    let expected = m.typed_api_version();
    let served = served_version_names(crd);
    if served.iter().any(|v| v == expected.as_ref()) {
        return;
    }
    let rv = crd
        .metadata
        .resource_version
        .clone()
        .unwrap_or_default();
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
        "CRD served versions do not include ugwc typed API version; watches may fail until the CRD is updated"
    );
}

fn stop_managed(
    m: ManagedCrd,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    active: &ActiveCrds,
) {
    if let Some(t) = running.remove(&m) {
        t.cancel();
        if let Ok(mut set) = active.write() {
            set.remove(&m);
        }
    }
}

fn stop_for_crd(
    crd: &CustomResourceDefinition,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    active: &ActiveCrds,
) {
    if let Some(m) = managed_for_crd(crd) {
        stop_managed(m, running, active);
    }
}

fn dispatch_spawn(
    m: ManagedCrd,
    client: Client,
    tx: mpsc::Sender<(String, String)>,
    child: CancellationToken,
    owned_controller_names: Vec<String>,
) {
    match m {
        ManagedCrd::Gateway => spawn_gateway_watcher(client, tx, child),
        ManagedCrd::GatewayClass => {
            spawn_gateway_class_watcher(client, tx, child, owned_controller_names)
        }
        ManagedCrd::HttpRoute => spawn_http_route_watcher(client, tx, child),
        ManagedCrd::GrpcRoute => spawn_grpc_route_watcher(client, tx, child),
        ManagedCrd::TlsRoute => spawn_tls_route_watcher(client, tx, child),
        ManagedCrd::TcpRoute => spawn_tcp_route_watcher(client, tx, child),
        ManagedCrd::UdpRoute => spawn_udp_route_watcher(client, tx, child),
        ManagedCrd::InferencePool => spawn_inference_pool_watcher(client, tx, child),
        ManagedCrd::ReferenceGrant => spawn_reference_grant_watcher(client, tx, child),
        ManagedCrd::ListenerSet => spawn_listener_set_watcher(client, tx, child),
        ManagedCrd::BackendTlsPolicy => spawn_backend_tls_policy_watcher(client, tx, child),
    }
}

async fn apply_crd(
    crd: &CustomResourceDefinition,
    running: &mut HashMap<ManagedCrd, CancellationToken>,
    last_warned_rv: &mut HashMap<ManagedCrd, String>,
    client: &Client,
    shutdown: &CancellationToken,
    tx: &mpsc::Sender<(String, String)>,
    owned_controller_names: &[String],
    active: &ActiveCrds,
) {
    let Some(m) = managed_for_crd(crd) else {
        return;
    };
    if !crd_eligible(crd) {
        stop_managed(m, running, active);
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
    );
}

pub(crate) async fn run_crd_supervisor(
    client: Client,
    shutdown: CancellationToken,
    tx: mpsc::Sender<(String, String)>,
    owned_controller_names: Vec<String>,
    active: ActiveCrds,
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
                )
                .await;
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
                        )
                        .await;
                    }
                    Some(Ok(Event::Delete(crd))) => {
                        stop_for_crd(&crd, &mut running, &active);
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

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn crd(group: &str, kind: &str, versions: &[(&str, bool)], deletion: bool) -> CustomResourceDefinition {
        CustomResourceDefinition {
            metadata: ObjectMeta {
                name: Some(format!("things.{group}")),
                resource_version: Some("1".to_string()),
                deletion_timestamp: if deletion {
                    Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                        chrono::Utc::now(),
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
        let c = crd("gateway.networking.k8s.io", "Gateway", &[("v1", true)], true);
        assert!(!crd_eligible(&c));
        let c2 = crd("gateway.networking.k8s.io", "Gateway", &[("v1", true)], false);
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
    fn version_warn_throttles_by_resource_version() {
        let mut map = HashMap::new();
        let c = crd("gateway.networking.k8s.io", "UDPRoute", &[("v1", true)], false);
        // typed UDPRoute is v1alpha2 in ugwc — expect mismatch with only v1 served
        warn_version_mismatch_if_needed(ManagedCrd::UdpRoute, &c, &mut map);
        assert!(map.contains_key(&ManagedCrd::UdpRoute));
        warn_version_mismatch_if_needed(ManagedCrd::UdpRoute, &c, &mut map);
        assert_eq!(map.len(), 1);
    }
}
