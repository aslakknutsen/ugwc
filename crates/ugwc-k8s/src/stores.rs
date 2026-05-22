//! In-memory reflector stores for Kubernetes resources used during reconcile.
//!
//! [`SharedStores`] holds a [`kube::runtime::reflector::Store`] per watched resource type.
//! Always-on watches (EndpointSlice, Service, Secret, ConfigMap, Namespace) have permanent
//! stores created at controller startup.  CRD-gated watches (Gateway, routes, policies, etc.)
//! have stores set/cleared when the CRD supervisor starts/stops the corresponding watcher.

use std::hash::Hash;
use std::sync::{Arc, RwLock};

use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::reflector::{ObjectRef, Store};

use crate::resources::{
    BackendTLSPolicy, GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet,
    ReferenceGrant, TCPRoute, TLSRoute, UDPRoute,
};

/// Thread-safe optional store for CRD-gated resource types.
type CrdStore<K> = Arc<RwLock<Option<Store<K>>>>;

fn new_crd_store<K: kube::Resource<DynamicType: Hash + Eq>>() -> CrdStore<K> {
    Arc::new(RwLock::new(None))
}

/// Reflector-backed in-memory caches for all watched Kubernetes resources.
///
/// Always-on stores are populated at controller startup.  CRD-gated stores are
/// set when the corresponding CRD watcher starts, and cleared when it stops.
pub struct SharedStores {
    // ── Always-on (created at controller startup) ────────────────────────
    pub endpoint_slices: Store<EndpointSlice>,
    pub services: Store<Service>,
    pub secrets: Store<Secret>,
    pub config_maps: Store<ConfigMap>,
    pub namespaces: Store<Namespace>,

    // ── CRD-gated (set when watcher starts, None before) ────────────────
    gateways: CrdStore<Gateway>,
    gateway_classes: CrdStore<GatewayClass>,
    http_routes: CrdStore<HTTPRoute>,
    grpc_routes: CrdStore<GRPCRoute>,
    tls_routes: CrdStore<TLSRoute>,
    tcp_routes: CrdStore<TCPRoute>,
    udp_routes: CrdStore<UDPRoute>,
    listener_sets: CrdStore<ListenerSet>,
    reference_grants: CrdStore<ReferenceGrant>,
    backend_tls_policies: CrdStore<BackendTLSPolicy>,
    inference_pools: CrdStore<InferencePool>,
}

impl SharedStores {
    /// Build a new `SharedStores` with the given always-on stores.
    /// CRD-gated slots start as `None`.
    pub fn new(
        endpoint_slices: Store<EndpointSlice>,
        services: Store<Service>,
        secrets: Store<Secret>,
        config_maps: Store<ConfigMap>,
        namespaces: Store<Namespace>,
    ) -> Self {
        Self {
            endpoint_slices,
            services,
            secrets,
            config_maps,
            namespaces,
            gateways: new_crd_store(),
            gateway_classes: new_crd_store(),
            http_routes: new_crd_store(),
            grpc_routes: new_crd_store(),
            tls_routes: new_crd_store(),
            tcp_routes: new_crd_store(),
            udp_routes: new_crd_store(),
            listener_sets: new_crd_store(),
            reference_grants: new_crd_store(),
            backend_tls_policies: new_crd_store(),
            inference_pools: new_crd_store(),
        }
    }

    // ── CRD-gated store lifecycle (called by crd_supervisor) ────────────

    pub fn set_gateway_store(&self, store: Store<Gateway>) {
        *self.gateways.write().expect("gateways lock") = Some(store);
    }
    pub fn clear_gateway_store(&self) {
        *self.gateways.write().expect("gateways lock") = None;
    }

    pub fn set_gateway_class_store(&self, store: Store<GatewayClass>) {
        *self.gateway_classes.write().expect("gateway_classes lock") = Some(store);
    }
    pub fn clear_gateway_class_store(&self) {
        *self.gateway_classes.write().expect("gateway_classes lock") = None;
    }

    pub fn set_http_route_store(&self, store: Store<HTTPRoute>) {
        *self.http_routes.write().expect("http_routes lock") = Some(store);
    }
    pub fn clear_http_route_store(&self) {
        *self.http_routes.write().expect("http_routes lock") = None;
    }

    pub fn set_grpc_route_store(&self, store: Store<GRPCRoute>) {
        *self.grpc_routes.write().expect("grpc_routes lock") = Some(store);
    }
    pub fn clear_grpc_route_store(&self) {
        *self.grpc_routes.write().expect("grpc_routes lock") = None;
    }

    pub fn set_tls_route_store(&self, store: Store<TLSRoute>) {
        *self.tls_routes.write().expect("tls_routes lock") = Some(store);
    }
    pub fn clear_tls_route_store(&self) {
        *self.tls_routes.write().expect("tls_routes lock") = None;
    }

    pub fn set_tcp_route_store(&self, store: Store<TCPRoute>) {
        *self.tcp_routes.write().expect("tcp_routes lock") = Some(store);
    }
    pub fn clear_tcp_route_store(&self) {
        *self.tcp_routes.write().expect("tcp_routes lock") = None;
    }

    pub fn set_udp_route_store(&self, store: Store<UDPRoute>) {
        *self.udp_routes.write().expect("udp_routes lock") = Some(store);
    }
    pub fn clear_udp_route_store(&self) {
        *self.udp_routes.write().expect("udp_routes lock") = None;
    }

    pub fn set_listener_set_store(&self, store: Store<ListenerSet>) {
        *self.listener_sets.write().expect("listener_sets lock") = Some(store);
    }
    pub fn clear_listener_set_store(&self) {
        *self.listener_sets.write().expect("listener_sets lock") = None;
    }

    pub fn set_reference_grant_store(&self, store: Store<ReferenceGrant>) {
        *self.reference_grants.write().expect("reference_grants lock") = Some(store);
    }
    pub fn clear_reference_grant_store(&self) {
        *self.reference_grants.write().expect("reference_grants lock") = None;
    }

    pub fn set_backend_tls_policy_store(&self, store: Store<BackendTLSPolicy>) {
        *self.backend_tls_policies.write().expect("backend_tls_policies lock") = Some(store);
    }
    pub fn clear_backend_tls_policy_store(&self) {
        *self.backend_tls_policies.write().expect("backend_tls_policies lock") = None;
    }

    pub fn set_inference_pool_store(&self, store: Store<InferencePool>) {
        *self.inference_pools.write().expect("inference_pools lock") = Some(store);
    }
    pub fn clear_inference_pool_store(&self) {
        *self.inference_pools.write().expect("inference_pools lock") = None;
    }

    // ── List accessors (equivalent to Api::list) ────────────────────────

    fn crd_state<K: 'static + kube::Resource<DynamicType = ()> + Clone>(
        slot: &CrdStore<K>,
    ) -> Vec<Arc<K>> {
        slot.read()
            .expect("crd store lock")
            .as_ref()
            .map_or_else(Vec::new, |s| s.state())
    }

    pub fn list_endpoint_slices(&self) -> Vec<Arc<EndpointSlice>> {
        self.endpoint_slices.state()
    }

    pub fn list_services(&self) -> Vec<Arc<Service>> {
        self.services.state()
    }

    pub fn list_secrets(&self) -> Vec<Arc<Secret>> {
        self.secrets.state()
    }

    pub fn list_config_maps(&self) -> Vec<Arc<ConfigMap>> {
        self.config_maps.state()
    }

    pub fn list_namespaces(&self) -> Vec<Arc<Namespace>> {
        self.namespaces.state()
    }

    pub fn list_gateways(&self) -> Vec<Arc<Gateway>> {
        Self::crd_state(&self.gateways)
    }

    pub fn list_gateway_classes(&self) -> Vec<Arc<GatewayClass>> {
        Self::crd_state(&self.gateway_classes)
    }

    pub fn list_http_routes(&self) -> Vec<Arc<HTTPRoute>> {
        Self::crd_state(&self.http_routes)
    }

    pub fn list_grpc_routes(&self) -> Vec<Arc<GRPCRoute>> {
        Self::crd_state(&self.grpc_routes)
    }

    pub fn list_tls_routes(&self) -> Vec<Arc<TLSRoute>> {
        Self::crd_state(&self.tls_routes)
    }

    pub fn list_tcp_routes(&self) -> Vec<Arc<TCPRoute>> {
        Self::crd_state(&self.tcp_routes)
    }

    pub fn list_udp_routes(&self) -> Vec<Arc<UDPRoute>> {
        Self::crd_state(&self.udp_routes)
    }

    pub fn list_listener_sets(&self) -> Vec<Arc<ListenerSet>> {
        Self::crd_state(&self.listener_sets)
    }

    pub fn list_reference_grants(&self) -> Vec<Arc<ReferenceGrant>> {
        Self::crd_state(&self.reference_grants)
    }

    pub fn list_backend_tls_policies(&self) -> Vec<Arc<BackendTLSPolicy>> {
        Self::crd_state(&self.backend_tls_policies)
    }

    pub fn list_inference_pools(&self) -> Vec<Arc<InferencePool>> {
        Self::crd_state(&self.inference_pools)
    }

    // ── Get accessors (equivalent to Api::get) ──────────────────────────

    fn crd_get<K>(slot: &CrdStore<K>, ns: &str, name: &str) -> Option<Arc<K>>
    where
        K: 'static + kube::Resource<DynamicType = ()> + Clone,
    {
        let guard = slot.read().expect("crd store lock");
        let store = guard.as_ref()?;
        store.get(&ObjectRef::new(name).within(ns))
    }

    fn crd_get_cluster<K>(slot: &CrdStore<K>, name: &str) -> Option<Arc<K>>
    where
        K: 'static + kube::Resource<DynamicType = ()> + Clone,
    {
        let guard = slot.read().expect("crd store lock");
        let store = guard.as_ref()?;
        store.get(&ObjectRef::new(name))
    }

    pub fn get_secret(&self, ns: &str, name: &str) -> Option<Arc<Secret>> {
        self.secrets.get(&ObjectRef::<Secret>::new(name).within(ns))
    }

    pub fn get_config_map(&self, ns: &str, name: &str) -> Option<Arc<ConfigMap>> {
        self.config_maps.get(&ObjectRef::<ConfigMap>::new(name).within(ns))
    }

    pub fn get_service(&self, ns: &str, name: &str) -> Option<Arc<Service>> {
        self.services.get(&ObjectRef::<Service>::new(name).within(ns))
    }

    pub fn get_namespace(&self, name: &str) -> Option<Arc<Namespace>> {
        self.namespaces.get(&ObjectRef::<Namespace>::new(name))
    }

    pub fn get_gateway(&self, ns: &str, name: &str) -> Option<Arc<Gateway>> {
        Self::crd_get(&self.gateways, ns, name)
    }

    pub fn get_gateway_class(&self, name: &str) -> Option<Arc<GatewayClass>> {
        Self::crd_get_cluster(&self.gateway_classes, name)
    }

    pub fn get_inference_pool(&self, ns: &str, name: &str) -> Option<Arc<InferencePool>> {
        Self::crd_get(&self.inference_pools, ns, name)
    }
}
