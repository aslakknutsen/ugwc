//! Kubernetes-agnostic cluster view fed into [`crate::compile::compile`].
//!
//! Populated by `ugwc-k8s` loaders from API objects — no `kube` types here.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use zeroize::Zeroizing;

use crate::ir::BackendTlsIr;

/// TLS stanza on a Gateway API listener (`ListenerTLSConfig` subset).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayListenerTlsSnap {
    /// `Terminate` | `Passthrough` when set.
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(rename = "certificateRefs", default)]
    pub certificate_refs: Vec<SecretObjectReferenceSnap>,
}

/// Reference to a Kubernetes `Secret` (TLS certs).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretObjectReferenceSnap {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
}

/// One `Gateway.spec.listeners[]` row (HTTP/HTTPS focused).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayListenerSnap {
    pub name: String,
    pub port: u16,
    /// Uppercase `HTTP` / `HTTPS` / other.
    pub protocol: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(rename = "allowedRoutes", default)]
    pub allowed_routes: Option<AllowedRoutesSnap>,
    #[serde(default)]
    pub tls: Option<GatewayListenerTlsSnap>,
}

/// Requested `Gateway.spec.addresses[]` entry.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayAddressSnap {
    #[serde(rename = "type", default)]
    pub type_: Option<String>,
    #[serde(default)]
    pub value: String,
}

/// `Gateway.spec.infrastructure` subset for dataplane objects.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayInfrastructureSnap {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AllowedRoutesSnap {
    #[serde(default)]
    pub namespaces: Option<AllowedRoutesNamespacesSnap>,
    #[serde(default)]
    pub kinds: Vec<RouteGroupKindSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AllowedRoutesNamespacesSnap {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub selector_match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteGroupKindSnap {
    #[serde(default)]
    pub group: Option<String>,
    pub kind: String,
}

/// ReferenceGrant — minimal fields for cross-namespace ref checks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceGrantSnap {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub from: Vec<ReferenceGrantFromSnap>,
    #[serde(default)]
    pub to: Vec<ReferenceGrantToSnap>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceGrantFromSnap {
    #[serde(default)]
    pub group: Option<String>,
    pub kind: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceGrantToSnap {
    #[serde(default)]
    pub group: Option<String>,
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
}

/// Parent reference (Gateway API) pointing at a parent resource.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParentRefSnap {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(rename = "namespace", default)]
    pub namespace: Option<String>,
    pub name: String,
    #[serde(rename = "sectionName", default)]
    pub section_name: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpBackendRefSnap {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(rename = "namespace", default)]
    pub namespace: Option<String>,
    pub name: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(rename = "portName", default)]
    pub port_name: Option<String>,
    #[serde(default)]
    pub weight: Option<u32>,
}

impl HttpBackendRefSnap {
    #[must_use]
    pub fn is_inference_pool(&self) -> bool {
        if self.kind.as_deref() != Some("InferencePool") {
            return false;
        }
        let g = self.group.as_deref().unwrap_or("");
        g.is_empty() || g == "inference.networking.k8s.io"
    }

    /// Core `Service` backend (default kind/group), excluding `InferencePool`.
    #[must_use]
    pub fn is_core_service(&self) -> bool {
        if self.is_inference_pool() {
            return false;
        }
        self.kind.as_deref().unwrap_or("Service") == "Service"
            && self.group.as_deref().unwrap_or("").is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpPathMatchSnap {
    #[serde(rename = "type", default)]
    pub path_type: Option<String>,
    pub value: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpHeaderMatchSnap {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpQueryParamMatchSnap {
    pub name: String,
    pub value: String,
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRequestMatchSnap {
    #[serde(default)]
    pub path: Option<HttpPathMatchSnap>,
    #[serde(default)]
    pub headers: Vec<HttpHeaderMatchSnap>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(rename = "queryParams", default)]
    pub query_params: Vec<HttpQueryParamMatchSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteTimeoutsSnap {
    #[serde(default)]
    pub request: Option<String>,
    #[serde(rename = "backendRequest", default)]
    pub backend_request: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteRuleSnap {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub matches: Vec<HttpRequestMatchSnap>,
    #[serde(default)]
    pub filters: Vec<HttpRouteFilterSnap>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
    #[serde(default)]
    pub timeouts: Option<HttpRouteTimeoutsSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpUrlRewriteFilterSnap {
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub path: Option<HttpPathModifierSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteFilterSnap {
    #[serde(rename = "type")]
    pub filter_type: String,
    #[serde(rename = "requestRedirect", default)]
    pub request_redirect: Option<HttpRequestRedirectFilterSnap>,
    #[serde(rename = "requestHeaderModifier", default)]
    pub request_header_modifier: Option<HttpHeaderFilterSnap>,
    #[serde(rename = "responseHeaderModifier", default)]
    pub response_header_modifier: Option<HttpHeaderFilterSnap>,
    #[serde(rename = "urlRewrite", default)]
    pub url_rewrite: Option<HttpUrlRewriteFilterSnap>,
    #[serde(default)]
    pub cors: Option<HttpCorsFilterSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpCorsFilterSnap {
    #[serde(rename = "allowOrigins", default)]
    pub allow_origins: Vec<String>,
    #[serde(rename = "allowMethods", default)]
    pub allow_methods: Vec<String>,
    #[serde(rename = "allowHeaders", default)]
    pub allow_headers: Vec<String>,
    #[serde(rename = "exposeHeaders", default)]
    pub expose_headers: Vec<String>,
    #[serde(rename = "maxAge", default)]
    pub max_age: Option<i32>,
    #[serde(rename = "allowCredentials", default)]
    pub allow_credentials: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRequestRedirectFilterSnap {
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub path: Option<HttpPathModifierSnap>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(rename = "statusCode", default)]
    pub status_code: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpPathModifierSnap {
    #[serde(rename = "type")]
    pub modifier_type: String,
    #[serde(rename = "replaceFullPath", default)]
    pub replace_full_path: Option<String>,
    #[serde(rename = "replacePrefixMatch", default)]
    pub replace_prefix_match: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpHeaderFilterSnap {
    #[serde(default)]
    pub set: Vec<HttpHeaderNameValueSnap>,
    #[serde(default)]
    pub add: Vec<HttpHeaderNameValueSnap>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpHeaderNameValueSnap {
    pub name: String,
    pub value: String,
}

/// One `HTTPRoute` document relevant to compilation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteSnap {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub hostnames: Vec<String>,
    #[serde(default)]
    pub rules: Vec<HttpRouteRuleSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcMethodMatchSnap {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcHeaderMatchSnap {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcRouteMatchSnap {
    #[serde(default)]
    pub method: Option<GrpcMethodMatchSnap>,
    #[serde(default)]
    pub headers: Vec<GrpcHeaderMatchSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcRouteFilterSnap {
    #[serde(rename = "type")]
    pub filter_type: String,
    #[serde(rename = "requestHeaderModifier", default)]
    pub request_header_modifier: Option<HttpHeaderFilterSnap>,
    #[serde(rename = "responseHeaderModifier", default)]
    pub response_header_modifier: Option<HttpHeaderFilterSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcRouteRuleSnap {
    #[serde(default)]
    pub matches: Vec<GrpcRouteMatchSnap>,
    #[serde(default)]
    pub filters: Vec<GrpcRouteFilterSnap>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
}

/// One `GRPCRoute` document relevant to compilation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrpcRouteSnap {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub hostnames: Vec<String>,
    #[serde(default)]
    pub rules: Vec<GrpcRouteRuleSnap>,
}

/// One `TLSRoute` rule (backend refs only).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TlsRouteRuleSnap {
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
}

/// One `TLSRoute` document relevant to compilation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TlsRouteSnap {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub hostnames: Vec<String>,
    #[serde(default)]
    pub rules: Vec<TlsRouteRuleSnap>,
}

/// One `TCPRoute` rule (backend refs only).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TcpRouteRuleSnap {
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
}

/// One `TCPRoute` document relevant to compilation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TcpRouteSnap {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub rules: Vec<TcpRouteRuleSnap>,
}

/// One `UDPRoute` rule (backend refs only).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UdpRouteRuleSnap {
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
}

/// One `UDPRoute` document relevant to compilation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UdpRouteSnap {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub rules: Vec<UdpRouteRuleSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServicePortSnap {
    pub name: String,
    pub port: u16,
    #[serde(rename = "targetPort", default)]
    pub target_port: Option<u16>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceSnap {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub ports: Vec<ServicePortSnap>,
    #[serde(default, rename = "clusterIP")]
    pub cluster_ip: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointPortSnap {
    pub name: Option<String>,
    pub port: u16,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointSnap {
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub ports: Vec<EndpointPortSnap>,
}

/// Subset of `EndpointSlice` used for backend resolution.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointSliceSnap {
    pub namespace: String,
    pub name: String,
    /// Value of `kubernetes.io/service-name` when present.
    #[serde(rename = "serviceName", default)]
    pub service_name: Option<String>,
    #[serde(default)]
    pub endpoints: Vec<EndpointSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GatewayClassSnap {
    pub name: String,
    #[serde(rename = "controllerName", default)]
    pub controller_name: String,
}

/// `Gateway.spec.allowedListeners` subset for merge policy (snapshot).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AllowedListenersSnap {
    #[serde(default)]
    pub namespaces: Option<ListenerNamespacesSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListenerNamespacesSnap {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub selector_match_labels: BTreeMap<String, String>,
}

/// A `ListenerSet` that references this Gateway but was rejected (not allowed or wrong namespace).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RejectedListenerSetSnap {
    pub namespace: String,
    pub name: String,
    pub reason: String,
}

/// Attached `ListenerSet` documents selected by the loader for this Gateway.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListenerSetSnap {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub uid: String,
    #[serde(rename = "creationTimestamp", default)]
    pub creation_timestamp_rfc3339: Option<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentRefSnap>,
    #[serde(default)]
    pub listeners: Vec<GatewayListenerSnap>,
}

/// Provenance for one logical listener after Gateway + ListenerSet merge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ListenerParentSnap {
    InlineGateway,
    ListenerSet { namespace: String, name: String },
}

/// Winning BackendTLS validation for one backend key `namespace/name:port` (loader).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendTlsEffectiveSnap {
    pub tls: BackendTlsIr,
}

/// Status-related outcome for one `BackendTLSPolicy` during a gateway-scoped load.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendTlsPolicyOutcomeSnap {
    pub namespace: String,
    pub name: String,
    /// Lost precedence for every overlapping target with another policy.
    #[serde(default)]
    pub conflicted: bool,
    #[serde(default)]
    pub resolved_refs_ok: bool,
    #[serde(default)]
    pub accepted: bool,
    #[serde(default)]
    pub message: Option<String>,
    /// Gateway API reason for `Accepted` on `status.ancestors` (when set).
    #[serde(rename = "acceptedConditionReason", default)]
    pub accepted_condition_reason: Option<String>,
    /// Gateway API reason for `ResolvedRefs` on `status.ancestors` (when set).
    #[serde(rename = "resolvedRefsConditionReason", default)]
    pub resolved_refs_condition_reason: Option<String>,
}

/// One listener row after merge — [`crate::listener_merge::merge_effective_listeners`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectiveListenerSnap {
    pub parent: ListenerParentSnap,
    /// Listener name within the Gateway or ListenerSet (`sectionName`).
    pub section_name: String,
    pub port: u16,
    pub protocol: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub conflicted: bool,
    /// When `conflicted` is true, the Gateway API reason string (e.g. `HostnameConflict`, `ProtocolConflict`).
    #[serde(rename = "conflictReason", default)]
    pub conflict_reason: Option<String>,
    #[serde(rename = "allowedRoutes", default)]
    pub allowed_routes: Option<AllowedRoutesSnap>,
    #[serde(default)]
    pub tls: Option<GatewayListenerTlsSnap>,
    /// Resolved `tls.crt` / `tls.key` from referenced Secret (loader-only).
    #[serde(default)]
    pub tls_cert_pem: Vec<u8>,
    #[serde(skip)]
    pub tls_key_pem: Zeroizing<Vec<u8>>,
    #[serde(default)]
    pub tls_resolve_error: Option<String>,
}

/// [`InferencePool`](https://gateway-api-inference-extension.sigs.k8s.io/reference/spec/) snapshot (inference.networking.k8s.io/v1).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InferencePoolSnap {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub selector: InferencePoolSelectorSnap,
    #[serde(rename = "targetPorts", default)]
    pub target_ports: Vec<InferencePoolPortSnap>,
    /// `http` or `kubernetes.io/h2c`.
    #[serde(rename = "appProtocol", default)]
    pub app_protocol: Option<String>,
    #[serde(rename = "endpointPickerRef", default)]
    pub endpoint_picker_ref: EndpointPickerRefSnap,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct InferencePoolSelectorSnap {
    #[serde(rename = "matchLabels", default)]
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InferencePoolPortSnap {
    pub number: u16,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointPickerRefSnap {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default)]
    pub port: Option<InferencePoolPortSnap>,
    #[serde(rename = "failureMode", default)]
    pub failure_mode: Option<String>,
}

/// Bounded cluster state for compiling one `Gateway`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub gateway_namespace: String,
    pub gateway_name: String,
    #[serde(rename = "gatewayClassName", default)]
    pub gateway_class_name: String,
    #[serde(rename = "gatewayGeneration", default)]
    pub gateway_generation: i64,
    /// `Gateway.spec.addresses` (requested frontend addresses).
    #[serde(rename = "gatewayAddresses", default)]
    pub gateway_addresses: Vec<GatewayAddressSnap>,
    /// `Gateway.spec.infrastructure` labels/annotations for provisioned workloads.
    #[serde(rename = "gatewayInfrastructure", default)]
    pub gateway_infrastructure: Option<GatewayInfrastructureSnap>,
    /// Inline `Gateway.spec.listeners` only (status rows match this list).
    #[serde(default)]
    pub listeners: Vec<GatewayListenerSnap>,
    #[serde(rename = "effectiveListeners", default)]
    pub effective_listeners: Vec<EffectiveListenerSnap>,
    #[serde(rename = "listenerSets", default)]
    pub listener_sets: Vec<ListenerSetSnap>,
    #[serde(rename = "rejectedListenerSets", default)]
    pub rejected_listener_sets: Vec<RejectedListenerSetSnap>,
    #[serde(rename = "allowedListeners", default)]
    pub allowed_listeners: Option<AllowedListenersSnap>,
    #[serde(rename = "httpRoutes", default)]
    pub http_routes: Vec<HttpRouteSnap>,
    #[serde(rename = "grpcRoutes", default)]
    pub grpc_routes: Vec<GrpcRouteSnap>,
    #[serde(rename = "tlsRoutes", default)]
    pub tls_routes: Vec<TlsRouteSnap>,
    #[serde(rename = "tcpRoutes", default)]
    pub tcp_routes: Vec<TcpRouteSnap>,
    #[serde(rename = "udpRoutes", default)]
    pub udp_routes: Vec<UdpRouteSnap>,
    #[serde(default)]
    pub services: Vec<ServiceSnap>,
    #[serde(rename = "endpointSlices", default)]
    pub endpoint_slices: Vec<EndpointSliceSnap>,
    #[serde(rename = "gatewayClass", default)]
    pub gateway_class: Option<GatewayClassSnap>,
    #[serde(rename = "referenceGrants", default)]
    pub reference_grants: Vec<ReferenceGrantSnap>,
    /// Winning TLS policy per backend key `svc_ns/svc_name:port_number`.
    #[serde(rename = "backendTlsEffective", default)]
    pub backend_tls_effective: BTreeMap<String, BackendTlsEffectiveSnap>,
    /// Per-policy rollup for `BackendTLSPolicy` status patches.
    #[serde(rename = "backendTlsPolicyOutcomes", default)]
    pub backend_tls_policy_outcomes: Vec<BackendTlsPolicyOutcomeSnap>,
    /// Namespace → labels (for `allowedRoutes.namespaces.selector`).
    #[serde(default)]
    pub namespace_labels: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(rename = "inferencePools", default)]
    pub inference_pools: Vec<InferencePoolSnap>,
}

impl ClusterSnapshot {
    /// Distinct `(namespace, name)` pairs for `InferencePool` backends on routes in this snapshot.
    #[must_use]
    pub fn referenced_inference_pools(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        for r in &self.http_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_inference_pool() {
                        let pns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((pns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.grpc_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_inference_pool() {
                        let pns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((pns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.tls_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_inference_pool() {
                        let pns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((pns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.tcp_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_inference_pool() {
                        let pns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((pns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.udp_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_inference_pool() {
                        let pns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((pns, br.name.clone()));
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// Distinct `(namespace, name)` pairs for `Service` backends on routes in this snapshot,
    /// plus any resolved `Service` rows loaded into [`Self::services`].
    #[must_use]
    pub fn referenced_services(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        for r in &self.http_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_core_service() {
                        let sns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((sns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.grpc_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_core_service() {
                        let sns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((sns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.tls_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_core_service() {
                        let sns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((sns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.tcp_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_core_service() {
                        let sns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((sns, br.name.clone()));
                    }
                }
            }
        }
        for r in &self.udp_routes {
            for rule in &r.rules {
                for br in &rule.backend_refs {
                    if br.is_core_service() {
                        let sns = br
                            .namespace
                            .as_deref()
                            .unwrap_or(r.namespace.as_str())
                            .to_string();
                        out.push((sns, br.name.clone()));
                    }
                }
            }
        }
        for s in &self.services {
            out.push((s.namespace.clone(), s.name.clone()));
        }
        out.sort();
        out.dedup();
        out
    }

    /// TLS `certificateRefs` on inline listeners, effective listeners, and attached ListenerSets.
    #[must_use]
    pub fn referenced_tls_secrets(&self, gateway_ns: &str) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        Self::collect_listener_tls_secrets(gateway_ns, &self.listeners, &mut out);
        Self::collect_effective_listener_tls_secrets(
            gateway_ns,
            &self.effective_listeners,
            &mut out,
        );
        for ls in &self.listener_sets {
            Self::collect_listener_tls_secrets(&ls.namespace, &ls.listeners, &mut out);
        }
        out.sort();
        out.dedup();
        out
    }

    fn collect_listener_tls_secrets(
        default_ns: &str,
        listeners: &[GatewayListenerSnap],
        out: &mut Vec<(String, String)>,
    ) {
        for l in listeners {
            let Some(ref tls) = l.tls else {
                continue;
            };
            for r in &tls.certificate_refs {
                let ns = r.namespace.as_deref().unwrap_or(default_ns);
                out.push((ns.to_string(), r.name.clone()));
            }
        }
    }

    fn collect_effective_listener_tls_secrets(
        default_ns: &str,
        listeners: &[EffectiveListenerSnap],
        out: &mut Vec<(String, String)>,
    ) {
        for l in listeners {
            let Some(ref tls) = l.tls else {
                continue;
            };
            for r in &tls.certificate_refs {
                let ns = r.namespace.as_deref().unwrap_or(default_ns);
                out.push((ns.to_string(), r.name.clone()));
            }
        }
    }
}
