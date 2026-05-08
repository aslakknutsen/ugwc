//! Kubernetes-agnostic cluster view fed into [`crate::compile::compile`].
//!
//! Populated by `ugwc-k8s` loaders from API objects — no `kube` types here.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
pub struct HttpRequestMatchSnap {
    #[serde(default)]
    pub path: Option<HttpPathMatchSnap>,
    #[serde(default)]
    pub headers: Vec<HttpHeaderMatchSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteRuleSnap {
    #[serde(default)]
    pub matches: Vec<HttpRequestMatchSnap>,
    #[serde(default)]
    pub filters: Vec<HttpRouteFilterSnap>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HttpBackendRefSnap>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpRouteFilterSnap {
    #[serde(rename = "type")]
    pub filter_type: String,
    #[serde(rename = "requestRedirect", default)]
    pub request_redirect: Option<HttpRequestRedirectFilterSnap>,
    #[serde(rename = "requestHeaderModifier", default)]
    pub request_header_modifier: Option<HttpHeaderFilterSnap>,
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
    #[serde(rename = "allowedRoutes", default)]
    pub allowed_routes: Option<AllowedRoutesSnap>,
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
    /// Inline `Gateway.spec.listeners` only (status rows match this list).
    #[serde(default)]
    pub listeners: Vec<GatewayListenerSnap>,
    #[serde(rename = "effectiveListeners", default)]
    pub effective_listeners: Vec<EffectiveListenerSnap>,
    #[serde(rename = "listenerSets", default)]
    pub listener_sets: Vec<ListenerSetSnap>,
    #[serde(rename = "allowedListeners", default)]
    pub allowed_listeners: Option<AllowedListenersSnap>,
    #[serde(rename = "httpRoutes", default)]
    pub http_routes: Vec<HttpRouteSnap>,
    #[serde(default)]
    pub services: Vec<ServiceSnap>,
    #[serde(rename = "endpointSlices", default)]
    pub endpoint_slices: Vec<EndpointSliceSnap>,
    #[serde(rename = "gatewayClass", default)]
    pub gateway_class: Option<GatewayClassSnap>,
    #[serde(rename = "referenceGrants", default)]
    pub reference_grants: Vec<ReferenceGrantSnap>,
    /// Namespace → labels (for `allowedRoutes.namespaces.selector`).
    #[serde(default)]
    pub namespace_labels: BTreeMap<String, BTreeMap<String, String>>,
}
