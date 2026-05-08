//! Gateway API objects via `kube::CustomResource` (typed watches without OpenAPI embedding).

use std::collections::BTreeMap;

use kube::CustomResource;
use serde::{Deserialize, Serialize};

/// Namespaced `Gateway` (spec subset sufficient for watches / status in early phases).
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "Gateway",
    namespaced,
    status = "GatewayStatus",
    schema = "disabled"
)]
pub struct GatewaySpec {
    #[serde(rename = "gatewayClassName", default)]
    pub gateway_class_name: String,
    /// Listener names only (full `Listener` shape is ignored on deserialize).
    #[serde(default)]
    pub listeners: Vec<GatewayListenerSpec>,
    /// Which namespaces may attach `ListenerSet` resources (default: none).
    #[serde(rename = "allowedListeners", default)]
    pub allowed_listeners: Option<AllowedListenersSpec>,
}

/// Subset of `Gateway.spec.listeners[]` needed for status writes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayListenerSpec {
    pub name: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub hostname: Option<String>,
}

/// `Gateway.spec.allowedListeners`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AllowedListenersSpec {
    #[serde(default)]
    pub namespaces: Option<ListenerNamespacesSpec>,
}

/// Namespace selector for attached ListenerSets (Gateway API).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListenerNamespacesSpec {
    /// `None` | `Same` | `All` | `Selector` — omitted means None (no ListenerSets).
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub selector: Option<LabelSelectorSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LabelSelectorSpec {
    #[serde(rename = "matchLabels", default)]
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayStatus {}

/// `ListenerSet` — extra listeners merged onto a parent `Gateway` (Gateway API v1.5+ standard).
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "ListenerSet",
    namespaced,
    status = "ListenerSetStatus",
    schema = "disabled"
)]
pub struct ListenerSetSpec {
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentReferenceSpec>,
    /// Same shape as `Gateway.spec.listeners[]` for the fields we consume.
    #[serde(default)]
    pub listeners: Vec<GatewayListenerSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListenerSetStatus {}

/// Cluster-scoped `GatewayClass`.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "GatewayClass",
    status = "GatewayClassStatus",
    schema = "disabled"
)]
pub struct GatewayClassSpec {
    #[serde(rename = "controllerName", default)]
    pub controller_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayClassStatus {}

/// `HTTPRoute` — parentRefs + rules for IR compilation.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "HTTPRoute",
    namespaced,
    status = "HTTPRouteStatus",
    schema = "disabled"
)]
pub struct HTTPRouteSpec {
    #[serde(rename = "hostnames", default)]
    pub hostnames: Vec<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentReferenceSpec>,
    #[serde(default)]
    pub rules: Vec<HTTPRouteRuleSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ParentReferenceSpec {
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteRuleSpec {
    #[serde(default)]
    pub matches: Vec<HTTPRouteMatchSpec>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HTTPBackendRefSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteMatchSpec {
    pub path: Option<HTTPPathMatchSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPPathMatchSpec {
    #[serde(rename = "type", default)]
    pub path_type: Option<String>,
    pub value: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPBackendRefSpec {
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
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteStatus {
    /// Populated by the apiserver; preserved across ugwc status merges.
    #[serde(default)]
    pub parents: Vec<serde_json::Value>,
}
