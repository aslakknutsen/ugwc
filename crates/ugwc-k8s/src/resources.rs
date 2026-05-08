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
    /// [`Gateway` listener `allowedRoutes`](https://gateway-api.sigs.k8s.io/reference/spec/#gateway.networking.k8s.io/v1.Listener).
    #[serde(rename = "allowedRoutes", default)]
    pub allowed_routes: Option<AllowedRoutesSpec>,
}

/// `Listener.allowedRoutes` — which routes may attach.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AllowedRoutesSpec {
    #[serde(default)]
    pub namespaces: Option<AllowedRoutesNamespacesSpec>,
    #[serde(default)]
    pub kinds: Vec<RouteGroupKindSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AllowedRoutesNamespacesSpec {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub selector: Option<LabelSelectorSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RouteGroupKindSpec {
    #[serde(default)]
    pub group: Option<String>,
    pub kind: String,
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

/// [`ReferenceGrant`](https://gateway-api.sigs.k8s.io/reference/spec/#gateway.networking.k8s.io/v1beta1.ReferenceGrant) — cross-namespace reference policy.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1beta1",
    kind = "ReferenceGrant",
    namespaced,
    status = "ReferenceGrantStatus",
    schema = "disabled"
)]
pub struct ReferenceGrantSpec {
    #[serde(default)]
    pub from: Vec<ReferenceGrantFromSpec>,
    #[serde(default)]
    pub to: Vec<ReferenceGrantToSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReferenceGrantStatus {}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReferenceGrantFromSpec {
    #[serde(rename = "group", default)]
    pub group: Option<String>,
    pub kind: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReferenceGrantToSpec {
    #[serde(rename = "group", default)]
    pub group: Option<String>,
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
}

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
    #[serde(default)]
    pub filters: Vec<HTTPRouteFilterSpec>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HTTPBackendRefSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteMatchSpec {
    pub path: Option<HTTPPathMatchSpec>,
    #[serde(default)]
    pub headers: Vec<HTTPHeaderMatchSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPHeaderMatchSpec {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteFilterSpec {
    #[serde(rename = "type")]
    pub filter_type: String,
    #[serde(rename = "requestRedirect", default)]
    pub request_redirect: Option<HTTPRequestRedirectFilterSpec>,
    #[serde(rename = "requestHeaderModifier", default)]
    pub request_header_modifier: Option<HTTPHeaderFilterSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRequestRedirectFilterSpec {
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub path: Option<HTTPPathModifierSpec>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(rename = "statusCode", default)]
    pub status_code: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPPathModifierSpec {
    #[serde(rename = "type")]
    pub modifier_type: String,
    #[serde(rename = "replaceFullPath", default)]
    pub replace_full_path: Option<String>,
    #[serde(rename = "replacePrefixMatch", default)]
    pub replace_prefix_match: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPHeaderFilterSpec {
    #[serde(default)]
    pub set: Vec<HTTPHeaderNameValueSpec>,
    #[serde(default)]
    pub add: Vec<HTTPHeaderNameValueSpec>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPHeaderNameValueSpec {
    pub name: String,
    pub value: String,
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
    #[serde(default)]
    pub weight: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HTTPRouteStatus {
    /// Populated by the apiserver; preserved across ugwc status merges.
    #[serde(default)]
    pub parents: Vec<serde_json::Value>,
}
