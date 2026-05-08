//! Gateway API objects via `kube::CustomResource` (typed watches without OpenAPI embedding).

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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayStatus {}

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
pub struct HTTPRouteStatus {}
