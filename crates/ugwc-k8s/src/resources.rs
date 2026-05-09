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
    /// Requested addresses for this Gateway (Extended).
    #[serde(default)]
    pub addresses: Vec<GatewayAddressSpec>,
    /// Labels/annotations for implementation-managed infrastructure (Extended).
    #[serde(default)]
    pub infrastructure: Option<GatewayInfrastructureSpec>,
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
    #[serde(default)]
    pub tls: Option<ListenerTlsSpec>,
}

/// Subset of `ListenerTLSConfig`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListenerTlsSpec {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(rename = "certificateRefs", default)]
    pub certificate_refs: Vec<SecretObjectReferenceSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SecretObjectReferenceSpec {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
}

/// `Gateway.spec.addresses[]`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayAddressSpec {
    #[serde(rename = "type", default)]
    pub type_: Option<String>,
    #[serde(default)]
    pub value: String,
}

/// `Gateway.spec.infrastructure`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GatewayInfrastructureSpec {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
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

/// Returns true if any listener TLS `certificateRefs` targets `secret_ns`/`secret_name`.
#[must_use]
pub fn gateway_spec_references_secret(
    spec: &GatewaySpec,
    gateway_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    for l in &spec.listeners {
        let Some(ref tls) = l.tls else {
            continue;
        };
        for r in &tls.certificate_refs {
            let ns = r.namespace.as_deref().unwrap_or(gateway_ns);
            if ns == secret_ns && r.name == secret_name {
                return true;
            }
        }
    }
    false
}

/// Same as [`gateway_spec_references_secret`] for a `ListenerSet` spec (namespace defaults to ListenerSet namespace).
#[must_use]
pub fn listenerset_spec_references_secret(
    spec: &ListenerSetSpec,
    listenerset_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    for l in &spec.listeners {
        let Some(ref tls) = l.tls else {
            continue;
        };
        for r in &tls.certificate_refs {
            let ns = r.namespace.as_deref().unwrap_or(listenerset_ns);
            if ns == secret_ns && r.name == secret_name {
                return true;
            }
        }
    }
    false
}

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

/// `GRPCRoute` — parentRefs + rules (Gateway API v1).
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "GRPCRoute",
    namespaced,
    status = "GRPCRouteStatus",
    schema = "disabled"
)]
pub struct GRPCRouteSpec {
    #[serde(rename = "hostnames", default)]
    pub hostnames: Vec<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentReferenceSpec>,
    #[serde(default)]
    pub rules: Vec<GRPCRouteRuleSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCRouteRuleSpec {
    #[serde(default)]
    pub matches: Vec<GRPCRouteMatchSpec>,
    #[serde(default)]
    pub filters: Vec<GRPCRouteFilterSpec>,
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HTTPBackendRefSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCRouteMatchSpec {
    #[serde(default)]
    pub method: Option<GRPCMethodMatchSpec>,
    #[serde(default)]
    pub headers: Vec<GRPCHeaderMatchSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCMethodMatchSpec {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCHeaderMatchSpec {
    #[serde(rename = "type", default)]
    pub match_type: Option<String>,
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCRouteFilterSpec {
    #[serde(rename = "type")]
    pub filter_type: String,
    #[serde(rename = "requestHeaderModifier", default)]
    pub request_header_modifier: Option<HTTPHeaderFilterSpec>,
    #[serde(rename = "responseHeaderModifier", default)]
    pub response_header_modifier: Option<HTTPHeaderFilterSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GRPCRouteStatus {
    #[serde(default)]
    pub parents: Vec<serde_json::Value>,
}

/// `TLSRoute` — SNI-based routing for `protocol: TLS` listeners (Gateway API v1, experimental CRDs).
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "TLSRoute",
    namespaced,
    status = "TLSRouteStatus",
    schema = "disabled"
)]
pub struct TLSRouteSpec {
    #[serde(rename = "hostnames", default)]
    pub hostnames: Vec<String>,
    #[serde(rename = "parentRefs", default)]
    pub parent_refs: Vec<ParentReferenceSpec>,
    #[serde(default)]
    pub rules: Vec<TLSRouteRuleSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TLSRouteRuleSpec {
    #[serde(rename = "backendRefs", default)]
    pub backend_refs: Vec<HTTPBackendRefSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TLSRouteStatus {
    #[serde(default)]
    pub parents: Vec<serde_json::Value>,
}
