//! Gateway API CRD types re-exported from `gateway_api` crate, plus hand-rolled `InferencePool`.

use std::collections::BTreeMap;

use kube::CustomResource;
use serde::{Deserialize, Serialize};

// ── Standard channel re-exports ──────────────────────────────────────────────
pub use gateway_api::backendtlspolicies::*;
pub use gateway_api::gatewayclasses::*;
pub use gateway_api::gateways::*;
pub use gateway_api::grpcroutes::*;
pub use gateway_api::httproutes::*;
pub use gateway_api::listenersets::*;
pub use gateway_api::referencegrants::*;
pub use gateway_api::tlsroutes::*;

// ── Experimental channel re-exports (TCPRoute, UDPRoute) ─────────────────────
pub use gateway_api::experimental::tcproutes::*;
pub use gateway_api::experimental::udproutes::*;

// ── Convenience aliases for the old type names ───────────────────────────────
// The gateway-api crate uses `GwcGateway`-style prefixes for nested types from
// kopium codegen (e.g. `GatewayListeners` not `GatewayListenerSpec`).
// These aliases let other crates transition gradually.
pub type GatewaySpec = gateway_api::gateways::GatewaySpec;
pub type ListenerSetSpec = gateway_api::listenersets::ListenerSetSpec;

// ── InferencePool (hand-rolled — no upstream crate covers this CRD) ──────────

/// [`InferencePool`](https://gateway-api-inference-extension.sigs.k8s.io/reference/spec/) — inference.networking.k8s.io/v1.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize)]
#[kube(
    group = "inference.networking.k8s.io",
    version = "v1",
    kind = "InferencePool",
    namespaced,
    status = "InferencePoolStatus",
    schema = "disabled"
)]
pub struct InferencePoolSpec {
    pub selector: InferencePoolSelectorSpec,
    #[serde(rename = "targetPorts")]
    pub target_ports: Vec<InferencePoolPortSpec>,
    #[serde(rename = "appProtocol", default)]
    pub app_protocol: Option<String>,
    #[serde(rename = "endpointPickerRef")]
    pub endpoint_picker_ref: EndpointPickerRefSpec,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InferencePoolSelectorSpec {
    #[serde(rename = "matchLabels", default)]
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InferencePoolPortSpec {
    pub number: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EndpointPickerRefSpec {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default)]
    pub port: Option<InferencePoolPortSpec>,
    #[serde(rename = "failureMode", default)]
    pub failure_mode: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InferencePoolStatus {
    #[serde(default)]
    pub parents: Vec<serde_json::Value>,
}

// ── Utility functions ────────────────────────────────────────────────────────

/// Returns true if any Gateway listener TLS `certificateRefs` targets `secret_ns`/`secret_name`.
#[must_use]
pub fn gateway_spec_references_secret(
    spec: &gateway_api::gateways::GatewaySpec,
    gateway_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    for l in &spec.listeners {
        let Some(ref tls) = l.tls else { continue };
        for r in tls.certificate_refs.as_deref().unwrap_or_default() {
            let ns = r.namespace.as_deref().unwrap_or(gateway_ns);
            if ns == secret_ns && r.name == secret_name {
                return true;
            }
        }
    }
    false
}

/// Same as [`gateway_spec_references_secret`] for a `ListenerSet` spec.
#[must_use]
pub fn listenerset_spec_references_secret(
    spec: &gateway_api::listenersets::ListenerSetSpec,
    listenerset_ns: &str,
    secret_ns: &str,
    secret_name: &str,
) -> bool {
    for l in &spec.listeners {
        let Some(ref tls) = l.tls else { continue };
        for r in tls.certificate_refs.as_deref().unwrap_or_default() {
            let ns = r.namespace.as_deref().unwrap_or(listenerset_ns);
            if ns == secret_ns && r.name == secret_name {
                return true;
            }
        }
    }
    false
}

/// True when a backend ref targets an `InferencePool`.
#[must_use]
pub fn is_inference_pool_backend_ref(br: &HttpRouteRulesBackendRefs) -> bool {
    if br.kind.as_deref() != Some("InferencePool") {
        return false;
    }
    let g = br.group.as_deref().unwrap_or("");
    g.is_empty() || g == "inference.networking.k8s.io"
}

#[must_use]
pub fn is_inference_pool_grpc_backend_ref(br: &GrpcRouteRulesBackendRefs) -> bool {
    if br.kind.as_deref() != Some("InferencePool") {
        return false;
    }
    let g = br.group.as_deref().unwrap_or("");
    g.is_empty() || g == "inference.networking.k8s.io"
}

#[must_use]
pub fn is_inference_pool_tls_backend_ref(br: &TlsRouteRulesBackendRefs) -> bool {
    if br.kind.as_deref() != Some("InferencePool") {
        return false;
    }
    let g = br.group.as_deref().unwrap_or("");
    g.is_empty() || g == "inference.networking.k8s.io"
}

#[must_use]
pub fn is_inference_pool_tcp_backend_ref(br: &TcpRouteRulesBackendRefs) -> bool {
    if br.kind.as_deref() != Some("InferencePool") {
        return false;
    }
    let g = br.group.as_deref().unwrap_or("");
    g.is_empty() || g == "inference.networking.k8s.io"
}

#[must_use]
pub fn is_inference_pool_udp_backend_ref(br: &UdpRouteRulesBackendRefs) -> bool {
    if br.kind.as_deref() != Some("InferencePool") {
        return false;
    }
    let g = br.group.as_deref().unwrap_or("");
    g.is_empty() || g == "inference.networking.k8s.io"
}

/// True if `BackendTLSPolicy` validation references this ConfigMap.
#[must_use]
pub fn backend_tls_policy_spec_references_configmap(
    spec: &BackendTlsPolicySpec,
    _policy_namespace: &str,
    cm_namespace: &str,
    cm_name: &str,
) -> bool {
    for ca in spec
        .validation
        .ca_certificate_refs
        .as_deref()
        .unwrap_or_default()
    {
        let is_cm =
            ca.kind.eq_ignore_ascii_case("ConfigMap") && (ca.group.is_empty() || ca.group == "");
        if is_cm && ca.name == cm_name {
            // CACertificateRefs is LocalObjectReference (no namespace field) — always local.
            if _policy_namespace == cm_namespace {
                return true;
            }
        }
    }
    false
}

/// True if `BackendTLS` validation references this `Secret`.
#[must_use]
pub fn backend_tls_policy_spec_references_secret(
    spec: &BackendTlsPolicySpec,
    policy_namespace: &str,
    secret_namespace: &str,
    secret_name: &str,
) -> bool {
    for ca in spec
        .validation
        .ca_certificate_refs
        .as_deref()
        .unwrap_or_default()
    {
        if ca.kind.eq_ignore_ascii_case("Secret")
            && (ca.group.is_empty() || ca.group == "")
            && ca.name == secret_name
            && policy_namespace == secret_namespace
        {
            return true;
        }
    }
    false
}

/// True if any target ref selects `Service` `svc_name` in `svc_namespace`.
#[must_use]
pub fn backend_tls_policy_targets_service(
    spec: &BackendTlsPolicySpec,
    policy_namespace: &str,
    svc_namespace: &str,
    svc_name: &str,
) -> bool {
    if policy_namespace != svc_namespace {
        return false;
    }
    for t in &spec.target_refs {
        let is_svc =
            t.kind.eq_ignore_ascii_case("Service") && (t.group.is_empty() || t.group == "");
        if is_svc && t.name == svc_name {
            return true;
        }
    }
    false
}

/// True if any target ref selects `InferencePool` `pool_name` in `pool_namespace`.
#[must_use]
pub fn backend_tls_policy_targets_inference_pool(
    spec: &BackendTlsPolicySpec,
    policy_namespace: &str,
    pool_namespace: &str,
    pool_name: &str,
) -> bool {
    if policy_namespace != pool_namespace {
        return false;
    }
    for t in &spec.target_refs {
        let is_pool = t.kind.eq_ignore_ascii_case("InferencePool")
            && (t.group.is_empty() || t.group.eq_ignore_ascii_case("inference.networking.k8s.io"));
        if is_pool && t.name == pool_name {
            return true;
        }
    }
    false
}

fn push_core_service_http_backend_ref(
    route_ns: &str,
    br: &HttpRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    if br.kind.as_deref().unwrap_or("Service") != "Service" {
        return;
    }
    let g = br.group.as_deref().unwrap_or("");
    if !g.is_empty() {
        return;
    }
    let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
    out.push((sns, br.name.clone()));
}

fn push_core_service_grpc_backend_ref(
    route_ns: &str,
    br: &GrpcRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    if br.kind.as_deref().unwrap_or("Service") != "Service" {
        return;
    }
    let g = br.group.as_deref().unwrap_or("");
    if !g.is_empty() {
        return;
    }
    let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
    out.push((sns, br.name.clone()));
}

fn push_core_service_tls_backend_ref(
    route_ns: &str,
    br: &TlsRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    if br.kind.as_deref().unwrap_or("Service") != "Service" {
        return;
    }
    let g = br.group.as_deref().unwrap_or("");
    if !g.is_empty() {
        return;
    }
    let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
    out.push((sns, br.name.clone()));
}

fn push_core_service_tcp_backend_ref(
    route_ns: &str,
    br: &TcpRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    if br.kind.as_deref().unwrap_or("Service") != "Service" {
        return;
    }
    let g = br.group.as_deref().unwrap_or("");
    if !g.is_empty() {
        return;
    }
    let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
    out.push((sns, br.name.clone()));
}

fn push_core_service_udp_backend_ref(
    route_ns: &str,
    br: &UdpRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    if br.kind.as_deref().unwrap_or("Service") != "Service" {
        return;
    }
    let g = br.group.as_deref().unwrap_or("");
    if !g.is_empty() {
        return;
    }
    let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
    out.push((sns, br.name.clone()));
}

fn collect_http_filter_service_refs(
    route_ns: &str,
    f: &HttpRouteRulesFilters,
    out: &mut Vec<(String, String)>,
) {
    if let Some(ref m) = f.request_mirror {
        let br = &m.backend_ref;
        let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        if br.kind.as_deref().unwrap_or("Service") == "Service"
            && br.group.as_deref().unwrap_or("").is_empty()
        {
            out.push((sns, br.name.clone()));
        }
    }
}

fn collect_http_backend_ref_service_refs(
    route_ns: &str,
    br: &HttpRouteRulesBackendRefs,
    out: &mut Vec<(String, String)>,
) {
    push_core_service_http_backend_ref(route_ns, br, out);
    for f in br.filters.as_deref().unwrap_or_default() {
        if let Some(ref m) = f.request_mirror {
            let mbr = &m.backend_ref;
            let sns = mbr.namespace.as_deref().unwrap_or(route_ns).to_string();
            if mbr.kind.as_deref().unwrap_or("Service") == "Service"
                && mbr.group.as_deref().unwrap_or("").is_empty()
            {
                out.push((sns, mbr.name.clone()));
            }
        }
    }
}

/// Service backends referenced from HTTP route rule.
#[must_use]
pub fn collect_http_route_rule_service_refs(
    route_ns: &str,
    rule: &HttpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in rule.backend_refs.as_deref().unwrap_or_default() {
        collect_http_backend_ref_service_refs(route_ns, br, &mut out);
    }
    for f in rule.filters.as_deref().unwrap_or_default() {
        collect_http_filter_service_refs(route_ns, f, &mut out);
    }
    out
}

fn collect_grpc_filter_service_refs(
    route_ns: &str,
    f: &GrpcRouteRulesFilters,
    out: &mut Vec<(String, String)>,
) {
    if let Some(ref m) = f.request_mirror {
        let br = &m.backend_ref;
        let sns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        if br.kind.as_deref().unwrap_or("Service") == "Service"
            && br.group.as_deref().unwrap_or("").is_empty()
        {
            out.push((sns, br.name.clone()));
        }
    }
}

/// Service backends referenced from GRPC route rule.
#[must_use]
pub fn collect_grpc_route_rule_service_refs(
    route_ns: &str,
    rule: &GrpcRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in rule.backend_refs.as_deref().unwrap_or_default() {
        push_core_service_grpc_backend_ref(route_ns, br, &mut out);
    }
    for f in rule.filters.as_deref().unwrap_or_default() {
        collect_grpc_filter_service_refs(route_ns, f, &mut out);
    }
    out
}

/// `InferencePool` backends referenced on this HTTP rule.
#[must_use]
pub fn collect_http_route_rule_inference_pool_refs(
    route_ns: &str,
    rule: &HttpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in rule.backend_refs.as_deref().unwrap_or_default() {
        if !is_inference_pool_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// `InferencePool` backends referenced on this GRPC rule.
#[must_use]
pub fn collect_grpc_route_rule_inference_pool_refs(
    route_ns: &str,
    rule: &GrpcRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in rule.backend_refs.as_deref().unwrap_or_default() {
        if !is_inference_pool_grpc_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// Service backends referenced from TLS route rule.
#[must_use]
pub fn collect_tls_route_rule_service_refs(
    route_ns: &str,
    rule: &TlsRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        push_core_service_tls_backend_ref(route_ns, br, &mut out);
    }
    out
}

/// `InferencePool` backends referenced on this TLS route rule.
#[must_use]
pub fn collect_tls_route_rule_inference_pool_refs(
    route_ns: &str,
    rule: &TlsRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        if !is_inference_pool_tls_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// Service backends referenced from TCP route rule.
#[must_use]
pub fn collect_tcp_route_rule_service_refs(
    route_ns: &str,
    rule: &TcpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        push_core_service_tcp_backend_ref(route_ns, br, &mut out);
    }
    out
}

/// `InferencePool` backends referenced on this TCP route rule.
#[must_use]
pub fn collect_tcp_route_rule_inference_pool_refs(
    route_ns: &str,
    rule: &TcpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        if !is_inference_pool_tcp_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// Service backends referenced from UDP route rule.
#[must_use]
pub fn collect_udp_route_rule_service_refs(
    route_ns: &str,
    rule: &UdpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        push_core_service_udp_backend_ref(route_ns, br, &mut out);
    }
    out
}

/// `InferencePool` backends referenced on this UDP route rule.
#[must_use]
pub fn collect_udp_route_rule_inference_pool_refs(
    route_ns: &str,
    rule: &UdpRouteRules,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in &rule.backend_refs {
        if !is_inference_pool_udp_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// `InferencePool` backends from HTTP `backendRefs`.
#[must_use]
pub fn collect_backend_refs_inference_pool_refs(
    route_ns: &str,
    backend_refs: &[HttpRouteRulesBackendRefs],
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in backend_refs {
        if !is_inference_pool_backend_ref(br) {
            continue;
        }
        let pns = br.namespace.as_deref().unwrap_or(route_ns).to_string();
        out.push((pns, br.name.clone()));
    }
    out
}

/// Service backends from HTTP `backendRefs`.
#[must_use]
pub fn collect_backend_refs_service_refs(
    route_ns: &str,
    backend_refs: &[HttpRouteRulesBackendRefs],
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for br in backend_refs {
        collect_http_backend_ref_service_refs(route_ns, br, &mut out);
    }
    out
}
