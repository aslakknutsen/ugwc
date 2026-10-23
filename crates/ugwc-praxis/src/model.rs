//! Serde model for Praxis native YAML configuration.
//!
//! Field names and shapes mirror `praxis_core::config`; every struct here
//! serializes into a document that Praxis accepts with its
//! `deny_unknown_fields` deserializers. Keep this module mechanical: mapping
//! decisions live in [`crate::encode`].

use std::collections::BTreeMap;

use serde::Serialize;

/// Top-level Praxis configuration document.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PraxisConfig {
    pub listeners: Vec<Listener>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_chains: Vec<FilterChain>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insecure_options: Option<InsecureOptions>,
}

/// Listener transport protocol. Praxis only supports these two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Http,
    Tcp,
}

/// One listening socket.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Listener {
    pub name: String,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_chains: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<ListenerTls>,
    /// Direct upstream for L4 TCP listeners.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
}

/// Terminating TLS material for a listener.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ListenerTls {
    pub certificates: Vec<CertKeyPair>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_reload: Option<bool>,
}

/// Certificate/key file paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CertKeyPair {
    pub cert_path: String,
    pub key_path: String,
}

/// Named ordered filter pipeline.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FilterChain {
    pub name: String,
    pub filters: Vec<Filter>,
}

/// One filter entry. `#[serde(tag = "filter")]` produces the
/// `filter: <name>` shape Praxis expects.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "filter", rename_all = "snake_case")]
pub enum Filter {
    Router(RouterFilter),
    LoadBalancer(LoadBalancerFilter),
    Redirect(RedirectFilter),
    Headers(HeadersFilter),
    Cors(CorsFilter),
    PathRewrite(PathRewriteFilter),
    Timeout(TimeoutFilter),
    SniRouter(SniRouterFilter),
}

/// Request-phase predicate gating a filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Condition {
    pub when: ConditionMatch,
}

/// Predicate fields understood by Praxis request conditions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ConditionMatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
}

impl ConditionMatch {
    /// True when this predicate constrains nothing (matches every request).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.grpc.is_none()
            && self.path.is_none()
            && self.path_prefix.is_none()
            && self.methods.is_empty()
            && self.headers.is_empty()
    }
}

/// Router filter: ordered route table.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RouterFilter {
    pub routes: Vec<RouterRoute>,
}

/// One router route. Exactly one of `path`/`path_prefix` is set.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RouterRoute {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    pub cluster: String,
}

/// Load balancer filter holding cluster definitions.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LoadBalancerFilter {
    pub clusters: Vec<Cluster>,
}

/// Upstream cluster.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Cluster {
    pub name: String,
    pub endpoints: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<ClusterTls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<ClusterHttp>,
}

/// Upstream TLS (client role); presence enables TLS.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClusterTls {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<CaConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cert: Option<CertKeyPair>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sni: Option<String>,
    pub verify: bool,
}

/// Custom CA bundle file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CaConfig {
    pub ca_path: String,
}

/// HTTP options for a cluster (gRPC needs `version: h2`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterHttp {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Fixed upstream `Host` / HTTP/2 `:authority` for this cluster.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authority: Option<String>,
}

/// Request redirect filter.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RedirectFilter {
    pub status: u16,
    pub location: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_hosts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Header manipulation filter.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct HeadersFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_add: Vec<HeaderPair>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_remove: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_set: Vec<HeaderPair>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_add: Vec<HeaderPair>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_remove: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_set: Vec<HeaderPair>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Header name/value pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HeaderPair {
    pub name: String,
    pub value: String,
}

/// CORS filter.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CorsFilter {
    pub allow_origins: Vec<String>,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age: u32,
    pub allow_credentials: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Path rewrite filter (strip/add/replace pipeline).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PathRewriteFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strip_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replace: Option<RegexReplace>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_rewrite_override: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Regex replacement pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RegexReplace {
    pub pattern: String,
    pub replacement: String,
}

/// Response timeout filter.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TimeoutFilter {
    pub timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// SNI router for TLS passthrough listeners.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SniRouterFilter {
    pub routes: Vec<SniRoute>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_upstream: Option<String>,
}

/// SNI route entry.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SniRoute {
    pub server_names: Vec<String>,
    pub upstream: String,
}

/// Security overrides. Cluster endpoints are private Service/pod IPs, so the
/// generated config always allows private endpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct InsecureOptions {
    pub allow_private_endpoints: bool,
}
