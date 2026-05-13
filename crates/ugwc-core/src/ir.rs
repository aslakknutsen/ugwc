//! Intermediate representation types — Envoy-neutral wire types stay out of IR.

use std::collections::BTreeMap;

use uuid::Uuid;

/// Schema version embedded on every snapshot (bump when wire/IR changes).
pub const IR_VERSION: u32 = 8;

/// Per-dataplane scope identity for sharded xDS versions (see module docs).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct IrScopeId(pub String);

impl IrScopeId {
    #[must_use]
    pub fn new(ns: impl Into<String>, name: impl Into<String>) -> Self {
        Self(format!("{}/{}", ns.into(), name.into()))
    }
}

/// Content revision for snapshots (hash or monotonic counter — v0 uses hex sha256 prefix).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IrRevision {
    /// Short deterministic hash over canonical snapshot body (see `IrSnapshot::revision_token`).
    ContentHexSha256([u8; 32]),
    /// Monotonic counter for tests / controlled environments.
    Sequence(u64),
}

impl IrRevision {
    #[must_use]
    pub fn as_prefix_hex(&self) -> String {
        match self {
            IrRevision::ContentHexSha256(b) => hex::encode(&b[..8]),
            IrRevision::Sequence(n) => format!("seq-{n:016x}"),
        }
    }
}

/// High-level TLS intent on a listener (no PEM material in IR).
#[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub enum TlsMode {
    /// Plain HTTP listener on port.
    #[default]
    Plaintext,
    /// Terminated TLS at the gateway (certs live in cluster refs, not in IR v0).
    Terminate,
    /// TLS passthrough (SNI routing; opaque TLS to backends). Used with `protocol: TLS` listeners.
    Passthrough,
}

/// Single listener row after Gateway + ListenerSet merge.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ListenerIr {
    /// `sectionName` within the Gateway or ListenerSet spec.
    pub section_name: String,
    pub bind_port: u16,
    pub tls: TlsMode,
    /// Gateway listener application protocol: HTTP, HTTPS, TLS, TCP, UDP, …
    #[serde(default)]
    pub gateway_protocol: String,
    pub parent_kind: String,
    pub parent_namespace: String,
    pub parent_name: String,
    #[serde(default)]
    pub conflicted: bool,
    /// Listener hostname (SNI / virtual host); unset ⇒ wildcard in xDS.
    #[serde(default)]
    pub hostname: Option<String>,
    /// PEM TLS certificate (terminate mode), resolved by loader.
    #[serde(default)]
    pub tls_cert_pem: Vec<u8>,
    /// PEM TLS private key (terminate mode), resolved by loader.
    #[serde(default)]
    pub tls_key_pem: Vec<u8>,
    /// Effective `allowedRoutes` as `group/kind` strings (empty = infer HTTP + GRPC on HTTP-like listeners in encoder).
    #[serde(default)]
    pub allowed_route_group_kinds: Vec<String>,
}

/// Route binding to a specific effective listener (parent + `sectionName` + port).
#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ListenerAttachmentIr {
    pub parent_kind: String,
    pub parent_namespace: String,
    pub parent_name: String,
    pub section_name: String,
    pub port: u16,
}

/// Route protocol family (Gateway API HTTPRoute vs GRPCRoute vs TLSRoute vs TCPRoute vs UDPRoute).
#[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub enum RouteFamily {
    #[default]
    Http,
    Grpc,
    /// TLS/SNI routing (`TLSRoute` on `protocol: TLS` listeners).
    Tls,
    /// L4 TCP proxy routing (`TCPRoute` on `protocol: TCP` listeners).
    Tcp,
    /// L4 UDP routing (`UDPRoute` on `protocol: UDP` listeners).
    Udp,
}

/// One HTTP match clause (Core: path prefix or exact + header exact matches).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct RouteMatchIr {
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub path_exact: Option<String>,
    /// Lowercase header name → exact value.
    #[serde(default)]
    pub headers_exact: BTreeMap<String, String>,
    /// gRPC full service name (e.g. `helloworld.Greeter`); unset = wildcard for GRPCRoute matches.
    #[serde(default)]
    pub grpc_service: Option<String>,
    /// gRPC method name; unset = wildcard.
    #[serde(default)]
    pub grpc_method: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WeightedBackendIr {
    pub backend: BackendIr,
    #[serde(default = "default_backend_weight")]
    pub weight: u32,
}

#[inline]
fn default_backend_weight() -> u32 {
    1
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestRedirectIr {
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub status_code: Option<u32>,
    #[serde(default)]
    pub path_replace_full: Option<String>,
    #[serde(default)]
    pub path_replace_prefix: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NamedHeaderIr {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestHeaderModifierIr {
    #[serde(default)]
    pub set: Vec<NamedHeaderIr>,
    #[serde(default)]
    pub add: Vec<NamedHeaderIr>,
    #[serde(default)]
    pub remove: Vec<String>,
}

/// Normalized Gateway API route rule (HTTPRoute or GRPCRoute).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RouteRuleIr {
    pub name: String,
    #[serde(default)]
    pub route_family: RouteFamily,
    /// Effective hostnames (`empty` ⇒ any / `*` in encoders).
    #[serde(default)]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub matches: Vec<RouteMatchIr>,
    #[serde(default)]
    pub backends: Vec<WeightedBackendIr>,
    #[serde(default)]
    pub request_redirect: Option<RequestRedirectIr>,
    #[serde(default)]
    pub request_header_modifier: Option<RequestHeaderModifierIr>,
    #[serde(default)]
    pub response_header_modifier: Option<RequestHeaderModifierIr>,
    #[serde(default)]
    pub listener: Option<ListenerAttachmentIr>,
}

/// Upstream TLS settings for a Service backend ([`BackendTLSPolicy`] validation), mirrored for xDS.
#[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct BackendTlsIr {
    /// PEM CA bundle; empty when [`Self::trust_system_ca`] is true.
    #[serde(default)]
    pub ca_cert_pem: Vec<u8>,
    /// SNI / verify hostname for the backend TLS handshake.
    pub hostname: String,
    /// Subject Alternative Names (DNS hostnames or URI strings per Gateway API).
    #[serde(default)]
    pub sans: Vec<String>,
    /// Set when `spec.validation.wellKnownCACertificates` was `System`.
    #[serde(default)]
    pub trust_system_ca: bool,
    /// PEM client certificate for mutual TLS to the backend (`clientCertificateRefs`).
    #[serde(default)]
    pub client_cert_pem: Vec<u8>,
    /// PEM client private key for mutual TLS to the backend.
    #[serde(default)]
    pub client_key_pem: Vec<u8>,
    /// CA resolution or validation config failed; dataplane must fail closed.
    #[serde(default)]
    pub invalid: bool,
}

/// Resolved InferencePool backend: EPP service DNS + failure mode (datapath picks pod endpoints).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InferencePoolBackendIr {
    pub pool_namespace: String,
    pub pool_name: String,
    pub picker_host: String,
    pub picker_port: u16,
    #[serde(default)]
    pub failure_mode_fail_open: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BackendIr {
    #[serde(default)]
    pub service_namespace: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub port_name: Option<String>,
    #[serde(default)]
    pub port_number: Option<u16>,
    /// Populated when a winning [`BackendTLSPolicy`] applies to this backend port.
    #[serde(default)]
    pub backend_tls: Option<BackendTlsIr>,
    #[serde(default)]
    pub inference: Option<InferencePoolBackendIr>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Ord, PartialOrd)]
pub struct BackendEndpointIr {
    pub address: String,
    pub port: u16,
}

/// Snapshot metadata carried alongside domain data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotMeta {
    pub correlation_id: Uuid,
    pub has_errors: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IrSnapshot {
    pub ir_version: u32,
    pub revision: IrRevision,
    pub scope: IrScopeId,
    pub listeners: Vec<ListenerIr>,
    pub routes: Vec<RouteRuleIr>,
    /// Keys are [`backend_endpoints_map_key`] (`namespace/name:port`).
    pub backends: BTreeMap<String, Vec<BackendEndpointIr>>,
    pub meta: SnapshotMeta,
    /// Policy / inference overlays (Phase 5–6).
    pub policy: crate::policy::PolicyOverlayIr,
    pub inference: crate::inference::InferenceIr,
}

/// Stable map key for [`IrSnapshot::backends`] (must match `ir_from_cluster` and xDS encoders).
#[must_use]
pub fn backend_endpoints_map_key(service_namespace: &str, service_name: &str, port: u16) -> String {
    format!("{service_namespace}/{service_name}:{port}")
}

impl IrSnapshot {
    /// Stable token used in xDS version strings / logs (per-scope).
    #[must_use]
    pub fn revision_token(&self) -> String {
        format!(
            "ir{}-{}-{}",
            self.ir_version,
            self.scope.0,
            self.revision.as_prefix_hex()
        )
    }

    /// Empty routing state for `scope` (e.g. xDS client requested an unknown scope key).
    #[must_use]
    pub fn empty_for_scope(scope: IrScopeId) -> Self {
        Self {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope,
            listeners: vec![],
            routes: vec![],
            backends: Default::default(),
            meta: SnapshotMeta {
                correlation_id: Uuid::nil(),
                has_errors: false,
            },
            policy: crate::policy::PolicyOverlayIr::default(),
            inference: crate::inference::InferenceIr::default(),
        }
    }
}
