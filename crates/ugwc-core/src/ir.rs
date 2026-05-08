//! Intermediate representation types — Envoy-neutral wire types stay out of IR.

use std::collections::BTreeMap;

use uuid::Uuid;

/// Schema version embedded on every snapshot (bump when wire/IR changes).
pub const IR_VERSION: u32 = 1;

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
}

/// Single listener row after Gateway + ListenerSet merge.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ListenerIr {
    /// `sectionName` within the Gateway or ListenerSet spec.
    pub section_name: String,
    pub bind_port: u16,
    pub tls: TlsMode,
    pub parent_kind: String,
    pub parent_namespace: String,
    pub parent_name: String,
    #[serde(default)]
    pub conflicted: bool,
}

/// Route binding to a specific effective listener (parent + `sectionName` + port).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct ListenerAttachmentIr {
    pub parent_kind: String,
    pub parent_namespace: String,
    pub parent_name: String,
    pub section_name: String,
    pub port: u16,
}

/// Normalized HTTP route match + backend (Phase 4+).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RouteRuleIr {
    pub name: String,
    /// Hostname literal or prefix, `None` means any.
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub backend: BackendIr,
    #[serde(default)]
    pub listener: Option<ListenerAttachmentIr>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BackendIr {
    pub service_namespace: String,
    pub service_name: String,
    pub port_name: Option<String>,
    pub port_number: Option<u16>,
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
    pub backends: BTreeMap<String, Vec<BackendEndpointIr>>,
    pub meta: SnapshotMeta,
    /// Policy / inference overlays (Phase 5–6).
    pub policy: crate::policy::PolicyOverlayIr,
    pub inference: crate::inference::InferenceIr,
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
