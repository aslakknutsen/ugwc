//! Pure compile pipeline: [`crate::ir::IrSnapshot`] + [`CompileReport`].

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::failure_policy::FailurePolicy;
use crate::http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
use crate::inference::InferenceIr;
use crate::ir::{IrRevision, IrSnapshot, SnapshotMeta, IR_VERSION};
use crate::policy::PolicyOverlayIr;

/// Stable cross-layer reference to a Kubernetes object when something goes wrong.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRefErr {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CoreError {
    #[error("compile input invalid: {0}")]
    InvalidInput(String),
}

/// Collected non-fatal issues during compile (per-object bookkeeping).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompileReport {
    pub errors: Vec<ObjectRefErr>,
}

impl CompileReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Fixture-driven compile input — grows with phases (HTTPRoute, policies, …).
#[derive(Clone, Debug, Default)]
pub struct CompileInput {
    pub fixture: Option<HttpCompileFixture>,
    /// Synthetic invalid ref for exercising FailurePolicy paths in tests.
    pub invalid_refs: Vec<ObjectRefErr>,
    /// When reconciling from Kubernetes, pin IR scope to the parent Gateway identity.
    pub reconcile_gateway: Option<(String, String)>,
}

/// Result envelope: always carries a [`CompileReport`]; fatal errors use [`Result`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileOutcome {
    pub snapshot: IrSnapshot,
    pub report: CompileReport,
}

/// Deterministic v0 compile — hermetic (no kube client).
///
/// Error model: returns `Err` for programmer/contract mistakes; user-facing
/// object-level faults live in [`CompileReport`].
pub fn compile(policy: &FailurePolicy, input: &CompileInput) -> Result<CompileOutcome, CoreError> {
    let correlation_id = Uuid::nil();
    let scope = if let Some((ns, name)) = &input.reconcile_gateway {
        crate::ir::IrScopeId::new(ns.clone(), name.clone())
    } else {
        crate::ir::IrScopeId::new("default", "compile-stub")
    };

    let report = CompileReport {
        errors: input.invalid_refs.clone(),
    };

    let mut snapshot = if let Some(fix) = &input.fixture {
        build_http_ir_from_fixture(fix)
    } else {
        IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(0),
            scope: scope.clone(),
            listeners: vec![],
            routes: vec![],
            backends: Default::default(),
            meta: SnapshotMeta {
                correlation_id,
                has_errors: false,
            },
            policy: PolicyOverlayIr::default(),
            inference: InferenceIr::default(),
        }
    };

    if report.has_errors() {
        if matches!(
            policy.on_partial_compile,
            crate::failure_policy::OnPartialCompile::Reject
        ) {
            snapshot.meta.has_errors = true;
            snapshot.revision = IrRevision::Sequence(0);
            snapshot.listeners.clear();
            snapshot.routes.clear();
            snapshot.backends.clear();
        } else {
            snapshot.meta.has_errors = true;
        }
    }

    snapshot.revision = revision_for(&snapshot);

    Ok(CompileOutcome { snapshot, report })
}

fn revision_for(snapshot: &IrSnapshot) -> IrRevision {
    use std::io::Write as _;

    let mut h = Sha256::new();
    h.write_all(b"ugwc-ir-v1\0").ok();
    h.write_all(&snapshot.ir_version.to_le_bytes()).ok();
    h.write_all(snapshot.scope.0.as_bytes()).ok();
    h.write_all(format!("{:?}", snapshot.listeners).as_bytes())
        .ok();
    h.write_all(format!("{:?}", snapshot.routes).as_bytes())
        .ok();
    h.write_all(format!("{:?}", snapshot.backends).as_bytes())
        .ok();
    let b = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&b);
    IrRevision::ContentHexSha256(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure_policy::{FailurePolicy, OnPartialCompile};
    use crate::http_ir::HttpCompileFixture;

    #[test]
    fn empty_input_empty_ir() {
        let pol = FailurePolicy::default();
        let out = compile(&pol, &CompileInput::default()).unwrap();
        assert!(out.snapshot.routes.is_empty());
        assert_eq!(out.snapshot.ir_version, IR_VERSION);
        assert!(!out.report.has_errors());
    }

    #[test]
    fn invalid_ref_populates_report_and_strips_under_default_policy() {
        let pol = FailurePolicy::default();
        let input = CompileInput {
            fixture: None,
            invalid_refs: vec![ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: "ns".into(),
                name: "bad".into(),
                reason: "RefNotPermitted".into(),
            }],
            ..Default::default()
        };
        let out = compile(&pol, &input).unwrap();
        assert!(out.report.has_errors());
        assert!(out.snapshot.routes.is_empty());
    }

    #[test]
    fn partial_allowed_keeps_routes() {
        let pol = FailurePolicy {
            on_partial_compile: OnPartialCompile::AllowBestEffort,
            ..FailurePolicy::default()
        };
        let fixture = HttpCompileFixture::minimal_http();
        let input = CompileInput {
            fixture: Some(fixture),
            invalid_refs: vec![ObjectRefErr {
                group: "x".into(),
                version: "v1".into(),
                kind: "Y".into(),
                namespace: "ns".into(),
                name: "z".into(),
                reason: "warn".into(),
            }],
            ..Default::default()
        };
        let out = compile(&pol, &input).unwrap();
        assert!(!out.snapshot.routes.is_empty());
        assert!(out.snapshot.meta.has_errors);
    }
}
