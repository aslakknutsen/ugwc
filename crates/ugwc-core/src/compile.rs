//! Pure compile pipeline: [`crate::ir::IrSnapshot`] + [`CompileReport`].

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::failure_policy::FailurePolicy;
use crate::from_cluster::ir_from_cluster;
use crate::http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
use crate::inference::InferenceIr;
use crate::ir::{IrRevision, IrSnapshot, SnapshotMeta, IR_VERSION};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{ClusterSnapshot, HttpRouteSnap};

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

fn http_route_service_backends(route: &HttpRouteSnap) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for rule in &route.rules {
        for br in &rule.backend_refs {
            if br.kind.as_deref().unwrap_or("Service") != "Service" {
                continue;
            }
            let ns = br
                .namespace
                .as_deref()
                .unwrap_or(route.namespace.as_str())
                .to_string();
            v.push((ns, br.name.clone()));
        }
    }
    v.sort();
    v.dedup();
    v
}

/// `ResolvedRefs` condition fields for one `HTTPRoute` vs [`CompileReport`] errors.
#[must_use]
pub fn http_route_resolved_refs_row(
    report: &CompileReport,
    route: &HttpRouteSnap,
) -> (&'static str, &'static str, &'static str) {
    let backends = http_route_service_backends(route);
    for e in &report.errors {
        if e.kind == "HTTPRoute" && e.namespace == route.namespace && e.name == route.name {
            let reason = httproute_error_reason(&e.reason);
            return (
                "False",
                reason,
                "compile report lists ref issues for this HTTPRoute",
            );
        }
        if e.kind == "Service"
            && e.reason == "BackendNotFound"
            && backends
                .iter()
                .any(|(ns, n)| ns == &e.namespace && n == &e.name)
        {
            return ("False", "BackendNotFound", "backend Service not found");
        }
        if e.kind == "Endpoints"
            && e.reason == "NoReadyEndpoints"
            && backends
                .iter()
                .any(|(ns, n)| ns == &e.namespace && n == &e.name)
        {
            return (
                "False",
                "NoReadyEndpoints",
                "no ready endpoints for backend Service",
            );
        }
    }
    ("True", "ResolvedRefs", "refs resolved for v0 slice")
}

fn httproute_error_reason(reason: &str) -> &'static str {
    match reason {
        "UnsupportedBackendKind" => "UnsupportedBackendKind",
        "BackendNotFound" => "BackendNotFound",
        "NoReadyEndpoints" => "NoReadyEndpoints",
        _ => "InvalidReference",
    }
}

/// Fixture-driven compile input — grows with phases (HTTPRoute, policies, …).
#[derive(Clone, Debug, Default)]
pub struct CompileInput {
    pub fixture: Option<HttpCompileFixture>,
    /// Cluster-derived snapshot (from `ugwc-k8s` loaders). Ignored when `fixture` is set.
    pub cluster: Option<ClusterSnapshot>,
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

    let mut report = CompileReport {
        errors: input.invalid_refs.clone(),
    };

    let mut snapshot = if let Some(fix) = &input.fixture {
        build_http_ir_from_fixture(fix)
    } else if let Some(cluster) = &input.cluster {
        let (mut snap, cluster_report) = ir_from_cluster(cluster);
        snap.scope = scope.clone();
        report.errors.extend(cluster_report.errors);
        snap
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

    snapshot.meta.correlation_id = correlation_id;
    snapshot.meta.has_errors = report.has_errors();

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
    use crate::snapshot::{HttpBackendRefSnap, HttpRouteRuleSnap, HttpRouteSnap};

    #[test]
    fn http_route_resolved_refs_true_when_clean() {
        let route = HttpRouteSnap {
            namespace: "ns".into(),
            name: "r".into(),
            generation: 1,
            ..Default::default()
        };
        let report = CompileReport::default();
        assert_eq!(
            http_route_resolved_refs_row(&report, &route),
            ("True", "ResolvedRefs", "refs resolved for v0 slice")
        );
    }

    #[test]
    fn http_route_resolved_refs_false_on_service_missing() {
        let route = HttpRouteSnap {
            namespace: "ns".into(),
            name: "r".into(),
            generation: 1,
            rules: vec![HttpRouteRuleSnap {
                backend_refs: vec![HttpBackendRefSnap {
                    name: "svc".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let report = CompileReport {
            errors: vec![ObjectRefErr {
                group: "".into(),
                version: "v1".into(),
                kind: "Service".into(),
                namespace: "ns".into(),
                name: "svc".into(),
                reason: "BackendNotFound".into(),
            }],
        };
        assert_eq!(
            http_route_resolved_refs_row(&report, &route),
            ("False", "BackendNotFound", "backend Service not found")
        );
    }

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
