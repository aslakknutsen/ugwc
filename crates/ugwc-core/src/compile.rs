//! Pure compile pipeline: [`crate::ir::IrSnapshot`] + [`CompileReport`].

use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::failure_policy::FailurePolicy;
use crate::from_cluster::ir_from_cluster;
use crate::http_route_attachment::{
    attachment_keys_for_parent_ref, find_effective_for_key, grpc_attachment_keys_for_parent_ref,
    grpc_route_listener_hostname_ok, route_listener_hostname_ok,
};
use crate::http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
use crate::inference::InferenceIr;
use crate::ir::{IrRevision, IrSnapshot, SnapshotMeta, IR_VERSION};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{ClusterSnapshot, GrpcRouteSnap, HttpRouteSnap, ParentRefSnap};

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

/// HTTPRoute vs GRPCRoute hostname conflict loser (per listener attachment); used for route status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpGrpcConflictLoser {
    pub route_kind: String,
    pub route_namespace: String,
    pub route_name: String,
    pub listener: crate::ir::ListenerAttachmentIr,
}

/// Collected non-fatal issues during compile (per-object bookkeeping).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompileReport {
    pub errors: Vec<ObjectRefErr>,
    /// Count of compiled route rules attached per listener (`parent_kind|parent_ns|parent_name|section|port`).
    pub listener_route_attachment_counts: BTreeMap<String, u32>,
    /// Routes rejected because another route kind won HTTP-vs-gRPC hostname conflict on the same listener.
    pub http_grpc_conflict_losers: Vec<HttpGrpcConflictLoser>,
}

impl CompileReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    #[must_use]
    pub fn http_grpc_conflict_loser_at(
        &self,
        route_kind: &str,
        route_namespace: &str,
        route_name: &str,
        lk: &crate::ir::ListenerAttachmentIr,
    ) -> bool {
        self.http_grpc_conflict_losers.iter().any(|x| {
            x.route_kind == route_kind
                && x.route_namespace == route_namespace
                && x.route_name == route_name
                && x.listener == *lk
        })
    }

    pub fn record_listener_route_attachment(&mut self, lk: &crate::ir::ListenerAttachmentIr) {
        let k = format!(
            "{}|{}|{}|{}|{}",
            lk.parent_kind, lk.parent_namespace, lk.parent_name, lk.section_name, lk.port
        );
        *self.listener_route_attachment_counts.entry(k).or_insert(0) += 1;
    }

    #[must_use]
    pub fn attachment_count_for_listener_key(&self, lk: &crate::ir::ListenerAttachmentIr) -> i32 {
        let k = format!(
            "{}|{}|{}|{}|{}",
            lk.parent_kind, lk.parent_namespace, lk.parent_name, lk.section_name, lk.port
        );
        self.listener_route_attachment_counts
            .get(&k)
            .copied()
            .unwrap_or(0) as i32
    }
}

/// `Accepted` condition tuple for one HTTPRoute `parentRef`, including HTTP-vs-gRPC hostname conflicts.
#[must_use]
pub fn http_route_accepted_condition_for_parent(
    route: &HttpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> (&'static str, &'static str, &'static str) {
    let keys = attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener hostname intersection or allowedRoutes policy",
        );
    }
    let mut eligible = false;
    let mut accepted = false;
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !route_listener_hostname_ok(route, eff) {
            continue;
        }
        eligible = true;
        if report.http_grpc_conflict_loser_at("HTTPRoute", &route.namespace, &route.name, lk) {
            continue;
        }
        accepted = true;
        break;
    }
    if accepted {
        (
            "True",
            "Accepted",
            "ugwc reconciled parent attachment",
        )
    } else if eligible {
        (
            "False",
            "HTTPGRPCHostnameConflict",
            "HTTPRoute lost hostname precedence vs GRPCRoute on all matching listeners",
        )
    } else {
        (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener hostname intersection or allowedRoutes policy",
        )
    }
}

/// `Accepted` condition tuple for one GRPCRoute `parentRef`, including HTTP-vs-gRPC hostname conflicts.
#[must_use]
pub fn grpc_route_accepted_condition_for_parent(
    route: &GrpcRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> (&'static str, &'static str, &'static str) {
    let keys = grpc_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener hostname intersection or allowedRoutes policy",
        );
    }
    let mut eligible = false;
    let mut accepted = false;
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !grpc_route_listener_hostname_ok(route, eff) {
            continue;
        }
        eligible = true;
        if report.http_grpc_conflict_loser_at("GRPCRoute", &route.namespace, &route.name, lk) {
            continue;
        }
        accepted = true;
        break;
    }
    if accepted {
        (
            "True",
            "Accepted",
            "ugwc reconciled parent attachment",
        )
    } else if eligible {
        (
            "False",
            "HTTPGRPCHostnameConflict",
            "GRPCRoute lost hostname precedence vs HTTPRoute on all matching listeners",
        )
    } else {
        (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener hostname intersection or allowedRoutes policy",
        )
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
            let reason = route_ref_error_reason(&e.reason);
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

/// `ResolvedRefs` condition fields for one `GRPCRoute` vs [`CompileReport`] errors.
#[must_use]
pub fn grpc_route_resolved_refs_row(
    report: &CompileReport,
    route: &GrpcRouteSnap,
) -> (&'static str, &'static str, &'static str) {
    let backends = grpc_route_service_backends(route);
    for e in &report.errors {
        if e.kind == "GRPCRoute" && e.namespace == route.namespace && e.name == route.name {
            let reason = route_ref_error_reason(&e.reason);
            return (
                "False",
                reason,
                "compile report lists ref issues for this GRPCRoute",
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

fn grpc_route_service_backends(route: &GrpcRouteSnap) -> Vec<(String, String)> {
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

fn route_ref_error_reason(reason: &str) -> &'static str {
    match reason {
        "UnsupportedBackendKind" => "UnsupportedBackendKind",
        "BackendNotFound" => "BackendNotFound",
        "NoReadyEndpoints" => "NoReadyEndpoints",
        "RefNotPermitted" => "RefNotPermitted",
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
        listener_route_attachment_counts: BTreeMap::new(),
        http_grpc_conflict_losers: Vec::new(),
    };

    let mut snapshot = if let Some(fix) = &input.fixture {
        build_http_ir_from_fixture(fix)
    } else if let Some(cluster) = &input.cluster {
        let (mut snap, cluster_report) = ir_from_cluster(cluster);
        snap.scope = scope.clone();
        report.errors.extend(cluster_report.errors);
        report.listener_route_attachment_counts = cluster_report.listener_route_attachment_counts;
        report.http_grpc_conflict_losers = cluster_report.http_grpc_conflict_losers;
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
            ..Default::default()
        };
        assert_eq!(
            http_route_resolved_refs_row(&report, &route),
            ("False", "BackendNotFound", "backend Service not found")
        );
    }

    #[test]
    fn http_route_resolved_refs_ref_not_permitted() {
        let route = HttpRouteSnap {
            namespace: "ns".into(),
            name: "r".into(),
            generation: 1,
            ..Default::default()
        };
        let report = CompileReport {
            errors: vec![ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "HTTPRoute".into(),
                namespace: "ns".into(),
                name: "r".into(),
                reason: "RefNotPermitted".into(),
            }],
            ..Default::default()
        };
        assert_eq!(
            http_route_resolved_refs_row(&report, &route),
            ("False", "RefNotPermitted", "compile report lists ref issues for this HTTPRoute")
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
