//! Pure compile pipeline: [`crate::ir::IrSnapshot`] + [`CompileReport`].

use std::collections::{BTreeMap, HashSet};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::failure_policy::FailurePolicy;
use crate::from_cluster::ir_from_cluster;
use crate::http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
use crate::http_route_attachment::{
    attachment_keys_for_parent_ref, find_effective_for_key, grpc_attachment_keys_for_parent_ref,
    grpc_route_listener_hostname_ok, route_listener_hostname_ok,
    route_parent_attachment_ref_not_permitted, tls_attachment_keys_for_parent_ref,
    tls_route_listener_hostname_ok, tcp_attachment_keys_for_parent_ref, tcp_route_listener_ok,
    udp_attachment_keys_for_parent_ref, udp_route_listener_ok,
};
use crate::inference::InferenceIr;
use crate::ir::{IrRevision, IrSnapshot, SnapshotMeta, IR_VERSION};
use crate::policy::PolicyOverlayIr;
use crate::snapshot::{
    ClusterSnapshot, GrpcRouteSnap, HttpRouteSnap, ParentRefSnap, TcpRouteSnap, TlsRouteSnap,
    UdpRouteSnap,
};

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
    /// TLSRoutes rejected due to lower precedence vs another TLSRoute on the same TLS listener (overlapping SNI).
    pub tls_route_conflict_losers: Vec<HttpGrpcConflictLoser>,
    /// TCPRoutes rejected due to lower precedence vs another TCPRoute on the same TCP listener.
    pub tcp_route_conflict_losers: Vec<HttpGrpcConflictLoser>,
    /// UDPRoutes rejected due to lower precedence vs another UDPRoute on the same UDP listener.
    pub udp_route_conflict_losers: Vec<HttpGrpcConflictLoser>,
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

    #[must_use]
    pub fn tls_route_conflict_loser_at(
        &self,
        route_kind: &str,
        route_namespace: &str,
        route_name: &str,
        lk: &crate::ir::ListenerAttachmentIr,
    ) -> bool {
        self.tls_route_conflict_losers.iter().any(|x| {
            x.route_kind == route_kind
                && x.route_namespace == route_namespace
                && x.route_name == route_name
                && x.listener == *lk
        })
    }

    #[must_use]
    pub fn tcp_route_conflict_loser_at(
        &self,
        route_kind: &str,
        route_namespace: &str,
        route_name: &str,
        lk: &crate::ir::ListenerAttachmentIr,
    ) -> bool {
        self.tcp_route_conflict_losers.iter().any(|x| {
            x.route_kind == route_kind
                && x.route_namespace == route_namespace
                && x.route_name == route_name
                && x.listener == *lk
        })
    }

    #[must_use]
    pub fn udp_route_conflict_loser_at(
        &self,
        route_kind: &str,
        route_namespace: &str,
        route_name: &str,
        lk: &crate::ir::ListenerAttachmentIr,
    ) -> bool {
        self.udp_route_conflict_losers.iter().any(|x| {
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
    if route_parent_attachment_ref_not_permitted(
        snapshot,
        &route.namespace,
        "HTTPRoute",
        parent,
        attached_ls,
    ) {
        return (
            "False",
            "RefNotPermitted",
            "cross-namespace route attachment requires a ReferenceGrant in the parent resource namespace",
        );
    }
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
        ("True", "Accepted", "ugwc reconciled parent attachment")
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
    if route_parent_attachment_ref_not_permitted(
        snapshot,
        &route.namespace,
        "GRPCRoute",
        parent,
        attached_ls,
    ) {
        return (
            "False",
            "RefNotPermitted",
            "cross-namespace route attachment requires a ReferenceGrant in the parent resource namespace",
        );
    }
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
        ("True", "Accepted", "ugwc reconciled parent attachment")
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

/// `Accepted` condition tuple for one `TLSRoute` `parentRef`, including TLS-vs-TLS hostname precedence.
#[must_use]
pub fn tls_route_accepted_condition_for_parent(
    route: &TlsRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> (&'static str, &'static str, &'static str) {
    if route_parent_attachment_ref_not_permitted(
        snapshot,
        &route.namespace,
        "TLSRoute",
        parent,
        attached_ls,
    ) {
        return (
            "False",
            "RefNotPermitted",
            "cross-namespace route attachment requires a ReferenceGrant in the parent resource namespace",
        );
    }
    let keys = tls_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
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
        if !tls_route_listener_hostname_ok(route, eff) {
            continue;
        }
        eligible = true;
        if report.tls_route_conflict_loser_at("TLSRoute", &route.namespace, &route.name, lk) {
            continue;
        }
        accepted = true;
        break;
    }
    if accepted {
        ("True", "Accepted", "ugwc reconciled parent attachment")
    } else if eligible {
        (
            "False",
            "TLSHostnameConflict",
            "TLSRoute lost hostname precedence vs another TLSRoute on all matching listeners",
        )
    } else {
        (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener hostname intersection or allowedRoutes policy",
        )
    }
}

/// `Accepted` condition tuple for one `TCPRoute` `parentRef`, including TCP-vs-TCP precedence on same listener.
#[must_use]
pub fn tcp_route_accepted_condition_for_parent(
    route: &TcpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> (&'static str, &'static str, &'static str) {
    if route_parent_attachment_ref_not_permitted(
        snapshot,
        &route.namespace,
        "TCPRoute",
        parent,
        attached_ls,
    ) {
        return (
            "False",
            "RefNotPermitted",
            "cross-namespace route attachment requires a ReferenceGrant in the parent resource namespace",
        );
    }
    let keys = tcp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener policy or allowedRoutes",
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
        if !tcp_route_listener_ok(route, eff) {
            continue;
        }
        eligible = true;
        if report.tcp_route_conflict_loser_at("TCPRoute", &route.namespace, &route.name, lk) {
            continue;
        }
        accepted = true;
        break;
    }
    if accepted {
        ("True", "Accepted", "ugwc reconciled parent attachment")
    } else if eligible {
        (
            "False",
            "TCPRouteConflict",
            "TCPRoute lost precedence vs another TCPRoute on all matching listeners",
        )
    } else {
        (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener policy or allowedRoutes",
        )
    }
}

/// `Accepted` condition tuple for one `UDPRoute` `parentRef`, including UDP-vs-UDP precedence on same listener.
#[must_use]
pub fn udp_route_accepted_condition_for_parent(
    route: &UdpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> (&'static str, &'static str, &'static str) {
    if route_parent_attachment_ref_not_permitted(
        snapshot,
        &route.namespace,
        "UDPRoute",
        parent,
        attached_ls,
    ) {
        return (
            "False",
            "RefNotPermitted",
            "cross-namespace route attachment requires a ReferenceGrant in the parent resource namespace",
        );
    }
    let keys = udp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener policy or allowedRoutes",
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
        if !udp_route_listener_ok(route, eff) {
            continue;
        }
        eligible = true;
        if report.udp_route_conflict_loser_at("UDPRoute", &route.namespace, &route.name, lk) {
            continue;
        }
        accepted = true;
        break;
    }
    if accepted {
        ("True", "Accepted", "ugwc reconciled parent attachment")
    } else if eligible {
        (
            "False",
            "UDPRouteConflict",
            "UDPRoute lost precedence vs another UDPRoute on all matching listeners",
        )
    } else {
        (
            "False",
            "NotAcceptedByListeners",
            "route not accepted by listener policy or allowedRoutes",
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

fn tls_route_service_backends(route: &TlsRouteSnap) -> Vec<(String, String)> {
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

fn tcp_route_service_backends(route: &TcpRouteSnap) -> Vec<(String, String)> {
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

/// `ResolvedRefs` condition fields for one `TLSRoute` vs [`CompileReport`] errors.
#[must_use]
pub fn tls_route_resolved_refs_row(
    report: &CompileReport,
    route: &TlsRouteSnap,
) -> (&'static str, &'static str, &'static str) {
    let backends = tls_route_service_backends(route);
    for e in &report.errors {
        if e.kind == "TLSRoute" && e.namespace == route.namespace && e.name == route.name {
            let reason = route_ref_error_reason(&e.reason);
            return (
                "False",
                reason,
                "compile report lists ref issues for this TLSRoute",
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

/// `ResolvedRefs` condition fields for one `TCPRoute` vs [`CompileReport`] errors.
#[must_use]
pub fn tcp_route_resolved_refs_row(
    report: &CompileReport,
    route: &TcpRouteSnap,
) -> (&'static str, &'static str, &'static str) {
    let backends = tcp_route_service_backends(route);
    for e in &report.errors {
        if e.kind == "TCPRoute" && e.namespace == route.namespace && e.name == route.name {
            let reason = route_ref_error_reason(&e.reason);
            return (
                "False",
                reason,
                "compile report lists ref issues for this TCPRoute",
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

fn udp_route_service_backends(route: &UdpRouteSnap) -> Vec<(String, String)> {
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

/// `ResolvedRefs` condition fields for one `UDPRoute` vs [`CompileReport`] errors.
#[must_use]
pub fn udp_route_resolved_refs_row(
    report: &CompileReport,
    route: &UdpRouteSnap,
) -> (&'static str, &'static str, &'static str) {
    let backends = udp_route_service_backends(route);
    for e in &report.errors {
        if e.kind == "UDPRoute" && e.namespace == route.namespace && e.name == route.name {
            let reason = route_ref_error_reason(&e.reason);
            return (
                "False",
                reason,
                "compile report lists ref issues for this UDPRoute",
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

fn route_ref_error_reason(reason: &str) -> &'static str {
    match reason {
        "UnsupportedBackendKind" => "UnsupportedBackendKind",
        "BackendNotFound" => "BackendNotFound",
        "NoReadyEndpoints" => "NoReadyEndpoints",
        "RefNotPermitted" => "RefNotPermitted",
        "InvalidCACertificateRef" => "InvalidCACertificateRef",
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
        tls_route_conflict_losers: Vec::new(),
        tcp_route_conflict_losers: Vec::new(),
        udp_route_conflict_losers: Vec::new(),
    };

    let mut snapshot = if let Some(fix) = &input.fixture {
        build_http_ir_from_fixture(fix)
    } else if let Some(cluster) = &input.cluster {
        let (mut snap, cluster_report) = ir_from_cluster(cluster);
        snap.scope = scope.clone();
        report.errors.extend(cluster_report.errors);
        report.listener_route_attachment_counts = cluster_report.listener_route_attachment_counts;
        report.http_grpc_conflict_losers = cluster_report.http_grpc_conflict_losers;
        report.tls_route_conflict_losers = cluster_report.tls_route_conflict_losers;
        report.tcp_route_conflict_losers = cluster_report.tcp_route_conflict_losers;
        report.udp_route_conflict_losers = cluster_report.udp_route_conflict_losers;
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
    use crate::listener_merge::merge_effective_listeners;
    use crate::snapshot::{
        AllowedRoutesNamespacesSnap, AllowedRoutesSnap, ClusterSnapshot, GatewayListenerSnap,
        HttpBackendRefSnap, HttpRouteRuleSnap, HttpRouteSnap, ParentRefSnap,
        ReferenceGrantFromSnap, ReferenceGrantSnap, ReferenceGrantToSnap, TlsRouteSnap,
    };

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
    fn tls_route_resolved_refs_true_when_clean() {
        let route = TlsRouteSnap {
            namespace: "ns".into(),
            name: "r".into(),
            generation: 1,
            ..Default::default()
        };
        let report = CompileReport::default();
        assert_eq!(
            tls_route_resolved_refs_row(&report, &route),
            ("True", "ResolvedRefs", "refs resolved for v0 slice")
        );
    }

    #[test]
    fn tls_route_resolved_refs_ref_not_permitted() {
        let route = TlsRouteSnap {
            namespace: "ns".into(),
            name: "r".into(),
            generation: 1,
            ..Default::default()
        };
        let report = CompileReport {
            errors: vec![ObjectRefErr {
                group: "gateway.networking.k8s.io".into(),
                version: "v1".into(),
                kind: "TLSRoute".into(),
                namespace: "ns".into(),
                name: "r".into(),
                reason: "RefNotPermitted".into(),
            }],
            ..Default::default()
        };
        assert_eq!(
            tls_route_resolved_refs_row(&report, &route),
            (
                "False",
                "RefNotPermitted",
                "compile report lists ref issues for this TLSRoute"
            )
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
            (
                "False",
                "RefNotPermitted",
                "compile report lists ref issues for this HTTPRoute"
            )
        );
    }

    #[test]
    fn http_route_accepted_cross_namespace_parent_requires_grant() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: Some(AllowedRoutesSnap {
                namespaces: Some(AllowedRoutesNamespacesSnap {
                    from: Some("All".into()),
                    selector_match_labels: Default::default(),
                }),
                kinds: vec![],
            }),
            tls: None,
        }];
        let snap = ClusterSnapshot {
            gateway_namespace: "infra".into(),
            gateway_name: "gw".into(),
            gateway_class_name: "gc".into(),
            gateway_generation: 1,
            gateway_addresses: vec![],
            gateway_infrastructure: None,
            listeners: listeners.clone(),
            effective_listeners: merge_effective_listeners(&listeners, &[]),
            listener_sets: vec![],
            allowed_listeners: None,
            http_routes: vec![],
            grpc_routes: vec![],
            tls_routes: vec![],
            tcp_routes: vec![],
            udp_routes: vec![],
            services: vec![],
            endpoint_slices: vec![],
            gateway_class: None,
            reference_grants: vec![],
            backend_tls_effective: Default::default(),
            backend_tls_policy_outcomes: vec![],
            namespace_labels: Default::default(),
            inference_pools: vec![],
        };
        let route = HttpRouteSnap {
            namespace: "team-a".into(),
            name: "r".into(),
            generation: 1,
            parent_refs: vec![ParentRefSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: Some("Gateway".into()),
                namespace: Some("infra".into()),
                name: "gw".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let attached_ls = HashSet::new();
        let report = CompileReport::default();
        let parent = &route.parent_refs[0];
        let (st, reason, _) =
            http_route_accepted_condition_for_parent(&route, parent, &snap, &attached_ls, &report);
        assert_eq!(st, "False");
        assert_eq!(reason, "RefNotPermitted");

        let mut snap_ok = snap.clone();
        snap_ok.reference_grants.push(ReferenceGrantSnap {
            namespace: "infra".into(),
            name: "rg".into(),
            from: vec![ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "team-a".into(),
            }],
            to: vec![ReferenceGrantToSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "Gateway".into(),
                name: Some("gw".into()),
            }],
        });
        let (st2, reason2, _) = http_route_accepted_condition_for_parent(
            &route,
            parent,
            &snap_ok,
            &attached_ls,
            &report,
        );
        assert_eq!(st2, "True");
        assert_eq!(reason2, "Accepted");
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
