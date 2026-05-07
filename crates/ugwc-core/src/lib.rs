//! `ugwc-core` — internal intermediate representation, compile reports, and
//! failure policy data (no Kubernetes or gRPC imports in the hot compile path).
//!
//! ## v0 IR snapshot **scope**
//!
//! Snapshots are **scoped per data-plane shard** using [`IrScopeId`]. For v0 the
//! controller maps one *primary reconciliation key* (typically a parent `Gateway`
//! namespace/name in the cluster watch graph) into a single scope, so xDS can
//! adopt **per-scope versions** (see `design/output-distribution.md` §8). Whether
//! this remains cluster-wide or moves to per-`GatewayClass` is a living decision;
//! changing it requires updating **Living document** in
//! `design/implementation-plan.md`.

pub mod compile;
pub mod failure_policy;
pub mod http_ir;
pub mod inference;
pub mod ir;
pub mod policy;
pub mod publish;

pub use compile::{compile, CompileInput, CompileOutcome, CompileReport, CoreError, ObjectRefErr};
pub use failure_policy::{FailurePolicy, OnPartialCompile, OnPublishError};
pub use inference::InferenceIr;
pub use ir::{
    BackendEndpointIr, BackendIr, IrRevision, IrScopeId, IrSnapshot, ListenerIr, RouteRuleIr,
    TlsMode,
};
pub use policy::{PolicyAttachment, PolicyOverlayIr, PolicyRef};
pub use publish::{plan_publish, PublishAction, PublishPlan};

/// Placeholder export kept for older tests; prefer [`compile()`].
pub fn placeholder() -> &'static str {
    "ugwc-core"
}
