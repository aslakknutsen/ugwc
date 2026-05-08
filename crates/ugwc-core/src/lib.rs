//! Intermediate representation and the pure compile / publish planning pipeline.
//!
//! Kubernetes and xDS stay out of this crate — see `ugwc-k8s` and `ugwc-xds`.

pub mod compile;
pub mod failure_policy;
pub mod from_cluster;
pub mod http_ir;
pub mod inference;
pub mod ir;
pub mod policy;
pub mod publish;
pub mod snapshot;

pub use compile::{
    compile, http_route_resolved_refs_row, CompileInput, CompileOutcome, CompileReport, CoreError,
    ObjectRefErr,
};
pub use failure_policy::FailurePolicy;
pub use from_cluster::ir_from_cluster;
pub use http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
pub use ir::{
    BackendEndpointIr, BackendIr, IrRevision, IrScopeId, IrSnapshot, ListenerIr, RouteRuleIr,
    SnapshotMeta, IR_VERSION,
};
pub use publish::{plan_publish, PublishAction, PublishPlan};
pub use snapshot::ClusterSnapshot;
