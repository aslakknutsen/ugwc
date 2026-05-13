//! Intermediate representation and the pure compile / publish planning pipeline.
//!
//! Kubernetes and xDS stay out of this crate — see `ugwc-k8s` and `ugwc-xds`.

pub mod compile;
pub mod failure_policy;
pub mod from_cluster;
pub mod http_ir;
pub mod http_route_attachment;
pub mod inference;
pub mod ir;
pub mod listener_merge;
pub mod policy;
pub mod publish;
pub mod snapshot;

pub use compile::{
    compile, grpc_route_accepted_condition_for_parent, grpc_route_resolved_refs_row,
    http_route_accepted_condition_for_parent, http_route_resolved_refs_row,
    tcp_route_accepted_condition_for_parent, tcp_route_resolved_refs_row,
    tls_route_accepted_condition_for_parent, tls_route_resolved_refs_row,
    udp_route_accepted_condition_for_parent, udp_route_resolved_refs_row, CompileInput,
    CompileOutcome, CompileReport, CoreError, ObjectRefErr,
};
pub use failure_policy::FailurePolicy;
pub use from_cluster::ir_from_cluster;
pub use http_ir::{build_http_ir_from_fixture, HttpCompileFixture};
pub use http_route_attachment::{
    reference_grant_permits_backend_tls_policy_resource_ref,
    reference_grant_permits_gateway_secret_ref, reference_grant_permits_service_ref,
    route_accepted_for_parent_ref,
};
pub use ir::{
    backend_endpoints_map_key, BackendEndpointIr, BackendIr, BackendTlsIr, IrRevision, IrScopeId,
    IrSnapshot, ListenerAttachmentIr, ListenerIr, NamedHeaderIr, RequestHeaderModifierIr,
    RequestRedirectIr, RouteMatchIr, RouteRuleIr, SnapshotMeta, TlsMode, WeightedBackendIr,
    IR_VERSION,
};
pub use listener_merge::merge_effective_listeners;
pub use publish::{plan_publish, PublishAction, PublishPlan};
pub use snapshot::ClusterSnapshot;
