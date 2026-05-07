//! Publish planning between successive [`crate::ir::IrSnapshot`] values.
//!
//! Merge A wires this in front of the xDS snapshot cache.

use crate::failure_policy::FailurePolicy;
use crate::ir::IrSnapshot;

/// What the publisher should do with xDS / downstream notifies.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublishAction {
    /// Skip pushing a new snapshot.
    Skip,
    /// Replace the entire held snapshot with `next`.
    FullReplace,
    /// Reserved: incremental delta push (Phase 7.3 — feature gated).
    Delta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishPlan {
    pub action: PublishAction,
    /// Human/debug string correlating IR revision with publish decision.
    pub detail: String,
}

/// Decide how to react given previous snapshot, candidate snapshot, and failure policy.
#[must_use]
pub fn plan_publish(
    prev: Option<&IrSnapshot>,
    next: &IrSnapshot,
    pol: &FailurePolicy,
) -> PublishPlan {
    if !pol.allow_snapshot_after_compile(next) {
        return PublishPlan {
            action: PublishAction::Skip,
            detail: "failure_policy_rejected_partial_snapshot".into(),
        };
    }

    if let Some(p) = prev {
        if p.revision_token() == next.revision_token() {
            return PublishPlan {
                action: PublishAction::Skip,
                detail: "unchanged_revision_token".into(),
            };
        }
    }

    PublishPlan {
        action: PublishAction::FullReplace,
        detail: format!("publish_token={}", next.revision_token()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{compile, CompileInput};
    use crate::failure_policy::{FailurePolicy, OnPartialCompile};
    use crate::ir::IrRevision;

    #[test]
    fn compile_error_path_skips_publish_by_default() {
        let pol = FailurePolicy::default();
        let bad = CompileInput {
            fixture: None,
            invalid_refs: vec![crate::compile::ObjectRefErr {
                group: "g".into(),
                version: "v1".into(),
                kind: "K".into(),
                namespace: "ns".into(),
                name: "n".into(),
                reason: "e".into(),
            }],
            ..Default::default()
        };
        let co = compile(&pol, &bad).unwrap();
        let plan = plan_publish(None, &co.snapshot, &pol);
        assert_eq!(plan.action, PublishAction::Skip);
    }

    #[test]
    fn allow_partial_emits_replace() {
        let pol = FailurePolicy {
            on_partial_compile: OnPartialCompile::AllowBestEffort,
            ..FailurePolicy::default()
        };
        let mut snap = compile(
            &pol,
            &CompileInput {
                fixture: None,
                invalid_refs: vec![crate::compile::ObjectRefErr {
                    group: "g".into(),
                    version: "v1".into(),
                    kind: "K".into(),
                    namespace: "ns".into(),
                    name: "n".into(),
                    reason: "e".into(),
                }],
                ..Default::default()
            },
        )
        .unwrap()
        .snapshot;
        snap.meta.has_errors = true;
        snap.revision = IrRevision::Sequence(7);
        let plan = plan_publish(None, &snap, &pol);
        assert_eq!(plan.action, PublishAction::FullReplace);
    }
}
