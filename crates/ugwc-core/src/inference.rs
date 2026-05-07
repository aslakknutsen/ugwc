//! Inference Extension fields (Phase 6) — kept free of `kube` types.

use std::collections::BTreeMap;

/// Pool/model routing hints for EPP / ext_proc wiring (IR-neutral).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InferenceIr {
    /// pool name -> model weight hints (0–100) for Phase 7 encoders.
    pub pool_weights: BTreeMap<String, u32>,
    /// Arbitrary criticality tag for scheduling (stringly for v0).
    pub criticality_class: Option<String>,
}
