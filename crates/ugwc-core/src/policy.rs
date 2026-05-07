//! Policy attachment engine (Phase 5). v1 supported kinds are listed in
//! `design/implementation-plan.md` — extend `PolicyRef` with new variants deliberately.

use std::collections::BTreeSet;

/// v1 **frozen** policy reference — extend only via explicit design review (plan §5.1).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum PolicyRef {
    /// Placeholder for a namespaced policy kind not yet implemented.
    Unsupported {
        group: String,
        kind: String,
        namespace: String,
        name: String,
    },
}

/// Bundle attached to route/listener computation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PolicyOverlayIr {
    pub attached: BTreeSet<PolicyRef>,
}

/// Direct or inherited attachment (precedence decided in Phase 5.2).
#[derive(Clone, Debug)]
pub struct PolicyAttachment {
    pub policy: PolicyRef,
    pub inherited_from: Option<String>,
}
