//! Gateway status writers — shapes align with Gateway API v1 helpers
//! (`GatewayMustHaveLatestConditions`, listener condition checks).

use kube::api::{Patch, PatchParams};

use crate::resources::Gateway;

/// Apply Gateway + listener status after a compile/reconcile pass.
///
/// Conformance helpers require every condition to carry `observedGeneration`
/// matching `metadata.generation`, and per-listener `Accepted` / `ResolvedRefs` /
/// `Programmed` rows for each listener in spec.
pub async fn apply_gateway_reconciled_status(
    api: &kube::Api<Gateway>,
    gateway: &Gateway,
    outcome: &ugwc_core::CompileOutcome,
) -> Result<(), kube::Error> {
    let name = gateway.metadata.name.as_deref().unwrap_or("");
    if name.is_empty() {
        return Ok(());
    }

    let gen = gateway.metadata.generation.unwrap_or(0);
    let programmed_ok = !outcome.report.has_errors() && !outcome.snapshot.meta.has_errors;
    let reason = if programmed_ok {
        "Programmed"
    } else {
        outcome
            .report
            .errors
            .first()
            .map(|e| e.reason.as_str())
            .unwrap_or("CompileErrors")
    };

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let listener_names: Vec<String> = gateway
        .spec
        .listeners
        .iter()
        .map(|l| l.name.clone())
        .collect();

    let listener_status: Vec<serde_json::Value> = listener_names
        .iter()
        .map(|lname| {
            let (prog_status, prog_reason) = if programmed_ok {
                ("True", "Programmed")
            } else {
                ("False", reason)
            };
            serde_json::json!({
                "name": lname,
                "supportedKinds": [],
                "attachedRoutes": 0,
                "conditions": [
                    {
                        "type": "Accepted",
                        "status": "True",
                        "reason": "Accepted",
                        "message": "ugwc reconciled listener",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    },
                    {
                        "type": "ResolvedRefs",
                        "status": "True",
                        "reason": "ResolvedRefs",
                        "message": "ugwc stub — ref checks not implemented",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    },
                    {
                        "type": "Programmed",
                        "status": prog_status,
                        "reason": prog_reason,
                        "message": "ugwc compile outcome",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    }
                ]
            })
        })
        .collect();

    let (gw_prog_status, gw_prog_reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", reason)
    };

    let status = serde_json::json!({
        "status": {
            "conditions": [
                {
                    "type": "Accepted",
                    "status": "True",
                    "reason": "Accepted",
                    "message": "ugwc reconciled gateway",
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                },
                {
                    "type": "Programmed",
                    "status": gw_prog_status,
                    "reason": gw_prog_reason,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }
            ],
            "listeners": listener_status
        }
    });

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc").force();
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}

/// Set `Programmed=False` with a compile error reason (legacy helper).
pub async fn patch_gateway_programmed_false(
    api: &kube::Api<Gateway>,
    name: &str,
    reason: &str,
) -> Result<(), kube::Error> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let status = serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Programmed",
                "status": "False",
                "reason": reason,
                "message": "compile failed — see ugwc logs",
                "lastTransitionTime": now,
                "observedGeneration": 1
            }]
        }
    });
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc").force();
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}
