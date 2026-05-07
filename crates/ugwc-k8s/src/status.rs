//! Gateway / route status helpers (Merge A.3+).

use kube::api::{Patch, PatchParams};

use crate::resources::Gateway;

/// Set `Programmed=False` with a compile error reason.
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
            }]
        }
    });
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc").force();
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}
