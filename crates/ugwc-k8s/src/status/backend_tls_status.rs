use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use tracing::warn;

use crate::resources::BackendTLSPolicy;

use super::status_rows_to_json;

/// Merge `BackendTLSPolicy` `status.ancestors` for the reconciled Gateway (policy attachment status).
pub async fn apply_backend_tls_policies_for_gateway(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    controller_name: &str,
    snapshot: &ugwc_core::ClusterSnapshot,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> Result<(), kube::Error> {
    for outcome in &snapshot.backend_tls_policy_outcomes {
        let api: Api<BackendTLSPolicy> = Api::namespaced(client.clone(), &outcome.namespace);
        let pol = match api.get(&outcome.name).await {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    error = %e,
                    policy_ns = %outcome.namespace,
                    policy_name = %outcome.name,
                    "get BackendTLSPolicy for status"
                );
                continue;
            }
        };
        let gen = pol.metadata.generation.unwrap_or(0);

        let mut ancestors: Vec<serde_json::Value> = pol
            .status
            .as_ref()
            .map(|s| status_rows_to_json(&s.ancestors))
            .unwrap_or_default();

        ancestors.retain(|row| {
            let ctrl = row.get("controllerName").and_then(|x| x.as_str());
            if ctrl != Some(controller_name) {
                return true;
            }
            let Some(aref) = row.get("ancestorRef") else {
                return true;
            };
            let g = aref.get("group").and_then(|x| x.as_str());
            let k = aref.get("kind").and_then(|x| x.as_str());
            let ns = aref.get("namespace").and_then(|x| x.as_str());
            let n = aref.get("name").and_then(|x| x.as_str());
            !(g == Some("gateway.networking.k8s.io")
                && k == Some("Gateway")
                && ns == Some(gateway_ns)
                && n == Some(gateway_name))
        });

        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let acc_st = if outcome.accepted { "True" } else { "False" };
        let acc_reason = outcome
            .accepted_condition_reason
            .clone()
            .unwrap_or_else(|| {
                if outcome.conflicted {
                    "Conflicted".into()
                } else if outcome.accepted {
                    "Accepted".into()
                } else {
                    "Invalid".into()
                }
            });
        let acc_msg = outcome
            .message
            .clone()
            .filter(|_| !outcome.accepted)
            .unwrap_or_else(|| "ugwc backend TLS policy".into());

        let rr_st = if outcome.resolved_refs_ok {
            "True"
        } else {
            "False"
        };
        let rr_reason = if outcome.resolved_refs_ok {
            outcome
                .resolved_refs_condition_reason
                .clone()
                .unwrap_or_else(|| "ResolvedRefs".into())
        } else {
            outcome
                .resolved_refs_condition_reason
                .clone()
                .or_else(|| outcome.message.clone().filter(|m| !m.is_empty()))
                .unwrap_or_else(|| "InvalidCACertificateRef".into())
        };
        let rr_msg: String = if outcome.resolved_refs_ok {
            "backend TLS refs resolved".into()
        } else {
            outcome
                .message
                .clone()
                .unwrap_or_else(|| "InvalidCACertificateRef".into())
        };

        ancestors.push(serde_json::json!({
            "ancestorRef": {
                "group": "gateway.networking.k8s.io",
                "kind": "Gateway",
                "namespace": gateway_ns,
                "name": gateway_name,
            },
            "controllerName": controller_name,
            "conditions": [
                {
                    "type": "Accepted",
                    "status": acc_st,
                    "reason": acc_reason,
                    "message": acc_msg,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                },
                {
                    "type": "ResolvedRefs",
                    "status": rr_st,
                    "reason": rr_reason,
                    "message": rr_msg,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }
            ]
        }));

        let status = serde_json::json!({ "status": { "ancestors": ancestors } });

        let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
            let key = format!(
                "backendtlspolicy-merge:{}/{}/{gateway_ns}/{gateway_name}",
                outcome.namespace, outcome.name
            );
            let serialized = serde_json::to_string(&status).unwrap_or_default();
            if cache.lock().unwrap().get(&key) == Some(&serialized) {
                continue;
            }
            Some((key, serialized))
        } else {
            None
        };

        let patch = Patch::Merge(status);
        let params = PatchParams::apply("ugwc");
        api.patch_status(&outcome.name, &params, &patch).await?;

        if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
            cache.lock().unwrap().insert(key, serialized);
        }
    }
    Ok(())
}
