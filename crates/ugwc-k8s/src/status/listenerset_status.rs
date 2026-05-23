use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use crate::resources::ListenerSet;

use super::supported_kinds_json;
use super::gateway_helpers::{
    listener_route_kinds_broken_for_status, listenerset_generation_for_status,
    listenerset_listener_cert_ref_not_permitted,
};

/// Update `ListenerSet` status for listeners merged onto a reconciled Gateway.
pub async fn apply_listenerset_reconciled_status(
    client: &Client,
    ls: &ListenerSet,
    snapshot: &ugwc_core::ClusterSnapshot,
    programmed_ok: bool,
    outcome: &ugwc_core::CompileOutcome,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> Result<(), kube::Error> {
    let name = ls.metadata.name.as_deref().unwrap_or("");
    let ns = ls.metadata.namespace.as_deref().unwrap_or("");
    if name.is_empty() || ns.is_empty() {
        return Ok(());
    }
    let api: Api<ListenerSet> = Api::namespaced(client.clone(), ns);
    let gen = listenerset_generation_for_status(&api, name, ns).await?;
    let ls_ns_name = (ns.to_string(), name.to_string());

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let mut any_listener_valid = false;

    let listeners_json: Vec<serde_json::Value> = ls
        .spec
        .listeners
        .iter()
        .map(|l| {
            let conflicted = snapshot.effective_listeners.iter().any(|e| {
                matches!(
                    &e.parent,
                    ugwc_core::snapshot::ListenerParentSnap::ListenerSet {
                        namespace,
                        name,
                    } if namespace == &ls_ns_name.0 && name == &ls_ns_name.1
                ) && e.section_name == l.name
                    && e.conflicted
            });
            let eff = snapshot.effective_listeners.iter().find(|e| {
                matches!(
                    &e.parent,
                    ugwc_core::snapshot::ListenerParentSnap::ListenerSet {
                        namespace,
                        name,
                    } if namespace == &ls_ns_name.0 && name == &ls_ns_name.1
                ) && e.section_name == l.name
            });
            let supported = supported_kinds_json(eff);
            let cert_broken = eff
                .map(|e| {
                    e.protocol.trim().to_ascii_uppercase() == "HTTPS"
                        && (e.tls_resolve_error.is_some()
                            || e.tls_cert_pem.is_empty()
                            || e.tls_key_pem.is_empty())
                })
                .unwrap_or(false);
            let route_kinds_broken = listener_route_kinds_broken_for_status(eff, Some(l));
            let lk = ugwc_core::ListenerAttachmentIr {
                parent_kind: "ListenerSet".into(),
                parent_namespace: ls_ns_name.0.clone(),
                parent_name: ls_ns_name.1.clone(),
                section_name: l.name.clone(),
                port: u16::try_from(l.port).unwrap_or(0),
            };
            let attached = outcome.report.attachment_count_for_listener_key(&lk);

            let listener_invalid = eff.is_none() || conflicted || cert_broken || route_kinds_broken;
            if !listener_invalid {
                any_listener_valid = true;
            }

            let (acc_s, acc_r, acc_m) = if conflicted {
                let cr = eff
                    .and_then(|e| e.conflict_reason.as_deref())
                    .unwrap_or("HostnameConflict");
                (
                    "False",
                    cr,
                    "indistinct listener after merge",
                )
            } else {
                ("True", "Accepted", "ugwc reconciled listener entry")
            };
            let (prog_s, prog_r) = if conflicted {
                let cr = eff
                    .and_then(|e| e.conflict_reason.as_deref())
                    .unwrap_or("HostnameConflict");
                ("False", cr)
            } else if programmed_ok {
                ("True", "Programmed")
            } else {
                ("False", "Invalid")
            };
            let ref_not_permitted_ls = eff
                .and_then(|e| e.tls_resolve_error.as_deref())
                .map(|e| e.contains("RefNotPermitted"))
                .unwrap_or(false)
                || listenerset_listener_cert_ref_not_permitted(Some(l), ns, Some(snapshot));
            let (res_s, res_r, res_m) = if cert_broken && ref_not_permitted_ls {
                (
                    "False",
                    "RefNotPermitted",
                    "cross-namespace Secret reference not permitted by any ReferenceGrant",
                )
            } else if cert_broken {
                (
                    "False",
                    "InvalidCertificateRef",
                    "TLS certificate could not be resolved for this listener",
                )
            } else if route_kinds_broken {
                (
                    "False",
                    "InvalidRouteKinds",
                    "allowedRoutes lists route kind(s) not supported for this listener protocol",
                )
            } else {
                ("True", "ResolvedRefs", "listener certificate refs resolved")
            };
            let mut conditions = vec![
                serde_json::json!({
                    "type": "Accepted",
                    "status": acc_s,
                    "reason": acc_r,
                    "message": acc_m,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
                serde_json::json!({
                    "type": "ResolvedRefs",
                    "status": res_s,
                    "reason": res_r,
                    "message": res_m,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
                serde_json::json!({
                    "type": "Programmed",
                    "status": prog_s,
                    "reason": prog_r,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
            ];
            if conflicted {
                let conflict_reason_str = eff
                    .and_then(|e| e.conflict_reason.as_deref())
                    .unwrap_or("HostnameConflict");
                conditions.insert(
                    1,
                    serde_json::json!({
                        "type": "Conflicted",
                        "status": "True",
                        "reason": conflict_reason_str,
                        "message": "indistinct listener after merge",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    }),
                );
            }
            serde_json::json!({
                "name": l.name,
                "supportedKinds": supported,
                "attachedRoutes": attached,
                "conditions": conditions
            })
        })
        .collect();

    let (acc_g, acc_reason_g) = if any_listener_valid {
        ("True", "Accepted")
    } else {
        ("False", "ListenersNotValid")
    };
    let (prog_g, prog_rs) = if any_listener_valid {
        ("True", "Programmed")
    } else {
        ("False", "ListenersNotValid")
    };

    let status_doc = serde_json::json!({
        "conditions": [
            {
                "type": "Accepted",
                "status": acc_g,
                "reason": acc_reason_g,
                "message": "ugwc listenerset",
                "lastTransitionTime": now,
                "observedGeneration": gen
            },
            {
                "type": "Programmed",
                "status": prog_g,
                "reason": prog_rs,
                "message": "ugwc compile outcome",
                "lastTransitionTime": now,
                "observedGeneration": gen
            }
        ],
        "listeners": listeners_json
    });

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!("listenerset:{ns}/{name}");
        let serialized = serde_json::to_string(&status_doc).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let apply_doc = serde_json::json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "ListenerSet",
        "metadata": {
            "name": name,
            "namespace": ns,
        },
        "status": status_doc,
    });
    let patch = Patch::Apply(&apply_doc);
    let params = PatchParams::apply("ugwc").force();
    api.patch_status(name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
    Ok(())
}

/// Write rejection status for a `ListenerSet` that references this Gateway
/// but was not allowed (e.g. `allowedListeners` absent or namespace mismatch).
pub async fn apply_listenerset_rejected_status(
    client: &Client,
    ls: &ListenerSet,
    reason: &str,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> Result<(), kube::Error> {
    let name = ls.metadata.name.as_deref().unwrap_or("");
    let ns = ls.metadata.namespace.as_deref().unwrap_or("");
    if name.is_empty() || ns.is_empty() {
        return Ok(());
    }
    let api: Api<ListenerSet> = Api::namespaced(client.clone(), ns);
    let gen = listenerset_generation_for_status(&api, name, ns).await?;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let listeners_json: Vec<serde_json::Value> = ls
        .spec
        .listeners
        .iter()
        .map(|l| {
            let supported = supported_kinds_json(None);
            serde_json::json!({
                "name": l.name,
                "supportedKinds": supported,
                "attachedRoutes": 0,
                "conditions": [
                    {
                        "type": "Accepted",
                        "status": "False",
                        "reason": reason,
                        "message": "listener set not allowed by gateway",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    },
                    {
                        "type": "Programmed",
                        "status": "False",
                        "reason": reason,
                        "message": "listener set not allowed by gateway",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    }
                ]
            })
        })
        .collect();

    let status_doc = serde_json::json!({
        "conditions": [
            {
                "type": "Accepted",
                "status": "False",
                "reason": reason,
                "message": "listener set not allowed by gateway",
                "lastTransitionTime": now,
                "observedGeneration": gen
            },
            {
                "type": "Programmed",
                "status": "False",
                "reason": reason,
                "message": "listener set not allowed by gateway",
                "lastTransitionTime": now,
                "observedGeneration": gen
            }
        ],
        "listeners": listeners_json
    });

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!("listenerset:{ns}/{name}");
        let serialized = serde_json::to_string(&status_doc).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let apply_doc = serde_json::json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "ListenerSet",
        "metadata": {
            "name": name,
            "namespace": ns,
        },
        "status": status_doc,
    });
    let patch = Patch::Apply(&apply_doc);
    let params = PatchParams::apply("ugwc").force();
    api.patch_status(name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
    Ok(())
}

/// Spec-only ListenerSet status patch to clear CRD Pending defaults before full reconcile.
pub async fn apply_listenerset_spec_only_status(
    client: &Client,
    ls: &ListenerSet,
) -> Result<(), kube::Error> {
    let ns = ls.metadata.namespace.as_deref().unwrap_or("default");
    let name = ls.metadata.name.as_deref().unwrap_or("listenerset");
    let co = ugwc_core::CompileOutcome::status_only_placeholder(ns, name);
    apply_listenerset_reconciled_status(
        client,
        ls,
        &ugwc_core::ClusterSnapshot::default(),
        false,
        &co,
        None,
    )
    .await
}
