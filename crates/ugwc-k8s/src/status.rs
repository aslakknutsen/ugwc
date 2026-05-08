//! Gateway status writers — shapes align with Gateway API v1 helpers
//! (`GatewayMustHaveLatestConditions`, listener condition checks).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use crate::resources::{Gateway, GatewayClass, HTTPRoute};

/// Arguments for [`apply_http_route_parent_status`].
pub struct HttpRouteParentStatusArgs<'a> {
    pub client: &'a Client,
    pub route_ns: &'a str,
    pub route_name: &'a str,
    pub gateway_ns: &'a str,
    pub gateway_name: &'a str,
    pub controller_name: &'a str,
    pub route_generation: i64,
    pub programmed_ok: bool,
    pub resolved_refs_status: &'a str,
    pub resolved_refs_reason: &'a str,
    pub resolved_refs_message: &'a str,
    pub status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
}

/// Apply Gateway + listener status after a compile/reconcile pass.
///
/// Conformance helpers require every condition to carry `observedGeneration`
/// matching `metadata.generation`, and per-listener `Accepted` / `ResolvedRefs` /
/// `Programmed` rows for each listener in spec.
pub async fn apply_gateway_reconciled_status(
    api: &kube::Api<Gateway>,
    gateway: &Gateway,
    outcome: &ugwc_core::CompileOutcome,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> Result<(), kube::Error> {
    let name = gateway.metadata.name.as_deref().unwrap_or("");
    if name.is_empty() {
        return Ok(());
    }

    let gen = gateway.metadata.generation.unwrap_or(0);
    let programmed_ok = !outcome.report.has_errors() && !outcome.snapshot.meta.has_errors;
    let refs_broken = outcome.report.errors.iter().any(|e| {
        e.reason == "BackendNotFound"
            || e.reason == "NoReadyEndpoints"
            || e.reason == "UnsupportedBackendKind"
    });
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
            let (res_status, res_reason, res_msg) = if refs_broken {
                (
                    "False",
                    "BackendNotFound",
                    "compile report lists backend/ref issues",
                )
            } else {
                ("True", "ResolvedRefs", "refs resolved for v0 slice")
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
                        "status": res_status,
                        "reason": res_reason,
                        "message": res_msg,
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

    if let Some(cache) = &status_cache {
        let key = format!(
            "{}/{}",
            gateway.metadata.namespace.as_deref().unwrap_or(""),
            name
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        let mut c = cache.lock().unwrap();
        if c.get(&key) == Some(&serialized) {
            return Ok(());
        }
        c.insert(key, serialized);
    }

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}

/// Patch `HTTPRoute` status for the parent `Gateway` entry owned by this controller.
pub async fn apply_http_route_parent_status(
    args: HttpRouteParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let HttpRouteParentStatusArgs {
        client,
        route_ns,
        route_name,
        gateway_ns,
        gateway_name,
        controller_name,
        route_generation,
        programmed_ok,
        resolved_refs_status,
        resolved_refs_reason,
        resolved_refs_message,
        status_cache,
    } = args;
    let api: Api<HTTPRoute> = Api::namespaced(client.clone(), route_ns);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };
    let status = serde_json::json!({
        "status": {
            "parents": [{
                "parentRef": {
                    "group": "gateway.networking.k8s.io",
                    "kind": "Gateway",
                    "namespace": gateway_ns,
                    "name": gateway_name
                },
                "controllerName": controller_name,
                "conditions": [
                    {
                        "type": "Accepted",
                        "status": "True",
                        "reason": "Accepted",
                        "message": "ugwc",
                        "lastTransitionTime": now,
                        "observedGeneration": route_generation
                    },
                    {
                        "type": "ResolvedRefs",
                        "status": resolved_refs_status,
                        "reason": resolved_refs_reason,
                        "message": resolved_refs_message,
                        "lastTransitionTime": now,
                        "observedGeneration": route_generation
                    },
                    {
                        "type": "Programmed",
                        "status": p,
                        "reason": reason,
                        "message": "ugwc compile outcome",
                        "lastTransitionTime": now,
                        "observedGeneration": route_generation
                    }
                ]
            }]
        }
    });

    if let Some(cache) = &status_cache {
        let key = format!("httproute:{route_ns}/{route_name}/{gateway_ns}/{gateway_name}");
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        let mut c = cache.lock().unwrap();
        if c.get(&key) == Some(&serialized) {
            return Ok(());
        }
        c.insert(key, serialized);
    }

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;
    Ok(())
}

/// Minimal `GatewayClass` status for classes owned by this controller (conformance stub).
pub async fn apply_gateway_class_accepted_stub(
    client: &Client,
    gc: &GatewayClass,
) -> Result<(), kube::Error> {
    let name = gc.metadata.name.as_deref().unwrap_or("");
    if name.is_empty() {
        return Ok(());
    }
    let gen = gc.metadata.generation.unwrap_or(0);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let status = serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Accepted",
                "status": "True",
                "reason": "Accepted",
                "message": "ugwc stub — supportedFeatures not enumerated",
                "lastTransitionTime": now,
                "observedGeneration": gen
            }],
            "supportedFeatures": []
        }
    });
    let api: Api<GatewayClass> = Api::all(client.clone());
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
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
    let params = PatchParams::apply("ugwc");
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}
