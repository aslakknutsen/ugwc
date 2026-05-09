//! Gateway status writers — shapes align with Gateway API v1 helpers
//! (`GatewayMustHaveLatestConditions`, listener condition checks).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use tracing::warn;

use crate::loader::parent_reference_to_snap;
use crate::resources::{
    BackendTLSPolicy, Gateway, GatewayClass, GRPCRoute, HTTPRoute, ListenerSet, TLSRoute,
};
use ugwc_core::{
    grpc_route_accepted_condition_for_parent, http_route_accepted_condition_for_parent,
    tls_route_accepted_condition_for_parent,
};

fn supported_kinds_json(
    eff: Option<&ugwc_core::snapshot::EffectiveListenerSnap>,
) -> Vec<serde_json::Value> {
    let default_http_grpc = || {
        vec![
            serde_json::json!({"group": "gateway.networking.k8s.io", "kind": "HTTPRoute"}),
            serde_json::json!({"group": "gateway.networking.k8s.io", "kind": "GRPCRoute"}),
        ]
    };
    let Some(e) = eff else {
        return default_http_grpc();
    };
    let proto = e.protocol.trim().to_ascii_uppercase();
    if proto == "TLS" {
        let kinds = e.allowed_routes.as_ref().map(|a| &a.kinds[..]).unwrap_or(&[]);
        if kinds.is_empty() {
            return vec![serde_json::json!({"group": "gateway.networking.k8s.io", "kind": "TLSRoute"})];
        }
        return kinds
            .iter()
            .map(|k| {
                serde_json::json!({"group": k.group.as_deref().unwrap_or("gateway.networking.k8s.io"), "kind": &k.kind})
            })
            .collect();
    }
    let http_like = proto == "HTTP" || proto == "HTTPS";
    if !http_like {
        return vec![];
    }
    let kinds = e.allowed_routes.as_ref().map(|a| &a.kinds[..]).unwrap_or(&[]);
    if kinds.is_empty() {
        return default_http_grpc();
    }
    kinds
        .iter()
        .map(|k| {
            serde_json::json!({"group": k.group.as_deref().unwrap_or("gateway.networking.k8s.io"), "kind": &k.kind})
        })
        .collect()
}

fn gateway_listener_https_cert_unresolved(
    cluster: Option<&ugwc_core::ClusterSnapshot>,
    lname: &str,
) -> bool {
    let Some(c) = cluster else {
        return false;
    };
    let Some(eff) = c.effective_listeners.iter().find(|e| {
        matches!(
            e.parent,
            ugwc_core::snapshot::ListenerParentSnap::InlineGateway
        ) && e.section_name == lname
    }) else {
        return false;
    };
    if eff.protocol.trim().to_ascii_uppercase() != "HTTPS" {
        return false;
    }
    eff.tls_resolve_error.is_some()
        || eff.tls_cert_pem.is_empty()
        || eff.tls_key_pem.is_empty()
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
    cluster: Option<&ugwc_core::ClusterSnapshot>,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> Result<(), kube::Error> {
    let name = gateway.metadata.name.as_deref().unwrap_or("");
    if name.is_empty() {
        return Ok(());
    }

    let gen = gateway.metadata.generation.unwrap_or(0);
    let gw_ns = gateway.metadata.namespace.as_deref().unwrap_or("");
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

    let gateway_listener_conflicted = |lname: &str| -> bool {
        cluster
            .map(|c| {
                c.effective_listeners.iter().any(|e| {
                    matches!(
                        e.parent,
                        ugwc_core::snapshot::ListenerParentSnap::InlineGateway
                    ) && e.section_name == lname
                        && e.conflicted
                })
            })
            .unwrap_or(false)
    };

    let inline_listener_conflicted = cluster
        .map(|c| {
            c.effective_listeners.iter().any(|e| {
                matches!(
                    e.parent,
                    ugwc_core::snapshot::ListenerParentSnap::InlineGateway
                ) && e.conflicted
            })
        })
        .unwrap_or(false);

    let gateway_spec_errors = outcome.report.errors.iter().any(|e| {
        e.kind == "Gateway" && e.namespace == gw_ns && e.name == name
    });

    let (gw_acc_status, gw_acc_reason, gw_acc_msg) = if inline_listener_conflicted {
        (
            "False",
            "ListenersNotValid",
            "one or more inline listeners are conflicted",
        )
    } else if gateway_spec_errors {
        (
            "False",
            "Invalid",
            "gateway has spec or TLS resolution errors — see compile report",
        )
    } else {
        ("True", "Accepted", "ugwc reconciled gateway")
    };

    let listener_status: Vec<serde_json::Value> = listener_names
        .iter()
        .map(|lname| {
            let conflicted = gateway_listener_conflicted(lname);
            let spec_row = gateway.spec.listeners.iter().find(|l| l.name == *lname);
            let port = spec_row.map(|l| l.port).unwrap_or(0);
            let lk = ugwc_core::ListenerAttachmentIr {
                parent_kind: "Gateway".into(),
                parent_namespace: gw_ns.to_string(),
                parent_name: name.to_string(),
                section_name: (*lname).clone(),
                port,
            };
            let attached = outcome.report.attachment_count_for_listener_key(&lk);

            let eff = cluster.and_then(|c| {
                c.effective_listeners.iter().find(|e| {
                    matches!(
                        e.parent,
                        ugwc_core::snapshot::ListenerParentSnap::InlineGateway
                    ) && e.section_name == *lname
                })
            });
            let supported = supported_kinds_json(eff);

            let (prog_status, prog_reason) = if programmed_ok && !conflicted {
                ("True", "Programmed")
            } else {
                ("False", reason)
            };
            let (acc_status, acc_reason, acc_msg) = if conflicted {
                (
                    "False",
                    "ListenersConflicted",
                    "listener not distinct after merge with ListenerSets",
                )
            } else {
                ("True", "Accepted", "ugwc reconciled listener")
            };
            let cert_broken = gateway_listener_https_cert_unresolved(cluster, lname);
            let (res_status, res_reason, res_msg) = if cert_broken {
                (
                    "False",
                    "InvalidCertificateRef",
                    "TLS certificate could not be resolved for this listener",
                )
            } else {
                ("True", "ResolvedRefs", "listener certificate refs resolved")
            };
            let mut conditions = vec![
                serde_json::json!({
                    "type": "Accepted",
                    "status": acc_status,
                    "reason": acc_reason,
                    "message": acc_msg,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
                serde_json::json!({
                    "type": "ResolvedRefs",
                    "status": res_status,
                    "reason": res_reason,
                    "message": res_msg,
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
                serde_json::json!({
                    "type": "Programmed",
                    "status": prog_status,
                    "reason": prog_reason,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": gen
                }),
            ];
            if conflicted {
                conditions.insert(
                    1,
                    serde_json::json!({
                        "type": "Conflicted",
                        "status": "True",
                        "reason": "HostnameConflict",
                        "message": "indistinct listener after merge",
                        "lastTransitionTime": now,
                        "observedGeneration": gen
                    }),
                );
            }
            serde_json::json!({
                "name": lname,
                "supportedKinds": supported,
                "attachedRoutes": attached,
                "conditions": conditions
            })
        })
        .collect();

    let (gw_prog_status, gw_prog_reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", reason)
    };

    let attached_ls = cluster.map(|c| c.listener_sets.len()).unwrap_or(0) as i32;

    let status = serde_json::json!({
        "status": {
            "conditions": [
                {
                    "type": "Accepted",
                    "status": gw_acc_status,
                    "reason": gw_acc_reason,
                    "message": gw_acc_msg,
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
            "listeners": listener_status,
            "attachedListenerSets": attached_ls
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

/// Merge `HTTPRoute.status.parents` for parentRefs owned by this reconcile (Gateway + attached ListenerSets).
pub struct HttpRouteMergedParentStatusArgs<'a> {
    pub client: &'a Client,
    pub route_ns: &'a str,
    pub route_name: &'a str,
    pub controller_name: &'a str,
    pub route_generation: i64,
    pub programmed_ok: bool,
    pub resolved_refs_status: &'a str,
    pub resolved_refs_reason: &'a str,
    pub resolved_refs_message: &'a str,
    pub reconciled_gateway_ns: &'a str,
    pub reconciled_gateway_name: &'a str,
    pub snapshot: &'a ugwc_core::ClusterSnapshot,
    pub compile_report: &'a ugwc_core::CompileReport,
    pub status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
}

/// Merge `GRPCRoute.status.parents` for parentRefs owned by this reconcile (Gateway + attached ListenerSets).
pub struct GrpcRouteMergedParentStatusArgs<'a> {
    pub client: &'a Client,
    pub route_ns: &'a str,
    pub route_name: &'a str,
    pub controller_name: &'a str,
    pub route_generation: i64,
    pub programmed_ok: bool,
    pub resolved_refs_status: &'a str,
    pub resolved_refs_reason: &'a str,
    pub resolved_refs_message: &'a str,
    pub reconciled_gateway_ns: &'a str,
    pub reconciled_gateway_name: &'a str,
    pub snapshot: &'a ugwc_core::ClusterSnapshot,
    pub compile_report: &'a ugwc_core::CompileReport,
    pub status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
}

/// Merge `TLSRoute.status.parents` for parentRefs owned by this reconcile (Gateway + attached ListenerSets).
pub struct TlsRouteMergedParentStatusArgs<'a> {
    pub client: &'a Client,
    pub route_ns: &'a str,
    pub route_name: &'a str,
    pub controller_name: &'a str,
    pub route_generation: i64,
    pub programmed_ok: bool,
    pub resolved_refs_status: &'a str,
    pub resolved_refs_reason: &'a str,
    pub resolved_refs_message: &'a str,
    pub reconciled_gateway_ns: &'a str,
    pub reconciled_gateway_name: &'a str,
    pub snapshot: &'a ugwc_core::ClusterSnapshot,
    pub compile_report: &'a ugwc_core::CompileReport,
    pub status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
}

fn parent_ref_json_from_spec(
    p: &crate::resources::ParentReferenceSpec,
    route_ns: &str,
) -> serde_json::Value {
    let mut m = serde_json::json!({
        "group": p.group.as_deref().unwrap_or("gateway.networking.k8s.io"),
        "kind": p.kind.as_deref().unwrap_or("Gateway"),
        "namespace": p.namespace.as_deref().unwrap_or(route_ns),
        "name": p.name,
    });
    if let Some(obj) = m.as_object_mut() {
        if let Some(sn) = &p.section_name {
            obj.insert("sectionName".into(), serde_json::json!(sn));
        }
        if let Some(port) = p.port {
            obj.insert("port".into(), serde_json::json!(port));
        }
    }
    m
}

fn parent_ref_matches_json(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let group_match =
        a.get("group").and_then(|x| x.as_str()) == b.get("group").and_then(|x| x.as_str());
    let kind_match =
        a.get("kind").and_then(|x| x.as_str()) == b.get("kind").and_then(|x| x.as_str());
    let ns_match =
        a.get("namespace").and_then(|x| x.as_str()) == b.get("namespace").and_then(|x| x.as_str());
    let name_match =
        a.get("name").and_then(|x| x.as_str()) == b.get("name").and_then(|x| x.as_str());
    let sec_match = a.get("sectionName").and_then(|x| x.as_str())
        == b.get("sectionName").and_then(|x| x.as_str());
    let port_match = a.get("port") == b.get("port");
    group_match && kind_match && ns_match && name_match && sec_match && port_match
}

pub async fn apply_http_route_merged_parent_status(
    args: HttpRouteMergedParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let HttpRouteMergedParentStatusArgs {
        client,
        route_ns,
        route_name,
        controller_name,
        route_generation,
        programmed_ok,
        resolved_refs_status,
        resolved_refs_reason,
        resolved_refs_message,
        reconciled_gateway_ns,
        reconciled_gateway_name,
        snapshot,
        compile_report,
        status_cache,
    } = args;

    let attached_ls: std::collections::HashSet<(String, String)> = snapshot
        .listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let api: Api<HTTPRoute> = Api::namespaced(client.clone(), route_ns);
    let route = api.get(route_name).await?;

    let mut ours: Vec<&crate::resources::ParentReferenceSpec> = Vec::new();
    for p in &route.spec.parent_refs {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" {
            continue;
        }
        let pns = p.namespace.as_deref().unwrap_or(route_ns);
        if k == "Gateway"
            && pns == reconciled_gateway_ns
            && p.name.as_str() == reconciled_gateway_name
        {
            ours.push(p);
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p);
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| s.parents.clone())
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours
            .iter()
            .any(|p| parent_ref_matches_json(pref, &parent_ref_json_from_spec(p, route_ns)))
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p_st, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };

    let route_snap = snapshot
        .http_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for p in ours {
        let pref_snap = parent_reference_to_snap(p);
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => http_route_accepted_condition_for_parent(
                rs,
                &pref_snap,
                snapshot,
                &attached_ls,
                compile_report,
            ),
            None => (
                "False",
                "Pending",
                "HTTPRoute not loaded into gateway snapshot",
            ),
        };

        let pref = parent_ref_json_from_spec(p, route_ns);
        parents.push(serde_json::json!({
            "parentRef": pref,
            "controllerName": controller_name,
            "conditions": [
                {
                    "type": "Accepted",
                    "status": acc_status,
                    "reason": acc_reason,
                    "message": acc_message,
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
                    "status": p_st,
                    "reason": reason,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": route_generation
                }
            ]
        }));
    }

    let status = serde_json::json!({ "status": { "parents": parents } });

    if let Some(cache) = &status_cache {
        let key = format!(
            "httproute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
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
    api.patch_status(route_name, &params, &patch).await?;
    Ok(())
}

pub async fn apply_grpc_route_merged_parent_status(
    args: GrpcRouteMergedParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let GrpcRouteMergedParentStatusArgs {
        client,
        route_ns,
        route_name,
        controller_name,
        route_generation,
        programmed_ok,
        resolved_refs_status,
        resolved_refs_reason,
        resolved_refs_message,
        reconciled_gateway_ns,
        reconciled_gateway_name,
        snapshot,
        compile_report,
        status_cache,
    } = args;

    let attached_ls: std::collections::HashSet<(String, String)> = snapshot
        .listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let api: Api<GRPCRoute> = Api::namespaced(client.clone(), route_ns);
    let route = api.get(route_name).await?;

    let mut ours: Vec<&crate::resources::ParentReferenceSpec> = Vec::new();
    for p in &route.spec.parent_refs {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" {
            continue;
        }
        let pns = p.namespace.as_deref().unwrap_or(route_ns);
        if k == "Gateway"
            && pns == reconciled_gateway_ns
            && p.name.as_str() == reconciled_gateway_name
        {
            ours.push(p);
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p);
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| s.parents.clone())
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours
            .iter()
            .any(|p| parent_ref_matches_json(pref, &parent_ref_json_from_spec(p, route_ns)))
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p_st, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };

    let route_snap = snapshot
        .grpc_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for p in ours {
        let pref_snap = parent_reference_to_snap(p);
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => grpc_route_accepted_condition_for_parent(
                rs,
                &pref_snap,
                snapshot,
                &attached_ls,
                compile_report,
            ),
            None => (
                "False",
                "Pending",
                "GRPCRoute not loaded into gateway snapshot",
            ),
        };

        let pref = parent_ref_json_from_spec(p, route_ns);
        parents.push(serde_json::json!({
            "parentRef": pref,
            "controllerName": controller_name,
            "conditions": [
                {
                    "type": "Accepted",
                    "status": acc_status,
                    "reason": acc_reason,
                    "message": acc_message,
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
                    "status": p_st,
                    "reason": reason,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": route_generation
                }
            ]
        }));
    }

    let status = serde_json::json!({ "status": { "parents": parents } });

    if let Some(cache) = &status_cache {
        let key = format!(
            "grpcroute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
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
    api.patch_status(route_name, &params, &patch).await?;
    Ok(())
}

pub async fn apply_tls_route_merged_parent_status(
    args: TlsRouteMergedParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let TlsRouteMergedParentStatusArgs {
        client,
        route_ns,
        route_name,
        controller_name,
        route_generation,
        programmed_ok,
        resolved_refs_status,
        resolved_refs_reason,
        resolved_refs_message,
        reconciled_gateway_ns,
        reconciled_gateway_name,
        snapshot,
        compile_report,
        status_cache,
    } = args;

    let attached_ls: std::collections::HashSet<(String, String)> = snapshot
        .listener_sets
        .iter()
        .map(|ls| (ls.namespace.clone(), ls.name.clone()))
        .collect();

    let api: Api<TLSRoute> = Api::namespaced(client.clone(), route_ns);
    let route = api.get(route_name).await?;

    let mut ours: Vec<&crate::resources::ParentReferenceSpec> = Vec::new();
    for p in &route.spec.parent_refs {
        let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        let k = p.kind.as_deref().unwrap_or("Gateway");
        if g != "gateway.networking.k8s.io" {
            continue;
        }
        let pns = p.namespace.as_deref().unwrap_or(route_ns);
        if k == "Gateway"
            && pns == reconciled_gateway_ns
            && p.name.as_str() == reconciled_gateway_name
        {
            ours.push(p);
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p);
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| s.parents.clone())
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours
            .iter()
            .any(|p| parent_ref_matches_json(pref, &parent_ref_json_from_spec(p, route_ns)))
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p_st, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };

    let route_snap = snapshot
        .tls_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for p in ours {
        let pref_snap = parent_reference_to_snap(p);
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => tls_route_accepted_condition_for_parent(
                rs,
                &pref_snap,
                snapshot,
                &attached_ls,
                compile_report,
            ),
            None => (
                "False",
                "Pending",
                "TLSRoute not loaded into gateway snapshot",
            ),
        };

        let pref = parent_ref_json_from_spec(p, route_ns);
        parents.push(serde_json::json!({
            "parentRef": pref,
            "controllerName": controller_name,
            "conditions": [
                {
                    "type": "Accepted",
                    "status": acc_status,
                    "reason": acc_reason,
                    "message": acc_message,
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
                    "status": p_st,
                    "reason": reason,
                    "message": "ugwc compile outcome",
                    "lastTransitionTime": now,
                    "observedGeneration": route_generation
                }
            ]
        }));
    }

    let status = serde_json::json!({ "status": { "parents": parents } });

    if let Some(cache) = &status_cache {
        let key = format!(
            "tlsroute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
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
    api.patch_status(route_name, &params, &patch).await?;
    Ok(())
}

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
    let gen = ls.metadata.generation.unwrap_or(0);
    let ls_ns_name = (ns.to_string(), name.to_string());
    let ls_specific_err =
        outcome.report.errors.iter().any(|e| {
            e.kind == "ListenerSet" && e.namespace == ls_ns_name.0 && e.name == ls_ns_name.1
        });
    let ok = programmed_ok && !ls_specific_err;

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

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
            let lk = ugwc_core::ListenerAttachmentIr {
                parent_kind: "ListenerSet".into(),
                parent_namespace: ls_ns_name.0.clone(),
                parent_name: ls_ns_name.1.clone(),
                section_name: l.name.clone(),
                port: l.port,
            };
            let attached = outcome.report.attachment_count_for_listener_key(&lk);

            let (acc_s, acc_r, acc_m) = if conflicted {
                (
                    "False",
                    "ListenersConflicted",
                    "indistinct listener after merge",
                )
            } else {
                ("True", "Accepted", "ugwc reconciled listener entry")
            };
            let (prog_s, prog_r) = if ok && !conflicted {
                ("True", "Programmed")
            } else {
                ("False", "Invalid")
            };
            let (res_s, res_r, res_m) = if cert_broken {
                (
                    "False",
                    "InvalidCertificateRef",
                    "TLS certificate could not be resolved for this listener",
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
                conditions.insert(
                    1,
                    serde_json::json!({
                        "type": "Conflicted",
                        "status": "True",
                        "reason": "HostnameConflict",
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

    let (acc_g, prog_g, prog_rs) = if ok {
        ("True", "True", "Programmed")
    } else {
        ("True", "False", "Invalid")
    };

    let status = serde_json::json!({
        "status": {
            "conditions": [
                {
                    "type": "Accepted",
                    "status": acc_g,
                    "reason": "Accepted",
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
        }
    });

    if let Some(cache) = &status_cache {
        let key = format!("listenerset:{ns}/{name}");
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        let mut c = cache.lock().unwrap();
        if c.get(&key) == Some(&serialized) {
            return Ok(());
        }
        c.insert(key, serialized);
    }

    let api: Api<ListenerSet> = Api::namespaced(client.clone(), ns);
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}

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
            .map(|s| s.ancestors.clone())
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
        let acc_reason = if outcome.conflicted {
            "Conflicted"
        } else if outcome.accepted {
            "Accepted"
        } else {
            "Invalid"
        };
        let acc_msg = outcome
            .message
            .clone()
            .filter(|_| !outcome.accepted)
            .unwrap_or_else(|| "ugwc backend TLS policy".into());

        let rr_st = if outcome.resolved_refs_ok { "True" } else { "False" };
        let rr_reason = if outcome.resolved_refs_ok {
            "ResolvedRefs"
        } else {
            outcome.message.as_deref().unwrap_or("InvalidCACertificateRef")
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

        if let Some(cache) = &status_cache {
            let key = format!(
                "backendtlspolicy-merge:{}/{}/{gateway_ns}/{gateway_name}",
                outcome.namespace, outcome.name
            );
            let serialized = serde_json::to_string(&status).unwrap_or_default();
            let mut c = cache.lock().unwrap();
            if c.get(&key) == Some(&serialized) {
                continue;
            }
            c.insert(key, serialized);
        }

        let patch = Patch::Merge(status);
        let params = PatchParams::apply("ugwc");
        api.patch_status(&outcome.name, &params, &patch).await?;
    }
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
