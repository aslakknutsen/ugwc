use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use crate::resources::{
    GRPCRoute, HTTPRoute, TCPRoute, TLSRoute, UDPRoute,
};
use ugwc_core::snapshot::ParentRefSnap;
use ugwc_core::{
    grpc_route_accepted_condition_for_parent, http_route_accepted_condition_for_parent,
    tcp_route_accepted_condition_for_parent, tls_route_accepted_condition_for_parent,
    udp_route_accepted_condition_for_parent,
};

use super::{status_rows_to_json, ToParentRefSnap};

fn parent_ref_json_from_snap(p: &ParentRefSnap, route_ns: &str) -> serde_json::Value {
    let mut m = serde_json::json!({
        "group": p.group.as_deref().unwrap_or("gateway.networking.k8s.io"),
        "kind": p.kind.as_deref().unwrap_or("Gateway"),
        "namespace": p.namespace.as_deref().unwrap_or(route_ns),
        "name": &p.name,
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

/// Merge `TCPRoute.status.parents` for parentRefs owned by this reconcile (Gateway + attached ListenerSets).
pub struct TcpRouteMergedParentStatusArgs<'a> {
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

/// Merge `UDPRoute.status.parents` for parentRefs owned by this reconcile (Gateway + attached ListenerSets).
pub struct UdpRouteMergedParentStatusArgs<'a> {
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

    let mut ours: Vec<ParentRefSnap> = Vec::new();
    for p in route.spec.parent_refs.as_deref().unwrap_or_default() {
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
            ours.push(p.to_parent_ref_snap());
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p.to_parent_ref_snap());
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| status_rows_to_json(s.parents.as_slice()))
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours.iter().any(|snap| {
            parent_ref_matches_json(pref, &parent_ref_json_from_snap(snap, route_ns))
        })
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

    for snap in &ours {
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => http_route_accepted_condition_for_parent(
                rs,
                snap,
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

        let pref = parent_ref_json_from_snap(snap, route_ns);
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "httproute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
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

    let mut ours: Vec<ParentRefSnap> = Vec::new();
    for p in route.spec.parent_refs.as_deref().unwrap_or_default() {
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
            ours.push(p.to_parent_ref_snap());
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p.to_parent_ref_snap());
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| status_rows_to_json(s.parents.as_slice()))
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours.iter().any(|snap| {
            parent_ref_matches_json(pref, &parent_ref_json_from_snap(snap, route_ns))
        })
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

    for snap in &ours {
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => grpc_route_accepted_condition_for_parent(
                rs,
                snap,
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

        let pref = parent_ref_json_from_snap(snap, route_ns);
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "grpcroute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
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

    let mut ours: Vec<ParentRefSnap> = Vec::new();
    for p in route.spec.parent_refs.as_deref().unwrap_or_default() {
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
            ours.push(p.to_parent_ref_snap());
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p.to_parent_ref_snap());
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| status_rows_to_json(s.parents.as_slice()))
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours.iter().any(|snap| {
            parent_ref_matches_json(pref, &parent_ref_json_from_snap(snap, route_ns))
        })
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

    for snap in &ours {
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => tls_route_accepted_condition_for_parent(
                rs,
                snap,
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

        let pref = parent_ref_json_from_snap(snap, route_ns);
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "tlsroute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
    Ok(())
}

pub async fn apply_tcp_route_merged_parent_status(
    args: TcpRouteMergedParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let TcpRouteMergedParentStatusArgs {
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

    let api: Api<TCPRoute> = Api::namespaced(client.clone(), route_ns);
    let route = api.get(route_name).await?;

    let mut ours: Vec<ParentRefSnap> = Vec::new();
    for p in route.spec.parent_refs.as_deref().unwrap_or_default() {
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
            ours.push(p.to_parent_ref_snap());
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p.to_parent_ref_snap());
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| status_rows_to_json(s.parents.as_slice()))
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours.iter().any(|snap| {
            parent_ref_matches_json(pref, &parent_ref_json_from_snap(snap, route_ns))
        })
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p_st, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };

    let route_snap = snapshot
        .tcp_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for snap in &ours {
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => tcp_route_accepted_condition_for_parent(
                rs,
                snap,
                snapshot,
                &attached_ls,
                compile_report,
            ),
            None => (
                "False",
                "Pending",
                "TCPRoute not loaded into gateway snapshot",
            ),
        };

        let pref = parent_ref_json_from_snap(snap, route_ns);
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "tcproute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
    Ok(())
}

pub async fn apply_udp_route_merged_parent_status(
    args: UdpRouteMergedParentStatusArgs<'_>,
) -> Result<(), kube::Error> {
    let UdpRouteMergedParentStatusArgs {
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

    let api: Api<UDPRoute> = Api::namespaced(client.clone(), route_ns);
    let route = api.get(route_name).await?;

    let mut ours: Vec<ParentRefSnap> = Vec::new();
    for p in route.spec.parent_refs.as_deref().unwrap_or_default() {
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
            ours.push(p.to_parent_ref_snap());
            continue;
        }
        if k == "ListenerSet" && attached_ls.contains(&(pns.to_string(), p.name.clone())) {
            ours.push(p.to_parent_ref_snap());
        }
    }

    if ours.is_empty() {
        return Ok(());
    }

    let mut parents: Vec<serde_json::Value> = route
        .status
        .as_ref()
        .map(|s| status_rows_to_json(s.parents.as_slice()))
        .unwrap_or_default();

    parents.retain(|row| {
        let ctrl = row.get("controllerName").and_then(|x| x.as_str());
        if ctrl != Some(controller_name) {
            return true;
        }
        let Some(pref) = row.get("parentRef") else {
            return true;
        };
        !ours.iter().any(|snap| {
            parent_ref_matches_json(pref, &parent_ref_json_from_snap(snap, route_ns))
        })
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (p_st, reason) = if programmed_ok {
        ("True", "Programmed")
    } else {
        ("False", "CompileErrors")
    };

    let route_snap = snapshot
        .udp_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for snap in &ours {
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => udp_route_accepted_condition_for_parent(
                rs,
                snap,
                snapshot,
                &attached_ls,
                compile_report,
            ),
            None => (
                "False",
                "Pending",
                "UDPRoute not loaded into gateway snapshot",
            ),
        };

        let pref = parent_ref_json_from_snap(snap, route_ns);
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "udproute-merge:{route_ns}/{route_name}/{reconciled_gateway_ns}/{reconciled_gateway_name}"
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(route_name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
    Ok(())
}
