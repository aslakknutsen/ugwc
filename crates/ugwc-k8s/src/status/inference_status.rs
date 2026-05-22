use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use tracing::warn;

use crate::resources::InferencePool;
use ugwc_core::compile::CompileReport;

fn http_route_snap_refs_inference_pool(
    r: &ugwc_core::snapshot::HttpRouteSnap,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    for rule in &r.rules {
        for br in &rule.backend_refs {
            if !br.is_inference_pool() {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(r.namespace.as_str());
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn grpc_route_snap_refs_inference_pool(
    r: &ugwc_core::snapshot::GrpcRouteSnap,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    for rule in &r.rules {
        for br in &rule.backend_refs {
            if !br.is_inference_pool() {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(r.namespace.as_str());
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tls_route_snap_refs_inference_pool(
    r: &ugwc_core::snapshot::TlsRouteSnap,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    for rule in &r.rules {
        for br in &rule.backend_refs {
            if !br.is_inference_pool() {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(r.namespace.as_str());
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn tcp_route_snap_refs_inference_pool(
    r: &ugwc_core::snapshot::TcpRouteSnap,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    for rule in &r.rules {
        for br in &rule.backend_refs {
            if !br.is_inference_pool() {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(r.namespace.as_str());
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn udp_route_snap_refs_inference_pool(
    r: &ugwc_core::snapshot::UdpRouteSnap,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    for rule in &r.rules {
        for br in &rule.backend_refs {
            if !br.is_inference_pool() {
                continue;
            }
            let pns = br.namespace.as_deref().unwrap_or(r.namespace.as_str());
            if pns == pool_ns && br.name == pool_name {
                return true;
            }
        }
    }
    false
}

fn route_report_breaks_inference_pool_backend_ref(
    report: &CompileReport,
    route_kind: &str,
    route_ns: &str,
    route_name: &str,
) -> bool {
    report.errors.iter().any(|e| {
        e.kind == route_kind
            && e.namespace == route_ns
            && e.name == route_name
            && (e.reason == "RefNotPermitted" || e.reason.starts_with("InvalidExtensionRef"))
    })
}

fn inference_pool_resolved_refs_ok(
    snapshot: &ugwc_core::ClusterSnapshot,
    report: &CompileReport,
    pool_ns: &str,
    pool_name: &str,
) -> bool {
    let Some(pool_snap) = snapshot
        .inference_pools
        .iter()
        .find(|p| p.namespace == pool_ns && p.name == pool_name)
    else {
        return false;
    };
    let epp_name = pool_snap.endpoint_picker_ref.name.as_str();
    for e in &report.errors {
        if e.kind == "InferencePool" && e.namespace == pool_ns && e.name == pool_name {
            return false;
        }
        if e.kind == "Service" && e.namespace == pool_ns && e.name == epp_name {
            return false;
        }
    }
    for r in &snapshot.http_routes {
        if !http_route_snap_refs_inference_pool(r, pool_ns, pool_name) {
            continue;
        }
        if route_report_breaks_inference_pool_backend_ref(
            report,
            "HTTPRoute",
            &r.namespace,
            &r.name,
        ) {
            return false;
        }
    }
    for r in &snapshot.grpc_routes {
        if !grpc_route_snap_refs_inference_pool(r, pool_ns, pool_name) {
            continue;
        }
        if route_report_breaks_inference_pool_backend_ref(
            report,
            "GRPCRoute",
            &r.namespace,
            &r.name,
        ) {
            return false;
        }
    }
    for r in &snapshot.tls_routes {
        if !tls_route_snap_refs_inference_pool(r, pool_ns, pool_name) {
            continue;
        }
        if route_report_breaks_inference_pool_backend_ref(report, "TLSRoute", &r.namespace, &r.name)
        {
            return false;
        }
    }
    for r in &snapshot.tcp_routes {
        if !tcp_route_snap_refs_inference_pool(r, pool_ns, pool_name) {
            continue;
        }
        if route_report_breaks_inference_pool_backend_ref(report, "TCPRoute", &r.namespace, &r.name)
        {
            return false;
        }
    }
    for r in &snapshot.udp_routes {
        if !udp_route_snap_refs_inference_pool(r, pool_ns, pool_name) {
            continue;
        }
        if route_report_breaks_inference_pool_backend_ref(report, "UDPRoute", &r.namespace, &r.name)
        {
            return false;
        }
    }
    true
}

use super::run_bounded;

async fn apply_one_inference_pool(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    controller_name: &str,
    snapshot: &ugwc_core::ClusterSnapshot,
    programmed_ok: bool,
    report: &CompileReport,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
    pool_ns: String,
    pool_name: String,
) {
    let pool_loaded = snapshot
        .inference_pools
        .iter()
        .any(|p| p.namespace == pool_ns && p.name == pool_name);
    let resolved_refs_ok =
        inference_pool_resolved_refs_ok(snapshot, report, &pool_ns, &pool_name);
    let accepted_ok = programmed_ok && resolved_refs_ok;

    let resolved_refs_reason = if resolved_refs_ok {
        "ResolvedRefs"
    } else if !pool_loaded {
        "BackendNotFound"
    } else {
        "InvalidExtensionRef"
    };
    let resolved_refs_message: String = if resolved_refs_ok {
        "inference pool refs resolved".into()
    } else if !pool_loaded {
        format!("InferencePool {pool_ns}/{pool_name} not found")
    } else {
        "see route compile errors for InvalidExtensionRef / RefNotPermitted".into()
    };

    let accepted_reason = if accepted_ok {
        "Accepted"
    } else if !programmed_ok {
        "HTTPRouteNotAccepted"
    } else {
        "InvalidExtensionRef"
    };
    let accepted_message: String = if accepted_ok {
        "inference pool accepted for this gateway".into()
    } else if !programmed_ok {
        "gateway compile did not succeed".into()
    } else {
        resolved_refs_message.clone()
    };

    let api: Api<InferencePool> = Api::namespaced(client.clone(), &pool_ns);
    let pool = match api.get(&pool_name).await {
        Ok(p) => p,
        Err(e) => {
            warn!(
                error = %e,
                pool_ns = %pool_ns,
                pool_name = %pool_name,
                "get InferencePool for status"
            );
            return;
        }
    };
    let gen = pool.metadata.generation.unwrap_or(0);
    let mut parents: Vec<serde_json::Value> = pool
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
        let g = pref.get("group").and_then(|x| x.as_str());
        let k = pref.get("kind").and_then(|x| x.as_str());
        let ns = pref.get("namespace").and_then(|x| x.as_str());
        let n = pref.get("name").and_then(|x| x.as_str());
        !(g == Some("gateway.networking.k8s.io")
            && k == Some("Gateway")
            && ns == Some(gateway_ns)
            && n == Some(gateway_name))
    });

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let acc_st = if accepted_ok { "True" } else { "False" };
    let rr_st = if resolved_refs_ok { "True" } else { "False" };

    parents.push(serde_json::json!({
        "parentRef": {
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
                "reason": accepted_reason,
                "message": accepted_message,
                "lastTransitionTime": now,
                "observedGeneration": gen
            },
            {
                "type": "ResolvedRefs",
                "status": rr_st,
                "reason": resolved_refs_reason,
                "message": resolved_refs_message,
                "lastTransitionTime": now,
                "observedGeneration": gen
            }
        ]
    }));

    let status = serde_json::json!({ "status": { "parents": parents } });

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!("inferencepool-merge:{pool_ns}/{pool_name}/{gateway_ns}/{gateway_name}");
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return;
        }
        Some((key, serialized))
    } else {
        None
    };

    if let Err(e) = api
        .patch_status(&pool_name, &PatchParams::apply("ugwc"), &Patch::Merge(status))
        .await
    {
        warn!(
            error = %e,
            pool_ns = %pool_ns,
            pool_name = %pool_name,
            "inferencepool status patch"
        );
        return;
    }

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
}

/// Merge `InferencePool` `status.parents` for pools referenced by routes attached to this Gateway.
pub async fn apply_inference_pools_for_gateway(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    controller_name: &str,
    snapshot: &ugwc_core::ClusterSnapshot,
    programmed_ok: bool,
    report: &CompileReport,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
    status_patch_concurrency: usize,
) -> Result<(), kube::Error> {
    let pools = snapshot.referenced_inference_pools();
    let futs = pools.into_iter().map(|(pool_ns, pool_name)| {
        apply_one_inference_pool(
            client,
            gateway_ns,
            gateway_name,
            controller_name,
            snapshot,
            programmed_ok,
            report,
            status_cache.clone(),
            pool_ns,
            pool_name,
        )
    });

    run_bounded(status_patch_concurrency, futs).await;
    Ok(())
}
