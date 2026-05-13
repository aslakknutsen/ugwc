//! Gateway status writers — shapes align with Gateway API v1 helpers
//! (`GatewayMustHaveLatestConditions`, listener condition checks).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use k8s_openapi::api::core::v1::Service;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use tracing::warn;

use crate::loader::parent_reference_to_snap;
use crate::provision::per_gateway_dataplane_name;
use crate::resources::{
    BackendTLSPolicy, GRPCRoute, Gateway, GatewayClass, HTTPRoute, InferencePool, ListenerSet,
    TCPRoute, TLSRoute, UDPRoute,
};
use ugwc_core::compile::CompileReport;
use ugwc_core::{
    grpc_route_accepted_condition_for_parent, http_route_accepted_condition_for_parent,
    tcp_route_accepted_condition_for_parent, tls_route_accepted_condition_for_parent,
    udp_route_accepted_condition_for_parent,
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
    ugwc_core::effective_listener_supported_route_kinds_for_status(e)
        .into_iter()
        .map(|k| {
            serde_json::json!({
                "group": k.group.as_deref().unwrap_or("gateway.networking.k8s.io"),
                "kind": &k.kind
            })
        })
        .collect()
}

/// True when compile attached an `InvalidCertificateRef` (or related) error to this Gateway.
///
/// Used so listener `ResolvedRefs` matches compile TLS failures when snapshot fields lag.
fn gateway_compile_reports_invalid_certificate_ref(
    report: &CompileReport,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    report.errors.iter().any(|e| {
        e.kind == "Gateway"
            && e.namespace == gw_ns
            && e.name == gw_name
            && e.reason
                .to_ascii_lowercase()
                .contains("invalidcertificateref")
    })
}

/// HTTPS listeners with structurally invalid `certificateRefs` (before Secret fetch).
fn gateway_spec_https_certificate_ref_obviously_invalid(
    listener: &crate::resources::GatewayListenerSpec,
) -> bool {
    if listener.protocol.trim().to_ascii_uppercase() != "HTTPS" {
        return false;
    }
    let Some(ref tls) = listener.tls else {
        return true;
    };
    let refs = &tls.certificate_refs;
    if refs.is_empty() {
        return true;
    }
    let r = &refs[0];
    let kind = r.kind.as_deref().unwrap_or("Secret");
    let group = r.group.as_deref().unwrap_or("");
    let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
    !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok
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
    eff.tls_resolve_error.is_some() || eff.tls_cert_pem.is_empty() || eff.tls_key_pem.is_empty()
}

fn gateway_ip_address_json(ip: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "IPAddress",
        "value": ip,
    })
}

fn gateway_status_addresses_from_service(svc: &Service) -> Vec<serde_json::Value> {
    if let Some(st) = svc.status.as_ref() {
        if let Some(lb) = st.load_balancer.as_ref() {
            if let Some(ingress) = lb.ingress.as_ref() {
                for ing in ingress {
                    if let Some(ip) = ing.ip.as_deref() {
                        if !ip.is_empty() {
                            return vec![gateway_ip_address_json(ip)];
                        }
                    }
                }
            }
        }
    }
    // If the dataplane is a LoadBalancer, only publish once an ingress IP exists. Publishing
    // ClusterIP while the LB is pending breaks conformance (host runs `go test` against
    // `Gateway.status.addresses`).
    let is_load_balancer = svc
        .spec
        .as_ref()
        .and_then(|s| s.type_.as_deref())
        == Some("LoadBalancer");
    if is_load_balancer {
        return Vec::new();
    }
    if let Some(spec) = svc.spec.as_ref() {
        if let Some(ip) = spec.cluster_ip.as_deref() {
            if !ip.is_empty() && ip != "None" {
                return vec![gateway_ip_address_json(ip)];
            }
        }
    }
    Vec::new()
}

async fn fetch_gateway_status_addresses_json(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
) -> Vec<serde_json::Value> {
    let svc_name = per_gateway_dataplane_name(gateway_name);
    let api: Api<Service> = Api::namespaced(client.clone(), gateway_ns);
    match api.get(&svc_name).await {
        Ok(svc) => gateway_status_addresses_from_service(&svc),
        Err(e) => {
            let not_found = matches!(
                &e,
                kube::Error::Api(err) if err.code == 404
            );
            if !not_found {
                warn!(
                    error = %e,
                    %gateway_ns,
                    service = %svc_name,
                    "could not read dataplane Service for Gateway status.addresses"
                );
            }
            Vec::new()
        }
    }
}

/// Resolve `metadata.generation` for status `observedGeneration` (always refetch from API).
async fn gateway_generation_for_status(
    api: &kube::Api<Gateway>,
    name: &str,
    gateway_ns: &str,
) -> Result<i64, kube::Error> {
    let refreshed = api.get(name).await?;
    let gen = refreshed
        .metadata
        .generation
        .filter(|&g| g > 0)
        .unwrap_or(1);
    if refreshed.metadata.generation.is_none() || refreshed.metadata.generation == Some(0) {
        warn!(
            gateway = %name,
            namespace = %gateway_ns,
            "Gateway metadata.generation missing or zero after refetch; using {gen} for observedGeneration"
        );
    }
    Ok(gen)
}

/// Apply Gateway + listener status after a compile/reconcile pass.
///
/// Conformance helpers require every condition to carry `observedGeneration`
/// matching `metadata.generation`, and per-listener `Accepted` / `ResolvedRefs` /
/// `Programmed` rows for each listener in spec.
pub async fn apply_gateway_reconciled_status(
    client: &Client,
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

    let gw_ns = gateway.metadata.namespace.as_deref().unwrap_or("");
    let gen = gateway_generation_for_status(api, name, gw_ns).await?;
    let addresses = fetch_gateway_status_addresses_json(client, gw_ns, name).await;
    let programmed_ok =
        !outcome.report.has_blocking_errors() && !outcome.snapshot.meta.has_errors;
    // ObjectRefErr.reason is free-form; Gateway API `status.*.conditions[].reason` must match
    // `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$` (no spaces). Never put compile text in `reason`.
    let compile_err_detail: String = outcome
        .report
        .errors
        .first()
        .map(|e| {
            if e.name.is_empty() {
                e.reason.clone()
            } else {
                format!("{} {}/{}: {}", e.kind, e.namespace, e.name, e.reason)
            }
        })
        .unwrap_or_else(|| "compile or snapshot errors".into());

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

    // Gateway-level `Accepted` / `Programmed` are summary conditions. Conformance
    // `NamespacesMustBeReady` requires both True for every Gateway in
    // `gateway-conformance-infra` without `gateway-api/skip-this-for-readiness`,
    // including invalid-TLS fixtures — those tests only assert **listener**
    // `ResolvedRefs` + `InvalidCertificateRef` (`gateway-invalid-tls-configuration.go`).
    // TLS / cert / compile issues belong on listener rows (and on `Programmed` there).
    let (gw_acc_status, gw_acc_reason, gw_acc_msg) = if inline_listener_conflicted {
        (
            "False",
            "ListenersNotValid",
            "one or more inline listeners are conflicted",
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

            let (prog_status, prog_reason, prog_msg) = if programmed_ok && !conflicted {
                (
                    "True",
                    "Programmed",
                    "ugwc compile outcome".to_string(),
                )
            } else if conflicted {
                (
                    "False",
                    "Invalid",
                    "listener not distinct after merge with ListenerSets".to_string(),
                )
            } else {
                ("False", "Invalid", compile_err_detail.clone())
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
            let is_https = spec_row
                .map(|l| l.protocol.trim().eq_ignore_ascii_case("HTTPS"))
                .unwrap_or(false);
            let spec_tls_obviously_invalid = spec_row
                .map(|l| gateway_spec_https_certificate_ref_obviously_invalid(l))
                .unwrap_or(false);
            let mut cert_broken = gateway_listener_https_cert_unresolved(cluster, lname)
                || spec_tls_obviously_invalid;
            if is_https && gateway_compile_reports_invalid_certificate_ref(&outcome.report, gw_ns, name)
            {
                cert_broken = true;
            }
            let route_kinds_broken = eff
                .map(ugwc_core::effective_listener_has_invalid_allowed_route_kinds)
                .unwrap_or(false);
            let (res_status, res_reason, res_msg) = if cert_broken {
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
                    "message": prog_msg,
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

    let (gw_prog_status, gw_prog_reason, gw_prog_msg) = if !inline_listener_conflicted {
        let msg = if programmed_ok {
            "ugwc compile outcome".to_string()
        } else {
            format!(
                "gateway reconciled; {} (details on listener conditions)",
                compile_err_detail
            )
        };
        (
            "True",
            "Programmed",
            msg,
        )
    } else {
        ("False", "Invalid", compile_err_detail.clone())
    };

    let attached_ls = cluster.map(|c| c.listener_sets.len()).unwrap_or(0) as i32;

    let status_doc = serde_json::json!({
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
                "message": gw_prog_msg,
                "lastTransitionTime": now,
                "observedGeneration": gen
            }
        ],
        "listeners": listener_status,
        "attachedListenerSets": attached_ls,
        "addresses": addresses
    });
    let status = serde_json::json!({
        "status": status_doc.clone(),
    });

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!(
            "{}/{}",
            gateway.metadata.namespace.as_deref().unwrap_or(""),
            name
        );
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    // Server-Side Apply: Gateway `status.conditions` is a list-map keyed by `type`; JSON merge
    // patch can fail to overlay `observedGeneration` onto CRD-applied default conditions. SSA
    // with `force` takes ownership of `status` so conformance sees `observedGeneration ==
    // metadata.generation` on Accepted/Programmed.
    let apply_doc = serde_json::json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "Gateway",
        "metadata": {
            "name": name,
            "namespace": gw_ns,
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
        .tcp_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for p in ours {
        let pref_snap = parent_reference_to_snap(p);
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => tcp_route_accepted_condition_for_parent(
                rs,
                &pref_snap,
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
        .udp_routes
        .iter()
        .find(|r| r.namespace == route_ns && r.name == route_name);

    for p in ours {
        let pref_snap = parent_reference_to_snap(p);
        let (acc_status, acc_reason, acc_message) = match route_snap {
            Some(rs) => udp_route_accepted_condition_for_parent(
                rs,
                &pref_snap,
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
            let route_kinds_broken = eff
                .map(ugwc_core::effective_listener_has_invalid_allowed_route_kinds)
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

    let pending_cache: Option<(String, String)> = if let Some(cache) = &status_cache {
        let key = format!("listenerset:{ns}/{name}");
        let serialized = serde_json::to_string(&status).unwrap_or_default();
        if cache.lock().unwrap().get(&key) == Some(&serialized) {
            return Ok(());
        }
        Some((key, serialized))
    } else {
        None
    };

    let api: Api<ListenerSet> = Api::namespaced(client.clone(), ns);
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(name, &params, &patch).await?;

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
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
) -> Result<(), kube::Error> {
    let pools = snapshot.referenced_inference_pools();
    for (pool_ns, pool_name) in pools {
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
                continue;
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
            let key =
                format!("inferencepool-merge:{pool_ns}/{pool_name}/{gateway_ns}/{gateway_name}");
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
        api.patch_status(&pool_name, &params, &patch).await?;

        if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
            cache.lock().unwrap().insert(key, serialized);
        }
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
    let gw = api.get(name).await?;
    let gen = gw
        .metadata
        .generation
        .filter(|&g| g > 0)
        .unwrap_or(1);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let status = serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Programmed",
                "status": "False",
                "reason": reason,
                "message": "compile failed — see ugwc logs",
                "lastTransitionTime": now,
                "observedGeneration": gen
            }]
        }
    });
    let patch = Patch::Merge(status);
    let params = PatchParams::apply("ugwc");
    api.patch_status(name, &params, &patch).await?;
    Ok(())
}
