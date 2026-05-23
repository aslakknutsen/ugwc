//! Gateway status writers — shapes align with Gateway API v1 helpers
//! (`GatewayMustHaveLatestConditions`, listener condition checks).

mod gateway_helpers;
mod route_status;
mod listenerset_status;
mod backend_tls_status;
mod inference_status;
mod parallel;

pub use parallel::run_bounded;

pub use route_status::{
    apply_http_route_merged_parent_status, apply_grpc_route_merged_parent_status,
    apply_tls_route_merged_parent_status, apply_tcp_route_merged_parent_status,
    apply_udp_route_merged_parent_status,
    HttpRouteMergedParentStatusArgs, GrpcRouteMergedParentStatusArgs,
    TlsRouteMergedParentStatusArgs, TcpRouteMergedParentStatusArgs,
    UdpRouteMergedParentStatusArgs,
};
pub use listenerset_status::{
    apply_listenerset_reconciled_status, apply_listenerset_rejected_status,
    apply_listenerset_spec_only_status,
};
pub use backend_tls_status::apply_backend_tls_policies_for_gateway;
pub use inference_status::apply_inference_pools_for_gateway;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde::Serialize;

use crate::resources::{
    GatewayListeners, GatewayListenersTlsMode, GatewayClass, Gateway,
    GrpcRouteParentRefs, HttpRouteParentRefs, ListenerSetListeners,
    TcpRouteParentRefs, TlsRouteParentRefs, UdpRouteParentRefs,
};
use ugwc_core::snapshot::{ParentRefSnap, RouteGroupKindSnap};

use gateway_helpers::{
    fetch_gateway_status_addresses_json, gateway_compile_reports_invalid_certificate_ref,
    gateway_compile_reports_ref_not_permitted, gateway_generation_for_status,
    gateway_listener_cert_ref_not_permitted, gateway_listener_https_cert_unresolved,
    gateway_spec_https_certificate_ref_obviously_invalid, listener_route_kinds_broken_for_status,
};

trait ToParentRefSnap {
    fn to_parent_ref_snap(&self) -> ParentRefSnap;
}

macro_rules! impl_to_parent_ref_snap {
    ($($t:ty),* $(,)?) => {$(
        impl ToParentRefSnap for $t {
            fn to_parent_ref_snap(&self) -> ParentRefSnap {
                ParentRefSnap {
                    group: self.group.clone(),
                    kind: self.kind.clone(),
                    namespace: self.namespace.clone(),
                    name: self.name.clone(),
                    section_name: self.section_name.clone(),
                    port: self.port.and_then(|p| u16::try_from(p).ok()),
                }
            }
        }
    )*};
}

impl_to_parent_ref_snap!(
    HttpRouteParentRefs,
    GrpcRouteParentRefs,
    TlsRouteParentRefs,
    TcpRouteParentRefs,
    UdpRouteParentRefs,
);

/// Gateway listeners and ListenerSet entries both expose protocol + optional allowed route kinds.
trait ListenerAllowedRouteKinds {
    fn protocol_for_route_kinds(&self) -> &str;
    fn route_group_kinds(&self) -> Vec<RouteGroupKindSnap>;
}

impl ListenerAllowedRouteKinds for GatewayListeners {
    fn protocol_for_route_kinds(&self) -> &str {
        self.protocol.as_str()
    }

    fn route_group_kinds(&self) -> Vec<RouteGroupKindSnap> {
        self.allowed_routes
            .as_ref()
            .and_then(|a| a.kinds.as_ref())
            .map(|kinds| {
                kinds
                    .iter()
                    .map(|k| RouteGroupKindSnap {
                        group: k.group.clone(),
                        kind: k.kind.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl ListenerAllowedRouteKinds for ListenerSetListeners {
    fn protocol_for_route_kinds(&self) -> &str {
        self.protocol.as_str()
    }

    fn route_group_kinds(&self) -> Vec<RouteGroupKindSnap> {
        self.allowed_routes
            .as_ref()
            .and_then(|a| a.kinds.as_ref())
            .map(|kinds| {
                kinds
                    .iter()
                    .map(|k| RouteGroupKindSnap {
                        group: k.group.clone(),
                        kind: k.kind.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn status_rows_to_json<T: Serialize>(rows: &[T]) -> Vec<serde_json::Value> {
    rows.iter().filter_map(|r| serde_json::to_value(r).ok()).collect()
}

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

    let (gw_acc_status, gw_acc_reason, gw_acc_msg) = ("True", "Accepted", "ugwc reconciled gateway");

    let listener_status: Vec<serde_json::Value> = listener_names
        .iter()
        .map(|lname| {
            let conflicted = gateway_listener_conflicted(lname);
            let spec_row = gateway.spec.listeners.iter().find(|l| l.name == *lname);
            let port_u16 = spec_row
                .and_then(|l| u16::try_from(l.port).ok())
                .unwrap_or(0);
            let lk = ugwc_core::ListenerAttachmentIr {
                parent_kind: "Gateway".into(),
                parent_namespace: gw_ns.to_string(),
                parent_name: name.to_string(),
                section_name: (*lname).clone(),
                port: port_u16,
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
            let tls_mode_unsupported = spec_row
                .map(|l| {
                    let proto = l.protocol.trim().to_ascii_uppercase();
                    if proto != "TLS" {
                        return false;
                    }
                    matches!(
                        l.tls.as_ref().and_then(|t| t.mode.as_ref()),
                        Some(GatewayListenersTlsMode::Terminate)
                    )
                })
                .unwrap_or(false);
            let supported = if tls_mode_unsupported {
                Vec::new()
            } else {
                supported_kinds_json(eff)
            };

            let (prog_status, prog_reason, prog_msg) = if programmed_ok && !conflicted {
                (
                    "True",
                    "Programmed",
                    "ugwc compile outcome".to_string(),
                )
            } else if conflicted {
                let cr = eff
                    .and_then(|e| e.conflict_reason.as_deref())
                    .unwrap_or("HostnameConflict");
                (
                    "False",
                    cr,
                    "listener not distinct after merge with ListenerSets".to_string(),
                )
            } else {
                ("False", "Invalid", compile_err_detail.clone())
            };
            let (acc_status, acc_reason, acc_msg) = if tls_mode_unsupported {
                (
                    "False",
                    "UnsupportedValue",
                    "TLS listener mode Terminate is not supported",
                )
            } else if conflicted {
                let cr = eff
                    .and_then(|e| e.conflict_reason.as_deref())
                    .unwrap_or("HostnameConflict");
                (
                    "False",
                    cr,
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
            let ref_not_permitted = eff
                .and_then(|e| e.tls_resolve_error.as_deref())
                .map(|e| e.contains("RefNotPermitted"))
                .unwrap_or(false)
                || gateway_listener_cert_ref_not_permitted(spec_row, gw_ns, cluster)
                || gateway_compile_reports_ref_not_permitted(&outcome.report, gw_ns, name);
            if ref_not_permitted {
                cert_broken = true;
            }
            let route_kinds_broken = listener_route_kinds_broken_for_status(eff, spec_row);
            let (res_status, res_reason, res_msg) = if ref_not_permitted {
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
                "name": lname,
                "supportedKinds": supported,
                "attachedRoutes": attached,
                "conditions": conditions
            })
        })
        .collect();

    let gw_prog_msg = if programmed_ok {
        "ugwc compile outcome".to_string()
    } else {
        format!(
            "gateway reconciled; {} (details on listener conditions)",
            compile_err_detail
        )
    };
    let (gw_prog_status, gw_prog_reason, gw_prog_msg) =
        ("True", "Programmed", gw_prog_msg);

    let attached_ls = cluster
        .map(|c| {
            c.listener_sets
                .iter()
                .filter(|ls| {
                    !ls.listeners.iter().all(|l| {
                        let eff = c.effective_listeners.iter().find(|e| {
                            matches!(
                                &e.parent,
                                ugwc_core::snapshot::ListenerParentSnap::ListenerSet {
                                    namespace,
                                    name,
                                } if namespace == &ls.namespace && name == &ls.name
                            ) && e.section_name == l.name
                        });
                        match eff {
                            Some(e) if e.conflicted => true,
                            Some(e) => {
                                let proto = e.protocol.trim().to_ascii_uppercase();
                                proto == "HTTPS"
                                    && (e.tls_resolve_error.is_some()
                                        || e.tls_cert_pem.is_empty()
                                        || e.tls_key_pem.is_empty())
                            }
                            None => true,
                        }
                    })
                })
                .count()
        })
        .unwrap_or(0) as i32;

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
