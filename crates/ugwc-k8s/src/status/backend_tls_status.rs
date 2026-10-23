use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use crate::loader::{backend_tls, reference_grant_snaps_from_stores};
use crate::resources::{
    backend_tls_policy_spec_references_configmap, backend_tls_policy_spec_references_secret,
    BackendTLSPolicy,
};
use crate::stores::SharedStores;
use crate::watch_helpers_store::gateway_targets_for_backend_tls_policy_from_stores;

use super::{run_bounded, status_rows_to_json};

async fn apply_one_backend_tls_policy(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    controller_name: &str,
    outcome: ugwc_core::snapshot::BackendTlsPolicyOutcomeSnap,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
) {
    let api: Api<BackendTLSPolicy> = Api::namespaced(client.clone(), &outcome.namespace);
    let pol = match api.get(&outcome.name).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                error = %e,
                policy_ns = %outcome.namespace,
                policy_name = %outcome.name,
                "get BackendTLSPolicy for status"
            );
            return;
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
            return;
        }
        Some((key, serialized))
    } else {
        None
    };

    if let Err(e) = api
        .patch_status(
            &outcome.name,
            &PatchParams::apply("ugwc"),
            &Patch::Merge(status),
        )
        .await
    {
        tracing::warn!(
            error = %e,
            policy_ns = %outcome.namespace,
            policy_name = %outcome.name,
            "backend TLS policy status patch"
        );
        return;
    }

    if let (Some(cache), Some((key, serialized))) = (status_cache.as_ref(), pending_cache) {
        cache.lock().unwrap().insert(key, serialized);
    }
}

/// Fast-path BackendTLSPolicy status when a CA ConfigMap or Secret changes.
pub async fn apply_backend_tls_status_for_ca_ref_change(
    client: &Client,
    stores: &SharedStores,
    controller_name: &str,
    resource_ns: &str,
    resource_name: &str,
    is_configmap: bool,
) {
    let grants = reference_grant_snaps_from_stores(stores);
    for pol in stores.list_backend_tls_policies() {
        let pol_ns = pol.metadata.namespace.as_deref().unwrap_or("");
        let references = if is_configmap {
            backend_tls_policy_spec_references_configmap(
                &pol.spec,
                pol_ns,
                resource_ns,
                resource_name,
            )
        } else {
            backend_tls_policy_spec_references_secret(&pol.spec, pol_ns, resource_ns, resource_name)
        };
        if !references {
            continue;
        }
        let outcome = backend_tls::policy_outcome_from_policy_stores(stores, &pol, &grants, false);
        let gateways = gateway_targets_for_backend_tls_policy_from_stores(stores, &pol);
        for (gw_ns, gw_name) in gateways {
            apply_one_backend_tls_policy(
                client,
                &gw_ns,
                &gw_name,
                controller_name,
                outcome.clone(),
                None,
            )
            .await;
        }
    }
}

/// Merge `BackendTLSPolicy` `status.ancestors` for the reconciled Gateway (policy attachment status).
pub async fn apply_backend_tls_policies_for_gateway(
    client: &Client,
    gateway_ns: &str,
    gateway_name: &str,
    controller_name: &str,
    snapshot: &ugwc_core::ClusterSnapshot,
    stores: &SharedStores,
    status_cache: Option<Arc<Mutex<HashMap<String, String>>>>,
    status_patch_concurrency: usize,
) -> Result<(), kube::Error> {
    let grants = reference_grant_snaps_from_stores(stores);
    let futs: Vec<_> = snapshot
        .backend_tls_policy_outcomes
        .iter()
        .cloned()
        .map(|outcome| {
            let refreshed = backend_tls::refresh_outcome_from_stores(stores, &outcome, &grants)
                .unwrap_or(outcome);
            apply_one_backend_tls_policy(
                client,
                gateway_ns,
                gateway_name,
                controller_name,
                refreshed,
                status_cache.clone(),
            )
        })
        .collect();

    run_bounded(status_patch_concurrency, futs).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use ugwc_core::snapshot::BackendTlsPolicyOutcomeSnap;

    use super::*;

    #[test]
    fn refresh_outcome_prefers_invalid_ca_over_stale_accepted() {
        use k8s_openapi::api::core::v1::ConfigMap;

        let mut cm_writer = kube::runtime::reflector::store::Writer::<ConfigMap>::default();
        let cm_store = cm_writer.as_reader();
        cm_writer.apply_watcher_event(&kube::runtime::watcher::Event::Apply(ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("ca".into()),
                namespace: Some("ns".into()),
                ..Default::default()
            },
            data: Some(std::collections::BTreeMap::from([(
                "ca.crt".into(),
                String::new(),
            )])),
            ..Default::default()
        }));

        let mut btp_writer = kube::runtime::reflector::store::Writer::<
            crate::resources::BackendTLSPolicy,
        >::default();
        let btp_store = btp_writer.as_reader();
        btp_writer.apply_watcher_event(&kube::runtime::watcher::Event::Apply(
            crate::resources::BackendTLSPolicy {
                metadata: kube::api::ObjectMeta {
                    name: Some("pol".into()),
                    namespace: Some("ns".into()),
                    ..Default::default()
                },
                spec: crate::resources::BackendTlsPolicySpec {
                    target_refs: vec![crate::resources::BackendTlsPolicyTargetRefs {
                        group: String::new(),
                        kind: "Service".into(),
                        name: "svc".into(),
                        section_name: None,
                    }],
                    validation: crate::resources::BackendTlsPolicyValidation {
                        hostname: "host".into(),
                        ca_certificate_refs: Some(vec![
                            crate::resources::BackendTlsPolicyValidationCaCertificateRefs {
                                group: String::new(),
                                kind: "ConfigMap".into(),
                                name: "ca".into(),
                            },
                        ]),
                        ..Default::default()
                    },
                    options: None,
                },
                ..Default::default()
            },
        ));

        let ep = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::discovery::v1::EndpointSlice,
        >::default();
        let svc =
            kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::Service>::default(
            );
        let sec =
            kube::runtime::reflector::store::Writer::<k8s_openapi::api::core::v1::Secret>::default(
            );
        let ns = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::core::v1::Namespace,
        >::default();
        let stores = SharedStores::new(
            ep.as_reader(),
            svc.as_reader(),
            sec.as_reader(),
            cm_store,
            ns.as_reader(),
        );
        stores.set_backend_tls_policy_store(btp_store);

        let stale = BackendTlsPolicyOutcomeSnap {
            namespace: "ns".into(),
            name: "pol".into(),
            conflicted: false,
            resolved_refs_ok: true,
            accepted: true,
            message: None,
            accepted_condition_reason: Some("Accepted".into()),
            resolved_refs_condition_reason: Some("ResolvedRefs".into()),
        };
        let refreshed = backend_tls::refresh_outcome_from_stores(&stores, &stale, &[]).unwrap();
        assert!(!refreshed.accepted);
        assert!(!refreshed.resolved_refs_ok);
        assert_eq!(
            refreshed.accepted_condition_reason.as_deref(),
            Some("NoValidCACertificate")
        );
    }
}
