//! Resolve [`BackendTLSPolicy`](crate::resources::BackendTLSPolicy) into snapshot TLS material.

use std::collections::{BTreeMap, HashSet};

use kube::api::ListParams;
use kube::{Api, Client};
use tracing::warn;
use zeroize::Zeroizing;
use ugwc_core::http_route_attachment::reference_grant_permits_backend_tls_policy_resource_ref;
use ugwc_core::ir::BackendTlsIr;
use ugwc_core::snapshot::{
    BackendTlsEffectiveSnap, BackendTlsPolicyOutcomeSnap, InferencePoolSnap, ReferenceGrantSnap,
    ServiceSnap,
};

use crate::resources::{
    BackendTLSPolicy, BackendTlsPolicyValidation, BackendTlsPolicyValidationCaCertificateRefs,
    BackendTlsPolicyValidationSubjectAltNamesType,
};

use super::LoaderError;

fn backend_tls_target_is_service(group: &str, kind: &str) -> bool {
    let k = if kind.is_empty() { "Service" } else { kind };
    k.eq_ignore_ascii_case("Service") && (group.is_empty() || group.eq_ignore_ascii_case(""))
}

fn backend_tls_target_is_inference_pool(group: &str, kind: &str) -> bool {
    let k = kind;
    k.eq_ignore_ascii_case("InferencePool")
        && (group.is_empty() || group.eq_ignore_ascii_case("inference.networking.k8s.io"))
}

fn inference_pool_backend_tls_key(pool_ns: &str, pool_name: &str, port: u16) -> String {
    format!("inferencepool/{pool_ns}/{pool_name}:{port}")
}

pub(crate) fn ca_pem_looks_valid(pem: &[u8]) -> bool {
    pem.windows(b"CERTIFICATE".len())
        .any(|w| w == b"CERTIFICATE")
}

pub(crate) fn backend_tls_section_applies(section: Option<&str>, service_port_name: &str) -> bool {
    section.map_or(true, |sn| sn == service_port_name)
}

fn backend_tls_section_applies_pool(section: Option<&str>, port_number: u16) -> bool {
    section.map_or(true, |sn| sn == port_number.to_string())
}

#[derive(Clone)]
struct LoadedBackendTlsPolicy {
    policy_ns: String,
    policy_name: String,
    created_rfc3339: String,
    tls_ir: BackendTlsIr,
    tls_resolve_reason: Option<String>,
    service_targets: Vec<(String, Option<String>)>,
    pool_targets: Vec<(String, Option<String>)>,
}

fn tls_invalid(
    hostname: String,
    sans: Vec<String>,
    reason: &'static str,
) -> (BackendTlsIr, Option<String>) {
    (
        BackendTlsIr {
            ca_cert_pem: vec![],
            hostname,
            sans,
            trust_system_ca: false,
            client_cert_pem: vec![],
            client_key_pem: Zeroizing::new(vec![]),
            invalid: true,
        },
        Some(reason.into()),
    )
}

async fn fetch_configmap_ca_pem(
    client: &Client,
    policy_ns: &str,
    ca: &BackendTlsPolicyValidationCaCertificateRefs,
    grants: &[ReferenceGrantSnap],
) -> Result<Vec<u8>, &'static str> {
    let kind = ca.kind.as_str();
    let kind = if kind.is_empty() { "ConfigMap" } else { kind };
    let group = ca.group.as_str();
    if !kind.eq_ignore_ascii_case("ConfigMap")
        || !(group.is_empty() || group.eq_ignore_ascii_case(""))
    {
        return Err("InvalidKind");
    }
    let res_ns = policy_ns;
    if !reference_grant_permits_backend_tls_policy_resource_ref(
        grants,
        policy_ns,
        res_ns,
        "ConfigMap",
        &ca.name,
    ) {
        return Err("RefNotPermitted");
    }
    let cm_api: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(client.clone(), res_ns);
    let cm = cm_api
        .get(&ca.name)
        .await
        .map_err(|_| "InvalidCACertificateRef")?;
    let data = cm.data.as_ref().ok_or("InvalidCACertificateRef")?;
    let pem = data.get("ca.crt").ok_or("InvalidCACertificateRef")?;
    let pem_bytes = pem.as_bytes().to_vec();
    if !ca_pem_looks_valid(&pem_bytes) {
        return Err("InvalidCACertificateRef");
    }
    Ok(pem_bytes)
}

async fn fetch_secret_ca_pem(
    client: &Client,
    policy_ns: &str,
    ca: &BackendTlsPolicyValidationCaCertificateRefs,
    grants: &[ReferenceGrantSnap],
) -> Result<Vec<u8>, &'static str> {
    let kind = ca.kind.as_str();
    let kind = if kind.is_empty() { "Secret" } else { kind };
    let group = ca.group.as_str();
    if !kind.eq_ignore_ascii_case("Secret") || !(group.is_empty() || group.eq_ignore_ascii_case(""))
    {
        return Err("InvalidKind");
    }
    let res_ns = policy_ns;
    if !reference_grant_permits_backend_tls_policy_resource_ref(
        grants, policy_ns, res_ns, "Secret", &ca.name,
    ) {
        return Err("RefNotPermitted");
    }
    let sec_api: Api<k8s_openapi::api::core::v1::Secret> = Api::namespaced(client.clone(), res_ns);
    let sec = sec_api
        .get(&ca.name)
        .await
        .map_err(|_| "InvalidCACertificateRef")?;
    let data = sec.data.as_ref().ok_or("InvalidCACertificateRef")?;
    if let Some(b) = data.get("ca.crt") {
        if ca_pem_looks_valid(&b.0) {
            return Ok(b.0.clone());
        }
        return Err("InvalidCACertificateRef");
    }
    if let Some(b) = data.get("tls.crt") {
        if ca_pem_looks_valid(&b.0) {
            return Ok(b.0.clone());
        }
        return Err("InvalidCACertificateRef");
    }
    Err("InvalidCACertificateRef")
}

async fn backend_tls_ir_from_validation(
    client: &Client,
    policy_ns: &str,
    v: &BackendTlsPolicyValidation,
    grants: &[ReferenceGrantSnap],
) -> (BackendTlsIr, Option<String>) {
    let hostname = v.hostname.clone();
    let mut sans = Vec::new();
    for san in v.subject_alt_names.as_deref().unwrap_or_default() {
        match san.r#type {
            BackendTlsPolicyValidationSubjectAltNamesType::Hostname => {
                if let Some(ref h) = san.hostname {
                    sans.push(h.clone());
                }
            }
            BackendTlsPolicyValidationSubjectAltNamesType::Uri => {
                if let Some(ref u) = san.uri {
                    sans.push(u.clone());
                }
            }
        }
    }

    let client_cert_pem = Vec::new();
    let client_key_pem = Zeroizing::new(Vec::new());

    let ca_refs = v.ca_certificate_refs.as_deref().unwrap_or_default();
    let has_ca = !ca_refs.is_empty();
    let well = v
        .well_known_ca_certificates
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if has_ca && well.is_some() {
        return tls_invalid(hostname, sans, "Invalid");
    }

    if let Some(w) = well {
        if w == "System" {
            return (
                BackendTlsIr {
                    ca_cert_pem: vec![],
                    hostname,
                    sans,
                    trust_system_ca: true,
                    client_cert_pem,
                    client_key_pem,
                    invalid: false,
                },
                None,
            );
        }
        return tls_invalid(hostname, sans, "Invalid");
    }

    if !has_ca {
        return tls_invalid(hostname, sans, "NoValidCACertificate");
    }

    let mut bundle = Vec::new();
    for ca in ca_refs {
        let kind = ca.kind.as_str();
        let kind = if kind.is_empty() { "ConfigMap" } else { kind };
        let res = if kind.eq_ignore_ascii_case("ConfigMap") {
            fetch_configmap_ca_pem(client, policy_ns, ca, grants).await
        } else if kind.eq_ignore_ascii_case("Secret") {
            fetch_secret_ca_pem(client, policy_ns, ca, grants).await
        } else {
            Err("InvalidKind")
        };
        match res {
            Ok(pem) => {
                if !bundle.is_empty() {
                    bundle.push(b'\n');
                }
                bundle.extend(pem);
            }
            Err(reason) => return tls_invalid(hostname, sans, reason),
        }
    }

    (
        BackendTlsIr {
            ca_cert_pem: bundle,
            hostname,
            sans,
            trust_system_ca: false,
            client_cert_pem,
            client_key_pem,
            invalid: false,
        },
        None,
    )
}

/// Load winning TLS policies for Services and InferencePools referenced by this Gateway snapshot.
pub async fn load_backend_tls_for_gateway(
    client: &Client,
    needed_services: &[(String, String)],
    services: &[ServiceSnap],
    needed_pools: &[(String, String)],
    pools: &[InferencePoolSnap],
    grants: &[ReferenceGrantSnap],
) -> Result<
    (
        BTreeMap<String, BackendTlsEffectiveSnap>,
        Vec<BackendTlsPolicyOutcomeSnap>,
    ),
    LoaderError,
> {
    let mut effective: BTreeMap<String, BackendTlsEffectiveSnap> = BTreeMap::new();

    let needed_set: HashSet<(String, String)> = needed_services.iter().cloned().collect();
    let needed_pool_set: HashSet<(String, String)> = needed_pools.iter().cloned().collect();

    let mut namespaces: Vec<String> = needed_services
        .iter()
        .map(|(ns, _)| ns.clone())
        .chain(needed_pools.iter().map(|(ns, _)| ns.clone()))
        .collect();
    namespaces.sort();
    namespaces.dedup();

    let mut loaded: Vec<LoadedBackendTlsPolicy> = Vec::new();

    for ns in &namespaces {
        let api: Api<BackendTLSPolicy> = Api::namespaced(client.clone(), ns);
        let list = match api.list(&ListParams::default()).await {
            Ok(l) => l.items,
            Err(e) => {
                warn!(
                    error = %e,
                    namespace = %ns,
                    "BackendTLSPolicy list failed — clusters without CRD skip backend TLS policies"
                );
                continue;
            }
        };

        for pol in list {
            let policy_ns = pol.metadata.namespace.clone().unwrap_or_default();
            let policy_name = pol.metadata.name.clone().unwrap_or_default();
            let created_rfc3339 = pol
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| t.0.to_string())
                .unwrap_or_default();

            let mut service_targets: Vec<(String, Option<String>)> = Vec::new();
            let mut pool_targets: Vec<(String, Option<String>)> = Vec::new();
            for t in &pol.spec.target_refs {
                let section = t.section_name.clone().filter(|s| !s.trim().is_empty());
                if backend_tls_target_is_service(&t.group, &t.kind) {
                    if needed_set.contains(&(policy_ns.clone(), t.name.clone())) {
                        service_targets.push((t.name.clone(), section));
                    }
                } else if backend_tls_target_is_inference_pool(&t.group, &t.kind)
                    && needed_pool_set.contains(&(policy_ns.clone(), t.name.clone()))
                {
                    pool_targets.push((t.name.clone(), section));
                }
            }

            if service_targets.is_empty() && pool_targets.is_empty() {
                continue;
            }

            let (tls_ir, tls_resolve_reason) =
                backend_tls_ir_from_validation(client, &policy_ns, &pol.spec.validation, grants)
                    .await;

            loaded.push(LoadedBackendTlsPolicy {
                policy_ns,
                policy_name,
                created_rfc3339,
                tls_ir,
                tls_resolve_reason,
                service_targets,
                pool_targets,
            });
        }
    }

    #[derive(Clone)]
    struct Candidate {
        created_rfc3339: String,
        policy_ns: String,
        policy_name: String,
        tls_ir: BackendTlsIr,
        has_section: bool,
    }

    let mut conflicted: HashSet<(String, String)> = HashSet::new();

    for svc in services {
        for sp in &svc.ports {
            let backend_k = format!("{}/{}:{}", svc.namespace, svc.name, sp.port);

            let mut candidates: Vec<Candidate> = Vec::new();

            for lp in &loaded {
                for (svc_name, section) in &lp.service_targets {
                    if svc_name != &svc.name || lp.policy_ns != svc.namespace {
                        continue;
                    }
                    let applies = backend_tls_section_applies(section.as_deref(), sp.name.as_str());
                    if applies {
                        candidates.push(Candidate {
                            created_rfc3339: lp.created_rfc3339.clone(),
                            policy_ns: lp.policy_ns.clone(),
                            policy_name: lp.policy_name.clone(),
                            tls_ir: lp.tls_ir.clone(),
                            has_section: section.is_some(),
                        });
                    }
                }
            }

            let mut seen_pol = HashSet::<(String, String)>::new();
            let mut uniq: Vec<Candidate> = Vec::new();
            for c in candidates {
                let pk = (c.policy_ns.clone(), c.policy_name.clone());
                if seen_pol.insert(pk) {
                    uniq.push(c);
                }
            }
            let candidates = uniq;

            if candidates.is_empty() {
                continue;
            }

            let has_sectioned = candidates.iter().any(|c| c.has_section);
            let (mut effective_candidates, broad_candidates): (Vec<_>, Vec<_>) = if has_sectioned {
                candidates.into_iter().partition(|c| c.has_section)
            } else {
                (candidates, Vec::new())
            };

            effective_candidates.sort_by(|a, b| {
                a.created_rfc3339.cmp(&b.created_rfc3339).then_with(|| {
                    (&a.policy_ns, &a.policy_name).cmp(&(&b.policy_ns, &b.policy_name))
                })
            });

            let winner = effective_candidates[0].clone();
            for c in effective_candidates.iter().skip(1) {
                conflicted.insert((c.policy_ns.clone(), c.policy_name.clone()));
            }
            // Section-agnostic policies that lose to a section-specific one
            // are NOT conflicted — they still apply to other ports.
            let _ = broad_candidates;

            effective.insert(backend_k, BackendTlsEffectiveSnap { tls: winner.tls_ir });
        }
    }

    for pool in pools {
        for tp in &pool.target_ports {
            let backend_k = inference_pool_backend_tls_key(&pool.namespace, &pool.name, tp.number);
            let mut candidates: Vec<Candidate> = Vec::new();
            for lp in &loaded {
                for (pool_name, section) in &lp.pool_targets {
                    if pool_name != &pool.name || lp.policy_ns != pool.namespace {
                        continue;
                    }
                    if backend_tls_section_applies_pool(section.as_deref(), tp.number) {
                        candidates.push(Candidate {
                            created_rfc3339: lp.created_rfc3339.clone(),
                            policy_ns: lp.policy_ns.clone(),
                            policy_name: lp.policy_name.clone(),
                            tls_ir: lp.tls_ir.clone(),
                            has_section: section.is_some(),
                        });
                    }
                }
            }
            let mut seen_pol = HashSet::<(String, String)>::new();
            let mut uniq: Vec<Candidate> = Vec::new();
            for c in candidates {
                let pk = (c.policy_ns.clone(), c.policy_name.clone());
                if seen_pol.insert(pk) {
                    uniq.push(c);
                }
            }
            let candidates = uniq;
            if candidates.is_empty() {
                continue;
            }

            let has_sectioned = candidates.iter().any(|c| c.has_section);
            let (mut effective_candidates, _broad): (Vec<_>, Vec<_>) = if has_sectioned {
                candidates.into_iter().partition(|c| c.has_section)
            } else {
                (candidates, Vec::new())
            };

            effective_candidates.sort_by(|a, b| {
                a.created_rfc3339.cmp(&b.created_rfc3339).then_with(|| {
                    (&a.policy_ns, &a.policy_name).cmp(&(&b.policy_ns, &b.policy_name))
                })
            });

            let winner = effective_candidates[0].clone();
            for c in effective_candidates.iter().skip(1) {
                conflicted.insert((c.policy_ns.clone(), c.policy_name.clone()));
            }
            effective.insert(backend_k, BackendTlsEffectiveSnap { tls: winner.tls_ir });
        }
    }

    let mut outcomes: Vec<BackendTlsPolicyOutcomeSnap> = Vec::new();
    for lp in loaded {
        let k = (lp.policy_ns.clone(), lp.policy_name.clone());
        let is_conflicted = conflicted.contains(&k);
        let resolved_ok = !lp.tls_ir.invalid;
        let rr_reason = if resolved_ok {
            "ResolvedRefs".to_string()
        } else {
            lp.tls_resolve_reason
                .clone()
                .unwrap_or_else(|| "InvalidCACertificateRef".into())
        };
        let acc_reason = if is_conflicted {
            "Conflicted".to_string()
        } else if !resolved_ok {
            "NoValidCACertificate".to_string()
        } else {
            "Accepted".to_string()
        };
        outcomes.push(BackendTlsPolicyOutcomeSnap {
            namespace: lp.policy_ns,
            name: lp.policy_name,
            conflicted: is_conflicted,
            resolved_refs_ok: resolved_ok,
            accepted: resolved_ok && !is_conflicted,
            message: if is_conflicted {
                Some("Conflicted".into())
            } else if !resolved_ok {
                Some(rr_reason.clone())
            } else {
                None
            },
            accepted_condition_reason: Some(acc_reason),
            resolved_refs_condition_reason: Some(rr_reason),
        });
    }

    outcomes.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    Ok((effective, outcomes))
}
