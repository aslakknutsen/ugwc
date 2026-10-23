//! Resolve [`BackendTLSPolicy`](crate::resources::BackendTLSPolicy) into snapshot TLS material.

use std::collections::{BTreeMap, HashSet};

use kube::ResourceExt;
use ugwc_core::http_route_attachment::reference_grant_permits_backend_tls_policy_resource_ref;
use ugwc_core::ir::BackendTlsIr;
use ugwc_core::snapshot::{
    BackendTlsEffectiveSnap, BackendTlsPolicyOutcomeSnap, InferencePoolSnap, ReferenceGrantSnap,
    ServiceSnap,
};
use zeroize::Zeroizing;

use crate::resources::{
    BackendTlsPolicyValidation, BackendTlsPolicyValidationCaCertificateRefs,
    BackendTlsPolicyValidationSubjectAltNamesType,
};
use crate::stores::SharedStores;

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

fn fetch_configmap_ca_pem(
    stores: &SharedStores,
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
    let cm = stores
        .get_config_map(res_ns, &ca.name)
        .ok_or("InvalidCACertificateRef")?;
    let data = cm.data.as_ref().ok_or("InvalidCACertificateRef")?;
    let pem = data.get("ca.crt").ok_or("InvalidCACertificateRef")?;
    let pem_bytes = pem.as_bytes().to_vec();
    if !ca_pem_looks_valid(&pem_bytes) {
        return Err("InvalidCACertificateRef");
    }
    Ok(pem_bytes)
}

fn fetch_secret_ca_pem(
    stores: &SharedStores,
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
    let sec = stores
        .get_secret(res_ns, &ca.name)
        .ok_or("InvalidCACertificateRef")?;
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

fn backend_tls_ir_from_validation(
    stores: &SharedStores,
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
            fetch_configmap_ca_pem(stores, policy_ns, ca, grants)
        } else if kind.eq_ignore_ascii_case("Secret") {
            fetch_secret_ca_pem(stores, policy_ns, ca, grants)
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
pub fn load_backend_tls_for_gateway(
    stores: &SharedStores,
    needed_services: &[(String, String)],
    services: &[ServiceSnap],
    needed_pools: &[(String, String)],
    pools: &[InferencePoolSnap],
    grants: &[ReferenceGrantSnap],
) -> (
    BTreeMap<String, BackendTlsEffectiveSnap>,
    Vec<BackendTlsPolicyOutcomeSnap>,
) {
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

    let all_policies = stores.list_backend_tls_policies();
    let ns_set: HashSet<&str> = namespaces.iter().map(|s| s.as_str()).collect();

    for pol_arc in &all_policies {
        let policy_ns = pol_arc.namespace().unwrap_or_default();
        if !ns_set.contains(policy_ns.as_str()) {
            continue;
        }
        let policy_name = pol_arc.name_any();
        let created_rfc3339 = pol_arc
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|t| t.0.to_string())
            .unwrap_or_default();

        let mut service_targets: Vec<(String, Option<String>)> = Vec::new();
        let mut pool_targets: Vec<(String, Option<String>)> = Vec::new();
        for t in &pol_arc.spec.target_refs {
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
            backend_tls_ir_from_validation(stores, &policy_ns, &pol_arc.spec.validation, grants);

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
        outcomes.push(build_policy_outcome_snap(
            lp.policy_ns,
            lp.policy_name,
            lp.tls_ir,
            lp.tls_resolve_reason,
            is_conflicted,
        ));
    }

    outcomes.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    (effective, outcomes)
}

fn build_policy_outcome_snap(
    policy_ns: String,
    policy_name: String,
    tls_ir: BackendTlsIr,
    tls_resolve_reason: Option<String>,
    conflicted: bool,
) -> BackendTlsPolicyOutcomeSnap {
    let resolved_ok = !tls_ir.invalid;
    let rr_reason = if resolved_ok {
        "ResolvedRefs".to_string()
    } else {
        tls_resolve_reason
            .clone()
            .unwrap_or_else(|| "InvalidCACertificateRef".into())
    };
    let acc_reason = if conflicted {
        "Conflicted".to_string()
    } else if !resolved_ok {
        "NoValidCACertificate".to_string()
    } else {
        "Accepted".to_string()
    };
    BackendTlsPolicyOutcomeSnap {
        namespace: policy_ns,
        name: policy_name,
        conflicted,
        resolved_refs_ok: resolved_ok,
        accepted: resolved_ok && !conflicted,
        message: if conflicted {
            Some("Conflicted".into())
        } else if !resolved_ok {
            Some(rr_reason.clone())
        } else {
            None
        },
        accepted_condition_reason: Some(acc_reason),
        resolved_refs_condition_reason: Some(rr_reason),
    }
}

/// Re-evaluate BackendTLSPolicy CA validation from the current store (status fast-path / refresh).
#[must_use]
pub(crate) fn policy_outcome_from_policy_stores(
    stores: &SharedStores,
    pol: &crate::resources::BackendTLSPolicy,
    grants: &[ReferenceGrantSnap],
    conflicted: bool,
) -> BackendTlsPolicyOutcomeSnap {
    let policy_ns = pol
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default")
        .to_string();
    let policy_name = pol.metadata.name.as_deref().unwrap_or("").to_string();
    let (tls_ir, tls_resolve_reason) =
        backend_tls_ir_from_validation(stores, &policy_ns, &pol.spec.validation, grants);
    build_policy_outcome_snap(
        policy_ns,
        policy_name,
        tls_ir,
        tls_resolve_reason,
        conflicted,
    )
}

/// Refresh a snapshot outcome using live store CA material (preserves conflict flag).
#[must_use]
pub(crate) fn refresh_outcome_from_stores(
    stores: &SharedStores,
    outcome: &BackendTlsPolicyOutcomeSnap,
    grants: &[ReferenceGrantSnap],
) -> Option<BackendTlsPolicyOutcomeSnap> {
    let pol = stores.get_backend_tls_policy(&outcome.namespace, &outcome.name)?;
    Some(policy_outcome_from_policy_stores(
        stores,
        &pol,
        grants,
        outcome.conflicted,
    ))
}
