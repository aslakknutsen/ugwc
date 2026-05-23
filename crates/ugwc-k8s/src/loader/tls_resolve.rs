use k8s_openapi::api::core::v1::Secret;
use tracing::warn;

use ugwc_core::snapshot::{EffectiveListenerSnap, ListenerParentSnap, ReferenceGrantSnap};

use crate::stores::SharedStores;

pub(super) fn extract_tls_pem_from_secret(sec: &Secret) -> Result<(Vec<u8>, Vec<u8>), String> {
    let data = sec
        .data
        .as_ref()
        .ok_or_else(|| "Secret has no data".to_string())?;
    let crt = data
        .get("tls.crt")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.crt".to_string())?;
    let key = data
        .get("tls.key")
        .map(|b| b.0.clone())
        .ok_or_else(|| "missing tls.key".to_string())?;
    Ok((crt, key))
}

/// Reject obviously invalid `kubernetes.io/tls` material before xDS reaches the dataplane.
///
/// [`extract_tls_pem_from_secret`] only checks keys exist. Junk or cert-only blobs in `tls.key`
/// would still be forwarded and fail in the proxy (e.g. Pingora: "no private key found in PEM file").
pub(super) fn tls_k8s_secret_pem_looks_valid(cert_pem: &[u8], key_pem: &[u8]) -> bool {
    fn has_token(pem: &[u8], tok: &[u8]) -> bool {
        if tok.is_empty() {
            return false;
        }
        pem.windows(tok.len()).any(|w| w == tok)
    }
    has_token(key_pem, b"PRIVATE KEY") && has_token(cert_pem, b"CERTIFICATE")
}

/// Resolve `certificateRefs` into PEM material on HTTPS listeners (same-namespace or via ReferenceGrant).
pub(super) fn resolve_gateway_listener_tls(
    stores: &SharedStores,
    gateway_ns: &str,
    grants: &[ReferenceGrantSnap],
    effective: &mut [EffectiveListenerSnap],
) {
    for e in effective.iter_mut() {
        if e.conflicted {
            continue;
        }
        let proto = e.protocol.trim().to_ascii_uppercase();
        if proto != "HTTPS" {
            continue;
        }
        let mode = e
            .tls
            .as_ref()
            .and_then(|t| t.mode.as_deref())
            .unwrap_or("Terminate");
        if mode.eq_ignore_ascii_case("Passthrough") {
            e.tls_resolve_error = Some("UnsupportedValue: HTTPS TLS Passthrough".into());
            continue;
        }
        let refs = e
            .tls
            .as_ref()
            .map(|t| &t.certificate_refs[..])
            .unwrap_or(&[]);
        if refs.is_empty() {
            e.tls_resolve_error =
                Some("InvalidCertificateRef: HTTPS listener requires certificateRefs".into());
            continue;
        }
        let mut bad_ref: Option<String> = None;
        for r in refs.iter() {
            let kind = r.kind.as_deref().unwrap_or("Secret");
            let group = r.group.as_deref().unwrap_or("");
            let secret_group_ok = group.is_empty() || group.eq_ignore_ascii_case("core");
            if !kind.eq_ignore_ascii_case("Secret") || !secret_group_ok {
                bad_ref = Some(format!(
                    "InvalidCertificateRef: unsupported certificate ref {group}/{kind}"
                ));
                break;
            }
        }
        if let Some(err) = bad_ref {
            e.tls_resolve_error = Some(err);
            continue;
        }
        let (from_ns, from_kind) = match &e.parent {
            ListenerParentSnap::InlineGateway => (gateway_ns.to_string(), "Gateway"),
            ListenerParentSnap::ListenerSet { namespace, .. } => {
                (namespace.clone(), "ListenerSet")
            }
        };
        let mut grant_blocked = false;
        for r in refs.iter() {
            let secret_ns = r.namespace.as_deref().unwrap_or(&from_ns);
            if !ugwc_core::reference_grant_permits_listener_secret_ref(
                grants, &from_ns, from_kind, secret_ns, &r.name,
            ) {
                grant_blocked = true;
                break;
            }
        }
        if grant_blocked {
            e.tls_resolve_error = Some("RefNotPermitted".into());
            continue;
        }
        let r = &refs[0];
        let secret_ns = r.namespace.as_deref().unwrap_or(&from_ns);
        match stores.get_secret(secret_ns, &r.name) {
            Some(sec) => match extract_tls_pem_from_secret(&sec) {
                Ok((crt, key)) => {
                    if !tls_k8s_secret_pem_looks_valid(&crt, &key) {
                        e.tls_resolve_error = Some(
                            "InvalidCertificateRef: tls.crt/tls.key must contain PEM certificate and private key"
                                .into(),
                        );
                        continue;
                    }
                    e.tls_cert_pem = crt;
                    e.tls_key_pem = zeroize::Zeroizing::new(key);
                }
                Err(msg) => {
                    e.tls_resolve_error = Some(format!("InvalidCertificateRef: {msg}"));
                }
            },
            None => {
                warn!(secret_ns = %secret_ns, secret_name = %r.name, "tls certificate secret not found in store");
                e.tls_resolve_error = Some("InvalidCertificateRef: Secret not found".into());
            }
        }
    }
}
