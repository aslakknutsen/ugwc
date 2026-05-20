//! HTTPRoute → IR normalization for Phase 4 happy-path fixtures (pure, deterministic).

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::inference::InferenceIr;
use crate::ir::{
    BackendEndpointIr, BackendIr, IrRevision, IrScopeId, IrSnapshot, ListenerIr, RouteMatchIr,
    RouteRuleIr, SnapshotMeta, TlsMode, WeightedBackendIr, IR_VERSION,
};
use crate::policy::PolicyOverlayIr;

/// Test / offline harness input mimicking watched objects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpCompileFixture {
    pub gateway_namespace: String,
    pub gateway_name: String,
    pub listeners: Vec<ListenerIr>,
    pub routes: Vec<RouteRuleIr>,
    pub endpoints: BTreeMap<String, Vec<BackendEndpointIr>>,
}

impl HttpCompileFixture {
    #[must_use]
    pub fn minimal_http() -> Self {
        Self {
            gateway_namespace: "default".into(),
            gateway_name: "gw".into(),
            listeners: vec![ListenerIr {
                section_name: "http".into(),
                bind_port: 80,
                tls: TlsMode::Plaintext,
                gateway_protocol: "HTTP".into(),
                parent_kind: "Gateway".into(),
                parent_namespace: "default".into(),
                parent_name: "gw".into(),
                conflicted: false,
                hostname: None,
                tls_cert_pem: vec![],
                tls_key_pem: Zeroizing::new(vec![]),
                allowed_route_group_kinds: vec![],
            }],
            routes: vec![RouteRuleIr {
                name: "hr".into(),
                route_family: crate::ir::RouteFamily::Http,
                hosts: vec!["example.com".into()],
                matches: vec![RouteMatchIr {
                    path_prefix: Some("/".into()),
                    path_exact: None,
                    headers_exact: BTreeMap::new(),
                    method: None,
                    query_params_exact: BTreeMap::new(),
                    grpc_service: None,
                    grpc_method: None,
                }],
                backends: vec![WeightedBackendIr {
                    backend: BackendIr {
                        service_namespace: "default".into(),
                        service_name: "svc".into(),
                        port_name: Some("http".into()),
                        port_number: Some(80),
                        backend_tls: None,
                        inference: None,
                        dial_port: None,
                    },
                    weight: 1,
                }],
                request_redirect: None,
                request_header_modifier: None,
                response_header_modifier: None,
                url_rewrite: None,
                request_timeout_ms: None,
                backend_timeout_ms: None,
                cors: None,
                listener: Some(crate::ir::ListenerAttachmentIr {
                    parent_kind: "Gateway".into(),
                    parent_namespace: "default".into(),
                    parent_name: "gw".into(),
                    section_name: "http".into(),
                    port: 80,
                }),
            }],
            endpoints: BTreeMap::from([(
                "default/svc:80".into(),
                vec![BackendEndpointIr {
                    address: "10.0.0.1".into(),
                    port: 8080,
                }],
            )]),
        }
    }
}

/// Build an IR snapshot from a fixture (sorted maps for stability in goldens).
#[must_use]
pub fn build_http_ir_from_fixture(f: &HttpCompileFixture) -> IrSnapshot {
    let scope = IrScopeId::new(&f.gateway_namespace, &f.gateway_name);
    IrSnapshot {
        ir_version: IR_VERSION,
        revision: IrRevision::Sequence(1),
        scope,
        listeners: f.listeners.clone(),
        routes: f.routes.clone(),
        backends: f.endpoints.clone(),
        meta: SnapshotMeta {
            correlation_id: uuid::Uuid::nil(),
            has_errors: false,
        },
        policy: PolicyOverlayIr::default(),
        inference: InferenceIr::default(),
    }
}

/// Golden helper — sorted JSON for stable tests.
#[cfg(test)]
pub(crate) fn normalize_http_route_fixture(f: &HttpCompileFixture) -> serde_json::Value {
    let snap = build_http_ir_from_fixture(f);
    serde_json::json!({
        "listeners": snap.listeners,
        "routes": snap.routes,
        "backends": snap.backends,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_minimal_fixture_sorted() {
        let v = normalize_http_route_fixture(&HttpCompileFixture::minimal_http());
        let s = serde_json::to_string_pretty(&v).unwrap();
        assert!(s.contains("example.com"));
    }
}
