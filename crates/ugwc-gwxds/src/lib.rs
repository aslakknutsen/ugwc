//! Encode [`ugwc_core::IrSnapshot`] into `istio.gwxds` protobuf for Praxis xDS clients.

#![allow(clippy::missing_errors_doc)]

pub mod istio {
    pub mod gwxds {
        include!(concat!(env!("OUT_DIR"), "/istio.gwxds.rs"));
    }
}

use istio::gwxds::{
    Backend, HeaderNameValue, Listener, Protocol, RequestHeaderModifier, RequestRedirect, Resource,
    Route, RouteMatch, TlsConfig,
};
use ugwc_core::ir::TlsMode;
use ugwc_core::IrSnapshot;

/// ADS `type_url` for gwxds resources (matches Praxis `xds-client`).
pub const GW_TYPE_URL: &str = "type.googleapis.com/istio.gwxds.Resource";

fn parse_scope(scope: &ugwc_core::IrScopeId) -> (String, String) {
    let s = &scope.0;
    if let Some((ns, rest)) = s.split_once('/') {
        (ns.to_string(), rest.to_string())
    } else {
        ("default".into(), s.clone())
    }
}

fn qualify_host(service_ns: &str, host: &str) -> String {
    if host.parse::<std::net::IpAddr>().is_ok() || host.contains('.') {
        return host.to_owned();
    }
    format!("{host}.{service_ns}.svc.cluster.local")
}

fn redirect_proto(r: &ugwc_core::ir::RequestRedirectIr) -> RequestRedirect {
    let path = r
        .path_replace_full
        .clone()
        .or_else(|| r.path_replace_prefix.clone())
        .unwrap_or_default();
    RequestRedirect {
        hostname: r.hostname.clone().unwrap_or_default(),
        path,
        port: u32::from(r.port.unwrap_or(0)),
        scheme: r.scheme.clone().unwrap_or_default(),
        status_code: r.status_code.unwrap_or(302),
    }
}

fn header_modifier_proto(h: &ugwc_core::ir::RequestHeaderModifierIr) -> RequestHeaderModifier {
    RequestHeaderModifier {
        set: h
            .set
            .iter()
            .map(|x| HeaderNameValue {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        add: h
            .add
            .iter()
            .map(|x| HeaderNameValue {
                name: x.name.clone(),
                value: x.value.clone(),
            })
            .collect(),
        remove: h.remove.clone(),
    }
}

fn route_matches_proto(matches: &[ugwc_core::ir::RouteMatchIr]) -> Vec<RouteMatch> {
    let src = if matches.is_empty() {
        vec![ugwc_core::ir::RouteMatchIr {
            path_prefix: Some("/".into()),
            path_exact: None,
            headers_exact: Default::default(),
        }]
    } else {
        matches.to_vec()
    };
    src.iter()
        .map(|m| RouteMatch {
            path_prefix: m.path_prefix.clone().unwrap_or_default(),
            path_exact: m.path_exact.clone().unwrap_or_default(),
            path_regex: String::new(),
            headers: m.headers_exact.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            methods: vec![],
        })
        .collect()
}

/// Encode IR into one [`Resource`] per listener (routes duplicated per listener for v0).
#[must_use]
pub fn encode_ir(ir: &IrSnapshot) -> Vec<Resource> {
    let (gw_ns, gw_name) = parse_scope(&ir.scope);
    let mut out = Vec::new();
    for ln in &ir.listeners {
        if ln.conflicted {
            continue;
        }
        let listener_key = format!(
            "{gw_ns}/{gw_name}/{}/{}/{}/{}/{}",
            ln.parent_kind, ln.parent_namespace, ln.parent_name, ln.section_name, ln.bind_port
        );
        let protocol = if matches!(ln.tls, TlsMode::Terminate) {
            Protocol::Https as i32
        } else {
            Protocol::Http as i32
        };
        let hostname = ln
            .hostname
            .clone()
            .filter(|h| !h.trim().is_empty())
            .unwrap_or_else(|| "*".into());
        let tls = if matches!(ln.tls, TlsMode::Terminate)
            && !ln.tls_cert_pem.is_empty()
            && !ln.tls_key_pem.is_empty()
        {
            Some(TlsConfig {
                cert_pem: ln.tls_cert_pem.clone(),
                key_pem: ln.tls_key_pem.clone(),
                ca_pem: vec![],
                min_version: String::new(),
            })
        } else {
            None
        };
        let listener = Listener {
            key: listener_key.clone(),
            hostname,
            port: u32::from(ln.bind_port),
            protocol,
            tls,
            allowed_routes: vec!["gateway.networking.k8s.io/HTTPRoute".into()],
        };

        let mut routes = Vec::new();
        for (idx, r) in ir.routes.iter().enumerate() {
            let attach_ok = r.listener.as_ref().map_or(false, |att| {
                att.section_name == ln.section_name
                    && att.port == ln.bind_port
                    && att.parent_kind == ln.parent_kind
                    && att.parent_namespace == ln.parent_namespace
                    && att.parent_name == ln.parent_name
            });
            if !attach_ok {
                continue;
            }
            let route_key = format!("{}/{}/{}", gw_ns, r.name, idx);
            let hostnames: Vec<String> = if r.hosts.is_empty() {
                vec!["*".into()]
            } else {
                r.hosts.clone()
            };

            let matches_pb = route_matches_proto(&r.matches);

            let mut backends_pb = Vec::new();
            let mut invalid_backend_ref = false;
            if r.request_redirect.is_none() {
                for wb in &r.backends {
                    let key = format!(
                        "{}/{}/{}",
                        wb.backend.service_namespace,
                        wb.backend.service_name,
                        wb.backend.port_number.unwrap_or(80)
                    );
                    let eps = ir.backends.get(&key).cloned().unwrap_or_default();
                    let host = qualify_host(&wb.backend.service_namespace, &wb.backend.service_name);
                    let port = wb.backend.port_number.unwrap_or(80) as u32;
                    let dial_port = eps
                        .first()
                        .map(|e| u32::from(e.port))
                        .unwrap_or(port);
                    backends_pb.push(Backend {
                        host,
                        port,
                        weight: wb.weight,
                        inference_pool: None,
                        tls: None,
                        dial_port,
                    });
                }
                if backends_pb.is_empty() {
                    invalid_backend_ref = true;
                }
            }

            let request_redirect = r.request_redirect.as_ref().map(redirect_proto);
            let request_header_modifier = r
                .request_header_modifier
                .as_ref()
                .map(header_modifier_proto);

            routes.push(Route {
                key: route_key,
                listener_key: listener_key.clone(),
                hostnames,
                matches: matches_pb,
                backends: backends_pb,
                request_redirect,
                request_header_modifier,
                invalid_backend_ref,
            });
        }

        out.push(Resource {
            key: listener_key.clone(),
            listener: Some(listener),
            routes,
        });
    }
    if out.is_empty() {
        let listener_key = format!("{gw_ns}/{gw_name}/default");
        out.push(Resource {
            key: listener_key.clone(),
            listener: Some(Listener {
                key: listener_key,
                hostname: "*".into(),
                port: 80,
                protocol: Protocol::Http as i32,
                tls: None,
                allowed_routes: vec![],
            }),
            routes: vec![],
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::ir::TlsMode;
    use ugwc_core::http_ir::HttpCompileFixture;

    #[test]
    fn encode_minimal_fixture_non_empty() {
        let f = HttpCompileFixture::minimal_http();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        assert!(!r.is_empty());
        let rt = &r[0].routes[0];
        assert_eq!(rt.backends.len(), 1);
        assert_eq!(rt.backends[0].weight, 1);
        assert!(rt.matches.iter().any(|m| !m.path_prefix.is_empty()));
    }

    #[test]
    fn encode_exact_path_on_match() {
        let mut f = HttpCompileFixture::minimal_http();
        f.routes[0].matches[0].path_prefix = None;
        f.routes[0].matches[0].path_exact = Some("/foo".into());
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        let m = &r[0].routes[0].matches[0];
        assert_eq!(m.path_exact, "/foo");
        assert!(m.path_prefix.is_empty());
    }

    #[test]
    fn encode_https_tls_and_hostname() {
        let mut f = HttpCompileFixture::minimal_http();
        f.listeners[0].tls = TlsMode::Terminate;
        f.listeners[0].hostname = Some("gw.example.com".into());
        f.listeners[0].tls_cert_pem = b"BEGIN CERT".to_vec();
        f.listeners[0].tls_key_pem = b"BEGIN KEY".to_vec();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        let l = r[0].listener.as_ref().expect("listener");
        assert_eq!(l.hostname, "gw.example.com");
        assert!(
            l.tls.as_ref().is_some_and(|t| !t.cert_pem.is_empty()),
            "{l:?}"
        );
    }
}
