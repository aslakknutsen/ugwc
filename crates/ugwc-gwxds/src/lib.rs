//! Encode [`ugwc_core::IrSnapshot`] into `istio.gwxds` protobuf for Praxis xDS clients.

#![allow(clippy::missing_errors_doc)]

pub mod istio {
    pub mod gwxds {
        include!(concat!(env!("OUT_DIR"), "/istio.gwxds.rs"));
    }
}

use istio::gwxds::{Backend, Listener, Protocol, Resource, Route, RouteMatch};
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
        let listener = Listener {
            key: listener_key.clone(),
            hostname: String::new(),
            port: u32::from(ln.bind_port),
            protocol,
            tls: None,
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
            let mut backends = Vec::new();
            let key = format!(
                "{}/{}/{}",
                r.backend.service_namespace,
                r.backend.service_name,
                r.backend.port_number.unwrap_or(80)
            );
            let eps = ir.backends.get(&key).cloned().unwrap_or_default();
            let host = qualify_host(&r.backend.service_namespace, &r.backend.service_name);
            let port = r.backend.port_number.unwrap_or(80) as u32;
            if let Some(ep) = eps.first() {
                backends.push(Backend {
                    host,
                    port,
                    weight: 1,
                    inference_pool: None,
                    tls: None,
                    dial_port: u32::from(ep.port),
                });
            } else {
                backends.push(Backend {
                    host,
                    port,
                    weight: 1,
                    inference_pool: None,
                    tls: None,
                    dial_port: port,
                });
            }
            let hostnames: Vec<String> = r
                .host
                .clone()
                .map(|h| vec![h])
                .unwrap_or_else(|| vec!["*".into()]);
            let matches = vec![RouteMatch {
                path_prefix: r.path_prefix.clone().unwrap_or_else(|| "/".into()),
                path_exact: String::new(),
                path_regex: String::new(),
                headers: Default::default(),
                methods: vec![],
            }];
            routes.push(Route {
                key: route_key,
                listener_key: listener_key.clone(),
                hostnames,
                matches,
                backends,
                request_redirect: None,
                request_header_modifier: None,
                invalid_backend_ref: false,
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
                hostname: String::new(),
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
    use ugwc_core::http_ir::HttpCompileFixture;

    #[test]
    fn encode_minimal_fixture_non_empty() {
        let f = HttpCompileFixture::minimal_http();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let r = encode_ir(&ir);
        assert!(!r.is_empty());
    }
}
