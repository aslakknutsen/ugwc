//! IR → Envoy xDS snapshot encoding (SotW first; Delta gated at server).

use prost::Message;
use ugwc_core::IrSnapshot;
use xds_api::pb::envoy::config::route::v3::route_match::PathSpecifier;
use xds_api::pb::envoy::config::route::v3::{Route, RouteConfiguration, RouteMatch, VirtualHost};
use xds_api::pb::google::protobuf::Any;

/// Serialized resource bundle for ADS (`DiscoveryResponse.resources`).
#[derive(Clone, Debug, Default)]
pub struct XdsSnapshot {
    pub listeners: Vec<Any>,
    pub routes: Vec<Any>,
    pub clusters: Vec<Any>,
    pub endpoints: Vec<Any>,
    pub version: String,
}

fn pack<M: Message + prost::Name>(msg: &M) -> Any {
    Any {
        type_url: format!("type.googleapis.com/{}", M::full_name()),
        value: msg.encode_to_vec(),
    }
}

/// Encode a deterministic SotW snapshot (Phase 7.1).
#[must_use]
pub fn encode_sotw(ir: &IrSnapshot) -> XdsSnapshot {
    let version = ir.revision_token();
    let mut out = XdsSnapshot {
        version: version.clone(),
        ..Default::default()
    };

    let route_name = format!("route-{}-{}", ir.scope.0.replace('/', "-"), ir.ir_version);
    let mut vhosts = vec![];
    for r in &ir.routes {
        let domain = r.hosts.first().cloned().unwrap_or_else(|| "*".into());
        let first = r.matches.first();
        let path_spec = if let Some(ex) = first.and_then(|m| m.path_exact.as_ref()) {
            PathSpecifier::Path(ex.clone())
        } else {
            PathSpecifier::Prefix(
                first
                    .and_then(|m| m.path_prefix.as_ref())
                    .cloned()
                    .unwrap_or_else(|| "/".into()),
            )
        };
        let m = RouteMatch {
            path_specifier: Some(path_spec),
            ..Default::default()
        };
        let r_entry = Route {
            r#match: Some(m),
            ..Default::default()
        };
        vhosts.push(VirtualHost {
            domains: vec![domain],
            routes: vec![r_entry],
            ..Default::default()
        });
    }

    if vhosts.is_empty() {
        vhosts.push(VirtualHost {
            domains: vec!["*".into()],
            routes: vec![],
            ..Default::default()
        });
    }

    let rds = RouteConfiguration {
        name: route_name,
        virtual_hosts: vhosts,
        ..Default::default()
    };
    out.routes.push(pack(&rds));

    out
}

/// Delta placeholder — keep default **off** in CI until SotW goldens pass (plan §7.3).
#[must_use]
pub fn encode_delta(_ir: &IrSnapshot, _previous_version: Option<&str>) -> XdsSnapshot {
    XdsSnapshot::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ugwc_core::http_ir::HttpCompileFixture;

    #[test]
    fn encode_minimal_fixture_roundtrips_version_token() {
        let f = HttpCompileFixture::minimal_http();
        let ir = ugwc_core::http_ir::build_http_ir_from_fixture(&f);
        let snap = encode_sotw(&ir);
        assert_eq!(snap.version, ir.revision_token());
        assert!(!snap.routes.is_empty());
    }
}
