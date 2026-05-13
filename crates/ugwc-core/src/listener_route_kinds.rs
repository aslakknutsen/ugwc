//! Gateway listener `allowedRoutes.kinds` validation for Gateway API status (`InvalidRouteKinds`).

use crate::snapshot::{EffectiveListenerSnap, RouteGroupKindSnap};

#[must_use]
pub fn route_group_kind_valid_for_listener_protocol(
    protocol: &str,
    k: &RouteGroupKindSnap,
) -> bool {
    let proto = protocol.trim().to_ascii_uppercase();
    let g = k.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let kind = k.kind.as_str();
    match proto.as_str() {
        "HTTP" | "HTTPS" => {
            g == "gateway.networking.k8s.io" && (kind == "HTTPRoute" || kind == "GRPCRoute")
        }
        "TLS" => g == "gateway.networking.k8s.io" && kind == "TLSRoute",
        "TCP" => g == "gateway.networking.k8s.io" && kind == "TCPRoute",
        "UDP" => g == "gateway.networking.k8s.io" && kind == "UDPRoute",
        _ => true,
    }
}

/// True when `spec.allowedRoutes.kinds` is non-empty and lists at least one kind not permitted
/// for this listener's protocol (Gateway API `InvalidRouteKinds`).
/// True when `kinds` is non-empty and lists at least one route kind not permitted for `protocol`
/// (Gateway API `InvalidRouteKinds`).
#[must_use]
pub fn allowed_route_kinds_invalid_for_gateway_protocol(
    protocol: &str,
    kinds: &[RouteGroupKindSnap],
) -> bool {
    !kinds.is_empty()
        && kinds
            .iter()
            .any(|k| !route_group_kind_valid_for_listener_protocol(protocol, k))
}

#[must_use]
pub fn effective_listener_has_invalid_allowed_route_kinds(e: &EffectiveListenerSnap) -> bool {
    let kinds = e
        .allowed_routes
        .as_ref()
        .map(|a| &a.kinds[..])
        .unwrap_or(&[]);
    allowed_route_kinds_invalid_for_gateway_protocol(&e.protocol, kinds)
}

fn gw_route_group_kind(kind: &str) -> RouteGroupKindSnap {
    RouteGroupKindSnap {
        group: Some("gateway.networking.k8s.io".into()),
        kind: kind.to_string(),
    }
}

/// Supported route kinds for `Gateway.status.listeners[].supportedKinds`, including
/// conformance rules for `GatewayInvalidRouteKind` (empty when all kinds are invalid;
/// ensure `HTTPRoute` appears when some kinds remain after filtering invalid entries).
#[must_use]
pub fn effective_listener_supported_route_kinds_for_status(
    e: &EffectiveListenerSnap,
) -> Vec<RouteGroupKindSnap> {
    let proto = e.protocol.trim().to_ascii_uppercase();
    let kinds = e
        .allowed_routes
        .as_ref()
        .map(|a| &a.kinds[..])
        .unwrap_or(&[]);

    if proto == "TLS" {
        if kinds.is_empty() {
            return vec![gw_route_group_kind("TLSRoute")];
        }
        if !effective_listener_has_invalid_allowed_route_kinds(e) {
            return kinds.to_vec();
        }
        let mut filtered: Vec<RouteGroupKindSnap> = kinds
            .iter()
            .filter(|k| route_group_kind_valid_for_listener_protocol(&e.protocol, k))
            .cloned()
            .collect();
        if filtered.is_empty() {
            return Vec::new();
        }
        if !filtered.iter().any(|k| k.kind == "TLSRoute") {
            filtered.insert(0, gw_route_group_kind("TLSRoute"));
        }
        return filtered;
    }
    if proto == "TCP" {
        if kinds.is_empty() {
            return vec![gw_route_group_kind("TCPRoute")];
        }
        if !effective_listener_has_invalid_allowed_route_kinds(e) {
            return kinds.to_vec();
        }
        let mut filtered: Vec<RouteGroupKindSnap> = kinds
            .iter()
            .filter(|k| route_group_kind_valid_for_listener_protocol(&e.protocol, k))
            .cloned()
            .collect();
        if filtered.is_empty() {
            return Vec::new();
        }
        if !filtered.iter().any(|k| k.kind == "TCPRoute") {
            filtered.insert(0, gw_route_group_kind("TCPRoute"));
        }
        return filtered;
    }
    if proto == "UDP" {
        if kinds.is_empty() {
            return vec![gw_route_group_kind("UDPRoute")];
        }
        if !effective_listener_has_invalid_allowed_route_kinds(e) {
            return kinds.to_vec();
        }
        let mut filtered: Vec<RouteGroupKindSnap> = kinds
            .iter()
            .filter(|k| route_group_kind_valid_for_listener_protocol(&e.protocol, k))
            .cloned()
            .collect();
        if filtered.is_empty() {
            return Vec::new();
        }
        if !filtered.iter().any(|k| k.kind == "UDPRoute") {
            filtered.insert(0, gw_route_group_kind("UDPRoute"));
        }
        return filtered;
    }

    let http_like = proto == "HTTP" || proto == "HTTPS";
    if !http_like {
        return Vec::new();
    }

    let default_http_grpc = || vec![gw_route_group_kind("HTTPRoute"), gw_route_group_kind("GRPCRoute")];

    if kinds.is_empty() {
        return default_http_grpc();
    }

    let invalid = effective_listener_has_invalid_allowed_route_kinds(e);
    if !invalid {
        return kinds.to_vec();
    }

    let mut filtered: Vec<RouteGroupKindSnap> = kinds
        .iter()
        .filter(|k| route_group_kind_valid_for_listener_protocol(&e.protocol, k))
        .cloned()
        .collect();

    if filtered.is_empty() {
        return Vec::new();
    }

    if !filtered.iter().any(|k| k.kind == "HTTPRoute") {
        filtered.insert(0, gw_route_group_kind("HTTPRoute"));
    }
    filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listener_merge::merge_effective_listeners;
    use crate::snapshot::{
        AllowedRoutesSnap, GatewayListenerSnap, RouteGroupKindSnap,
    };

    #[test]
    fn invalid_http_listener_all_bad_kinds_yields_empty_supported() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: Some(AllowedRoutesSnap {
                namespaces: None,
                kinds: vec![RouteGroupKindSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: "InvalidRoute".into(),
                }],
            }),
            tls: None,
        }];
        let eff = &merge_effective_listeners(&listeners, &[])[0];
        assert!(effective_listener_has_invalid_allowed_route_kinds(eff));
        assert!(effective_listener_supported_route_kinds_for_status(eff).is_empty());
    }

    #[test]
    fn invalid_http_listener_inserts_http_route_when_filtered_non_empty() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: Some(AllowedRoutesSnap {
                namespaces: None,
                kinds: vec![
                    RouteGroupKindSnap {
                        group: Some("gateway.networking.k8s.io".into()),
                        kind: "InvalidRoute".into(),
                    },
                    RouteGroupKindSnap {
                        group: Some("gateway.networking.k8s.io".into()),
                        kind: "GRPCRoute".into(),
                    },
                ],
            }),
            tls: None,
        }];
        let eff = &merge_effective_listeners(&listeners, &[])[0];
        assert!(effective_listener_has_invalid_allowed_route_kinds(eff));
        let sk = effective_listener_supported_route_kinds_for_status(eff);
        assert!(sk.iter().any(|k| k.kind == "GRPCRoute"));
        assert!(sk.iter().any(|k| k.kind == "HTTPRoute"));
    }

    #[test]
    fn valid_http_listener_returns_spec_kinds() {
        let listeners = vec![GatewayListenerSnap {
            name: "http".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: Some(AllowedRoutesSnap {
                namespaces: None,
                kinds: vec![RouteGroupKindSnap {
                    group: Some("gateway.networking.k8s.io".into()),
                    kind: "HTTPRoute".into(),
                }],
            }),
            tls: None,
        }];
        let eff = &merge_effective_listeners(&listeners, &[])[0];
        assert!(!effective_listener_has_invalid_allowed_route_kinds(eff));
        let sk = effective_listener_supported_route_kinds_for_status(eff);
        assert_eq!(sk.len(), 1);
        assert_eq!(sk[0].kind, "HTTPRoute");
    }
}
