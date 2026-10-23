use ugwc_core::snapshot::{
    AllowedRoutesNamespacesSnap, AllowedRoutesSnap, GatewayListenerSnap, GatewayListenerTlsSnap,
    RouteGroupKindSnap, SecretObjectReferenceSnap,
};

use crate::resources::{
    GatewayListeners, GatewayListenersAllowedRoutes, GatewayListenersAllowedRoutesKinds,
    GatewayListenersAllowedRoutesNamespaces, GatewayListenersTls,
    GatewayListenersTlsCertificateRefs, GatewayListenersTlsMode, ListenerSetListeners,
    ListenerSetListenersAllowedRoutes, ListenerSetListenersAllowedRoutesKinds,
    ListenerSetListenersAllowedRoutesNamespaces, ListenerSetListenersTls,
    ListenerSetListenersTlsCertificateRefs, ListenerSetListenersTlsMode,
};

use super::snap_u16_port;

pub(super) fn gateway_allowed_routes_namespaces_snap(
    n: &GatewayListenersAllowedRoutesNamespaces,
) -> AllowedRoutesNamespacesSnap {
    AllowedRoutesNamespacesSnap {
        from: n.from.as_ref().map(|f| format!("{f:?}")),
        selector_match_labels: n
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.clone())
            .unwrap_or_default(),
    }
}

pub(super) fn gateway_allowed_routes_snap(ar: &GatewayListenersAllowedRoutes) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar
            .namespaces
            .as_ref()
            .map(gateway_allowed_routes_namespaces_snap),
        kinds: ar
            .kinds
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(
                |k: &GatewayListenersAllowedRoutesKinds| RouteGroupKindSnap {
                    group: k.group.clone(),
                    kind: k.kind.clone(),
                },
            )
            .collect(),
    }
}

pub(super) fn listenerset_allowed_routes_namespaces_snap(
    n: &ListenerSetListenersAllowedRoutesNamespaces,
) -> AllowedRoutesNamespacesSnap {
    AllowedRoutesNamespacesSnap {
        from: n.from.as_ref().map(|f| format!("{f:?}")),
        selector_match_labels: n
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.clone())
            .unwrap_or_default(),
    }
}

pub(super) fn listenerset_allowed_routes_snap(
    ar: &ListenerSetListenersAllowedRoutes,
) -> AllowedRoutesSnap {
    AllowedRoutesSnap {
        namespaces: ar
            .namespaces
            .as_ref()
            .map(listenerset_allowed_routes_namespaces_snap),
        kinds: ar
            .kinds
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(
                |k: &ListenerSetListenersAllowedRoutesKinds| RouteGroupKindSnap {
                    group: k.group.clone(),
                    kind: k.kind.clone(),
                },
            )
            .collect(),
    }
}

pub(super) fn gateway_listener_to_snap(l: &GatewayListeners) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: snap_u16_port(Some(l.port)).unwrap_or(0),
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l.allowed_routes.as_ref().map(gateway_allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_gateway_to_snap),
    }
}

pub(super) fn listenerset_listener_to_snap(l: &ListenerSetListeners) -> GatewayListenerSnap {
    GatewayListenerSnap {
        name: l.name.clone(),
        port: snap_u16_port(Some(l.port)).unwrap_or(0),
        protocol: if l.protocol.is_empty() {
            "HTTP".into()
        } else {
            l.protocol.clone()
        },
        hostname: l.hostname.clone(),
        allowed_routes: l
            .allowed_routes
            .as_ref()
            .map(listenerset_allowed_routes_snap),
        tls: l.tls.as_ref().map(listener_tls_listenerset_to_snap),
    }
}

fn tls_mode_gateway_str(mode: Option<GatewayListenersTlsMode>) -> Option<String> {
    mode.map(|m| match m {
        GatewayListenersTlsMode::Terminate => "Terminate".to_string(),
        GatewayListenersTlsMode::Passthrough => "Passthrough".to_string(),
    })
}

fn tls_mode_listenerset_str(mode: Option<ListenerSetListenersTlsMode>) -> Option<String> {
    mode.map(|m| match m {
        ListenerSetListenersTlsMode::Terminate => "Terminate".to_string(),
        ListenerSetListenersTlsMode::Passthrough => "Passthrough".to_string(),
    })
}

fn snap_from_gateway_cert_ref(r: &GatewayListenersTlsCertificateRefs) -> SecretObjectReferenceSnap {
    SecretObjectReferenceSnap {
        group: r.group.clone(),
        kind: r.kind.clone(),
        name: r.name.clone(),
        namespace: r.namespace.clone(),
    }
}

fn snap_from_listenerset_cert_ref(
    r: &ListenerSetListenersTlsCertificateRefs,
) -> SecretObjectReferenceSnap {
    SecretObjectReferenceSnap {
        group: r.group.clone(),
        kind: r.kind.clone(),
        name: r.name.clone(),
        namespace: r.namespace.clone(),
    }
}

fn listener_tls_gateway_to_snap(t: &GatewayListenersTls) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: tls_mode_gateway_str(t.mode.clone()),
        certificate_refs: t
            .certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(snap_from_gateway_cert_ref)
            .collect(),
    }
}

fn listener_tls_listenerset_to_snap(t: &ListenerSetListenersTls) -> GatewayListenerTlsSnap {
    GatewayListenerTlsSnap {
        mode: tls_mode_listenerset_str(t.mode.clone()),
        certificate_refs: t
            .certificate_refs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(snap_from_listenerset_cert_ref)
            .collect(),
    }
}
