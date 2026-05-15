//! Route attachment — listener keys, hostname intersection, `allowedRoutes`, ReferenceGrant helpers.

use std::collections::{BTreeMap, HashSet};

use crate::snapshot::{
    AllowedRoutesSnap, ClusterSnapshot, EffectiveListenerSnap, GrpcRouteSnap, HttpRouteSnap,
    ListenerParentSnap, ParentRefSnap, ReferenceGrantSnap, TcpRouteSnap, TlsRouteSnap,
    UdpRouteSnap,
};

pub(crate) fn gateway_api_group(g: &Option<String>) -> bool {
    g.as_deref().unwrap_or("gateway.networking.k8s.io") == "gateway.networking.k8s.io"
}

pub(crate) fn parent_namespace(p: &ParentRefSnap, route_ns: &str) -> String {
    p.namespace.clone().unwrap_or_else(|| route_ns.to_string())
}

fn effective_listener_matches_attachment(
    e: &EffectiveListenerSnap,
    k: &crate::ir::ListenerAttachmentIr,
    gw_ns: &str,
    gw_name: &str,
) -> bool {
    if e.conflicted || e.section_name != k.section_name || e.port != k.port {
        return false;
    }
    match k.parent_kind.as_str() {
        "Gateway" => {
            matches!(e.parent, ListenerParentSnap::InlineGateway)
                && k.parent_namespace == gw_ns
                && k.parent_name == gw_name
        }
        "ListenerSet" => {
            if let ListenerParentSnap::ListenerSet { namespace, name } = &e.parent {
                k.parent_namespace == *namespace && k.parent_name == *name
            } else {
                false
            }
        }
        _ => false,
    }
}

pub(crate) fn find_effective_for_key<'a>(
    snapshot: &'a ClusterSnapshot,
    k: &crate::ir::ListenerAttachmentIr,
) -> Option<&'a EffectiveListenerSnap> {
    snapshot.effective_listeners.iter().find(|e| {
        effective_listener_matches_attachment(
            e,
            k,
            &snapshot.gateway_namespace,
            &snapshot.gateway_name,
        )
    })
}

/// Cross-namespace `Service` backend ref requires a [`ReferenceGrant`] in the service namespace.
#[must_use]
pub fn reference_grant_permits_service_ref(
    grants: &[ReferenceGrantSnap],
    route_namespace: &str,
    service_namespace: &str,
    service_name: &str,
    route_kind: &str,
) -> bool {
    if route_namespace == service_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";

    for g in grants {
        if g.namespace != service_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == route_kind && fr.namespace == route_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "Service" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !tg.is_empty() {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == service_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// Cross-namespace `InferencePool` backend ref requires a [`ReferenceGrant`] in the pool namespace.
#[must_use]
pub fn reference_grant_permits_inference_pool_ref(
    grants: &[ReferenceGrantSnap],
    route_namespace: &str,
    pool_namespace: &str,
    pool_name: &str,
    route_kind: &str,
) -> bool {
    if route_namespace == pool_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";
    const G_POOL: &str = "inference.networking.k8s.io";

    for g in grants {
        if g.namespace != pool_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == route_kind && fr.namespace == route_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "InferencePool" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !(tg.is_empty() || tg == G_POOL) {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == pool_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// Whether an explicit [`ReferenceGrant`] in the `Gateway` namespace allows `from` (route ns/kind) to reference that `Gateway`.
///
/// Gateway API: cross-namespace **Gateway ↔ route attachment** is governed by the Gateway listener's
/// `allowedRoutes` (and related policy), **not** by `ReferenceGrant` (see spec: ReferenceGrant
/// exceptions). This helper remains for callers that want to model optional explicit grants.
#[must_use]
pub fn reference_grant_permits_route_to_gateway_parent(
    grants: &[ReferenceGrantSnap],
    route_namespace: &str,
    route_kind: &str,
    gateway_namespace: &str,
    gateway_name: &str,
) -> bool {
    if route_namespace == gateway_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";
    const G_TO: &str = "gateway.networking.k8s.io";

    for g in grants {
        if g.namespace != gateway_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == route_kind && fr.namespace == route_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "Gateway" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !(tg.is_empty() || tg == G_TO) {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == gateway_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// Cross-namespace route parentRef to a `ListenerSet` requires a [`ReferenceGrant`] in the ListenerSet namespace.
#[must_use]
pub fn reference_grant_permits_route_to_listener_set_parent(
    grants: &[ReferenceGrantSnap],
    route_namespace: &str,
    route_kind: &str,
    listener_set_namespace: &str,
    listener_set_name: &str,
) -> bool {
    if route_namespace == listener_set_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";
    const G_TO: &str = "gateway.networking.k8s.io";

    for g in grants {
        if g.namespace != listener_set_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == route_kind && fr.namespace == route_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "ListenerSet" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !(tg.is_empty() || tg == G_TO) {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == listener_set_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// True when a route is denied attachment to this reconcile scope solely because of a missing cross-namespace [`ReferenceGrant`] on the parent ref.
///
/// Note: `ReferenceGrant` does **not** apply to cross-namespace attachment of routes to a `Gateway`
/// parent (listener `allowedRoutes` / hostname rules decide); it **does** apply to `ListenerSet`
/// parents and to other cross-namespace object refs (e.g. backends, Secrets).
#[must_use]
pub fn route_parent_attachment_ref_not_permitted(
    snapshot: &ClusterSnapshot,
    route_namespace: &str,
    route_kind: &str,
    parent: &ParentRefSnap,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let g = parent
        .group
        .as_deref()
        .unwrap_or("gateway.networking.k8s.io");
    let k = parent.kind.as_deref().unwrap_or("Gateway");
    if !gateway_api_group(&Some(g.to_string())) {
        return false;
    }
    match k {
        "Gateway" => {
            let pns = parent_namespace(parent, route_namespace);
            if pns != snapshot.gateway_namespace || parent.name != snapshot.gateway_name {
                return false;
            }
            false
        }
        "ListenerSet" => {
            let ls_ns = parent_namespace(parent, route_namespace);
            if !attached_ls.contains(&(ls_ns.clone(), parent.name.clone())) {
                return false;
            }
            route_namespace != ls_ns.as_str()
                && listener_set_any_listener_accepts_route(
                    snapshot,
                    &ls_ns,
                    &parent.name,
                    route_namespace,
                    route_kind,
                )
                && !reference_grant_permits_route_to_listener_set_parent(
                    &snapshot.reference_grants,
                    route_namespace,
                    route_kind,
                    &ls_ns,
                    &parent.name,
                )
        }
        _ => false,
    }
}

/// Cross-namespace `Secret` certificate ref from a `Gateway` requires a [`ReferenceGrant`] in the Secret namespace.
#[must_use]
pub fn reference_grant_permits_gateway_secret_ref(
    grants: &[ReferenceGrantSnap],
    gateway_namespace: &str,
    secret_namespace: &str,
    secret_name: &str,
) -> bool {
    if gateway_namespace == secret_namespace {
        return true;
    }
    const G_FROM: &str = "gateway.networking.k8s.io";
    const K_FROM: &str = "Gateway";

    for g in grants {
        if g.namespace != secret_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(G_FROM);
            if fg == G_FROM && fr.kind == K_FROM && fr.namespace == gateway_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if to.kind != "Secret" {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !tg.is_empty() {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == secret_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// Cross-namespace `ConfigMap` / `Secret` refs from a `BackendTLSPolicy` require a [`ReferenceGrant`] in the resource namespace.
#[must_use]
pub fn reference_grant_permits_backend_tls_policy_resource_ref(
    grants: &[ReferenceGrantSnap],
    policy_namespace: &str,
    resource_namespace: &str,
    resource_kind: &str,
    resource_name: &str,
) -> bool {
    if policy_namespace == resource_namespace {
        return true;
    }
    const GWM: &str = "gateway.networking.k8s.io";
    const BTP: &str = "BackendTLSPolicy";
    let rk = resource_kind.trim();
    for g in grants {
        if g.namespace != resource_namespace {
            continue;
        }
        let mut from_ok = false;
        for fr in &g.from {
            let fg = fr.group.as_deref().unwrap_or(GWM);
            if fg == GWM && fr.kind == BTP && fr.namespace == policy_namespace {
                from_ok = true;
                break;
            }
        }
        if !from_ok {
            continue;
        }
        for to in &g.to {
            if !to.kind.eq_ignore_ascii_case(rk) {
                continue;
            }
            let tg = to.group.as_deref().unwrap_or("");
            if !tg.is_empty() {
                continue;
            }
            match &to.name {
                None => return true,
                Some(n) if n.is_empty() => return true,
                Some(n) if n == resource_name => return true,
                _ => {}
            }
        }
    }
    false
}

/// Whether `route_namespace` may attach to this listener per `allowedRoutes`.
///
/// `route_kind` is `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `TCPRoute`, or `UDPRoute`. When `allowedRoutes.kinds` is empty and the protocol
/// is HTTP-like, both HTTP and GRPC route kinds are permitted (ugwc implements GRPCRoute on HTTP/HTTPS listeners).
/// When the protocol is TLS, only `TLSRoute` is permitted by default.
/// When the protocol is TCP, only `TCPRoute` is permitted by default.
/// When the protocol is UDP, only `UDPRoute` is permitted by default.
#[must_use]
pub fn listener_accepts_route_kind(
    route_kind: &str,
    route_namespace: &str,
    gateway_namespace: &str,
    protocol: &str,
    allowed: Option<&AllowedRoutesSnap>,
    namespace_labels: &BTreeMap<String, BTreeMap<String, String>>,
) -> bool {
    let proto_u = protocol.trim().to_ascii_uppercase();
    let http_like = proto_u == "HTTP" || proto_u == "HTTPS";
    let tls_like = proto_u == "TLS";
    let tcp_like = proto_u == "TCP";
    let udp_like = proto_u == "UDP";

    let ns_from = allowed
        .and_then(|a| a.namespaces.as_ref())
        .and_then(|n| n.from.as_deref())
        .unwrap_or("Same");

    let ns_ok = match ns_from {
        "All" => true,
        "Same" => route_namespace == gateway_namespace,
        "Selector" => {
            let sel = allowed
                .and_then(|a| a.namespaces.as_ref())
                .map(|n| &n.selector_match_labels)
                .cloned()
                .unwrap_or_default();
            if sel.is_empty() {
                true
            } else {
                namespace_labels
                    .get(route_namespace)
                    .map(|lb| {
                        sel.iter()
                            .all(|(k, v)| lb.get(k).map(|x| x == v).unwrap_or(false))
                    })
                    .unwrap_or(false)
            }
        }
        _ => route_namespace == gateway_namespace,
    };
    if !ns_ok {
        return false;
    }

    let kinds = allowed.map(|a| &a.kinds[..]).unwrap_or(&[]);
    if kinds.is_empty() {
        if http_like {
            return matches!(route_kind, "HTTPRoute" | "GRPCRoute");
        }
        if tls_like {
            return route_kind == "TLSRoute";
        }
        if tcp_like {
            return route_kind == "TCPRoute";
        }
        if udp_like {
            return route_kind == "UDPRoute";
        }
        return false;
    }
    kinds.iter().any(|k| {
        let g = k.group.as_deref().unwrap_or("gateway.networking.k8s.io");
        g == "gateway.networking.k8s.io" && k.kind == route_kind
    })
}

/// Whether an [`HTTPRoute`] may attach (same as [`listener_accepts_route_kind`] with `HTTPRoute`).
#[must_use]
pub fn listener_accepts_http_route(
    route_namespace: &str,
    gateway_namespace: &str,
    protocol: &str,
    allowed: Option<&AllowedRoutesSnap>,
    namespace_labels: &BTreeMap<String, BTreeMap<String, String>>,
) -> bool {
    listener_accepts_route_kind(
        "HTTPRoute",
        route_namespace,
        gateway_namespace,
        protocol,
        allowed,
        namespace_labels,
    )
}

/// `pattern` is a concrete hostname or a leading-`*.` DNS wildcard from Gateway API.
#[must_use]
pub(crate) fn hostname_literal_or_wildcard_matches(pattern: &str, host: &str) -> bool {
    let p = pattern.trim();
    let h = host.trim();
    if p.is_empty() || h.is_empty() {
        return false;
    }
    if let Some(suffix) = p.strip_prefix("*.") {
        let suf = suffix.to_ascii_lowercase();
        let hh = h.to_ascii_lowercase();
        hh.ends_with(&format!(".{suf}"))
    } else {
        p.eq_ignore_ascii_case(h)
    }
}

/// Symmetric hostname intersection (route vs listener hostnames may each be concrete or `*.`).
#[must_use]
pub(crate) fn hostnames_intersect(a: &str, b: &str) -> bool {
    hostname_literal_or_wildcard_matches(a, b) || hostname_literal_or_wildcard_matches(b, a)
}

/// True if any non-conflicted inline Gateway listener admits this route namespace + kind.
#[must_use]
pub(crate) fn gateway_inline_any_listener_accepts_route(
    snapshot: &ClusterSnapshot,
    route_namespace: &str,
    route_kind: &str,
) -> bool {
    snapshot.effective_listeners.iter().any(|e| {
        matches!(e.parent, ListenerParentSnap::InlineGateway)
            && !e.conflicted
            && listener_accepts_route_kind(
                route_kind,
                route_namespace,
                &snapshot.gateway_namespace,
                &e.protocol,
                e.allowed_routes.as_ref(),
                &snapshot.namespace_labels,
            )
    })
}

/// True if any non-conflicted listener on this ListenerSet admits this route namespace + kind.
#[must_use]
pub(crate) fn listener_set_any_listener_accepts_route(
    snapshot: &ClusterSnapshot,
    listener_set_namespace: &str,
    listener_set_name: &str,
    route_namespace: &str,
    route_kind: &str,
) -> bool {
    snapshot.effective_listeners.iter().any(|e| {
        if let ListenerParentSnap::ListenerSet { namespace, name } = &e.parent {
            namespace == listener_set_namespace
                && name == listener_set_name
                && !e.conflicted
                && listener_accepts_route_kind(
                    route_kind,
                    route_namespace,
                    &snapshot.gateway_namespace,
                    &e.protocol,
                    e.allowed_routes.as_ref(),
                    &snapshot.namespace_labels,
                )
        } else {
            false
        }
    })
}

/// For a Gateway `parentRef` targeting this reconcile scope: if ignoring `sectionName`/`port`
/// still yields at least one listener that admits the route kind + namespace, the strict empty
/// key set is a parentRef shape mismatch ([`RouteReasonNoMatchingParent`]).
#[must_use]
pub(crate) fn gateway_parent_ref_has_relaxed_keys_non_empty(
    route_ns: &str,
    route_kind: &str,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if !gateway_api_group(&Some(g.to_string())) || k != "Gateway" {
        return false;
    }
    let pns = parent_namespace(p, route_ns);
    if pns != snapshot.gateway_namespace || p.name != snapshot.gateway_name {
        return false;
    }
    snapshot.effective_listeners.iter().any(|e| {
        matches!(e.parent, ListenerParentSnap::InlineGateway)
            && !e.conflicted
            && listener_accepts_route_kind(
                route_kind,
                route_ns,
                &snapshot.gateway_namespace,
                &e.protocol,
                e.allowed_routes.as_ref(),
                &snapshot.namespace_labels,
            )
    })
}

/// True when a Gateway parentRef targets this scope and at least one listener matches section/port
/// but the route kind is explicitly not accepted by any of those listeners.
///
/// This distinguishes "kind not allowed" (`NotAllowedByListeners`) from generic
/// "no attachment" (`NotAllowedByListeners`) which also covers hostname intersection failures.
#[must_use]
pub(crate) fn gateway_parent_ref_blocked_by_route_kind(
    route_ns: &str,
    route_kind: &str,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
) -> bool {
    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");
    if !gateway_api_group(&Some(g.to_string())) || k != "Gateway" {
        return false;
    }
    let pns = parent_namespace(p, route_ns);
    if pns != snapshot.gateway_namespace || p.name != snapshot.gateway_name {
        return false;
    }
    let mut has_section_port_match = false;
    for e in &snapshot.effective_listeners {
        if !matches!(e.parent, ListenerParentSnap::InlineGateway) || e.conflicted {
            continue;
        }
        if let Some(ref sn) = p.section_name {
            if sn != &e.section_name {
                continue;
            }
        }
        if let Some(port) = p.port {
            if port != e.port {
                continue;
            }
        }
        has_section_port_match = true;
        if listener_accepts_route_kind(
            route_kind,
            route_ns,
            &snapshot.gateway_namespace,
            &e.protocol,
            e.allowed_routes.as_ref(),
            &snapshot.namespace_labels,
        ) {
            return false;
        }
    }
    has_section_port_match
}

/// Hostnames after intersecting route `spec.hostnames` with listener hostname.
pub(crate) fn intersect_route_listener_hosts(
    route_hostnames: &[String],
    listener_hostname: &Option<String>,
) -> Option<Vec<String>> {
    let lh = listener_hostname
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if route_hostnames.is_empty() {
        return match lh {
            None => Some(Vec::new()),
            Some(h) => Some(vec![h.to_string()]),
        };
    }

    let Some(lh) = lh else {
        return Some(route_hostnames.to_vec());
    };

    let mut out = Vec::new();
    for rh in route_hostnames {
        let r = rh.trim();
        if r.is_empty() {
            continue;
        }
        if hostnames_intersect(r, lh) {
            out.push(r.to_string());
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

#[must_use]
pub fn route_listener_hostname_ok(route: &HttpRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_some()
}

#[must_use]
pub fn grpc_route_listener_hostname_ok(route: &GrpcRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_some()
}

#[must_use]
pub fn tls_route_listener_hostname_ok(route: &TlsRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    intersect_route_listener_hosts(&route.hostnames, &eff.hostname).is_some()
}

/// TCPRoute has no hostnames; attachment requires a TCP listener.
#[must_use]
pub fn tcp_route_listener_ok(_route: &TcpRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    eff.protocol.trim().eq_ignore_ascii_case("TCP")
}

/// UDPRoute has no hostnames; attachment requires a UDP listener.
#[must_use]
pub fn udp_route_listener_ok(_route: &UdpRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    eff.protocol.trim().eq_ignore_ascii_case("UDP")
}

/// Collect attachment keys from one route `parentRef` relevant to this Gateway reconcile scope.
pub(crate) fn attachment_keys_for_parent_ref_ns(
    route_ns: &str,
    route_kind: &str,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys = Vec::new();

    let g = p.group.as_deref().unwrap_or("gateway.networking.k8s.io");
    let k = p.kind.as_deref().unwrap_or("Gateway");

    if gateway_api_group(&Some(g.to_string())) && k == "Gateway" {
        let pns = parent_namespace(p, route_ns);
        if pns != snapshot.gateway_namespace || p.name != snapshot.gateway_name {
            return keys;
        }
        if route_ns != snapshot.gateway_namespace.as_str() {
            if !gateway_inline_any_listener_accepts_route(snapshot, route_ns, route_kind) {
                return keys;
            }
        }
        for e in &snapshot.effective_listeners {
            if !matches!(e.parent, ListenerParentSnap::InlineGateway) {
                continue;
            }
            if e.conflicted {
                continue;
            }
            if let Some(ref sn) = p.section_name {
                if sn != &e.section_name {
                    continue;
                }
            }
            if let Some(port) = p.port {
                if port != e.port {
                    continue;
                }
            }
            if !listener_accepts_route_kind(
                route_kind,
                route_ns,
                &snapshot.gateway_namespace,
                &e.protocol,
                e.allowed_routes.as_ref(),
                &snapshot.namespace_labels,
            ) {
                continue;
            }
            keys.push(crate::ir::ListenerAttachmentIr {
                parent_kind: "Gateway".into(),
                parent_namespace: snapshot.gateway_namespace.clone(),
                parent_name: snapshot.gateway_name.clone(),
                section_name: e.section_name.clone(),
                port: e.port,
            });
        }
        return keys;
    }

    if gateway_api_group(&Some(g.to_string())) && k == "ListenerSet" {
        let ls_ns = parent_namespace(p, route_ns);
        if !attached_ls.contains(&(ls_ns.clone(), p.name.clone())) {
            return keys;
        }
        if route_ns != ls_ns.as_str() {
            if !listener_set_any_listener_accepts_route(
                snapshot,
                &ls_ns,
                &p.name,
                route_ns,
                route_kind,
            ) {
                return keys;
            }
            if !reference_grant_permits_route_to_listener_set_parent(
                &snapshot.reference_grants,
                route_ns,
                route_kind,
                &ls_ns,
                &p.name,
            ) {
                return keys;
            }
        }
        for e in &snapshot.effective_listeners {
            let ListenerParentSnap::ListenerSet {
                namespace: ns,
                name: ln,
            } = &e.parent
            else {
                continue;
            };
            if ns != &ls_ns || ln != &p.name {
                continue;
            }
            if e.conflicted {
                continue;
            }
            if let Some(ref sn) = p.section_name {
                if sn != &e.section_name {
                    continue;
                }
            }
            if let Some(port) = p.port {
                if port != e.port {
                    continue;
                }
            }
            if !listener_accepts_route_kind(
                route_kind,
                route_ns,
                &snapshot.gateway_namespace,
                &e.protocol,
                e.allowed_routes.as_ref(),
                &snapshot.namespace_labels,
            ) {
                continue;
            }
            keys.push(crate::ir::ListenerAttachmentIr {
                parent_kind: "ListenerSet".into(),
                parent_namespace: ls_ns.clone(),
                parent_name: p.name.clone(),
                section_name: e.section_name.clone(),
                port: e.port,
            });
        }
    }

    keys
}

pub(crate) fn attachment_keys_for_parent_ref(
    route: &HttpRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    attachment_keys_for_parent_ref_ns(&route.namespace, "HTTPRoute", p, snapshot, attached_ls)
}

pub(crate) fn grpc_attachment_keys_for_parent_ref(
    route: &GrpcRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    attachment_keys_for_parent_ref_ns(&route.namespace, "GRPCRoute", p, snapshot, attached_ls)
}

pub(crate) fn tls_attachment_keys_for_parent_ref(
    route: &TlsRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    attachment_keys_for_parent_ref_ns(&route.namespace, "TLSRoute", p, snapshot, attached_ls)
}

pub(crate) fn tcp_attachment_keys_for_parent_ref(
    route: &TcpRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    attachment_keys_for_parent_ref_ns(&route.namespace, "TCPRoute", p, snapshot, attached_ls)
}

pub(crate) fn udp_attachment_keys_for_parent_ref(
    route: &UdpRouteSnap,
    p: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    attachment_keys_for_parent_ref_ns(&route.namespace, "UDPRoute", p, snapshot, attached_ls)
}

pub(crate) fn route_attachment_keys(
    route: &HttpRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys: Vec<crate::ir::ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) fn grpc_route_attachment_keys(
    route: &GrpcRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys: Vec<crate::ir::ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(grpc_attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) fn tls_route_attachment_keys(
    route: &TlsRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys: Vec<crate::ir::ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(tls_attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) fn tcp_route_attachment_keys(
    route: &TcpRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys: Vec<crate::ir::ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(tcp_attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) fn udp_route_attachment_keys(
    route: &UdpRouteSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<crate::ir::ListenerAttachmentIr> {
    let mut keys: Vec<crate::ir::ListenerAttachmentIr> = Vec::new();
    for p in &route.parent_refs {
        keys.extend(udp_attachment_keys_for_parent_ref(
            route,
            p,
            snapshot,
            attached_ls,
        ));
    }
    keys.sort();
    keys.dedup();
    keys
}

#[must_use]
pub(crate) fn listener_attachment_count_key(lk: &crate::ir::ListenerAttachmentIr) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        lk.parent_kind, lk.parent_namespace, lk.parent_name, lk.section_name, lk.port
    )
}

/// Whether this parentRef would accept the route (listener policy + hostname intersection).
#[must_use]
pub fn route_accepted_for_parent_ref(
    route: &HttpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let keys = attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return false;
    }
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !route_listener_hostname_ok(route, eff) {
            continue;
        }
        return true;
    }
    false
}

#[must_use]
pub fn grpc_route_accepted_for_parent_ref(
    route: &GrpcRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let keys = grpc_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return false;
    }
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !grpc_route_listener_hostname_ok(route, eff) {
            continue;
        }
        return true;
    }
    false
}

#[must_use]
pub fn tls_route_accepted_for_parent_ref(
    route: &TlsRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let keys = tls_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return false;
    }
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !tls_route_listener_hostname_ok(route, eff) {
            continue;
        }
        return true;
    }
    false
}

#[must_use]
pub fn tcp_route_accepted_for_parent_ref(
    route: &TcpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let keys = tcp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return false;
    }
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !tcp_route_listener_ok(route, eff) {
            continue;
        }
        return true;
    }
    false
}

#[must_use]
pub fn udp_route_accepted_for_parent_ref(
    route: &UdpRouteSnap,
    parent: &ParentRefSnap,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> bool {
    let keys = udp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
    if keys.is_empty() {
        return false;
    }
    for lk in &keys {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if !udp_route_listener_ok(route, eff) {
            continue;
        }
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{ParentRefSnap, ReferenceGrantSnap};

    #[test]
    fn gateway_secret_grant_same_namespace_implicit() {
        let grants: Vec<ReferenceGrantSnap> = vec![];
        assert!(reference_grant_permits_gateway_secret_ref(
            &grants,
            "ns-a",
            "ns-a",
            "tls-secret"
        ));
    }

    #[test]
    fn gateway_secret_grant_cross_namespace() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "ns-b".into(),
            name: "g".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "Gateway".into(),
                namespace: "ns-a".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("".into()),
                kind: "Secret".into(),
                name: Some("tls-secret".into()),
            }],
        }];
        assert!(reference_grant_permits_gateway_secret_ref(
            &grants,
            "ns-a",
            "ns-b",
            "tls-secret"
        ));
        assert!(!reference_grant_permits_gateway_secret_ref(
            &grants, "ns-a", "ns-b", "other"
        ));
    }

    #[test]
    fn inference_pool_grant_same_namespace_implicit() {
        let grants: Vec<ReferenceGrantSnap> = vec![];
        assert!(reference_grant_permits_inference_pool_ref(
            &grants,
            "ns-a",
            "ns-a",
            "pool-a",
            "HTTPRoute",
        ));
    }

    #[test]
    fn inference_pool_grant_cross_namespace_in_pool_ns() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "pool-ns".into(),
            name: "g".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "route-ns".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("inference.networking.k8s.io".into()),
                kind: "InferencePool".into(),
                name: Some("my-pool".into()),
            }],
        }];
        assert!(reference_grant_permits_inference_pool_ref(
            &grants,
            "route-ns",
            "pool-ns",
            "my-pool",
            "HTTPRoute",
        ));
        assert!(!reference_grant_permits_inference_pool_ref(
            &grants,
            "route-ns",
            "pool-ns",
            "other-pool",
            "HTTPRoute",
        ));
    }

    #[test]
    fn route_to_gateway_parent_grant_same_namespace() {
        let grants: Vec<ReferenceGrantSnap> = vec![];
        assert!(reference_grant_permits_route_to_gateway_parent(
            &grants,
            "default",
            "HTTPRoute",
            "default",
            "gw",
        ));
    }

    #[test]
    fn route_to_gateway_parent_grant_cross_namespace() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "infra".into(),
            name: "rg".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "team-a".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "Gateway".into(),
                name: Some("gw".into()),
            }],
        }];
        assert!(reference_grant_permits_route_to_gateway_parent(
            &grants,
            "team-a",
            "HTTPRoute",
            "infra",
            "gw",
        ));
        assert!(!reference_grant_permits_route_to_gateway_parent(
            &grants,
            "team-b",
            "HTTPRoute",
            "infra",
            "gw",
        ));
    }

    #[test]
    fn route_to_listener_set_parent_grant_cross_namespace() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "infra".into(),
            name: "rg".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "GRPCRoute".into(),
                namespace: "team-a".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "ListenerSet".into(),
                name: Some("shared".into()),
            }],
        }];
        assert!(reference_grant_permits_route_to_listener_set_parent(
            &grants,
            "team-a",
            "GRPCRoute",
            "infra",
            "shared",
        ));
        assert!(!reference_grant_permits_route_to_listener_set_parent(
            &grants,
            "team-a",
            "HTTPRoute",
            "infra",
            "shared",
        ));
    }

    #[test]
    fn tcp_listener_accepts_tcproute_when_route_kinds_unset() {
        use std::collections::BTreeMap;

        let labels = BTreeMap::new();
        assert!(listener_accepts_route_kind(
            "TCPRoute", "default", "default", "TCP", None, &labels,
        ));
        assert!(!listener_accepts_route_kind(
            "HTTPRoute",
            "default",
            "default",
            "TCP",
            None,
            &labels,
        ));
    }

    #[test]
    fn udp_listener_accepts_udproute_when_route_kinds_unset() {
        use std::collections::BTreeMap;

        let labels = BTreeMap::new();
        assert!(listener_accepts_route_kind(
            "UDPRoute", "default", "default", "UDP", None, &labels,
        ));
        assert!(!listener_accepts_route_kind(
            "HTTPRoute",
            "default",
            "default",
            "UDP",
            None,
            &labels,
        ));
    }

    #[test]
    fn service_grant_rejects_non_core_group_on_to() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "other".into(),
            name: "bad".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "HTTPRoute".into(),
                namespace: "default".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: Some("example.com".into()),
                kind: "Service".into(),
                name: None,
            }],
        }];
        assert!(!reference_grant_permits_service_ref(
            &grants,
            "default",
            "other",
            "svc",
            "HTTPRoute",
        ));
    }

    #[test]
    fn backend_tls_grant_allows_cross_namespace_configmap() {
        let grants = vec![ReferenceGrantSnap {
            namespace: "secrets".into(),
            name: "g".into(),
            from: vec![crate::snapshot::ReferenceGrantFromSnap {
                group: Some("gateway.networking.k8s.io".into()),
                kind: "BackendTLSPolicy".into(),
                namespace: "policies".into(),
            }],
            to: vec![crate::snapshot::ReferenceGrantToSnap {
                group: None,
                kind: "ConfigMap".into(),
                name: Some("ca-bundle".into()),
            }],
        }];
        assert!(reference_grant_permits_backend_tls_policy_resource_ref(
            &grants,
            "policies",
            "secrets",
            "ConfigMap",
            "ca-bundle",
        ));
        assert!(!reference_grant_permits_backend_tls_policy_resource_ref(
            &grants,
            "other",
            "secrets",
            "ConfigMap",
            "ca-bundle",
        ));
    }

    #[test]
    fn gateway_cross_namespace_parent_never_ref_not_permitted_without_grant() {
        use std::collections::HashSet;

        let mut snap = ClusterSnapshot::default();
        snap.gateway_namespace = "infra".into();
        snap.gateway_name = "gw".into();
        snap.reference_grants = vec![];
        let parent = ParentRefSnap {
            group: Some("gateway.networking.k8s.io".into()),
            kind: Some("Gateway".into()),
            namespace: Some("infra".into()),
            name: "gw".into(),
            section_name: None,
            port: None,
        };
        let attached_ls = HashSet::new();
        assert!(!route_parent_attachment_ref_not_permitted(
            &snap,
            "other-ns",
            "HTTPRoute",
            &parent,
            &attached_ls,
        ));
    }

    #[test]
    fn hostnames_intersect_route_wildcard_covers_listener_literal() {
        assert!(hostnames_intersect("*.specific.com", "very.specific.com"));
    }

    #[test]
    fn hostnames_intersect_listener_wildcard_covers_route_literal() {
        assert!(hostnames_intersect("foo.wildcard.io", "*.wildcard.io"));
    }

    #[test]
    fn hostnames_intersect_rejects_non_overlapping() {
        assert!(!hostnames_intersect("specific.but.wrong.com", "very.specific.com"));
    }

    #[test]
    fn wildcard_does_not_match_bare_domain() {
        assert!(!hostnames_intersect("wildcard.io", "*.wildcard.io"));
        assert!(!hostnames_intersect("*.wildcard.io", "wildcard.io"));
    }

    #[test]
    fn wildcard_matches_subdomain() {
        assert!(hostnames_intersect("foo.wildcard.io", "*.wildcard.io"));
        assert!(hostnames_intersect("*.wildcard.io", "foo.wildcard.io"));
        assert!(hostnames_intersect("foo.bar.wildcard.io", "*.wildcard.io"));
    }

    #[test]
    fn exact_hostnames_match() {
        assert!(hostnames_intersect("very.specific.com", "very.specific.com"));
    }

    #[test]
    fn wildcard_route_matches_specific_listener() {
        assert!(hostnames_intersect("*.specific.com", "very.specific.com"));
    }

    #[test]
    fn wildcard_route_matches_wildcard_listener() {
        assert!(hostnames_intersect("*.anotherwildcard.io", "*.anotherwildcard.io"));
    }
}
