use std::cmp::Ordering;
use std::collections::HashSet;

use super::normalize_protocol;
use crate::compile::{CompileReport, HttpGrpcConflictLoser, ObjectRefErr};
use crate::http_route_attachment::{
    find_effective_for_key, grpc_route_attachment_keys, grpc_route_listener_hostname_ok,
    intersect_route_listener_hosts, route_attachment_keys, route_listener_hostname_ok,
    tcp_route_attachment_keys, tcp_route_listener_ok, tls_route_attachment_keys,
    tls_route_listener_hostname_ok, udp_route_attachment_keys, udp_route_listener_ok,
};
use crate::snapshot::{
    ClusterSnapshot, EffectiveListenerSnap, GrpcRouteSnap, HttpRouteSnap, ListenerParentSnap,
    TcpRouteSnap, TlsRouteSnap, UdpRouteSnap,
};

pub(super) fn record_listener_conflicts(snapshot: &ClusterSnapshot, report: &mut CompileReport) {
    let mut seen_gateway = false;
    let mut seen_ls: HashSet<(String, String)> = HashSet::new();
    for e in &snapshot.effective_listeners {
        if !e.conflicted {
            continue;
        }
        match &e.parent {
            ListenerParentSnap::InlineGateway => {
                if !seen_gateway {
                    seen_gateway = true;
                    report.errors.push(ObjectRefErr {
                        group: "gateway.networking.k8s.io".into(),
                        version: "v1".into(),
                        kind: "Gateway".into(),
                        namespace: snapshot.gateway_namespace.clone(),
                        name: snapshot.gateway_name.clone(),
                        reason: "ListenersConflicted".into(),
                    });
                }
            }
            ListenerParentSnap::ListenerSet { namespace, name } => {
                if seen_ls.insert((namespace.clone(), name.clone())) {
                    report.errors.push(ObjectRefErr {
                        group: "gateway.networking.k8s.io".into(),
                        version: "v1".into(),
                        kind: "ListenerSet".into(),
                        namespace: namespace.clone(),
                        name: name.clone(),
                        reason: "ListenersConflicted".into(),
                    });
                }
            }
        }
    }
}

pub(super) fn hosts_for_grpc_route_on_listener(
    route: &GrpcRouteSnap,
    eff: &EffectiveListenerSnap,
) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

pub(super) fn hosts_for_tls_route_on_listener(
    route: &TlsRouteSnap,
    eff: &EffectiveListenerSnap,
) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

fn host_sets_overlap(a: &[String], b: &[String]) -> bool {
    if a.is_empty() || b.is_empty() {
        return true;
    }
    a.iter().any(|x| b.iter().any(|y| x == y))
}

fn grpc_route_loses_http_conflict(h: &HttpRouteSnap, g: &GrpcRouteSnap) -> bool {
    match h
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(g.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (h.namespace.as_str(), h.name.as_str()) < (g.namespace.as_str(), g.name.as_str())
        }
    }
}

fn http_route_loses_grpc_conflict(h: &HttpRouteSnap, g: &GrpcRouteSnap) -> bool {
    match h
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(g.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => false,
        Ordering::Greater => true,
        Ordering::Equal => {
            (h.namespace.as_str(), h.name.as_str()) > (g.namespace.as_str(), g.name.as_str())
        }
    }
}

pub(super) fn compute_http_grpc_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.http_routes {
        for k in route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }
    for r in &snapshot.grpc_routes {
        for k in grpc_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }

        let http_on: Vec<&HttpRouteSnap> = snapshot
            .http_routes
            .iter()
            .filter(|r| {
                route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && route_listener_hostname_ok(r, eff)
            })
            .collect();

        let grpc_on: Vec<&GrpcRouteSnap> = snapshot
            .grpc_routes
            .iter()
            .filter(|r| {
                grpc_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && grpc_route_listener_hostname_ok(r, eff)
            })
            .collect();

        for h in &http_on {
            for g in &grpc_on {
                let hh = hosts_for_route_on_listener(h, eff);
                let gh = hosts_for_grpc_route_on_listener(g, eff);
                if !host_sets_overlap(&hh, &gh) {
                    continue;
                }
                if grpc_route_loses_http_conflict(h, g) {
                    losers.push(HttpGrpcConflictLoser {
                        route_kind: "GRPCRoute".into(),
                        route_namespace: g.namespace.clone(),
                        route_name: g.name.clone(),
                        listener: lk.clone(),
                    });
                } else if http_route_loses_grpc_conflict(h, g) {
                    losers.push(HttpGrpcConflictLoser {
                        route_kind: "HTTPRoute".into(),
                        route_namespace: h.namespace.clone(),
                        route_name: h.name.clone(),
                        listener: lk.clone(),
                    });
                }
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

fn tls_route_hostname_specificity_score(host: &str) -> u64 {
    let h = host.trim();
    if let Some(rest) = h.strip_prefix("*.") {
        return 1_000 + rest.len() as u64;
    }
    10_000 + h.len() as u64
}

fn tls_route_max_score_on_listener(route: &TlsRouteSnap, eff: &EffectiveListenerSnap) -> u64 {
    let hosts = hosts_for_tls_route_on_listener(route, eff);
    hosts
        .iter()
        .map(|h| tls_route_hostname_specificity_score(h))
        .max()
        .unwrap_or(0)
}

/// Returns true if route `a` has precedence over route `b` on this listener (overlapping SNIs).
fn tls_route_beats(a: &TlsRouteSnap, b: &TlsRouteSnap, eff: &EffectiveListenerSnap) -> bool {
    let sa = tls_route_max_score_on_listener(a, eff);
    let sb = tls_route_max_score_on_listener(b, eff);
    match sa.cmp(&sb) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => match a
            .creation_timestamp_rfc3339
            .as_deref()
            .unwrap_or("")
            .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
        {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
            }
        },
    }
}

pub(super) fn compute_tls_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.tls_routes {
        for k in tls_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "TLS" {
            continue;
        }

        let routes_on: Vec<&TlsRouteSnap> = snapshot
            .tls_routes
            .iter()
            .filter(|r| {
                tls_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && tls_route_listener_hostname_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                let hi = hosts_for_tls_route_on_listener(ri, eff);
                let hj = hosts_for_tls_route_on_listener(rj, eff);
                if !host_sets_overlap(&hi, &hj) {
                    continue;
                }
                if tls_route_beats(rj, ri, eff) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "TLSRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

/// Returns true if TCPRoute `a` has precedence over `b` on the same TCP listener (older wins; tie-break name).
fn tcp_route_beats(a: &TcpRouteSnap, b: &TcpRouteSnap) -> bool {
    match a
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
        }
    }
}

pub(super) fn compute_tcp_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.tcp_routes {
        for k in tcp_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "TCP" {
            continue;
        }

        let routes_on: Vec<&TcpRouteSnap> = snapshot
            .tcp_routes
            .iter()
            .filter(|r| {
                tcp_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && tcp_route_listener_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                if tcp_route_beats(rj, ri) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "TCPRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

/// Returns true if UDPRoute `a` has precedence over `b` on the same UDP listener (older wins; tie-break name).
fn udp_route_beats(a: &UdpRouteSnap, b: &UdpRouteSnap) -> bool {
    match a
        .creation_timestamp_rfc3339
        .as_deref()
        .unwrap_or("")
        .cmp(b.creation_timestamp_rfc3339.as_deref().unwrap_or(""))
    {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (a.namespace.as_str(), a.name.as_str()) < (b.namespace.as_str(), b.name.as_str())
        }
    }
}

pub(super) fn compute_udp_route_conflict_losers(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
) -> Vec<HttpGrpcConflictLoser> {
    let mut all_lk: HashSet<crate::ir::ListenerAttachmentIr> = HashSet::new();
    for r in &snapshot.udp_routes {
        for k in udp_route_attachment_keys(r, snapshot, attached_ls) {
            all_lk.insert(k);
        }
    }

    let mut losers: Vec<HttpGrpcConflictLoser> = Vec::new();

    for lk in &all_lk {
        let Some(eff) = find_effective_for_key(snapshot, lk) else {
            continue;
        };
        if eff.conflicted {
            continue;
        }
        if normalize_protocol(&eff.protocol) != "UDP" {
            continue;
        }

        let routes_on: Vec<&UdpRouteSnap> = snapshot
            .udp_routes
            .iter()
            .filter(|r| {
                udp_route_attachment_keys(r, snapshot, attached_ls).contains(lk)
                    && udp_route_listener_ok(r, eff)
            })
            .collect();

        for ri in &routes_on {
            let mut loses = false;
            for rj in &routes_on {
                if ri.namespace == rj.namespace && ri.name == rj.name {
                    continue;
                }
                if udp_route_beats(rj, ri) {
                    loses = true;
                    break;
                }
            }
            if loses {
                losers.push(HttpGrpcConflictLoser {
                    route_kind: "UDPRoute".into(),
                    route_namespace: ri.namespace.clone(),
                    route_name: ri.name.clone(),
                    listener: lk.clone(),
                });
            }
        }
    }

    losers.sort_by(|a, b| {
        (
            &a.route_kind,
            &a.route_namespace,
            &a.route_name,
            &a.listener,
        )
            .cmp(&(
                &b.route_kind,
                &b.route_namespace,
                &b.route_name,
                &b.listener,
            ))
    });
    losers.dedup_by(|a, b| {
        a.route_kind == b.route_kind
            && a.route_namespace == b.route_namespace
            && a.route_name == b.route_name
            && a.listener == b.listener
    });
    losers
}

pub(super) fn hosts_for_route_on_listener(route: &HttpRouteSnap, eff: &EffectiveListenerSnap) -> Vec<String> {
    let Some(intersected) = intersect_route_listener_hosts(&route.hostnames, &eff.hostname) else {
        return Vec::new();
    };
    if intersected.is_empty() {
        Vec::new()
    } else {
        intersected
    }
}

