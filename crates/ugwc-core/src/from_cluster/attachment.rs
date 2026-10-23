use std::collections::{BTreeMap, HashSet};

use crate::compile::{
    grpc_route_accepted_condition_for_parent, http_route_accepted_condition_for_parent,
    tcp_route_accepted_condition_for_parent, tls_route_accepted_condition_for_parent,
    udp_route_accepted_condition_for_parent, CompileReport,
};
use crate::http_route_attachment::{
    attachment_keys_for_parent_ref, find_effective_for_key, grpc_attachment_keys_for_parent_ref,
    grpc_route_attachment_keys, grpc_route_listener_hostname_ok, listener_attachment_count_key,
    route_attachment_keys, route_listener_hostname_ok, tcp_attachment_keys_for_parent_ref,
    tcp_route_attachment_keys, tcp_route_listener_ok, tls_attachment_keys_for_parent_ref,
    tls_route_attachment_keys, tls_route_listener_hostname_ok, udp_attachment_keys_for_parent_ref,
    udp_route_attachment_keys, udp_route_listener_ok,
};
use crate::snapshot::{
    ClusterSnapshot, GrpcRouteSnap, HttpRouteSnap, TcpRouteSnap, TlsRouteSnap, UdpRouteSnap,
};

fn bump_listener_route_count(
    counts: &mut BTreeMap<String, u32>,
    lk: &crate::ir::ListenerAttachmentIr,
) {
    *counts.entry(listener_attachment_count_key(lk)).or_insert(0) += 1;
}

fn http_route_attachment_accepted_for_listener(
    route: &HttpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) =
            http_route_accepted_condition_for_parent(route, parent, snapshot, attached_ls, report);
        if st == "True" {
            return true;
        }
    }
    false
}

fn grpc_route_attachment_accepted_for_listener(
    route: &GrpcRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = grpc_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) =
            grpc_route_accepted_condition_for_parent(route, parent, snapshot, attached_ls, report);
        if st == "True" {
            return true;
        }
    }
    false
}

fn tls_route_attachment_accepted_for_listener(
    route: &TlsRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = tls_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) =
            tls_route_accepted_condition_for_parent(route, parent, snapshot, attached_ls, report);
        if st == "True" {
            return true;
        }
    }
    false
}

fn tcp_route_attachment_accepted_for_listener(
    route: &TcpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = tcp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) =
            tcp_route_accepted_condition_for_parent(route, parent, snapshot, attached_ls, report);
        if st == "True" {
            return true;
        }
    }
    false
}

fn udp_route_attachment_accepted_for_listener(
    route: &UdpRouteSnap,
    lk: &crate::ir::ListenerAttachmentIr,
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> bool {
    for parent in &route.parent_refs {
        let keys = udp_attachment_keys_for_parent_ref(route, parent, snapshot, attached_ls);
        if !keys.iter().any(|k| k == lk) {
            continue;
        }
        let (st, _, _) =
            udp_route_accepted_condition_for_parent(route, parent, snapshot, attached_ls, report);
        if st == "True" {
            return true;
        }
    }
    false
}

/// Gateway API `attachedRoutes`: count each route resource once per listener only when it would be
/// **Accepted** there (hostname intersection + HTTP/GRPC/TLS/TCP/UDP precedence), not merely policy-attached.
pub(super) fn compute_listener_route_attachment_counts(
    snapshot: &ClusterSnapshot,
    attached_ls: &HashSet<(String, String)>,
    report: &CompileReport,
) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();

    for route in &snapshot.http_routes {
        for lk in route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.http_grpc_conflict_loser_at("HTTPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !http_route_attachment_accepted_for_listener(
                route,
                &lk,
                snapshot,
                attached_ls,
                report,
            ) {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.grpc_routes {
        for lk in grpc_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !grpc_route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.http_grpc_conflict_loser_at("GRPCRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !grpc_route_attachment_accepted_for_listener(
                route,
                &lk,
                snapshot,
                attached_ls,
                report,
            ) {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.tls_routes {
        for lk in tls_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted {
                continue;
            }
            if !tls_route_listener_hostname_ok(route, eff) {
                continue;
            }
            if report.tls_route_conflict_loser_at("TLSRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !tls_route_attachment_accepted_for_listener(
                route,
                &lk,
                snapshot,
                attached_ls,
                report,
            ) {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.tcp_routes {
        for lk in tcp_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted || !tcp_route_listener_ok(route, eff) {
                continue;
            }
            if report.tcp_route_conflict_loser_at("TCPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !tcp_route_attachment_accepted_for_listener(
                route,
                &lk,
                snapshot,
                attached_ls,
                report,
            ) {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    for route in &snapshot.udp_routes {
        for lk in udp_route_attachment_keys(route, snapshot, attached_ls) {
            let Some(eff) = find_effective_for_key(snapshot, &lk) else {
                continue;
            };
            if eff.conflicted || !udp_route_listener_ok(route, eff) {
                continue;
            }
            if report.udp_route_conflict_loser_at("UDPRoute", &route.namespace, &route.name, &lk) {
                continue;
            }
            if !udp_route_attachment_accepted_for_listener(
                route,
                &lk,
                snapshot,
                attached_ls,
                report,
            ) {
                continue;
            }
            bump_listener_route_count(&mut counts, &lk);
        }
    }
    counts
}
