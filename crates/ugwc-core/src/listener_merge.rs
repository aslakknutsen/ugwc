//! Merge `Gateway.spec.listeners` with attached [`crate::snapshot::ListenerSetSnap`] rows.

use std::collections::{HashMap, HashSet};

use zeroize::Zeroizing;

use crate::snapshot::{
    EffectiveListenerSnap, GatewayListenerSnap, ListenerParentSnap, ListenerSetSnap,
};

#[must_use]
pub fn merge_effective_listeners(
    gateway_listeners: &[GatewayListenerSnap],
    listener_sets: &[ListenerSetSnap],
) -> Vec<EffectiveListenerSnap> {
    let mut ordered_sets: Vec<&ListenerSetSnap> = listener_sets.iter().collect();
    ordered_sets.sort_by(|a, b| {
        let ta = a.creation_timestamp_rfc3339.as_deref().unwrap_or("");
        let tb = b.creation_timestamp_rfc3339.as_deref().unwrap_or("");
        ta.cmp(tb)
            .then_with(|| a.namespace.cmp(&b.namespace))
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.uid.cmp(&b.uid))
    });

    let mut out: Vec<EffectiveListenerSnap> = gateway_listeners
        .iter()
        .map(|l| EffectiveListenerSnap {
            parent: ListenerParentSnap::InlineGateway,
            section_name: l.name.clone(),
            port: l.port,
            protocol: normalize_protocol(&l.protocol),
            hostname: l.hostname.clone(),
            conflicted: false,
            conflict_reason: None,
            allowed_routes: l.allowed_routes.clone(),
            tls: l.tls.clone(),
            tls_cert_pem: Vec::new(),
            tls_key_pem: Zeroizing::new(Vec::new()),
            tls_resolve_error: None,
        })
        .collect();

    for ls in ordered_sets {
        for l in &ls.listeners {
            out.push(EffectiveListenerSnap {
                parent: ListenerParentSnap::ListenerSet {
                    namespace: ls.namespace.clone(),
                    name: ls.name.clone(),
                },
                section_name: l.name.clone(),
                port: l.port,
                protocol: normalize_protocol(&l.protocol),
                hostname: l.hostname.clone(),
                conflicted: false,
                conflict_reason: None,
                allowed_routes: l.allowed_routes.clone(),
                tls: l.tls.clone(),
                tls_cert_pem: Vec::new(),
                tls_key_pem: Zeroizing::new(Vec::new()),
                tls_resolve_error: None,
            });
        }
    }

    mark_distinctness_conflicts(&mut out);
    mark_tcp_layer7_port_conflicts(&mut out);
    // Gateway API: treat UDP listeners sharing a port with HTTP/HTTPS/TLS like TCP+L7 (indistinct).
    mark_udp_layer7_port_conflicts(&mut out);
    out
}

fn normalize_protocol(p: &str) -> String {
    let t = p.trim();
    if t.is_empty() {
        "HTTP".into()
    } else {
        t.to_ascii_uppercase()
    }
}

fn hostname_norm(h: &Option<String>) -> String {
    h.as_deref().unwrap_or("").trim().to_ascii_lowercase()
}

/// Distinctness for HTTP-like listeners: duplicate `(port, protocol, hostname)` ⇒ conflicted.
///
/// Precedence: inline Gateway listeners beat ListenerSet listeners.  Within
/// ListenerSets the order in `effective` (oldest-first) determines who wins.
/// When ALL members of a collision group are `InlineGateway`, every member is
/// conflicted (Gateway spec violation — no precedence rescue).
fn mark_distinctness_conflicts(effective: &mut [EffectiveListenerSnap]) {
    let mut groups: HashMap<(u16, String, String), Vec<usize>> = HashMap::new();
    for (i, e) in effective.iter().enumerate() {
        let key = (e.port, e.protocol.clone(), hostname_norm(&e.hostname));
        groups.entry(key).or_default().push(i);
    }
    for idxs in groups.values() {
        if idxs.len() <= 1 {
            continue;
        }
        let inline_count = idxs
            .iter()
            .filter(|&&i| matches!(effective[i].parent, ListenerParentSnap::InlineGateway))
            .count();
        if inline_count >= 2 {
            for &i in idxs {
                effective[i].conflicted = true;
                if effective[i].conflict_reason.is_none() {
                    effective[i].conflict_reason = Some("HostnameConflict".into());
                }
            }
        } else if inline_count == 1 {
            for &i in idxs {
                if !matches!(effective[i].parent, ListenerParentSnap::InlineGateway) {
                    effective[i].conflicted = true;
                    if effective[i].conflict_reason.is_none() {
                        effective[i].conflict_reason = Some("HostnameConflict".into());
                    }
                }
            }
        } else {
            // All ListenerSet — first wins, rest conflicted.
            for &i in &idxs[1..] {
                effective[i].conflicted = true;
                if effective[i].conflict_reason.is_none() {
                    effective[i].conflict_reason = Some("HostnameConflict".into());
                }
            }
        }
    }
}

/// Gateway API: TCP listeners sharing a port with HTTP/HTTPS/TLS listeners are indistinct.
///
/// Precedence: if the Gateway's own (inline) listeners all belong to one protocol
/// family and only ListenerSet listeners introduce the opposing family, the
/// inline family wins and only the opposing listeners are conflicted.  When the
/// Gateway itself has both families on the same port, *all* listeners on that
/// port are conflicted.
fn mark_tcp_layer7_port_conflicts(effective: &mut [EffectiveListenerSnap]) {
    let mut tcp_ports: HashSet<u16> = HashSet::new();
    let mut l7_ports: HashSet<u16> = HashSet::new();
    for e in effective.iter() {
        match e.protocol.as_str() {
            "TCP" => {
                tcp_ports.insert(e.port);
            }
            "HTTP" | "HTTPS" | "TLS" => {
                l7_ports.insert(e.port);
            }
            _ => {}
        }
    }
    let conflict_ports: HashSet<u16> = tcp_ports.intersection(&l7_ports).copied().collect();
    if conflict_ports.is_empty() {
        return;
    }
    // Pre-compute per-port: does the Gateway itself have TCP / L7 inline,
    // and what is the first protocol family (for all-ListenerSet fallback).
    let mut port_info: HashMap<u16, (bool, bool, bool)> = HashMap::new(); // (inline_tcp, inline_l7, first_is_l7)
    for &port in &conflict_ports {
        let inline_has_tcp = effective.iter().any(|e| {
            e.port == port
                && e.protocol == "TCP"
                && matches!(e.parent, ListenerParentSnap::InlineGateway)
        });
        let inline_has_l7 = effective.iter().any(|e| {
            e.port == port
                && matches!(e.protocol.as_str(), "HTTP" | "HTTPS" | "TLS")
                && matches!(e.parent, ListenerParentSnap::InlineGateway)
        });
        let first_is_l7 = effective
            .iter()
            .find(|x| {
                x.port == port
                    && (x.protocol == "TCP"
                        || matches!(x.protocol.as_str(), "HTTP" | "HTTPS" | "TLS"))
            })
            .map(|x| matches!(x.protocol.as_str(), "HTTP" | "HTTPS" | "TLS"))
            .unwrap_or(true);
        port_info.insert(port, (inline_has_tcp, inline_has_l7, first_is_l7));
    }
    for e in effective.iter_mut() {
        let Some(&(inline_has_tcp, inline_has_l7, first_is_l7)) = port_info.get(&e.port) else {
            continue;
        };
        let is_tcp = e.protocol == "TCP";
        let is_l7 = matches!(e.protocol.as_str(), "HTTP" | "HTTPS" | "TLS");
        if !is_tcp && !is_l7 {
            continue;
        }
        let mark = if inline_has_tcp && inline_has_l7 {
            true
        } else if inline_has_l7 && is_tcp {
            true
        } else if inline_has_tcp && is_l7 {
            true
        } else if !inline_has_tcp && !inline_has_l7 {
            (first_is_l7 && is_tcp) || (!first_is_l7 && is_l7)
        } else {
            false
        };
        if mark {
            e.conflicted = true;
            e.conflict_reason = Some("ProtocolConflict".into());
        }
    }
}

/// Gateway API: UDP listeners sharing a port with HTTP/HTTPS/TLS listeners are indistinct (mirror TCP+L7).
fn mark_udp_layer7_port_conflicts(effective: &mut [EffectiveListenerSnap]) {
    let mut udp_ports: HashSet<u16> = HashSet::new();
    let mut l7_ports: HashSet<u16> = HashSet::new();
    for e in effective.iter() {
        match e.protocol.as_str() {
            "UDP" => {
                udp_ports.insert(e.port);
            }
            "HTTP" | "HTTPS" | "TLS" => {
                l7_ports.insert(e.port);
            }
            _ => {}
        }
    }
    let conflict_ports: HashSet<u16> = udp_ports.intersection(&l7_ports).copied().collect();
    if conflict_ports.is_empty() {
        return;
    }
    let mut port_info: HashMap<u16, (bool, bool, bool)> = HashMap::new();
    for &port in &conflict_ports {
        let inline_has_udp = effective.iter().any(|e| {
            e.port == port
                && e.protocol == "UDP"
                && matches!(e.parent, ListenerParentSnap::InlineGateway)
        });
        let inline_has_l7 = effective.iter().any(|e| {
            e.port == port
                && matches!(e.protocol.as_str(), "HTTP" | "HTTPS" | "TLS")
                && matches!(e.parent, ListenerParentSnap::InlineGateway)
        });
        let first_is_l7 = effective
            .iter()
            .find(|x| {
                x.port == port
                    && (x.protocol == "UDP"
                        || matches!(x.protocol.as_str(), "HTTP" | "HTTPS" | "TLS"))
            })
            .map(|x| matches!(x.protocol.as_str(), "HTTP" | "HTTPS" | "TLS"))
            .unwrap_or(true);
        port_info.insert(port, (inline_has_udp, inline_has_l7, first_is_l7));
    }
    for e in effective.iter_mut() {
        let Some(&(inline_has_udp, inline_has_l7, first_is_l7)) = port_info.get(&e.port) else {
            continue;
        };
        let is_udp = e.protocol == "UDP";
        let is_l7 = matches!(e.protocol.as_str(), "HTTP" | "HTTPS" | "TLS");
        if !is_udp && !is_l7 {
            continue;
        }
        let mark = if inline_has_udp && inline_has_l7 {
            true
        } else if inline_has_l7 && is_udp {
            true
        } else if inline_has_udp && is_l7 {
            true
        } else if !inline_has_udp && !inline_has_l7 {
            (first_is_l7 && is_udp) || (!first_is_l7 && is_l7)
        } else {
            false
        };
        if mark {
            e.conflicted = true;
            e.conflict_reason = Some("ProtocolConflict".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::ListenerSetSnap;

    #[test]
    fn ordering_listener_sets_oldest_first_then_name() {
        let gw = vec![GatewayListenerSnap {
            name: "a".into(),
            port: 80,
            protocol: "HTTP".into(),
            hostname: None,
            allowed_routes: None,
            tls: None,
        }];
        let ls = vec![
            ListenerSetSnap {
                namespace: "ns".into(),
                name: "z".into(),
                uid: "1".into(),
                creation_timestamp_rfc3339: Some("2020-01-02T00:00:00Z".into()),
                parent_refs: vec![],
                listeners: vec![GatewayListenerSnap {
                    name: "z1".into(),
                    port: 81,
                    protocol: "HTTP".into(),
                    hostname: None,
                    allowed_routes: None,
                    tls: None,
                }],
            },
            ListenerSetSnap {
                namespace: "ns".into(),
                name: "a".into(),
                uid: "2".into(),
                creation_timestamp_rfc3339: Some("2020-01-01T00:00:00Z".into()),
                parent_refs: vec![],
                listeners: vec![GatewayListenerSnap {
                    name: "a1".into(),
                    port: 82,
                    protocol: "HTTP".into(),
                    hostname: None,
                    allowed_routes: None,
                    tls: None,
                }],
            },
        ];
        let m = merge_effective_listeners(&gw, &ls);
        assert_eq!(m.len(), 3);
        assert!(matches!(m[0].parent, ListenerParentSnap::InlineGateway));
        assert_eq!(m[1].section_name, "a1");
        assert_eq!(m[2].section_name, "z1");
    }

    #[test]
    fn conflict_marks_duplicate_port_proto_host() {
        let gw = vec![
            GatewayListenerSnap {
                name: "x".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: Some("h.example.com".into()),
                allowed_routes: None,
                tls: None,
            },
            GatewayListenerSnap {
                name: "y".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: Some("h.example.com".into()),
                allowed_routes: None,
                tls: None,
            },
        ];
        let m = merge_effective_listeners(&gw, &[]);
        assert!(m[0].conflicted);
        assert!(m[1].conflicted);
    }

    #[test]
    fn tcp_and_https_same_port_conflict() {
        let gw = vec![
            GatewayListenerSnap {
                name: "tcp".into(),
                port: 443,
                protocol: "TCP".into(),
                hostname: None,
                allowed_routes: None,
                tls: None,
            },
            GatewayListenerSnap {
                name: "https".into(),
                port: 443,
                protocol: "HTTPS".into(),
                hostname: None,
                allowed_routes: None,
                tls: None,
            },
        ];
        let m = merge_effective_listeners(&gw, &[]);
        assert!(m.iter().all(|e| e.conflicted), "{m:?}");
        assert!(
            m.iter()
                .all(|e| e.conflict_reason.as_deref() == Some("ProtocolConflict")),
            "TCP+HTTPS should be ProtocolConflict: {m:?}"
        );
    }

    #[test]
    fn udp_and_https_same_port_conflict() {
        let gw = vec![
            GatewayListenerSnap {
                name: "udp".into(),
                port: 443,
                protocol: "UDP".into(),
                hostname: None,
                allowed_routes: None,
                tls: None,
            },
            GatewayListenerSnap {
                name: "https".into(),
                port: 443,
                protocol: "HTTPS".into(),
                hostname: None,
                allowed_routes: None,
                tls: None,
            },
        ];
        let m = merge_effective_listeners(&gw, &[]);
        assert!(m.iter().all(|e| e.conflicted), "{m:?}");
        assert!(
            m.iter()
                .all(|e| e.conflict_reason.as_deref() == Some("ProtocolConflict")),
            "UDP+HTTPS should be ProtocolConflict: {m:?}"
        );
    }

    #[test]
    fn hostname_conflict_reason() {
        let gw = vec![
            GatewayListenerSnap {
                name: "a".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: Some("example.com".into()),
                allowed_routes: None,
                tls: None,
            },
            GatewayListenerSnap {
                name: "b".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: Some("example.com".into()),
                allowed_routes: None,
                tls: None,
            },
        ];
        let m = merge_effective_listeners(&gw, &[]);
        assert!(m.iter().all(|e| e.conflicted), "{m:?}");
        assert!(
            m.iter()
                .all(|e| e.conflict_reason.as_deref() == Some("HostnameConflict")),
            "duplicate hostname should be HostnameConflict: {m:?}"
        );
    }
}
