//! Merge `Gateway.spec.listeners` with attached [`crate::snapshot::ListenerSetSnap`] rows.

use std::collections::HashMap;

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
            allowed_routes: l.allowed_routes.clone(),
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
                allowed_routes: l.allowed_routes.clone(),
            });
        }
    }

    mark_distinctness_conflicts(&mut out);
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
fn mark_distinctness_conflicts(effective: &mut [EffectiveListenerSnap]) {
    let mut groups: HashMap<(u16, String, String), Vec<usize>> = HashMap::new();
    for (i, e) in effective.iter().enumerate() {
        let key = (e.port, e.protocol.clone(), hostname_norm(&e.hostname));
        groups.entry(key).or_default().push(i);
    }
    for idxs in groups.values() {
        if idxs.len() > 1 {
            for &i in idxs {
                effective[i].conflicted = true;
            }
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
            },
            GatewayListenerSnap {
                name: "y".into(),
                port: 80,
                protocol: "HTTP".into(),
                hostname: Some("h.example.com".into()),
                allowed_routes: None,
            },
        ];
        let m = merge_effective_listeners(&gw, &[]);
        assert!(m[0].conflicted);
        assert!(m[1].conflicted);
    }
}
