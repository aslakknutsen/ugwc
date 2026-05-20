use ugwc_core::snapshot::ParentRefSnap;

use crate::resources::{
    GrpcRouteParentRefs, HttpRouteParentRefs, ListenerSetParentRef, TcpRouteParentRefs,
    TlsRouteParentRefs, UdpRouteParentRefs,
};

use super::snap_u16_port;

#[must_use]
pub(super) fn parent_ref_snap_from_route_fields(
    group: Option<String>,
    kind: Option<String>,
    namespace: Option<String>,
    name: String,
    section_name: Option<String>,
    port: Option<i32>,
) -> ParentRefSnap {
    ParentRefSnap {
        group,
        kind,
        namespace,
        name,
        section_name,
        port: snap_u16_port(port),
    }
}

/// Convert spec parentRef to snapshot form (HTTPRoute status merge).
#[must_use]
pub fn parent_reference_to_snap(p: &HttpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

pub(super) fn tls_parent_ref_to_snap(p: &TlsRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

pub(super) fn grpc_parent_ref_to_snap(p: &GrpcRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

pub(super) fn tcp_parent_ref_to_snap(p: &TcpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

pub(super) fn udp_parent_ref_to_snap(p: &UdpRouteParentRefs) -> ParentRefSnap {
    parent_ref_snap_from_route_fields(
        p.group.clone(),
        p.kind.clone(),
        p.namespace.clone(),
        p.name.clone(),
        p.section_name.clone(),
        p.port,
    )
}

#[must_use]
pub(super) fn listenerset_parent_ref_to_snap(p: &ListenerSetParentRef) -> ParentRefSnap {
    ParentRefSnap {
        group: p.group.clone(),
        kind: p.kind.clone(),
        namespace: p.namespace.clone(),
        name: p.name.clone(),
        section_name: None,
        port: None,
    }
}
