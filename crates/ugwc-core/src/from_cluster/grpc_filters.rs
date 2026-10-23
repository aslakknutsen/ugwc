use std::collections::BTreeMap;

use super::grpcroute_obj_err;
use crate::compile::CompileReport;
use crate::ir::{NamedHeaderIr, RequestHeaderModifierIr, RouteMatchIr};
use crate::snapshot::{GrpcRouteMatchSnap, GrpcRouteRuleSnap, GrpcRouteSnap};

pub(super) fn validate_and_convert_grpc_matches(
    route: &GrpcRouteSnap,
    rule_idx: usize,
    matches: &[GrpcRouteMatchSnap],
    report: &mut CompileReport,
) -> Option<Vec<RouteMatchIr>> {
    let src: Vec<GrpcRouteMatchSnap> = if matches.is_empty() {
        vec![GrpcRouteMatchSnap::default()]
    } else {
        matches.to_vec()
    };
    let mut out = Vec::new();
    for (mi, m) in src.iter().enumerate() {
        let mut headers_exact = BTreeMap::new();
        for h in &m.headers {
            let mt = h.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: gRPC header match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            headers_exact.insert(h.name.to_ascii_lowercase(), h.value.clone());
        }

        let (grpc_service, grpc_method) = match &m.method {
            None => (None, None),
            Some(mm) => {
                let typ = mm.match_type.as_deref().unwrap_or("Exact");
                if typ == "RegularExpression" {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "UnsupportedValue: gRPC method RegularExpression (rule {rule_idx} match {mi})"
                        ),
                    ));
                    return None;
                }
                if typ != "Exact" && !typ.is_empty() {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "UnsupportedValue: gRPC method match type {typ} (rule {rule_idx} match {mi})"
                        ),
                    ));
                    return None;
                }
                (
                    mm.service.clone().filter(|s| !s.is_empty()),
                    mm.method.clone().filter(|s| !s.is_empty()),
                )
            }
        };

        out.push(RouteMatchIr {
            path_prefix: None,
            path_exact: None,
            headers_exact,
            method: None,
            query_params_exact: BTreeMap::new(),
            grpc_service,
            grpc_method,
        });
    }
    Some(out)
}

pub(super) fn extract_grpc_rule_filters(
    route: &GrpcRouteSnap,
    rule: &GrpcRouteRuleSnap,
    rule_idx: usize,
    report: &mut CompileReport,
) -> (
    Option<RequestHeaderModifierIr>,
    Option<RequestHeaderModifierIr>,
) {
    let mut hdr_mod: Option<RequestHeaderModifierIr> = None;
    let mut resp_hdr: Option<RequestHeaderModifierIr> = None;

    for (fi, f) in rule.filters.iter().enumerate() {
        match f.filter_type.as_str() {
            "RequestHeaderModifier" => {
                let Some(hm) = f.request_header_modifier.as_ref() else {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "InvalidReference: RequestHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                hdr_mod = Some(RequestHeaderModifierIr {
                    set: hm
                        .set
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    add: hm
                        .add
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    remove: hm.remove.clone(),
                });
            }
            "ResponseHeaderModifier" => {
                let Some(hm) = f.response_header_modifier.as_ref() else {
                    report.errors.push(grpcroute_obj_err(
                        route,
                        format!(
                            "InvalidReference: ResponseHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                resp_hdr = Some(RequestHeaderModifierIr {
                    set: hm
                        .set
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    add: hm
                        .add
                        .iter()
                        .map(|x| NamedHeaderIr {
                            name: x.name.clone(),
                            value: x.value.clone(),
                        })
                        .collect(),
                    remove: hm.remove.clone(),
                });
            }
            "RequestMirror" | "ExtensionRef" => {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: filter type {} (rule {rule_idx} filter {fi})",
                        f.filter_type
                    ),
                ));
            }
            other => {
                report.errors.push(grpcroute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {other} (rule {rule_idx})"),
                ));
            }
        }
    }
    (hdr_mod, resp_hdr)
}
