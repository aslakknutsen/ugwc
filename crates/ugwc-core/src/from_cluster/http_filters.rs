use std::collections::BTreeMap;

use super::httproute_obj_err;
use crate::compile::CompileReport;
use crate::ir::{NamedHeaderIr, RequestHeaderModifierIr, RequestRedirectIr, RouteMatchIr};
use crate::snapshot::{HttpRequestMatchSnap, HttpRouteRuleSnap, HttpRouteSnap};

/// Gateway API match specificity score (GEP-1713). Higher = more specific = should sort earlier.
/// Computed per match clause; for a rule with multiple OR'd matches, use the maximum.
///
/// The tuple order matches the spec's precedence list:
///   1. Exact path > Prefix path
///   2. Longest path value
///   3. Method match
///   4. Largest number of header matches
///   5. Largest number of query param matches
pub(super) fn match_specificity(m: &RouteMatchIr) -> (u8, usize, u8, usize, usize) {
    let path_type_score = if m.path_exact.is_some() {
        3u8
    } else if m.path_prefix.is_some() {
        2
    } else {
        1
    };
    let path_len = m
        .path_exact
        .as_ref()
        .or(m.path_prefix.as_ref())
        .map_or(0, |p| p.len());
    let method_score = if m.method.is_some() { 1u8 } else { 0 };
    let header_count = m.headers_exact.len();
    let query_count = m.query_params_exact.len();
    (
        path_type_score,
        path_len,
        method_score,
        header_count,
        query_count,
    )
}

/// Max specificity across all match clauses in a rule (higher = more specific).
pub(super) fn rule_specificity(matches: &[RouteMatchIr]) -> (u8, usize, u8, usize, usize) {
    matches
        .iter()
        .map(match_specificity)
        .max()
        .unwrap_or((1, 1, 0, 0, 0))
}

pub(super) fn validate_and_convert_matches(
    route: &HttpRouteSnap,
    rule_idx: usize,
    matches: &[HttpRequestMatchSnap],
    report: &mut CompileReport,
) -> Option<Vec<RouteMatchIr>> {
    let src: Vec<HttpRequestMatchSnap> = if matches.is_empty() {
        vec![HttpRequestMatchSnap::default()]
    } else {
        matches.to_vec()
    };
    let mut out = Vec::new();
    for (mi, m) in src.iter().enumerate() {
        let mut headers_exact = BTreeMap::new();
        for h in &m.headers {
            let mt = h.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: header match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            headers_exact.insert(h.name.to_ascii_lowercase(), h.value.clone());
        }

        let (path_prefix, path_exact) = match &m.path {
            None => (Some("/".into()), None),
            Some(p) => {
                let typ = p.path_type.as_deref().unwrap_or("PathPrefix");
                let val = p.value.clone().unwrap_or_else(|| "/".into());
                match typ {
                    "Exact" => (None, Some(val)),
                    "PathPrefix" => (Some(val), None),
                    other => {
                        report.errors.push(httproute_obj_err(
                            route,
                            format!("UnsupportedValue: path match type {other}"),
                        ));
                        return None;
                    }
                }
            }
        };
        let mut query_params_exact = BTreeMap::new();
        for qp in &m.query_params {
            let mt = qp.match_type.as_deref().unwrap_or("Exact");
            if mt != "Exact" {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: query param match type {mt} (rule {rule_idx} match {mi})"
                    ),
                ));
                return None;
            }
            query_params_exact.insert(qp.name.clone(), qp.value.clone());
        }

        out.push(RouteMatchIr {
            path_prefix,
            path_exact,
            headers_exact,
            method: m.method.clone(),
            query_params_exact,
            grpc_service: None,
            grpc_method: None,
        });
    }
    Some(out)
}

pub(super) struct HttpRuleFilters {
    pub(super) redirect: Option<RequestRedirectIr>,
    pub(super) request_header_modifier: Option<RequestHeaderModifierIr>,
    pub(super) response_header_modifier: Option<RequestHeaderModifierIr>,
    pub(super) url_rewrite: Option<crate::ir::UrlRewriteIr>,
    pub(super) cors: Option<crate::ir::CorsFilterIr>,
}

fn header_filter_snap_to_ir(hm: &crate::snapshot::HttpHeaderFilterSnap) -> RequestHeaderModifierIr {
    RequestHeaderModifierIr {
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
    }
}

pub(super) fn extract_rule_filters(
    route: &HttpRouteSnap,
    rule: &HttpRouteRuleSnap,
    rule_idx: usize,
    report: &mut CompileReport,
) -> HttpRuleFilters {
    let mut redirect: Option<RequestRedirectIr> = None;
    let mut hdr_mod: Option<RequestHeaderModifierIr> = None;
    let mut resp_hdr: Option<RequestHeaderModifierIr> = None;
    let mut url_rewrite: Option<crate::ir::UrlRewriteIr> = None;
    let mut cors: Option<crate::ir::CorsFilterIr> = None;

    for (fi, f) in rule.filters.iter().enumerate() {
        match f.filter_type.as_str() {
            "RequestRedirect" => {
                let Some(rr) = f.request_redirect.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!("InvalidReference: RequestRedirect filter missing body (rule {rule_idx} filter {fi})"),
                    ));
                    continue;
                };
                let (path_replace_full, path_replace_prefix) = match &rr.path {
                    None => (None, None),
                    Some(pm) => match pm.modifier_type.as_str() {
                        "ReplaceFullPath" => (pm.replace_full_path.clone(), None),
                        "ReplacePrefixMatch" => (None, pm.replace_prefix_match.clone()),
                        _ => {
                            report.errors.push(httproute_obj_err(
                                route,
                                format!(
                                    "UnsupportedValue: redirect path modifier {} (rule {rule_idx})",
                                    pm.modifier_type
                                ),
                            ));
                            (None, None)
                        }
                    },
                };
                redirect = Some(RequestRedirectIr {
                    hostname: rr.hostname.clone(),
                    scheme: rr.scheme.clone(),
                    port: rr.port,
                    status_code: rr.status_code.map(|c| c.clamp(0, 599) as u32),
                    path_replace_full,
                    path_replace_prefix,
                });
            }
            "RequestHeaderModifier" => {
                let Some(hm) = f.request_header_modifier.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: RequestHeaderModifier missing body (rule {rule_idx})"
                        ),
                    ));
                    continue;
                };
                hdr_mod = Some(header_filter_snap_to_ir(hm));
            }
            "ResponseHeaderModifier" => {
                let Some(hm) = f.response_header_modifier.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: ResponseHeaderModifier missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                resp_hdr = Some(header_filter_snap_to_ir(hm));
            }
            "URLRewrite" => {
                let Some(uw) = f.url_rewrite.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!(
                            "InvalidReference: URLRewrite filter missing body (rule {rule_idx} filter {fi})"
                        ),
                    ));
                    continue;
                };
                let (path_replace_full, path_replace_prefix) = match &uw.path {
                    None => (None, None),
                    Some(pm) => match pm.modifier_type.as_str() {
                        "ReplaceFullPath" => (pm.replace_full_path.clone(), None),
                        "ReplacePrefixMatch" => (None, pm.replace_prefix_match.clone()),
                        _ => {
                            report.errors.push(httproute_obj_err(
                                route,
                                format!(
                                    "UnsupportedValue: url rewrite path modifier {} (rule {rule_idx})",
                                    pm.modifier_type
                                ),
                            ));
                            (None, None)
                        }
                    },
                };
                url_rewrite = Some(crate::ir::UrlRewriteIr {
                    hostname: uw.hostname.clone(),
                    path_replace_full,
                    path_replace_prefix,
                });
            }
            "CORS" => {
                let Some(c) = f.cors.as_ref() else {
                    report.errors.push(httproute_obj_err(
                        route,
                        format!("InvalidReference: CORS filter missing body (rule {rule_idx} filter {fi})"),
                    ));
                    continue;
                };
                cors = Some(crate::ir::CorsFilterIr {
                    allow_origins: c.allow_origins.clone(),
                    allow_methods: c.allow_methods.clone(),
                    allow_headers: c.allow_headers.clone(),
                    expose_headers: c.expose_headers.clone(),
                    max_age: c.max_age.map(|v| v.max(0) as u32).unwrap_or(5),
                    allow_credentials: c.allow_credentials,
                });
            }
            "RequestMirror" | "ExtensionRef" => {
                report.errors.push(httproute_obj_err(
                    route,
                    format!(
                        "UnsupportedValue: filter type {} (rule {rule_idx} filter {fi})",
                        f.filter_type
                    ),
                ));
            }
            other => {
                report.errors.push(httproute_obj_err(
                    route,
                    format!("UnsupportedValue: filter type {other} (rule {rule_idx})"),
                ));
            }
        }
    }
    HttpRuleFilters {
        redirect,
        request_header_modifier: hdr_mod,
        response_header_modifier: resp_hdr,
        url_rewrite,
        cors,
    }
}
