//! IR → Praxis native YAML encoder.
//!
//! The encoder is pure: it never touches Kubernetes, the filesystem, or the
//! network. It returns the configuration document plus the certificate/key
//! bytes the distribution layer must publish alongside it. TLS material is
//! returned as [`ConfigFile`]s keyed relative to [`TLS_MOUNT_DIR`].
//!
//! Features that Praxis cannot express are reported in
//! [`EncodeOutput::unsupported`] instead of being silently approximated.
//! Reported limitations are surfaced as Kubernetes events by the controller.

use std::collections::{BTreeMap, BTreeSet};

use regex::escape as regex_escape;
use ugwc_core::ir::{
    BackendIr, BackendTlsIr, IrSnapshot, ListenerIr, RequestRedirectIr, RouteFamily, RouteMatchIr,
    RouteRuleIr, TlsMode, WeightedBackendIr, backend_endpoints_map_key,
};

use crate::model::{
    AdminConfig, CaConfig, CertKeyPair, Cluster, ClusterHttp, ClusterTls, Condition,
    ConditionMatch, CorsFilter, Endpoint, Filter, FilterChain, HeaderPair, HeadersFilter,
    InsecureOptions, Listener, ListenerTls, LoadBalancerFilter, PathRewriteFilter, PraxisConfig,
    Protocol, RedirectFilter, RegexReplace, RouterFilter, RouterRoute, SkipPipelineChecks,
    SniRoute, SniRouterFilter, StaticResponseFilter, TimeoutFilter,
};

/// Directory where the distribution layer mounts `praxis.yaml`.
pub const CONFIG_MOUNT_DIR: &str = "/etc/praxis";
/// Directory where the distribution layer mounts the TLS Secret.
pub const TLS_MOUNT_DIR: &str = "/etc/praxis-tls";
/// Name of the config document inside the ConfigMap.
pub const CONFIG_FILE_NAME: &str = "praxis.yaml";
/// Port used for the no-op listener emitted when a Gateway has no routable
/// listener. It is deliberately outside the usual Gateway listener range and
/// distinct from the admin/metrics port.
const PLACEHOLDER_LISTENER_PORT: u16 = 15000;
/// Health/metrics listener address used by the per-gateway Deployment probes.
pub const ADMIN_METRICS_ADDRESS: &str = "0.0.0.0:15021";

/// One non-YAML file the distribution layer must publish (cert, key, CA).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigFile {
    /// Key relative to the TLS Secret mount.
    pub key: String,
    pub contents: Vec<u8>,
}

/// A Gateway API feature this backend cannot express in Praxis.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unsupported {
    /// Stable short feature name (used as an event reason).
    pub feature: String,
    /// Human-readable detail, including the route/listener name.
    pub detail: String,
}

/// Result of encoding one gateway IR.
#[derive(Clone, Debug, PartialEq)]
pub struct EncodeOutput {
    pub config: PraxisConfig,
    /// Serialized `praxis.yaml`.
    pub yaml: String,
    /// TLS material to publish in the Secret.
    pub files: Vec<ConfigFile>,
    /// Features that were dropped or approximated.
    pub unsupported: Vec<Unsupported>,
}

/// Encode `ir` into a Praxis document and side files.
///
/// # Errors
///
/// Returns a serialization error only if the generated model cannot be
/// represented as YAML (should not happen; all map keys are strings).
pub fn encode(ir: &IrSnapshot) -> Result<EncodeOutput, serde_yaml::Error> {
    let mut state = State::new(ir);
    let mut listeners = Vec::new();
    let mut filter_chains = Vec::new();

    // Praxis listeners own a socket, not a Gateway listener. Gateway API
    // allows several listeners to share a port (distinguished by hostname),
    // so group them and emit one Praxis listener per address. Effective route
    // hostnames already include the listener hostname intersection, which
    // keeps merged routing tables correct.
    let mut http_groups: BTreeMap<u16, Vec<&ListenerIr>> = BTreeMap::new();
    let mut tls_groups: BTreeMap<u16, Vec<&ListenerIr>> = BTreeMap::new();
    let mut tcp_groups: BTreeMap<u16, Vec<&ListenerIr>> = BTreeMap::new();

    for ln in &ir.listeners {
        if ln.conflicted {
            continue;
        }
        match effective_protocol(ln) {
            "HTTP" | "HTTPS" => http_groups.entry(ln.bind_port).or_default().push(ln),
            "TLS" => tls_groups.entry(ln.bind_port).or_default().push(ln),
            "TCP" => tcp_groups.entry(ln.bind_port).or_default().push(ln),
            "UDP" => state.unsupported(
                "UDPRoute",
                format!(
                    "listener {}/{} uses protocol UDP; Praxis ProtocolKind supports only http and tcp",
                    ln.parent_namespace, ln.section_name
                ),
            ),
            other => state.unsupported(
                "UnsupportedProtocol",
                format!(
                    "listener {}/{} uses unsupported protocol {other:?}",
                    ln.parent_namespace, ln.section_name
                ),
            ),
        }
    }

    for group in http_groups.values() {
        let routes = routes_for_group(ir, group);
        if let Some((listener, chain)) = state.http_listener(group, &routes) {
            listeners.push(listener);
            filter_chains.push(chain);
        }
    }
    for group in tls_groups.values() {
        let routes = routes_for_group(ir, group);
        if let Some((listener, chain)) = state.tls_listener(group, &routes) {
            listeners.push(listener);
            filter_chains.push(chain);
        }
    }
    for group in tcp_groups.values() {
        let routes = routes_for_group(ir, group);
        if let Some((listener, chain)) = state.tcp_listener(group, &routes) {
            listeners.push(listener);
            if let Some(chain) = chain {
                filter_chains.push(chain);
            }
        }
    }

    // Praxis rejects a document with no listeners. A Gateway can legitimately
    // have no routable listener (for example a TLS-passthrough listener whose
    // TLSRoutes have not been created yet); emit a bounded no-op listener so
    // the proxy still starts and becomes Ready. The fallback never receives
    // Gateway traffic because no Gateway listener is bound to this port.
    if listeners.is_empty() {
        let name = state.names.unique("ugwc-placeholder");
        listeners.push(Listener {
            name: name.clone(),
            address: format!("0.0.0.0:{PLACEHOLDER_LISTENER_PORT}"),
            protocol: Some(Protocol::Http),
            filter_chains: vec![name.clone()],
            tls: None,
            upstream: None,
        });
        filter_chains.push(FilterChain {
            name,
            filters: Vec::new(),
        });
    }

    let mut config = PraxisConfig {
        listeners,
        filter_chains,
        insecure_options: Some(InsecureOptions {
            allow_private_endpoints: true,
            allow_private_upstreams: true,
            skip_pipeline_checks: state.scoped_cors.then_some(SkipPipelineChecks {
                conditional_security: true,
            }),
        }),
        admin: Some(AdminConfig {
            metrics_address: Some(ADMIN_METRICS_ADDRESS.into()),
        }),
    };
    pin_shared_tls_pools(&mut config);
    let yaml = serde_yaml::to_string(&config)?;

    let mut unsupported = state.unsupported;
    unsupported.sort_by(|a, b| {
        a.feature
            .cmp(&b.feature)
            .then_with(|| a.detail.cmp(&b.detail))
    });
    unsupported.dedup();

    Ok(EncodeOutput {
        config,
        yaml,
        files: state.files,
        unsupported,
    })
}

// -----------------------------------------------------------------------------
// Encoder state
// -----------------------------------------------------------------------------

struct State<'a> {
    ir: &'a IrSnapshot,
    cluster_names: BTreeMap<String, String>,
    clusters: BTreeMap<String, Cluster>,
    names: UniqueNames,
    cluster_name_registry: UniqueNames,
    files: Vec<ConfigFile>,
    unsupported: Vec<Unsupported>,
    scoped_cors: bool,
}

impl<'a> State<'a> {
    fn new(ir: &'a IrSnapshot) -> Self {
        Self {
            ir,
            cluster_names: BTreeMap::new(),
            clusters: BTreeMap::new(),
            names: UniqueNames::default(),
            cluster_name_registry: UniqueNames::default(),
            files: Vec::new(),
            unsupported: Vec::new(),
            scoped_cors: false,
        }
    }

    fn unsupported(&mut self, feature: impl Into<String>, detail: impl Into<String>) {
        self.unsupported.push(Unsupported {
            feature: feature.into(),
            detail: detail.into(),
        });
    }

    // -- HTTP ---------------------------------------------------------------

    fn http_listener(
        &mut self,
        group: &[&ListenerIr],
        routes: &[&RouteRuleIr],
    ) -> Option<(Listener, FilterChain)> {
        let primary = *group.first()?;
        let name = self
            .names
            .unique(&format!("{}-{}", primary.parent_name, primary.section_name));
        let label = group
            .iter()
            .map(|l| l.section_name.as_str())
            .collect::<Vec<_>>()
            .join(",");

        let terminating: Vec<&&ListenerIr> = group
            .iter()
            .filter(|l| l.tls == TlsMode::Terminate)
            .collect();
        let mut cert_entries: Vec<(CertKeyPair, Option<String>)> = Vec::new();
        for (index, ln) in terminating.iter().enumerate() {
            if ln.tls_cert_pem.is_empty() || ln.tls_key_pem.is_empty() {
                self.unsupported(
                    "ListenerTlsMaterialMissing",
                    format!(
                        "listener {}/{} terminates TLS but has no certificate material",
                        ln.parent_namespace, ln.section_name
                    ),
                );
                continue;
            }
            let suffix = if terminating.len() == 1 {
                String::new()
            } else {
                format!("-{index}")
            };
            let cert_key = format!("listener-{name}{suffix}.crt");
            let key_key = format!("listener-{name}{suffix}.key");
            self.files.push(ConfigFile {
                key: cert_key.clone(),
                contents: ln.tls_cert_pem.clone(),
            });
            self.files.push(ConfigFile {
                key: key_key.clone(),
                contents: ln.tls_key_pem.to_vec(),
            });
            cert_entries.push((
                CertKeyPair {
                    cert_path: tls_path(&cert_key),
                    default: false,
                    key_path: tls_path(&key_key),
                    server_names: Vec::new(),
                },
                ln.hostname.clone(),
            ));
        }
        if !terminating.is_empty() && cert_entries.is_empty() {
            return None;
        }
        if cert_entries.len() > 1 {
            for (cert, hostname) in &mut cert_entries {
                match hostname {
                    Some(hostname) => cert.server_names = vec![hostname.clone()],
                    None => cert.default = true,
                }
            }
        }
        let certificates: Vec<CertKeyPair> =
            cert_entries.into_iter().map(|(cert, _)| cert).collect();
        if !terminating.is_empty() && terminating.len() != group.len() {
            self.unsupported(
                "MixedTlsOnPort",
                format!(
                    "port {} mixes TLS-terminating and plaintext listeners; Praxis binds one TLS mode per socket",
                    primary.bind_port
                ),
            );
        }
        let tls = if certificates.is_empty() {
            None
        } else {
            Some(ListenerTls {
                certificates,
                hot_reload: None,
            })
        };

        // CORS is a security filter, so Praxis refuses request conditions on
        // it unless `conditional_security` is relaxed. Route-scoped CORS needs
        // exactly that: the filter must apply only to the route's matches.
        let mut cors_filters = Vec::new();
        let mut emitted_cors: Vec<(ugwc_core::ir::CorsFilterIr, Vec<Condition>)> = Vec::new();
        let mut redirect_filters = Vec::new();
        let mut fail_closed = Vec::new();
        let mut invalid_catchall = false;
        let mut router_routes = Vec::new();
        let mut post_router = Vec::new();
        let mut referenced: BTreeSet<String> = BTreeSet::new();

        for route in routes {
            if !matches!(route.route_family, RouteFamily::Http | RouteFamily::Grpc) {
                continue;
            }
            let conditions = filter_conditions(route, &mut self.unsupported);
            let rewrites_path = route.url_rewrite.as_ref().is_some_and(|rewrite| {
                rewrite.path_replace_full.is_some() || rewrite.path_replace_prefix.is_some()
            });
            let has_rule_filters = route.cors.is_some()
                || route.request_redirect.is_some()
                || rewrites_path
                || route.request_header_modifier.is_some()
                || route.response_header_modifier.is_some()
                || route.request_timeout_ms.is_some()
                || route.backend_timeout_ms.is_some();
            if !route.hosts.is_empty() && has_rule_filters {
                self.unsupported(
                    "HostScopedFilters",
                    format!(
                        "route {} has host-specific rules; Praxis request conditions cannot match \
                         on host, so its filters may also run for other hosts on listener {}",
                        route.name, label
                    ),
                );
            }

            if let Some(redirect) = &route.request_redirect {
                if redirect.path_replace_prefix.is_some() {
                    self.unsupported(
                        "RedirectPathPrefix",
                        format!(
                            "route {} redirects with ReplacePrefixMatch; Praxis redirect templates \
                             cannot strip a prefix, so the original request path is kept",
                            route.name
                        ),
                    );
                }
                if redirect.status_code == Some(303) {
                    // Praxis 0.7.5's redirect filter accepts only
                    // 301/302/307/308. Emit a static 303 response instead.
                    // Without a templating engine the Location header cannot
                    // echo the original path, so report that gap while still
                    // returning the requested status.
                    let location = static_303_location(redirect, route, tls.is_some());
                    self.unsupported(
                        "RedirectStatus303Location",
                        format!(
                            "route {} requests a 303 redirect; the status is served by a static \
                             response with Location {location:?}, which cannot include a request \
                             path suffix or echo an unspecified request host",
                            route.name
                        ),
                    );
                    push_per_condition(&mut redirect_filters, &conditions, |conds| {
                        Filter::StaticResponse(StaticResponseFilter {
                            status: 303,
                            headers: vec![HeaderPair {
                                name: "Location".into(),
                                value: location.clone(),
                            }],
                            // A body keeps Praxis on its normal response path;
                            // header-only 3xx rejections currently fail.
                            body: Some("See Other".into()),
                            conditions: conds,
                        })
                    });
                } else {
                    push_per_condition(&mut redirect_filters, &conditions, |conds| {
                        Filter::Redirect(redirect_filter(
                            redirect,
                            &route.hosts,
                            primary.bind_port,
                            conds,
                        ))
                    });
                }
                continue;
            }

            // URLRewrite hostname becomes the cluster's fixed upstream
            // authority, so it must be part of cluster identity (one cluster
            // per backend + authority pair).
            let authority = route
                .url_rewrite
                .as_ref()
                .and_then(|rewrite| rewrite.hostname.as_deref());
            let Some(cluster) = self.route_cluster(route, authority) else {
                // Gateway API requires a 500 when a rule's backendRefs cannot
                // be resolved. Praxis rejects clusters with zero endpoints at
                // config load, so fail closed with a conditional response
                // before the router instead.
                if conditions.is_empty() {
                    invalid_catchall = true;
                } else {
                    push_per_condition(&mut fail_closed, &conditions, |conds| {
                        Filter::StaticResponse(StaticResponseFilter {
                            status: 500,
                            headers: Vec::new(),
                            body: Some("no resolvable backend".into()),
                            conditions: conds,
                        })
                    });
                }
                continue;
            };
            referenced.insert(cluster.clone());

            for m in route_matches(route) {
                for host in router_hosts(route) {
                    router_routes.push(router_route(
                        m,
                        host,
                        &cluster,
                        matches!(route.route_family, RouteFamily::Grpc),
                        &mut self.unsupported,
                    ));
                }
            }

            if let Some(cors) = &route.cors {
                let cors = self.praxis_cors(cors, &route.name);
                let condition_sets: Vec<Vec<Condition>> = if conditions.is_empty() {
                    vec![Vec::new()]
                } else {
                    self.scoped_cors = true;
                    conditions.iter().map(|c| vec![c.clone()]).collect()
                };
                for conds in condition_sets {
                    if emitted_cors.iter().any(|(existing, existing_conds)| {
                        *existing == cors && *existing_conds == conds
                    }) {
                        continue;
                    }
                    if let Some((_, _)) = emitted_cors
                        .iter()
                        .find(|(_, existing_conds)| *existing_conds == conds)
                    {
                        self.unsupported(
                            "MultipleCorsConfigs",
                            format!(
                                "route {} defines a CORS policy for a path already covered by a \
                                 different policy on listener {}; keeping the first",
                                route.name, label
                            ),
                        );
                        continue;
                    }
                    emitted_cors.push((cors.clone(), conds.clone()));
                    cors_filters.push(Filter::Cors(CorsFilter {
                        allow_origins: cors.allow_origins.clone(),
                        allow_methods: cors.allow_methods.clone(),
                        allow_headers: cors.allow_headers.clone(),
                        expose_headers: cors.expose_headers.clone(),
                        max_age: cors.max_age,
                        allow_credentials: cors.allow_credentials,
                        conditions: conds,
                    }));
                }
            }

            if let Some(rewrite) = &route.url_rewrite {
                if let Some(full) = &rewrite.path_replace_full {
                    let replacement = escape_replacement(full);
                    push_per_condition(&mut post_router, &conditions, |conds| {
                        Filter::PathRewrite(PathRewriteFilter {
                            strip_prefix: None,
                            add_prefix: None,
                            replace: Some(RegexReplace {
                                pattern: "^.*$".into(),
                                replacement: replacement.clone(),
                            }),
                            allow_rewrite_override: true,
                            conditions: conds,
                        })
                    });
                } else if let Some(new_prefix) = &rewrite.path_replace_prefix {
                    // ReplacePrefixMatch rewrites the prefix from the route
                    // match; the replacement alone does not identify it.
                    if route.matches.is_empty() {
                        self.unsupported(
                            "PrefixRewriteWithoutMatch",
                            format!(
                                "route {} uses ReplacePrefixMatch without a path match; cannot \
                                 determine which prefix to replace",
                                route.name
                            ),
                        );
                    }
                    let grpc = matches!(route.route_family, RouteFamily::Grpc);
                    let replacement = format!("{}$1", escape_replacement(new_prefix));
                    for m in &route.matches {
                        let Some(match_prefix) = &m.path_prefix else {
                            self.unsupported(
                                "PrefixRewriteWithoutPrefixMatch",
                                format!(
                                    "route {} uses ReplacePrefixMatch with a non-prefix path \
                                     match; the rewrite is skipped",
                                    route.name
                                ),
                            );
                            continue;
                        };
                        let cm = condition_match(m, grpc, &mut self.unsupported);
                        let conds = if cm.is_empty() {
                            Vec::new()
                        } else {
                            vec![Condition { when: cm }]
                        };
                        post_router.push(Filter::PathRewrite(PathRewriteFilter {
                            strip_prefix: None,
                            add_prefix: None,
                            replace: Some(RegexReplace {
                                pattern: format!("^{}(.*)$", regex_escape(match_prefix)),
                                replacement: replacement.clone(),
                            }),
                            allow_rewrite_override: true,
                            conditions: conds,
                        }));
                    }
                }
            }

            if route.request_header_modifier.is_some() || route.response_header_modifier.is_some() {
                push_per_condition(&mut post_router, &conditions, |conds| {
                    Filter::Headers(header_filter(route, conds))
                });
            }

            // Gateway API has separate request and backendRequest timeouts.
            // Praxis only exposes a whole-request deadline. `backendRequest`
            // is always <= `request`, so enforcing the tighter of the two
            // preserves the local-rule deadline (a disabled `0s` timeout is
            // ignored rather than disabling the other one).
            let effective_timeout = [route.request_timeout_ms, route.backend_timeout_ms]
                .into_iter()
                .flatten()
                .filter(|ms| *ms > 0)
                .min();
            if let Some(ms) = effective_timeout {
                push_per_condition(&mut post_router, &conditions, |conds| {
                    Filter::Timeout(TimeoutFilter {
                        timeout_ms: ms,
                        conditions: conds,
                    })
                });
            }
        }

        let clusters: Vec<Cluster> = referenced
            .iter()
            .filter_map(|name| self.clusters.get(name).cloned())
            .collect();

        // An invalid catch-all rule has no positive condition to attach to.
        // It can only be served as an unconditional 500 when no other route
        // makes it through; otherwise unmatched paths fall back to 404.
        if invalid_catchall && router_routes.is_empty() {
            fail_closed.push(Filter::StaticResponse(StaticResponseFilter {
                status: 500,
                headers: Vec::new(),
                body: Some("no resolvable backend".into()),
                conditions: Vec::new(),
            }));
        }

        let mut filters = Vec::new();
        filters.extend(cors_filters);
        filters.extend(redirect_filters);
        filters.extend(fail_closed);
        // Praxis rejects an empty router table. A listener with no routable
        // rules (for example before any HTTPRoute attaches) gets a chain
        // without a router instead; unmatched requests fail closed.
        if !router_routes.is_empty() {
            filters.push(Filter::Router(RouterFilter {
                routes: router_routes,
                multi_level_subdomain_matching: Some(true),
            }));
        }
        filters.extend(post_router);
        if !clusters.is_empty() {
            filters.push(Filter::LoadBalancer(LoadBalancerFilter { clusters }));
        }

        let listener = Listener {
            name: name.clone(),
            address: format!("0.0.0.0:{}", primary.bind_port),
            protocol: Some(Protocol::Http),
            filter_chains: vec![name.clone()],
            tls,
            upstream: None,
        };
        let chain = FilterChain { name, filters };
        Some((listener, chain))
    }

    // -- TLS passthrough ----------------------------------------------------

    fn tls_listener(
        &mut self,
        group: &[&ListenerIr],
        routes: &[&RouteRuleIr],
    ) -> Option<(Listener, FilterChain)> {
        let primary = *group.first()?;
        let name = self
            .names
            .unique(&format!("{}-{}", primary.parent_name, primary.section_name));
        let mut sni_routes: Vec<SniRoute> = Vec::new();
        let mut default_upstream = None;
        let mut seen_names: BTreeSet<String> = BTreeSet::new();

        for route in routes {
            if !matches!(route.route_family, RouteFamily::Tls) {
                continue;
            }
            let Some(upstream) = self.first_upstream(route) else {
                continue;
            };
            if route.hosts.is_empty() {
                if default_upstream.is_none() {
                    default_upstream = Some(upstream);
                }
            } else {
                let server_names: Vec<String> = route
                    .hosts
                    .iter()
                    .filter(|host| seen_names.insert((*host).clone()))
                    .cloned()
                    .collect();
                if !server_names.is_empty() {
                    sni_routes.push(SniRoute {
                        server_names,
                        upstream,
                    });
                }
            }
        }

        if sni_routes.is_empty() && default_upstream.is_none() {
            self.unsupported(
                "TlsListenerWithoutRoutes",
                format!(
                    "TLS listener {}/{} has no routable TLSRoute",
                    primary.parent_namespace, primary.section_name
                ),
            );
            // Bind the Gateway's port but leave the pipeline empty: the TCP
            // proxy accepts the connection, resolves no upstream, and closes
            // it. This is the correct reject behavior for a passthrough
            // listener whose routes are all invalid or not yet created.
            let listener = Listener {
                name: name.clone(),
                address: format!("0.0.0.0:{}", primary.bind_port),
                protocol: Some(Protocol::Tcp),
                filter_chains: vec![name.clone()],
                tls: None,
                upstream: None,
            };
            let chain = FilterChain {
                name,
                filters: Vec::new(),
            };
            return Some((listener, chain));
        }

        let listener = Listener {
            name: name.clone(),
            address: format!("0.0.0.0:{}", primary.bind_port),
            protocol: Some(Protocol::Tcp),
            filter_chains: vec![name.clone()],
            tls: None,
            upstream: None,
        };
        let chain = FilterChain {
            name,
            filters: vec![Filter::SniRouter(SniRouterFilter {
                routes: sni_routes,
                default_upstream,
            })],
        };
        Some((listener, chain))
    }

    // -- TCP ----------------------------------------------------------------

    fn tcp_listener(
        &mut self,
        group: &[&ListenerIr],
        routes: &[&RouteRuleIr],
    ) -> Option<(Listener, Option<FilterChain>)> {
        let primary = *group.first()?;
        let name = self
            .names
            .unique(&format!("{}-{}", primary.parent_name, primary.section_name));
        let tcp_routes: Vec<&&RouteRuleIr> = routes
            .iter()
            .filter(|r| matches!(r.route_family, RouteFamily::Tcp))
            .collect();
        if group.len() > 1 {
            self.unsupported(
                "MultipleTcpListenersSamePort",
                format!(
                    "port {} has multiple TCP listeners; without SNI Praxis can expose only one upstream",
                    primary.bind_port
                ),
            );
        }
        if tcp_routes.len() > 1 {
            self.unsupported(
                "MultipleTcpRoutesSamePort",
                format!(
                    "port {} has multiple TCP routes; Praxis TCP listeners support a single upstream",
                    primary.bind_port
                ),
            );
        }
        let reject = || {
            (
                Listener {
                    name: name.clone(),
                    address: format!("0.0.0.0:{}", primary.bind_port),
                    protocol: Some(Protocol::Tcp),
                    filter_chains: vec![name.clone()],
                    tls: None,
                    upstream: None,
                },
                FilterChain {
                    name: name.clone(),
                    filters: Vec::new(),
                },
            )
        };
        let Some(route) = tcp_routes.first().map(|route| **route) else {
            let (listener, chain) = reject();
            return Some((listener, Some(chain)));
        };
        let Some(upstream) = self.first_upstream(route) else {
            self.unsupported(
                "TcpListenerWithoutUpstream",
                format!(
                    "TCP listener {}/{} has no resolvable backend",
                    primary.parent_namespace, primary.section_name
                ),
            );
            let (listener, chain) = reject();
            return Some((listener, Some(chain)));
        };
        Some((
            Listener {
                name,
                address: format!("0.0.0.0:{}", primary.bind_port),
                protocol: Some(Protocol::Tcp),
                filter_chains: Vec::new(),
                tls: None,
                upstream: Some(upstream),
            },
            None,
        ))
    }

    /// Adapt Gateway API CORS to Praxis's stricter validation.
    ///
    /// Praxis refuses `allow_credentials` with wildcard origins, methods, or
    /// headers. Gateway API expects the implementation to echo the origin in
    /// that case, which this Praxis version cannot express; drop credentials
    /// and report the approximation instead of emitting a config that fails
    /// to load.
    fn praxis_cors(
        &mut self,
        cors: &ugwc_core::ir::CorsFilterIr,
        route_name: &str,
    ) -> ugwc_core::ir::CorsFilterIr {
        let mut out = cors.clone();
        let wildcard = |values: &[String]| values.iter().any(|v| v == "*");
        if out.allow_credentials
            && (wildcard(&out.allow_origins)
                || wildcard(&out.allow_methods)
                || wildcard(&out.allow_headers))
        {
            self.unsupported(
                "CorsCredentialsWildcard",
                format!(
                    "route {route_name} combines allowCredentials with a wildcard origin/method/\
                     header; Praxis cannot echo the request origin, so credentials are disabled"
                ),
            );
            out.allow_credentials = false;
        }
        out
    }

    // -- Backends -----------------------------------------------------------

    fn route_cluster(&mut self, route: &RouteRuleIr, authority: Option<&str>) -> Option<String> {
        let grpc = matches!(route.route_family, RouteFamily::Grpc);
        let authority = match authority {
            Some(value) if !value.is_empty() && !valid_authority(value) => {
                self.unsupported(
                    "UrlRewriteHostnameInvalid",
                    format!(
                        "route {} rewrites Host to {:?}, which is not a valid HTTP authority; \
                         forwarding the downstream Host instead",
                        route.name, value
                    ),
                );
                None
            }
            Some(value) if !value.is_empty() => Some(value),
            _ => None,
        };
        let active: Vec<&WeightedBackendIr> =
            route.backends.iter().filter(|wb| wb.weight > 0).collect();
        if active.is_empty() {
            self.unsupported(
                "NoBackend",
                format!("route {} has no backend with non-zero weight", route.name),
            );
            return None;
        }
        if active.len() == 1 {
            return self.cluster_for(&active[0].backend, grpc, authority);
        }
        self.weighted_cluster(route, &active, grpc, authority)
    }

    /// Merge several weighted backends into one cluster whose endpoints carry
    /// scaled weights.
    ///
    /// Gateway API weights apply per `backendRef`, while Praxis weights apply
    /// per endpoint. Scaling every backend by the LCM of its endpoint count
    /// keeps the aggregate proportion exact. Backends with different TLS
    /// policies cannot share a cluster; those are reported and only the first
    /// backend is used.
    fn weighted_cluster(
        &mut self,
        route: &RouteRuleIr,
        active: &[&WeightedBackendIr],
        grpc: bool,
        authority: Option<&str>,
    ) -> Option<String> {
        let first_tls = active[0].backend.backend_tls.as_ref();
        if active
            .iter()
            .any(|wb| wb.backend.backend_tls.as_ref() != first_tls)
        {
            self.unsupported(
                "WeightedBackendsMixedTls",
                format!(
                    "route {} weights backends with different TLS policies; using only the first",
                    route.name
                ),
            );
            return self.cluster_for(&active[0].backend, grpc, authority);
        }

        let mut weights: BTreeMap<String, u32> = BTreeMap::new();
        for wb in active {
            let Some(name) = self.cluster_for(&wb.backend, grpc, authority) else {
                continue;
            };
            let entry = weights.entry(name).or_insert(0);
            *entry = entry.saturating_add(wb.weight);
        }
        if weights.is_empty() {
            return None;
        }
        if weights.len() == 1 {
            return weights.into_keys().next();
        }

        // Scale backend weights by the LCM of endpoint counts so each
        // backend's total share is exact even when replica counts differ.
        let mut scale: u64 = 1;
        for name in weights.keys() {
            let count = self.clusters.get(name)?.endpoints.len() as u64;
            scale = lcm(scale, count);
            if scale > u64::from(u32::MAX) {
                self.unsupported(
                    "WeightedBackendsEndpointScale",
                    format!(
                        "route {} has endpoint counts that cannot be weighted within u32",
                        route.name
                    ),
                );
                return self.cluster_for(&active[0].backend, grpc, authority);
            }
        }

        let mut endpoints = Vec::new();
        let mut first_cluster = None;
        for (name, weight) in &weights {
            let cluster = self.clusters.get(name)?;
            if first_cluster.is_none() {
                first_cluster = Some(cluster.clone());
            }
            let count = cluster.endpoints.len() as u64;
            if count == 0 {
                continue;
            }
            let per_endpoint = (u64::from(*weight).saturating_mul(scale) / count)
                .clamp(1, u64::from(u32::MAX)) as u32;
            endpoints.extend(cluster.endpoints.iter().map(|endpoint| Endpoint::Weighted {
                address: endpoint.address().to_owned(),
                weight: per_endpoint,
            }));
        }
        if endpoints.is_empty() {
            return None;
        }

        // Praxis walks contiguous weight buckets, so a request window that is
        // not a whole number of cycles is biased toward the first buckets.
        // Reducing by the GCD keeps the cycle as short as possible while
        // preserving the requested ratio (e.g. 140/140/60/60 -> 7/7/3/3).
        let divisor = endpoints
            .iter()
            .filter_map(|endpoint| match endpoint {
                Endpoint::Weighted { weight, .. } => Some(u64::from(*weight)),
                Endpoint::Plain(_) => None,
            })
            .fold(0, gcd);
        if divisor > 1 {
            for endpoint in &mut endpoints {
                if let Endpoint::Weighted { weight, .. } = endpoint {
                    *weight = u32::try_from(u64::from(*weight) / divisor)
                        .unwrap_or(1)
                        .max(1);
                }
            }
        }

        let base = sanitize_name_component(&format!("{}-weighted", route.name));
        let name = self.cluster_name_registry.unique(&base);
        let first_cluster = first_cluster?;
        self.clusters.insert(
            name.clone(),
            Cluster {
                name: name.clone(),
                endpoints,
                tls: first_cluster.tls,
                http: first_cluster.http,
                idle_timeout_ms: None,
            },
        );
        Some(name)
    }

    fn cluster_for(
        &mut self,
        backend: &BackendIr,
        grpc: bool,
        authority: Option<&str>,
    ) -> Option<String> {
        if backend.inference.is_some() {
            self.unsupported(
                "InferencePool",
                format!(
                    "backend {}/{} is an InferencePool; EPP routing is not supported by this output",
                    backend.service_namespace, backend.service_name
                ),
            );
            return None;
        }
        if backend.backend_tls.as_ref().is_some_and(|tls| tls.invalid) {
            self.unsupported(
                "BackendTlsInvalid",
                format!(
                    "backend {}/{} has an invalid BackendTLSPolicy; refusing to route",
                    backend.service_namespace, backend.service_name
                ),
            );
            return None;
        }
        let Some(port) = backend.port_number else {
            self.unsupported(
                "BackendPortMissing",
                format!(
                    "backend {}/{} has no numeric port",
                    backend.service_namespace, backend.service_name
                ),
            );
            return None;
        };

        let key =
            backend_endpoints_map_key(&backend.service_namespace, &backend.service_name, port);
        let cluster_key = match authority {
            Some(authority) => format!("{key}|{authority}"),
            None => key,
        };
        if let Some(name) = self.cluster_names.get(&cluster_key) {
            if grpc {
                if let Some(cluster) = self.clusters.get_mut(name) {
                    let http = cluster.http.get_or_insert(ClusterHttp {
                        version: None,
                        authority: None,
                    });
                    http.version = Some("h2".into());
                }
            }
            return Some(name.clone());
        }

        let endpoints = endpoints_for(self.ir, backend);
        if endpoints.is_empty() {
            self.unsupported(
                "BackendEndpointsMissing",
                format!(
                    "backend {}/{} has no endpoints and no service name to resolve",
                    backend.service_namespace, backend.service_name
                ),
            );
            return None;
        }

        let base_name = match authority {
            Some(authority) => format!(
                "{}-{}-{}-{}",
                backend.service_namespace,
                backend.service_name,
                port,
                sanitize_name_component(authority)
            ),
            None => format!(
                "{}-{}-{}",
                backend.service_namespace, backend.service_name, port
            ),
        };
        let name = self.cluster_name_registry.unique(&base_name);
        let tls = backend
            .backend_tls
            .as_ref()
            .and_then(|tls| self.cluster_tls(&name, tls));
        let http = if grpc || authority.is_some() {
            Some(ClusterHttp {
                version: grpc.then(|| "h2".to_string()),
                authority: authority.map(str::to_owned),
            })
        } else {
            None
        };
        let cluster = Cluster {
            name: name.clone(),
            endpoints: endpoints.into_iter().map(Endpoint::Plain).collect(),
            tls,
            http,
            idle_timeout_ms: None,
        };
        self.cluster_names.insert(cluster_key, name.clone());
        self.clusters.insert(name.clone(), cluster);
        Some(name)
    }

    fn cluster_tls(&mut self, cluster: &str, tls: &BackendTlsIr) -> Option<ClusterTls> {
        let ca = if tls.ca_cert_pem.is_empty() {
            None
        } else {
            let key = format!("backend-{cluster}.ca.pem");
            self.files.push(ConfigFile {
                key: key.clone(),
                contents: tls.ca_cert_pem.clone(),
            });
            Some(CaConfig {
                ca_path: tls_path(&key),
            })
        };

        let client_cert = if tls.client_cert_pem.is_empty() && tls.client_key_pem.is_empty() {
            None
        } else if tls.client_cert_pem.is_empty() || tls.client_key_pem.is_empty() {
            self.unsupported(
                "BackendClientCertIncomplete",
                format!("backend TLS for cluster {cluster} has only one of client cert/key"),
            );
            None
        } else {
            let cert_key = format!("backend-{cluster}.crt");
            let key_key = format!("backend-{cluster}.key");
            self.files.push(ConfigFile {
                key: cert_key.clone(),
                contents: tls.client_cert_pem.clone(),
            });
            self.files.push(ConfigFile {
                key: key_key.clone(),
                contents: tls.client_key_pem.to_vec(),
            });
            Some(CertKeyPair {
                cert_path: tls_path(&cert_key),
                default: false,
                key_path: tls_path(&key_key),
                server_names: Vec::new(),
            })
        };

        let sni = if valid_sni(&tls.hostname) {
            Some(tls.hostname.clone())
        } else {
            self.unsupported(
                "BackendTlsSniInvalid",
                format!(
                    "backend TLS hostname {:?} is not a valid SNI name for cluster {cluster}; \
                     omitting sni",
                    tls.hostname
                ),
            );
            None
        };

        Some(ClusterTls {
            ca,
            client_cert,
            sni,
            verify: true,
        })
    }

    fn first_upstream(&mut self, route: &RouteRuleIr) -> Option<String> {
        let active: Vec<&WeightedBackendIr> =
            route.backends.iter().filter(|wb| wb.weight > 0).collect();
        if active.len() > 1 {
            self.unsupported(
                "WeightedBackends",
                format!(
                    "route {} has {} L4 backends; using only the first",
                    route.name,
                    active.len()
                ),
            );
        }
        let backend = active.first()?.backend.clone();
        if backend.inference.is_some() {
            self.unsupported(
                "InferencePool",
                format!("route {} targets an InferencePool", route.name),
            );
            return None;
        }
        if backend.backend_tls.as_ref().is_some_and(|tls| tls.invalid) {
            self.unsupported(
                "BackendTlsInvalid",
                format!("route {} targets an invalid backend TLS policy", route.name),
            );
            return None;
        }
        let endpoints = endpoints_for(self.ir, &backend);
        if endpoints.len() > 1 {
            self.unsupported(
                "L4MultipleEndpoints",
                format!(
                    "route {} has {} endpoints; Praxis L4 listeners use a single upstream",
                    route.name,
                    endpoints.len()
                ),
            );
        }
        endpoints.into_iter().next()
    }
}

// -----------------------------------------------------------------------------
// Route helpers
// -----------------------------------------------------------------------------

fn attached(route: &RouteRuleIr, ln: &ListenerIr) -> bool {
    let Some(a) = &route.listener else {
        return false;
    };
    a.parent_kind == ln.parent_kind
        && a.parent_namespace == ln.parent_namespace
        && a.parent_name == ln.parent_name
        && a.section_name == ln.section_name
        && a.port == ln.bind_port
}

fn routes_for_group<'a>(ir: &'a IrSnapshot, group: &[&ListenerIr]) -> Vec<&'a RouteRuleIr> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for route in &ir.routes {
        if !group.iter().any(|ln| attached(route, ln)) {
            continue;
        }
        if seen.insert(route.name.clone()) {
            out.push(route);
        }
    }
    out
}

fn effective_protocol(ln: &ListenerIr) -> &'static str {
    let normalized = ln.gateway_protocol.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "" => match ln.tls {
            TlsMode::Terminate => "HTTPS",
            TlsMode::Passthrough => "TLS",
            TlsMode::Plaintext => "HTTP",
        },
        "HTTP" => "HTTP",
        "HTTPS" => "HTTPS",
        "TLS" => "TLS",
        "TCP" => "TCP",
        "UDP" => "UDP",
        _ => "UNKNOWN",
    }
}

fn endpoints_for(ir: &IrSnapshot, backend: &BackendIr) -> Vec<String> {
    let Some(port) = backend.port_number else {
        return Vec::new();
    };
    let key = backend_endpoints_map_key(&backend.service_namespace, &backend.service_name, port);
    if let Some(endpoints) = ir.backends.get(&key) {
        if !endpoints.is_empty() {
            return endpoints
                .iter()
                .map(|e| endpoint_address(&e.address, e.port))
                .collect();
        }
    }
    if backend.service_namespace.is_empty() || backend.service_name.is_empty() {
        return Vec::new();
    }
    // No endpoint slices yet (rollout, selector mismatch): fall back to the
    // Service DNS name so kube-proxy can route once endpoints appear.
    let dial_port = backend.dial_port.unwrap_or(port);
    vec![format!(
        "{}.{}.svc.cluster.local:{}",
        backend.service_name, backend.service_namespace, dial_port
    )]
}

/// Format `host:port`, bracketing IPv6 literals.
fn endpoint_address(address: &str, port: u16) -> String {
    if address.starts_with('[') || !address.contains(':') {
        format!("{address}:{port}")
    } else {
        format!("[{address}]:{port}")
    }
}

fn route_matches(route: &RouteRuleIr) -> Vec<Option<&RouteMatchIr>> {
    if route.matches.is_empty() {
        vec![None]
    } else {
        route.matches.iter().map(Some).collect()
    }
}

fn router_hosts(route: &RouteRuleIr) -> Vec<Option<&str>> {
    if route.hosts.is_empty() {
        vec![None]
    } else {
        route.hosts.iter().map(|h| Some(h.as_str())).collect()
    }
}

fn router_route(
    m: Option<&RouteMatchIr>,
    host: Option<&str>,
    cluster: &str,
    grpc: bool,
    unsupported: &mut Vec<Unsupported>,
) -> RouterRoute {
    let mut route = RouterRoute {
        path: None,
        path_prefix: None,
        host: host.map(str::to_owned),
        headers: None,
        cluster: cluster.to_owned(),
    };

    let Some(m) = m else {
        route.path_prefix = Some("/".into());
        return route;
    };

    if !m.query_params_exact.is_empty() {
        unsupported.push(Unsupported {
            feature: "QueryParamRouting".into(),
            detail: format!(
                "route match on query parameters {:?} is ignored; Praxis routing cannot match \
                 query parameters",
                m.query_params_exact
            ),
        });
    }
    if m.method.is_some() {
        unsupported.push(Unsupported {
            feature: "MethodRouting".into(),
            detail: format!(
                "route match on method {:?} is ignored; Praxis routing selects clusters by path, \
                 host, and headers only",
                m.method
            ),
        });
    }

    if grpc {
        match (&m.grpc_service, &m.grpc_method) {
            (Some(service), Some(method)) => {
                route.path = Some(format!("/{service}/{method}"));
                return route;
            }
            (Some(service), None) => {
                route.path_prefix = Some(format!("/{service}/"));
                return route;
            }
            (None, Some(_)) => {
                unsupported.push(Unsupported {
                    feature: "GrpcMethodOnlyMatch".into(),
                    detail: "gRPC match without a service name cannot be expressed".into(),
                });
            }
            (None, None) => {}
        }
    }

    if let Some(path) = &m.path_exact {
        route.path = Some(path.clone());
    } else if let Some(prefix) = &m.path_prefix {
        route.path_prefix = Some(prefix.clone());
    } else {
        route.path_prefix = Some("/".into());
    }
    if !m.headers_exact.is_empty() {
        route.headers = Some(m.headers_exact.clone());
    }
    route
}

fn filter_conditions(route: &RouteRuleIr, unsupported: &mut Vec<Unsupported>) -> Vec<Condition> {
    if route.matches.is_empty() {
        return Vec::new();
    }
    let grpc = matches!(route.route_family, RouteFamily::Grpc);
    let mut out = Vec::new();
    for m in &route.matches {
        let cm = condition_match(m, grpc, unsupported);
        if cm.is_empty() {
            // One unconstrained match makes the whole rule unconstrained.
            return Vec::new();
        }
        out.push(Condition { when: cm });
    }
    out
}

fn condition_match(
    m: &RouteMatchIr,
    grpc: bool,
    unsupported: &mut Vec<Unsupported>,
) -> ConditionMatch {
    let mut cm = ConditionMatch {
        grpc: grpc.then_some(true),
        path: m.path_exact.clone(),
        path_prefix: m.path_prefix.clone(),
        methods: m.method.iter().cloned().collect(),
        headers: m.headers_exact.clone(),
    };
    if let Some(method) = &m.method {
        unsupported.push(Unsupported {
            feature: "MethodConditions".into(),
            detail: format!(
                "route match restricts method to {method:?}; the native router cannot select a \
                 cluster by method, so other methods may also reach this backend"
            ),
        });
    }
    if !m.query_params_exact.is_empty() {
        unsupported.push(Unsupported {
            feature: "QueryParamConditions".into(),
            detail: "route conditions cannot include query parameters; filtering may be broader \
                     than the Gateway API match"
                .into(),
        });
    }
    if grpc {
        match (&m.grpc_service, &m.grpc_method) {
            (Some(service), Some(method)) => {
                cm.path = Some(format!("/{service}/{method}"));
                cm.path_prefix = None;
            }
            (Some(service), None) => {
                cm.path_prefix = Some(format!("/{service}/"));
                cm.path = None;
            }
            _ => {}
        }
    }
    cm
}

fn push_per_condition(
    out: &mut Vec<Filter>,
    conditions: &[Condition],
    make: impl Fn(Vec<Condition>) -> Filter,
) {
    if conditions.is_empty() {
        out.push(make(Vec::new()));
    } else {
        for condition in conditions {
            out.push(make(vec![condition.clone()]));
        }
    }
}

// -----------------------------------------------------------------------------
// Post-processing
// -----------------------------------------------------------------------------

/// Praxis 0.7.5 hashes the upstream pool key from address, SNI, client
/// certificate and verify flags, but not from the trust anchors. Two clusters
/// that dial the same address with the same SNI but different CAs therefore
/// share pooled connections, and the second cluster can skip certificate
/// verification entirely. Give every cluster involved in such a collision a
/// 1 ms idle timeout so its connections are not kept for reuse.
fn pin_shared_tls_pools(config: &mut PraxisConfig) {
    type Key = (String, Option<String>, Option<String>, bool);

    let mut ca_by_key: BTreeMap<Key, BTreeSet<Option<String>>> = BTreeMap::new();
    for chain in &config.filter_chains {
        for filter in &chain.filters {
            let Filter::LoadBalancer(lb) = filter else {
                continue;
            };
            for cluster in &lb.clusters {
                let Some(tls) = &cluster.tls else {
                    continue;
                };
                let client_cert = tls.client_cert.as_ref().map(|cert| cert.cert_path.clone());
                for endpoint in &cluster.endpoints {
                    let key = (
                        endpoint.address().to_owned(),
                        tls.sni.clone(),
                        client_cert.clone(),
                        tls.verify,
                    );
                    ca_by_key
                        .entry(key)
                        .or_default()
                        .insert(tls.ca.as_ref().map(|ca| ca.ca_path.clone()));
                }
            }
        }
    }

    let conflicting: BTreeSet<Key> = ca_by_key
        .into_iter()
        .filter(|(_, cas)| cas.len() > 1)
        .map(|(key, _)| key)
        .collect();
    if conflicting.is_empty() {
        return;
    }

    for chain in &mut config.filter_chains {
        for filter in &mut chain.filters {
            let Filter::LoadBalancer(lb) = filter else {
                continue;
            };
            for cluster in &mut lb.clusters {
                let Some(tls) = &cluster.tls else {
                    continue;
                };
                let client_cert = tls.client_cert.as_ref().map(|cert| cert.cert_path.clone());
                let collides = cluster.endpoints.iter().any(|endpoint| {
                    conflicting.contains(&(
                        endpoint.address().to_owned(),
                        tls.sni.clone(),
                        client_cert.clone(),
                        tls.verify,
                    ))
                });
                if collides {
                    cluster.idle_timeout_ms = Some(1);
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Filter builders
// -----------------------------------------------------------------------------

/// Build a static `Location` for a 303 response, which cannot use Praxis's
/// `${...}` redirect templates. A relative path is emitted whenever the
/// redirect does not change the authority, so the client resolves it against
/// the original request.
fn static_303_location(
    redirect: &RequestRedirectIr,
    route: &RouteRuleIr,
    listener_https: bool,
) -> String {
    let path = redirect
        .path_replace_full
        .clone()
        .or_else(|| {
            route
                .matches
                .iter()
                .find_map(|m| m.path_exact.clone().or_else(|| m.path_prefix.clone()))
        })
        .unwrap_or_else(|| "/".into());
    let host = redirect
        .hostname
        .clone()
        .or_else(|| route.hosts.first().cloned());
    let Some(host) = host else {
        return path;
    };
    let scheme = redirect
        .scheme
        .clone()
        .unwrap_or_else(|| if listener_https { "https" } else { "http" }.into());
    let port = redirect.port.map(|p| format!(":{p}")).unwrap_or_default();
    format!("{scheme}://{host}{port}{path}")
}

fn redirect_filter(
    redirect: &RequestRedirectIr,
    route_hosts: &[String],
    listener_port: u16,
    conditions: Vec<Condition>,
) -> RedirectFilter {
    // Gateway API defaults RequestRedirect.statusCode to 302.
    let requested = redirect.status_code.unwrap_or(302);
    let status = match requested {
        301 | 302 | 307 | 308 => requested as u16,
        _ => 302,
    };

    let scheme = redirect
        .scheme
        .clone()
        .unwrap_or_else(|| "${scheme}".into());
    let host = redirect
        .hostname
        .clone()
        .unwrap_or_else(|| "${host}".into());
    // Gateway API defaults the redirect port to the listener port. Omit it
    // only for the default HTTP/HTTPS ports so the Location stays canonical.
    let port = redirect
        .port
        .map(|p| format!(":{p}"))
        .or_else(|| {
            (listener_port != 80 && listener_port != 443).then(|| format!(":{listener_port}"))
        })
        .unwrap_or_default();
    let path = redirect
        .path_replace_full
        .clone()
        .unwrap_or_else(|| "${path}".into());
    let location = format!("{scheme}://{host}{port}{path}${{query}}");

    let allowed_hosts = if redirect.hostname.is_none() {
        route_hosts
            .iter()
            .filter(|host| !host.is_empty() && host.as_str() != "*")
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    RedirectFilter {
        status,
        location,
        allowed_hosts,
        conditions,
    }
}

fn header_filter(route: &RouteRuleIr, conditions: Vec<Condition>) -> HeadersFilter {
    let mut filter = HeadersFilter {
        conditions,
        ..HeadersFilter::default()
    };
    if let Some(req) = &route.request_header_modifier {
        filter.request_set = header_pairs(&req.set);
        filter.request_add = header_pairs(&req.add);
        filter.request_remove = req.remove.clone();
    }
    if let Some(res) = &route.response_header_modifier {
        filter.response_set = header_pairs(&res.set);
        filter.response_add = header_pairs(&res.add);
        filter.response_remove = res.remove.clone();
    }
    filter
}

fn header_pairs(headers: &[ugwc_core::ir::NamedHeaderIr]) -> Vec<HeaderPair> {
    headers
        .iter()
        .map(|h| HeaderPair {
            name: h.name.clone(),
            value: h.value.clone(),
        })
        .collect()
}

/// `regex::Regex::replace` expands `$name`/`$1`; escape literal dollars.
fn escape_replacement(value: &str) -> String {
    value.replace('$', "$$")
}

fn lcm(a: u64, b: u64) -> u64 {
    if a == 0 || b == 0 {
        return 0;
    }
    a.checked_div(gcd(a, b))
        .and_then(|quotient| quotient.checked_mul(b))
        .unwrap_or(u64::MAX)
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let remainder = a % b;
        a = b;
        b = remainder;
    }
    a
}

fn sanitize_name_component(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn valid_authority(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | ':'))
        && !value.starts_with('-')
        && !value.ends_with('-')
}

fn valid_sni(hostname: &str) -> bool {
    if hostname.is_empty() || hostname.contains('*') {
        return false;
    }
    hostname.parse::<std::net::IpAddr>().is_err()
}

fn tls_path(key: &str) -> String {
    format!("{TLS_MOUNT_DIR}/{key}")
}

// -----------------------------------------------------------------------------
// Names
// -----------------------------------------------------------------------------

#[derive(Default)]
struct UniqueNames {
    used: BTreeSet<String>,
}

impl UniqueNames {
    fn unique(&mut self, raw: &str) -> String {
        let base = sanitize_name(raw);
        if self.used.insert(base.clone()) {
            return base;
        }
        for n in 2.. {
            let candidate = format!("{base}-{n}");
            if self.used.insert(candidate.clone()) {
                return candidate;
            }
        }
        unreachable!("unbounded suffix search")
    }
}

fn sanitize_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_dash = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    let mut result: String = trimmed.chars().take(63).collect();
    while result.ends_with('-') {
        result.pop();
    }
    if result.is_empty() {
        "x".into()
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;
    use ugwc_core::ir::{
        BackendEndpointIr, BackendTlsIr, CorsFilterIr, IR_VERSION, InferencePoolBackendIr,
        IrRevision, IrScopeId, ListenerAttachmentIr, NamedHeaderIr, RequestHeaderModifierIr,
        RouteMatchIr, SnapshotMeta, UrlRewriteIr,
    };
    use zeroize::Zeroizing;

    fn listener(protocol: &str, port: u16, tls: TlsMode) -> ListenerIr {
        ListenerIr {
            section_name: format!("l{port}"),
            bind_port: port,
            tls,
            gateway_protocol: protocol.into(),
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            conflicted: false,
            hostname: None,
            tls_cert_pem: Vec::new(),
            tls_key_pem: Zeroizing::new(Vec::new()),
            allowed_route_group_kinds: Vec::new(),
        }
    }

    fn backend(ns: &str, name: &str, port: u16) -> BackendIr {
        BackendIr {
            service_namespace: ns.into(),
            service_name: name.into(),
            port_name: None,
            port_number: Some(port),
            backend_tls: None,
            inference: None,
            dial_port: None,
        }
    }

    fn route(name: &str, family: RouteFamily, backend: BackendIr) -> RouteRuleIr {
        RouteRuleIr {
            name: name.into(),
            route_family: family,
            hosts: Vec::new(),
            matches: Vec::new(),
            backends: vec![WeightedBackendIr { backend, weight: 1 }],
            request_redirect: None,
            request_header_modifier: None,
            response_header_modifier: None,
            url_rewrite: None,
            request_timeout_ms: None,
            backend_timeout_ms: None,
            cors: None,
            listener: Some(ListenerAttachmentIr {
                parent_kind: "Gateway".into(),
                parent_namespace: "default".into(),
                parent_name: "gw".into(),
                section_name: "l80".into(),
                port: 80,
            }),
        }
    }

    fn snapshot(
        listeners: Vec<ListenerIr>,
        routes: Vec<RouteRuleIr>,
        endpoints: Vec<(&str, &str, u16, &str)>,
    ) -> IrSnapshot {
        let mut backends = BTreeMap::new();
        for (ns, name, port, address) in endpoints {
            backends
                .entry(backend_endpoints_map_key(ns, name, port))
                .or_insert_with(Vec::new)
                .push(BackendEndpointIr {
                    address: address.into(),
                    port,
                });
        }
        IrSnapshot {
            ir_version: IR_VERSION,
            revision: IrRevision::Sequence(1),
            scope: IrScopeId::new("default", "gw"),
            listeners,
            routes,
            backends,
            meta: SnapshotMeta {
                correlation_id: Default::default(),
                has_errors: false,
            },
            policy: Default::default(),
            inference: Default::default(),
        }
    }

    fn filter<'a>(value: &'a Value, name: &str) -> &'a Value {
        value["filter_chains"][0]["filters"]
            .as_sequence()
            .unwrap()
            .iter()
            .find(|f| f["filter"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("filter {name} not found in {:?}", value["filter_chains"]))
    }

    #[test]
    fn encodes_http_listener_route_and_cluster() {
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![route(
                "r1",
                RouteFamily::Http,
                backend("default", "svc", 8080),
            )],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();

        assert_eq!(
            value["listeners"][0]["address"].as_str(),
            Some("0.0.0.0:80")
        );
        assert_eq!(value["listeners"][0]["protocol"].as_str(), Some("http"));

        let router = filter(&value, "router");
        assert_eq!(
            router["multi_level_subdomain_matching"].as_bool(),
            Some(true)
        );
        assert_eq!(router["routes"][0]["path_prefix"].as_str(), Some("/"));
        let cluster_name = router["routes"][0]["cluster"].as_str().unwrap();

        let lb = filter(&value, "load_balancer");
        assert_eq!(lb["clusters"][0]["name"].as_str(), Some(cluster_name));
        assert_eq!(
            lb["clusters"][0]["endpoints"][0].as_str(),
            Some("10.0.0.1:8080")
        );
        assert_eq!(
            value["insecure_options"]["allow_private_endpoints"].as_bool(),
            Some(true)
        );
        assert_eq!(
            value["insecure_options"]["allow_private_upstreams"].as_bool(),
            Some(true)
        );
        assert_eq!(
            value["admin"]["metrics_address"].as_str(),
            Some(ADMIN_METRICS_ADDRESS)
        );
        assert!(out.unsupported.is_empty());
    }

    #[test]
    fn merges_tls_listeners_sharing_a_port() {
        let mut a = listener("HTTPS", 443, TlsMode::Terminate);
        a.section_name = "a".into();
        a.tls_cert_pem = b"CERT-A".to_vec();
        a.tls_key_pem = Zeroizing::new(b"KEY-A".to_vec());
        let mut b = listener("HTTPS", 443, TlsMode::Terminate);
        b.section_name = "b".into();
        b.hostname = Some("b.example.com".into());
        b.tls_cert_pem = b"CERT-B".to_vec();
        b.tls_key_pem = Zeroizing::new(b"KEY-B".to_vec());

        let mut ra = route("ra", RouteFamily::Http, backend("default", "svc", 8080));
        ra.hosts = vec!["a.example.com".into()];
        ra.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "a".into(),
            port: 443,
        });
        let mut rb = route("rb", RouteFamily::Http, backend("default", "svc", 8080));
        rb.hosts = vec!["b.example.com".into()];
        rb.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "b".into(),
            port: 443,
        });

        let out = encode(&snapshot(
            vec![b, a],
            vec![rb, ra],
            vec![("default", "svc", 8080, "10.0.0.1")],
        ))
        .unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let listeners = value["listeners"].as_sequence().unwrap();
        assert_eq!(listeners.len(), 1);
        assert_eq!(listeners[0]["address"].as_str(), Some("0.0.0.0:443"));
        let certs = listeners[0]["tls"]["certificates"].as_sequence().unwrap();
        assert_eq!(certs.len(), 2);
        assert!(certs[0]["default"].as_bool().is_none());
        assert_eq!(
            certs[0]["server_names"].as_sequence().unwrap()[0]
                .as_str()
                .unwrap(),
            "b.example.com"
        );
        assert_eq!(certs[1]["default"].as_bool(), Some(true));
        assert_eq!(
            filter(&value, "router")["routes"]
                .as_sequence()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(out.files.len(), 4);
    }

    #[test]
    fn maps_exact_path_host_and_headers() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.hosts = vec!["api.example.com".into()];
        r.matches = vec![RouteMatchIr {
            path_exact: Some("/v1/thing".into()),
            headers_exact: BTreeMap::from([("x-tenant".into(), "acme".into())]),
            ..empty_match()
        }];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let route_value = &filter(&value, "router")["routes"][0];
        assert_eq!(route_value["path"].as_str(), Some("/v1/thing"));
        assert_eq!(route_value["host"].as_str(), Some("api.example.com"));
        assert_eq!(route_value["headers"]["x-tenant"].as_str(), Some("acme"));
    }

    #[test]
    fn grpc_routes_use_exact_path_and_h2_cluster() {
        let mut r = route("r1", RouteFamily::Grpc, backend("default", "grpc", 50051));
        r.matches = vec![RouteMatchIr {
            grpc_service: Some("helloworld.Greeter".into()),
            grpc_method: Some("SayHello".into()),
            ..empty_match()
        }];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "grpc", 50051, "10.0.0.2")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        assert_eq!(
            filter(&value, "router")["routes"][0]["path"].as_str(),
            Some("/helloworld.Greeter/SayHello")
        );
        assert_eq!(
            filter(&value, "load_balancer")["clusters"][0]["http"]["version"].as_str(),
            Some("h2")
        );
    }

    fn empty_match() -> RouteMatchIr {
        RouteMatchIr {
            path_prefix: None,
            path_exact: None,
            headers_exact: BTreeMap::new(),
            method: None,
            query_params_exact: BTreeMap::new(),
            grpc_service: None,
            grpc_method: None,
        }
    }

    #[test]
    fn terminates_tls_into_secret_files() {
        let mut ln = listener("HTTPS", 443, TlsMode::Terminate);
        ln.tls_cert_pem = b"cert-pem".to_vec();
        ln.tls_key_pem = Zeroizing::new(b"key-pem".to_vec());
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "l443".into(),
            port: 443,
        });
        let ir = snapshot(
            vec![ln],
            vec![r],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        assert_eq!(out.files.len(), 2);
        assert!(out.files.iter().any(|f| f.key.ends_with(".crt")));
        assert!(out.files.iter().any(|f| f.key.ends_with(".key")));
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let cert_path = value["listeners"][0]["tls"]["certificates"][0]["cert_path"]
            .as_str()
            .unwrap();
        assert!(cert_path.starts_with(TLS_MOUNT_DIR));
    }

    #[test]
    fn backend_tls_emits_ca_sni_and_client_cert() {
        let mut b = backend("default", "svc", 8443);
        b.backend_tls = Some(BackendTlsIr {
            ca_cert_pem: b"ca-pem".to_vec(),
            hostname: "svc.default.svc.cluster.local".into(),
            sans: Vec::new(),
            trust_system_ca: false,
            client_cert_pem: b"client-cert".to_vec(),
            client_key_pem: Zeroizing::new(b"client-key".to_vec()),
            invalid: false,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![route("r1", RouteFamily::Http, b)],
            vec![("default", "svc", 8443, "10.0.0.3")],
        );
        let out = encode(&ir).unwrap();
        assert_eq!(out.files.len(), 3);
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let tls = &filter(&value, "load_balancer")["clusters"][0]["tls"];
        assert_eq!(tls["verify"].as_bool(), Some(true));
        assert_eq!(tls["sni"].as_str(), Some("svc.default.svc.cluster.local"));
        assert!(
            tls["ca"]["ca_path"]
                .as_str()
                .unwrap()
                .starts_with(TLS_MOUNT_DIR)
        );
        assert!(
            tls["client_cert"]["cert_path"]
                .as_str()
                .unwrap()
                .starts_with(TLS_MOUNT_DIR)
        );
    }

    #[test]
    fn distinct_cas_on_shared_upstream_disable_connection_reuse() {
        let tls = |ca: &[u8]| BackendTlsIr {
            ca_cert_pem: ca.to_vec(),
            hostname: "abc.example.com".into(),
            sans: Vec::new(),
            trust_system_ca: false,
            client_cert_pem: Vec::new(),
            client_key_pem: Zeroizing::new(Vec::new()),
            invalid: false,
        };
        let mut a = backend("default", "svc-a", 8443);
        a.backend_tls = Some(tls(b"ca-a"));
        let mut b = backend("default", "svc-b", 8443);
        b.backend_tls = Some(tls(b"ca-b"));
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![
                route("ra", RouteFamily::Http, a),
                route("rb", RouteFamily::Http, b),
            ],
            vec![
                ("default", "svc-a", 8443, "10.0.0.3"),
                ("default", "svc-b", 8443, "10.0.0.3"),
            ],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let clusters = filter(&value, "load_balancer")["clusters"]
            .as_sequence()
            .unwrap();
        assert_eq!(clusters.len(), 2);
        for cluster in clusters {
            assert_eq!(cluster["idle_timeout_ms"].as_u64(), Some(1));
        }
    }

    #[test]
    fn redirect_builds_location_and_status() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.request_redirect = Some(RequestRedirectIr {
            hostname: Some("new.example.com".into()),
            scheme: Some("https".into()),
            port: None,
            status_code: Some(308),
            path_replace_full: Some("/new".into()),
            path_replace_prefix: None,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let redirect = filter(&value, "redirect");
        assert_eq!(redirect["status"].as_u64(), Some(308));
        assert_eq!(
            redirect["location"].as_str(),
            Some("https://new.example.com/new${query}")
        );
        let filters = value["filter_chains"][0]["filters"].as_sequence().unwrap();
        assert!(
            filters
                .iter()
                .all(|f| f["filter"].as_str() != Some("router"))
        );
    }

    #[test]
    fn reports_udp_as_unsupported() {
        let ir = snapshot(
            vec![listener("UDP", 53, TlsMode::Plaintext)],
            vec![],
            vec![],
        );
        let out = encode(&ir).unwrap();
        assert!(out.unsupported.iter().any(|u| u.feature == "UDPRoute"));
        assert_eq!(out.config.listeners.len(), 1);
        assert_eq!(out.config.listeners[0].address, "0.0.0.0:15000");
    }

    #[test]
    fn tcp_listener_uses_single_upstream() {
        let mut r = route("r1", RouteFamily::Tcp, backend("default", "svc", 9000));
        r.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "l9000".into(),
            port: 9000,
        });
        let ir = snapshot(
            vec![listener("TCP", 9000, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "svc", 9000, "10.0.0.4")],
        );
        let out = encode(&ir).unwrap();
        assert_eq!(
            out.config.listeners[0].upstream.as_deref(),
            Some("10.0.0.4:9000")
        );
    }

    #[test]
    fn tls_passthrough_uses_sni_router() {
        let mut r = route("r1", RouteFamily::Tls, backend("default", "svc", 443));
        r.hosts = vec!["api.example.com".into()];
        r.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "l8443".into(),
            port: 8443,
        });
        let ir = snapshot(
            vec![listener("TLS", 8443, TlsMode::Passthrough)],
            vec![r],
            vec![("default", "svc", 443, "10.0.0.5")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let sni = filter(&value, "sni_router");
        assert_eq!(
            sni["routes"][0]["server_names"][0].as_str(),
            Some("api.example.com")
        );
        assert_eq!(sni["routes"][0]["upstream"].as_str(), Some("10.0.0.5:443"));
    }

    #[test]
    fn weighted_endpoint_weights_reduce_by_gcd() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "a", 8080));
        r.backends[0].weight = 70;
        r.backends.push(WeightedBackendIr {
            backend: backend("default", "b", 8080),
            weight: 30,
        });
        r.matches = vec![empty_match()];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![
                ("default", "a", 8080, "10.0.0.1"),
                ("default", "a", 8080, "10.0.0.2"),
                ("default", "b", 8080, "10.0.0.3"),
                ("default", "b", 8080, "10.0.0.4"),
            ],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let lb = filter(&value, "load_balancer");
        let endpoints = lb["clusters"][0]["endpoints"].as_sequence().unwrap();
        let weights: Vec<u64> = endpoints
            .iter()
            .map(|endpoint| endpoint["weight"].as_u64().unwrap())
            .collect();
        assert_eq!(weights, vec![7, 7, 3, 3]);
    }

    #[test]
    fn weighted_backends_scale_by_endpoint_count() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "a", 8080));
        r.backends.push(WeightedBackendIr {
            backend: backend("default", "b", 8080),
            weight: 1,
        });
        r.matches = vec![empty_match()];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![
                ("default", "a", 8080, "10.0.0.1"),
                ("default", "a", 8080, "10.0.0.2"),
                ("default", "b", 8080, "10.0.0.3"),
            ],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let lb = filter(&value, "load_balancer");
        let endpoints = lb["clusters"][0]["endpoints"].as_sequence().unwrap();
        assert_eq!(endpoints.len(), 3);
        assert_eq!(endpoints[0]["weight"].as_u64(), Some(1));
        assert_eq!(endpoints[1]["weight"].as_u64(), Some(1));
        assert_eq!(endpoints[2]["weight"].as_u64(), Some(2));
    }

    #[test]
    fn weighted_backends_merge_into_one_cluster() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "a", 8080));
        r.backends[0].weight = 3;
        r.backends.push(WeightedBackendIr {
            backend: backend("default", "b", 8080),
            weight: 1,
        });
        r.matches = vec![empty_match()];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![
                ("default", "a", 8080, "10.0.0.1"),
                ("default", "b", 8080, "10.0.0.2"),
            ],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let lb = filter(&value, "load_balancer");
        let clusters = lb["clusters"].as_sequence().unwrap();
        assert_eq!(clusters.len(), 1);
        let endpoints = clusters[0]["endpoints"].as_sequence().unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0]["address"].as_str(), Some("10.0.0.1:8080"));
        assert_eq!(endpoints[0]["weight"].as_u64(), Some(3));
        assert_eq!(endpoints[1]["address"].as_str(), Some("10.0.0.2:8080"));
        assert_eq!(endpoints[1]["weight"].as_u64(), Some(1));
        assert!(
            !out.unsupported
                .iter()
                .any(|u| u.feature == "WeightedBackends")
        );
    }

    #[test]
    fn inference_pool_is_reported_and_skipped() {
        let mut b = backend("default", "pool", 8000);
        b.inference = Some(InferencePoolBackendIr {
            pool_namespace: "default".into(),
            pool_name: "pool".into(),
            picker_host: "pool.default.svc".into(),
            picker_port: 8000,
            failure_mode_fail_open: false,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![route("r1", RouteFamily::Http, b)],
            vec![],
        );
        let out = encode(&ir).unwrap();
        assert!(out.unsupported.iter().any(|u| u.feature == "InferencePool"));
        assert!(out.config.listeners[0].filter_chains.len() == 1);
    }

    #[test]
    fn unresolvable_backends_fail_closed_with_static_500() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.backends.clear();
        r.matches = vec![RouteMatchIr {
            path_prefix: Some("/v2".into()),
            ..empty_match()
        }];
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![],
        );
        let out = encode(&ir).unwrap();
        assert!(out.unsupported.iter().any(|u| u.feature == "NoBackend"));
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let response = filter(&value, "static_response");
        assert_eq!(response["status"].as_u64(), Some(500));
        assert_eq!(
            response["conditions"][0]["when"]["path_prefix"].as_str(),
            Some("/v2")
        );
        assert!(
            !value["filter_chains"][0]["filters"]
                .as_sequence()
                .unwrap()
                .iter()
                .any(|f| f["filter"].as_str() == Some("load_balancer"))
        );
    }

    #[test]
    fn backend_timeout_is_enforced_as_a_request_deadline() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.backend_timeout_ms = Some(500);
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        assert_eq!(filter(&value, "timeout")["timeout_ms"].as_u64(), Some(500));
    }

    #[test]
    fn url_rewrite_and_headers_and_timeouts_are_emitted() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.matches = vec![RouteMatchIr {
            path_prefix: Some("/api".into()),
            ..empty_match()
        }];
        r.url_rewrite = Some(UrlRewriteIr {
            hostname: Some("upstream.internal".into()),
            path_replace_prefix: Some("/v1".into()),
            path_replace_full: None,
        });
        r.request_header_modifier = Some(RequestHeaderModifierIr {
            set: vec![NamedHeaderIr {
                name: "x-added".into(),
                value: "1".into(),
            }],
            add: Vec::new(),
            remove: vec!["x-remove".into()],
        });
        r.response_header_modifier = Some(RequestHeaderModifierIr {
            set: Vec::new(),
            add: Vec::new(),
            remove: Vec::new(),
        });
        r.request_timeout_ms = Some(1500);
        r.cors = Some(CorsFilterIr {
            allow_origins: vec!["https://app.example.com".into()],
            allow_methods: vec!["GET".into()],
            allow_headers: vec!["Content-Type".into()],
            expose_headers: Vec::new(),
            max_age: 600,
            allow_credentials: false,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();

        let path_rewrite = filter(&value, "path_rewrite");
        assert_eq!(
            path_rewrite["replace"]["pattern"].as_str(),
            Some("^/api(.*)$")
        );
        assert_eq!(
            path_rewrite["replace"]["replacement"].as_str(),
            Some("/v1$1")
        );
        assert_eq!(
            path_rewrite["conditions"][0]["when"]["path_prefix"].as_str(),
            Some("/api")
        );

        let headers = filter(&value, "headers");
        assert_eq!(headers["request_set"][0]["name"].as_str(), Some("x-added"));
        assert_eq!(headers["request_remove"][0].as_str(), Some("x-remove"));

        let lb = filter(&value, "load_balancer");
        assert_eq!(
            lb["clusters"][0]["http"]["authority"].as_str(),
            Some("upstream.internal")
        );
        assert!(lb["clusters"][0]["http"].get("version").is_none());

        let timeout = filter(&value, "timeout");
        assert_eq!(timeout["timeout_ms"].as_u64(), Some(1500));

        let cors = filter(&value, "cors");
        assert_eq!(cors["max_age"].as_u64(), Some(600));
        assert_eq!(
            cors["conditions"][0]["when"]["path_prefix"].as_str(),
            Some("/api")
        );
        assert_eq!(
            value["insecure_options"]["skip_pipeline_checks"]["conditional_security"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn wildcard_cors_credentials_are_reported_and_dropped() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.cors = Some(CorsFilterIr {
            allow_origins: vec!["*".into()],
            allow_methods: vec!["GET".into()],
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age: 60,
            allow_credentials: true,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![r],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        assert!(
            out.unsupported
                .iter()
                .any(|u| u.feature == "CorsCredentialsWildcard")
        );
        let cors = out.config.filter_chains[0]
            .filters
            .iter()
            .find_map(|f| match f {
                Filter::Cors(c) => Some(c),
                _ => None,
            })
            .expect("cors filter");
        assert!(!cors.allow_credentials);
    }

    #[test]
    fn distinct_scoped_cors_policies_are_all_emitted() {
        let mut a = route("a", RouteFamily::Http, backend("default", "svc", 8080));
        a.matches = vec![RouteMatchIr {
            path_prefix: Some("/a".into()),
            ..empty_match()
        }];
        a.cors = Some(CorsFilterIr {
            allow_origins: vec!["https://a.example.com".into()],
            allow_methods: vec!["GET".into()],
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age: 60,
            allow_credentials: false,
        });
        let mut b = route("b", RouteFamily::Http, backend("default", "svc", 8080));
        b.matches = vec![RouteMatchIr {
            path_prefix: Some("/b".into()),
            ..empty_match()
        }];
        b.cors = Some(CorsFilterIr {
            allow_origins: vec!["https://b.example.com".into()],
            allow_methods: vec!["POST".into()],
            allow_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age: 60,
            allow_credentials: true,
        });
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![a, b],
            vec![("default", "svc", 8080, "10.0.0.1")],
        );
        let out = encode(&ir).unwrap();
        assert!(
            !out.unsupported
                .iter()
                .any(|u| u.feature == "MultipleCorsConfigs")
        );
        let cors: Vec<_> = out.config.filter_chains[0]
            .filters
            .iter()
            .filter_map(|f| match f {
                Filter::Cors(c) => Some(c),
                _ => None,
            })
            .collect();
        assert_eq!(cors.len(), 2);
        assert_eq!(
            cors[0].conditions[0].when.path_prefix.as_deref(),
            Some("/a")
        );
        assert_eq!(
            cors[1].conditions[0].when.path_prefix.as_deref(),
            Some("/b")
        );
    }

    #[test]
    fn omits_router_for_route_less_listener() {
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![],
            vec![],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        assert!(
            value["filter_chains"][0]["filters"]
                .as_sequence()
                .unwrap()
                .is_empty()
        );
        assert!(out.unsupported.is_empty());
    }

    #[test]
    fn empty_listener_set_gets_a_noop_listener() {
        let ir = snapshot(Vec::new(), Vec::new(), Vec::new());
        let out = encode(&ir).unwrap();
        assert_eq!(out.config.listeners.len(), 1);
        assert_eq!(out.config.listeners[0].address, "0.0.0.0:15000");
        assert_eq!(out.config.listeners[0].filter_chains.len(), 1);
        assert!(out.config.filter_chains[0].filters.is_empty());
    }

    #[test]
    fn tls_listener_without_routes_binds_port_and_rejects() {
        let ir = snapshot(
            vec![listener("TLS", 443, TlsMode::Passthrough)],
            vec![],
            vec![],
        );
        let out = encode(&ir).unwrap();
        assert!(
            out.unsupported
                .iter()
                .any(|u| u.feature == "TlsListenerWithoutRoutes")
        );
        assert_eq!(out.config.listeners.len(), 1);
        assert_eq!(out.config.listeners[0].address, "0.0.0.0:443");
        assert_eq!(out.config.listeners[0].protocol, Some(Protocol::Tcp));
        assert!(out.config.listeners[0].upstream.is_none());
        assert!(out.config.filter_chains[0].filters.is_empty());
    }

    #[test]
    fn tcp_listener_without_routes_binds_port_and_rejects() {
        let ir = snapshot(
            vec![listener("TCP", 9000, TlsMode::Plaintext)],
            vec![],
            vec![],
        );
        let out = encode(&ir).unwrap();
        assert_eq!(out.config.listeners.len(), 1);
        assert_eq!(out.config.listeners[0].address, "0.0.0.0:9000");
        assert_eq!(out.config.listeners[0].protocol, Some(Protocol::Tcp));
        assert!(out.config.listeners[0].upstream.is_none());
        assert!(out.config.filter_chains[0].filters.is_empty());
    }

    #[test]
    fn redirect_303_uses_static_response() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.listener = Some(ListenerAttachmentIr {
            parent_kind: "Gateway".into(),
            parent_namespace: "default".into(),
            parent_name: "gw".into(),
            section_name: "l18080".into(),
            port: 18080,
        });
        r.matches = vec![RouteMatchIr {
            path_prefix: Some("/old".into()),
            ..empty_match()
        }];
        r.request_redirect = Some(RequestRedirectIr {
            hostname: None,
            scheme: None,
            port: None,
            status_code: Some(303),
            path_replace_full: Some("/new".into()),
            path_replace_prefix: None,
        });
        let ir = snapshot(
            vec![listener("HTTP", 18080, TlsMode::Plaintext)],
            vec![r],
            vec![],
        );
        let out = encode(&ir).unwrap();
        let value: Value = serde_yaml::from_str(&out.yaml).unwrap();
        let response = filter(&value, "static_response");
        assert_eq!(response["status"].as_u64(), Some(303));
        assert_eq!(response["headers"][0]["name"].as_str(), Some("Location"));
        assert_eq!(response["headers"][0]["value"].as_str(), Some("/new"));
        assert_eq!(response["body"].as_str(), Some("See Other"));
        assert!(
            out.unsupported
                .iter()
                .any(|u| u.feature == "RedirectStatus303Location")
        );
    }

    #[test]
    fn brackets_ipv6_endpoints() {
        let ir = snapshot(
            vec![listener("HTTP", 80, TlsMode::Plaintext)],
            vec![route(
                "r1",
                RouteFamily::Http,
                backend("default", "v6", 8080),
            )],
            vec![("default", "v6", 8080, "fd00::1")],
        );
        let out = encode(&ir).unwrap();
        assert_eq!(
            out.config.filter_chains[0]
                .filters
                .iter()
                .find_map(|f| match f {
                    Filter::LoadBalancer(lb) => {
                        lb.clusters[0]
                            .endpoints
                            .first()
                            .map(|e| e.address().to_owned())
                    }
                    _ => None,
                })
                .as_deref(),
            Some("[fd00::1]:8080")
        );
    }
}
