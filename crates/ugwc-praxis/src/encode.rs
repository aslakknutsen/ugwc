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
    backend_endpoints_map_key, BackendIr, BackendTlsIr, IrSnapshot, ListenerIr, RequestRedirectIr,
    RouteFamily, RouteMatchIr, RouteRuleIr, TlsMode, WeightedBackendIr,
};

use crate::model::{
    CaConfig, CertKeyPair, Cluster, ClusterHttp, ClusterTls, Condition, ConditionMatch, CorsFilter,
    Filter, FilterChain, HeaderPair, HeadersFilter, InsecureOptions, Listener, ListenerTls,
    LoadBalancerFilter, PathRewriteFilter, PraxisConfig, Protocol, RedirectFilter, RegexReplace,
    RouterFilter, RouterRoute, SniRoute, SniRouterFilter, TimeoutFilter,
};

/// Directory where the distribution layer mounts `praxis.yaml`.
pub const CONFIG_MOUNT_DIR: &str = "/etc/praxis";
/// Directory where the distribution layer mounts the TLS Secret.
pub const TLS_MOUNT_DIR: &str = "/etc/praxis-tls";
/// Name of the config document inside the ConfigMap.
pub const CONFIG_FILE_NAME: &str = "praxis.yaml";

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

    for ln in &ir.listeners {
        if ln.conflicted {
            continue;
        }
        let routes: Vec<&RouteRuleIr> = ir.routes.iter().filter(|r| attached(r, ln)).collect();
        let protocol = effective_protocol(ln);
        match protocol {
            "HTTP" | "HTTPS" => {
                if let Some((listener, chain)) = state.http_listener(ln, &routes) {
                    listeners.push(listener);
                    filter_chains.push(chain);
                }
            }
            "TLS" => {
                if let Some((listener, chain)) = state.tls_listener(ln, &routes) {
                    listeners.push(listener);
                    filter_chains.push(chain);
                }
            }
            "TCP" => {
                if let Some(listener) = state.tcp_listener(ln, &routes) {
                    listeners.push(listener);
                }
            }
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

    let config = PraxisConfig {
        listeners,
        filter_chains,
        insecure_options: Some(InsecureOptions {
            allow_private_endpoints: true,
        }),
    };
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
        ln: &ListenerIr,
        routes: &[&RouteRuleIr],
    ) -> Option<(Listener, FilterChain)> {
        let name = self
            .names
            .unique(&format!("{}-{}", ln.parent_name, ln.section_name));
        let tls = if ln.tls == TlsMode::Terminate {
            if ln.tls_cert_pem.is_empty() || ln.tls_key_pem.is_empty() {
                self.unsupported(
                    "ListenerTlsMaterialMissing",
                    format!(
                        "listener {}/{} terminates TLS but has no certificate material",
                        ln.parent_namespace, ln.section_name
                    ),
                );
                return None;
            }
            let cert_key = format!("listener-tls/{name}.crt");
            let key_key = format!("listener-tls/{name}.key");
            self.files.push(ConfigFile {
                key: cert_key.clone(),
                contents: ln.tls_cert_pem.clone(),
            });
            self.files.push(ConfigFile {
                key: key_key.clone(),
                contents: ln.tls_key_pem.to_vec(),
            });
            Some(ListenerTls {
                certificates: vec![CertKeyPair {
                    cert_path: tls_path(&cert_key),
                    key_path: tls_path(&key_key),
                }],
                hot_reload: None,
            })
        } else {
            None
        };

        let mut cors_filters = Vec::new();
        let mut redirect_filters = Vec::new();
        let mut router_routes = Vec::new();
        let mut post_router = Vec::new();
        let mut referenced: BTreeSet<String> = BTreeSet::new();

        for route in routes {
            if !matches!(route.route_family, RouteFamily::Http | RouteFamily::Grpc) {
                continue;
            }
            let conditions = filter_conditions(route, &mut self.unsupported);
            let has_rule_filters = route.cors.is_some()
                || route.request_redirect.is_some()
                || route.url_rewrite.is_some()
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
                        route.name, ln.section_name
                    ),
                );
            }

            if let Some(redirect) = &route.request_redirect {
                push_per_condition(&mut redirect_filters, &conditions, |conds| {
                    Filter::Redirect(redirect_filter(redirect, &route.hosts, conds))
                });
                continue;
            }

            let Some(cluster) = self.route_cluster(route) else {
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
                let cors_conditions = cors_conditions(route);
                push_per_condition(&mut cors_filters, &cors_conditions, |conds| {
                    Filter::Cors(CorsFilter {
                        allow_origins: cors.allow_origins.clone(),
                        allow_methods: cors.allow_methods.clone(),
                        allow_headers: cors.allow_headers.clone(),
                        expose_headers: cors.expose_headers.clone(),
                        max_age: cors.max_age,
                        allow_credentials: cors.allow_credentials,
                        conditions: conds,
                    })
                });
            }

            if let Some(rewrite) = &route.url_rewrite {
                if let Some((pattern, replacement)) = rewrite_path_pattern(rewrite) {
                    push_per_condition(&mut post_router, &conditions, |conds| {
                        Filter::PathRewrite(PathRewriteFilter {
                            strip_prefix: None,
                            add_prefix: None,
                            replace: Some(RegexReplace {
                                pattern: pattern.clone(),
                                replacement: replacement.clone(),
                            }),
                            allow_rewrite_override: true,
                            conditions: conds,
                        })
                    });
                }
            }

            let rewrite_host = route
                .url_rewrite
                .as_ref()
                .and_then(|rewrite| rewrite.hostname.clone());
            if rewrite_host.is_some()
                || route.request_header_modifier.is_some()
                || route.response_header_modifier.is_some()
            {
                push_per_condition(&mut post_router, &conditions, |conds| {
                    Filter::Headers(header_filter(route, rewrite_host.clone(), conds))
                });
            }

            if let Some(ms) = route.request_timeout_ms {
                if ms > 0 {
                    push_per_condition(&mut post_router, &conditions, |conds| {
                        Filter::Timeout(TimeoutFilter {
                            timeout_ms: ms,
                            conditions: conds,
                        })
                    });
                }
            } else if route.backend_timeout_ms.is_some() {
                self.unsupported(
                    "BackendRequestTimeout",
                    format!(
                        "route {} sets backendRequest timeout; Praxis timeout applies to the whole \
                         request",
                        route.name
                    ),
                );
            }
        }

        let clusters: Vec<Cluster> = referenced
            .iter()
            .filter_map(|name| self.clusters.get(name).cloned())
            .collect();

        let mut filters = Vec::new();
        filters.extend(cors_filters);
        filters.extend(redirect_filters);
        // An empty router keeps a route-less listener valid and ready for the
        // next config revision.
        filters.push(Filter::Router(RouterFilter {
            routes: router_routes,
        }));
        filters.extend(post_router);
        if !clusters.is_empty() {
            filters.push(Filter::LoadBalancer(LoadBalancerFilter { clusters }));
        }

        let listener = Listener {
            name: name.clone(),
            address: format!("0.0.0.0:{}", ln.bind_port),
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
        ln: &ListenerIr,
        routes: &[&RouteRuleIr],
    ) -> Option<(Listener, FilterChain)> {
        let name = self
            .names
            .unique(&format!("{}-{}", ln.parent_name, ln.section_name));
        let mut sni_routes = Vec::new();
        let mut default_upstream = None;

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
                sni_routes.push(SniRoute {
                    server_names: route.hosts.clone(),
                    upstream,
                });
            }
        }

        if sni_routes.is_empty() && default_upstream.is_none() {
            self.unsupported(
                "TlsListenerWithoutRoutes",
                format!(
                    "TLS listener {}/{} has no routable TLSRoute",
                    ln.parent_namespace, ln.section_name
                ),
            );
            return None;
        }

        let listener = Listener {
            name: name.clone(),
            address: format!("0.0.0.0:{}", ln.bind_port),
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

    fn tcp_listener(&mut self, ln: &ListenerIr, routes: &[&RouteRuleIr]) -> Option<Listener> {
        let route = routes
            .iter()
            .find(|r| matches!(r.route_family, RouteFamily::Tcp))?;
        let Some(upstream) = self.first_upstream(route) else {
            self.unsupported(
                "TcpListenerWithoutUpstream",
                format!(
                    "TCP listener {}/{} has no resolvable backend",
                    ln.parent_namespace, ln.section_name
                ),
            );
            return None;
        };
        let name = self
            .names
            .unique(&format!("{}-{}", ln.parent_name, ln.section_name));
        Some(Listener {
            name,
            address: format!("0.0.0.0:{}", ln.bind_port),
            protocol: Some(Protocol::Tcp),
            filter_chains: Vec::new(),
            tls: None,
            upstream: Some(upstream),
        })
    }

    // -- Backends -----------------------------------------------------------

    fn route_cluster(&mut self, route: &RouteRuleIr) -> Option<String> {
        let grpc = matches!(route.route_family, RouteFamily::Grpc);
        let active: Vec<&WeightedBackendIr> =
            route.backends.iter().filter(|wb| wb.weight > 0).collect();
        if active.is_empty() {
            self.unsupported(
                "NoBackend",
                format!("route {} has no backend with non-zero weight", route.name),
            );
            return None;
        }
        let mut names: Vec<String> = Vec::new();
        for wb in &active {
            if let Some(name) = self.cluster_for(&wb.backend, grpc) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        if names.is_empty() {
            return None;
        }
        if names.len() > 1 {
            self.unsupported(
                "WeightedBackends",
                format!(
                    "route {} splits traffic across {} clusters; using only {:?}",
                    route.name,
                    names.len(),
                    names[0]
                ),
            );
        }
        names.into_iter().next()
    }

    fn cluster_for(&mut self, backend: &BackendIr, grpc: bool) -> Option<String> {
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
        if let Some(name) = self.cluster_names.get(&key) {
            if grpc {
                if let Some(cluster) = self.clusters.get_mut(name) {
                    cluster.http = Some(ClusterHttp {
                        version: "h2".into(),
                    });
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

        let name = self.cluster_name_registry.unique(&format!(
            "{}-{}-{}",
            backend.service_namespace, backend.service_name, port
        ));
        let tls = backend
            .backend_tls
            .as_ref()
            .and_then(|tls| self.cluster_tls(&name, tls));
        let cluster = Cluster {
            name: name.clone(),
            endpoints,
            tls,
            http: grpc.then(|| ClusterHttp {
                version: "h2".into(),
            }),
        };
        self.cluster_names.insert(key, name.clone());
        self.clusters.insert(name.clone(), cluster);
        Some(name)
    }

    fn cluster_tls(&mut self, cluster: &str, tls: &BackendTlsIr) -> Option<ClusterTls> {
        let ca = if tls.ca_cert_pem.is_empty() {
            None
        } else {
            let key = format!("backend-tls/{cluster}.ca.pem");
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
            let cert_key = format!("backend-tls/{cluster}.crt");
            let key_key = format!("backend-tls/{cluster}.key");
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
                key_path: tls_path(&key_key),
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
                .map(|e| format!("{}:{}", e.address, e.port))
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

fn cors_conditions(route: &RouteRuleIr) -> Vec<Condition> {
    if route.matches.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for m in &route.matches {
        let cm = ConditionMatch {
            path: m.path_exact.clone(),
            path_prefix: m.path_prefix.clone(),
            ..ConditionMatch::default()
        };
        if cm.is_empty() {
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
// Filter builders
// -----------------------------------------------------------------------------

fn redirect_filter(
    redirect: &RequestRedirectIr,
    route_hosts: &[String],
    conditions: Vec<Condition>,
) -> RedirectFilter {
    let default_status = if redirect.path_replace_full.is_some() {
        301
    } else {
        302
    };
    let status = match redirect.status_code.unwrap_or(default_status) {
        301 | 302 | 307 | 308 => redirect.status_code.unwrap_or(default_status) as u16,
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
    let port = redirect.port.map(|p| format!(":{p}")).unwrap_or_default();
    let path = redirect
        .path_replace_full
        .clone()
        .unwrap_or_else(|| "${path}".into());
    // `path_replace_prefix` is not representable in a redirect template; keep
    // the request path rather than silently redirecting to the wrong tree.
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

fn rewrite_path_pattern(rewrite: &ugwc_core::ir::UrlRewriteIr) -> Option<(String, String)> {
    if let Some(full) = &rewrite.path_replace_full {
        return Some(("^.*$".into(), escape_replacement(full)));
    }
    if let Some(prefix) = &rewrite.path_replace_prefix {
        return Some((
            format!("^{}(.*)$", regex_escape(prefix)),
            format!("{}$1", escape_replacement(prefix)),
        ));
    }
    None
}

fn header_filter(
    route: &RouteRuleIr,
    rewrite_host: Option<String>,
    conditions: Vec<Condition>,
) -> HeadersFilter {
    let mut filter = HeadersFilter {
        conditions,
        ..HeadersFilter::default()
    };
    if let Some(host) = rewrite_host {
        filter.request_set.push(HeaderPair {
            name: "Host".into(),
            value: host,
        });
    }
    if let Some(req) = &route.request_header_modifier {
        filter.request_set.extend(
            header_pairs(&req.set)
                .into_iter()
                .filter(|h| !h.name.eq_ignore_ascii_case("host")),
        );
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
        BackendEndpointIr, BackendTlsIr, CorsFilterIr, InferencePoolBackendIr, IrRevision,
        IrScopeId, ListenerAttachmentIr, NamedHeaderIr, RequestHeaderModifierIr, RouteMatchIr,
        SnapshotMeta, UrlRewriteIr, IR_VERSION,
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
            backends.insert(
                backend_endpoints_map_key(ns, name, port),
                vec![BackendEndpointIr {
                    address: address.into(),
                    port,
                }],
            );
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
        assert!(out.unsupported.is_empty());
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
        assert!(tls["ca"]["ca_path"]
            .as_str()
            .unwrap()
            .starts_with(TLS_MOUNT_DIR));
        assert!(tls["client_cert"]["cert_path"]
            .as_str()
            .unwrap()
            .starts_with(TLS_MOUNT_DIR));
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
        assert!(filter(&value, "router")["routes"]
            .as_sequence()
            .unwrap()
            .is_empty());
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
        assert!(out.config.listeners.is_empty());
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
    fn weighted_multi_cluster_is_reported() {
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
                ("default", "b", 8080, "10.0.0.2"),
            ],
        );
        let out = encode(&ir).unwrap();
        assert!(out
            .unsupported
            .iter()
            .any(|u| u.feature == "WeightedBackends"));
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
    fn url_rewrite_and_headers_and_timeouts_are_emitted() {
        let mut r = route("r1", RouteFamily::Http, backend("default", "svc", 8080));
        r.matches = vec![RouteMatchIr {
            path_prefix: Some("/api".into()),
            ..empty_match()
        }];
        r.url_rewrite = Some(UrlRewriteIr {
            hostname: Some("upstream.internal".into()),
            path_replace_prefix: Some("/api".into()),
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
            Some("/api$1")
        );
        assert_eq!(
            path_rewrite["conditions"][0]["when"]["path_prefix"].as_str(),
            Some("/api")
        );

        let headers = filter(&value, "headers");
        assert_eq!(headers["request_set"][0]["name"].as_str(), Some("Host"));
        assert_eq!(headers["request_remove"][0].as_str(), Some("x-remove"));

        let timeout = filter(&value, "timeout");
        assert_eq!(timeout["timeout_ms"].as_u64(), Some(1500));

        let cors = filter(&value, "cors");
        assert_eq!(cors["max_age"].as_u64(), Some(600));
    }
}
