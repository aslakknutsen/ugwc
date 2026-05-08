//! Async wiring: admin + xDS + optional Kubernetes watches (Merge A).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use kube::api::Api;
use tracing::{info, warn};
use ugwc_core::publish::plan_publish;
use ugwc_core::{http_route_resolved_refs_row, CompileInput, FailurePolicy, PublishAction};
use ugwc_k8s::resources::Gateway;
use ugwc_xds::{
    serve as serve_xds, SharedSnapshot, SnapshotCache, XdsSecurityConfig, XdsServerConfig,
};

use crate::config::Config;
use crate::leader::LeaderState;
use crate::metrics;

pub async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let prom = metrics::install()?.clone();

    let leader = if cfg.leader_election {
        warn!("leader_election=true but Lease wiring not complete — acting as follower stub");
        metrics::set_leader_active(false);
        LeaderState::follower_stub()
    } else {
        metrics::set_leader_active(true);
        LeaderState::leader_always()
    };

    let fp = FailurePolicy::default();
    let initial_out = ugwc_core::compile(
        &fp,
        &CompileInput {
            reconcile_gateway: None,
            ..Default::default()
        },
    )?;
    let cache: Arc<SharedSnapshot> = Arc::new(SharedSnapshot::new(initial_out.snapshot));

    let shutdown = tokio_util::sync::CancellationToken::new();
    let admin_sd = shutdown.child_token();
    let xds_sd = shutdown.child_token();

    let admin_addr: SocketAddr = cfg.admin_listen_addr.parse()?;
    let admin_task = tokio::spawn(crate::admin::serve(admin_addr, admin_sd, prom));

    let xds_addr: SocketAddr = cfg.xds_listen_addr.parse()?;
    let xds_cfg = XdsServerConfig {
        listen_addr: xds_addr,
        security: XdsSecurityConfig {
            allow_insecure: cfg.xds_insecure,
            tls_cert_path: cfg.xds_tls_cert_path.clone(),
            tls_key_path: cfg.xds_tls_key_path.clone(),
            tls_ca_path: cfg.xds_tls_ca_path.clone(),
        },
        allow_delta: cfg.xds_delta,
        xds_wire: cfg.xds_wire.clone(),
    };

    let cache_xds: Arc<SharedSnapshot> = cache.clone();
    let xds_task = tokio::spawn(async move {
        if let Err(e) = serve_xds(xds_cfg, cache_xds, xds_sd).await {
            tracing::error!(error = %e, "xds server exited");
        }
    });

    if cfg.kube_enable {
        let fp_k = fp.clone();
        let cache_k = cache.clone();
        let client = match kube::Client::try_default().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "kube client unavailable — disable UGWC_KUBE_ENABLE or fix kubeconfig");
                wait_shutdown(shutdown, admin_task, xds_task, None).await;
                return Ok(());
            }
        };
        let kube_sd = shutdown.child_token();
        let debounce = std::time::Duration::from_millis(cfg.endpoint_debounce_ms);
        let leader_k = leader.clone();
        let watch_client = client.clone();
        let reconcile_client = client.clone();
        let controller_names = cfg.controller_name_list();
        let primary_controller = controller_names
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        let status_cache: std::sync::Arc<
            std::sync::Mutex<std::collections::HashMap<String, String>>,
        > = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let status_cache_k = status_cache.clone();
        let kube_task = tokio::spawn(async move {
            let rfn: ugwc_k8s::ReconcileFn = Arc::new(move |ns: String, name: String| {
                metrics::record_reconcile();
                let fp_k = fp_k.clone();
                let cache_k = cache_k.clone();
                let leader_k = leader_k.clone();
                let client_inner = reconcile_client.clone();
                let primary_controller = primary_controller.clone();
                let status_cache = status_cache_k.clone();
                Box::pin(async move {
                    let api: Api<Gateway> = Api::namespaced(client_inner.clone(), &ns);
                    let latest = api.get(&name).await.map_err(|e| e.to_string())?;

                    let t_load = Instant::now();
                    let cluster =
                        match ugwc_k8s::load_cluster_snapshot(&client_inner, &ns, &name).await {
                            Ok(c) => Some(c),
                            Err(e) => {
                                warn!(error = %e, %ns, %name, "cluster snapshot load");
                                None
                            }
                        };
                    metrics::observe_snapshot_load_seconds(t_load.elapsed().as_secs_f64());

                    let input = CompileInput {
                        reconcile_gateway: Some((ns.clone(), name.clone())),
                        cluster,
                        ..Default::default()
                    };
                    let t_compile = Instant::now();
                    let co = ugwc_core::compile(&fp_k, &input).map_err(|e| e.to_string())?;
                    metrics::observe_compile_seconds(t_compile.elapsed().as_secs_f64());

                    let plan = plan_publish(
                        Some(SnapshotCache::current(cache_k.as_ref()).as_ref()),
                        &co.snapshot,
                        &fp_k,
                    );

                    if co.report.has_errors() {
                        metrics::record_compile_error();
                    }

                    if !leader_k.is_leader() {
                        metrics::record_publish_skipped();
                        return Ok(());
                    }

                    let programmed_ok = !co.report.has_errors() && !co.snapshot.meta.has_errors;
                    if let Err(e) = ugwc_k8s::apply_gateway_reconciled_status(
                        &api,
                        &latest,
                        &co,
                        Some(status_cache.clone()),
                    )
                    .await
                    {
                        warn!(error = %e, %ns, %name, "gateway status patch");
                    }

                    if let Some(ref snap) = input.cluster {
                        for r in &snap.http_routes {
                            let (rs, rr, rm) = http_route_resolved_refs_row(&co.report, r);
                            if let Err(e) = ugwc_k8s::apply_http_route_parent_status(
                                ugwc_k8s::HttpRouteParentStatusArgs {
                                    client: &client_inner,
                                    route_ns: &r.namespace,
                                    route_name: &r.name,
                                    gateway_ns: &ns,
                                    gateway_name: &name,
                                    controller_name: &primary_controller,
                                    route_generation: r.generation,
                                    programmed_ok,
                                    resolved_refs_status: rs,
                                    resolved_refs_reason: rr,
                                    resolved_refs_message: rm,
                                    status_cache: Some(status_cache.clone()),
                                },
                            )
                            .await
                            {
                                warn!(error = %e, route = %r.name, "httproute status patch");
                            }
                        }
                    }

                    match plan.action {
                        PublishAction::Skip => metrics::record_publish_skipped(),
                        PublishAction::FullReplace => {
                            cache_k.store(co.snapshot);
                            metrics::record_publish_applied();
                        }
                        PublishAction::Delta => {}
                        _ => {}
                    }
                    Ok(())
                })
            });
            ugwc_k8s::run_gateway_controller(
                watch_client,
                kube_sd,
                debounce,
                rfn,
                controller_names.clone(),
            )
            .await;
        });
        wait_shutdown(shutdown, admin_task, xds_task, Some(kube_task)).await;
    } else {
        wait_shutdown(shutdown, admin_task, xds_task, None).await;
    }

    Ok(())
}

async fn wait_shutdown(
    shutdown: tokio_util::sync::CancellationToken,
    admin: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    xds: tokio::task::JoinHandle<()>,
    kube: Option<tokio::task::JoinHandle<()>>,
) {
    tokio::signal::ctrl_c()
        .await
        .expect("install ctrl-c handler");
    info!("signal received — draining");
    shutdown.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), admin).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), xds).await;
    if let Some(k) = kube {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), k).await;
    }
}
