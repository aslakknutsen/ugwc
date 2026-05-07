//! Async wiring: admin + xDS + optional Kubernetes watches (Merge A).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use kube::ResourceExt;
use tracing::{info, warn};
use ugwc_core::publish::plan_publish;
use ugwc_core::{CompileInput, FailurePolicy, PublishAction};
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
    };

    let cache_xds: Arc<SharedSnapshot> = cache.clone();
    let xds_task = tokio::spawn(async move {
        if let Err(e) = serve_xds(xds_cfg, cache_xds, xds_sd).await {
            tracing::error!(error = %e, "xds server exited");
        }
    });

    if cfg.kube_enable {
        let fp_k = fp.clone();
        let ns_default = cfg.kube_namespace.clone();
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
        let run_ns = cfg.kube_namespace.clone();
        let leader_k = leader.clone();
        let kube_task = tokio::spawn(async move {
            let rfn: ugwc_k8s::ReconcileFn = Arc::new(move |gw: Gateway| {
                metrics::record_reconcile();
                let t0 = Instant::now();
                let ns = gw.namespace().unwrap_or_else(|| ns_default.clone());
                let name = gw.name_any();
                let input = CompileInput {
                    reconcile_gateway: Some((ns, name)),
                    ..Default::default()
                };
                let co = ugwc_core::compile(&fp_k, &input).map_err(|e| e.to_string())?;

                let plan = plan_publish(
                    Some(SnapshotCache::current(cache_k.as_ref()).as_ref()),
                    &co.snapshot,
                    &fp_k,
                );

                metrics::observe_compile_seconds(t0.elapsed().as_secs_f64());
                if co.report.has_errors() {
                    metrics::record_compile_error();
                }

                if !leader_k.is_leader() {
                    metrics::record_publish_skipped();
                    return Ok(());
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
            });
            ugwc_k8s::run_gateway_controller(client, run_ns, kube_sd, rfn).await;
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
