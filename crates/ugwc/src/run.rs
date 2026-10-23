//! Async wiring: admin + xDS + optional Kubernetes watches (Merge A).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};
use ugwc_core::{CompileInput, FailurePolicy};
use ugwc_xds::{
    serve as serve_xds, MultiScopeSnapshot, XdsSecurityConfig, XdsServerConfig, XdsSnapshotSource,
};

use crate::config::Config;
use crate::leader::LeaderState;
use crate::leader_election::LeaseElectionHandle;
use crate::metrics;
use crate::readiness::Readiness;

pub async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let m = metrics::install()?;
    crate::tokio_runtime_metrics::spawn(m.tokio.clone());
    if let Some(ref ep) = cfg.otlp_endpoint {
        if !ep.trim().is_empty() {
            crate::otlp::init_metrics(
                ep,
                std::time::Duration::from_millis(cfg.otlp_push_interval_ms),
            )
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
        }
    }

    let fp = FailurePolicy::default();
    let initial_out = ugwc_core::compile(
        &fp,
        &CompileInput {
            reconcile_gateway: None,
            ..Default::default()
        },
    )?;
    let cache: Arc<MultiScopeSnapshot> =
        Arc::new(MultiScopeSnapshot::new(initial_out.snapshot, m.xds.scope_miss.clone()));
    

    let shutdown = tokio_util::sync::CancellationToken::new();
    let admin_sd = shutdown.child_token();
    let xds_sd = shutdown.child_token();

    let readiness = Arc::new(Readiness::new());
    if !cfg.kube_enable {
        readiness.set_cache_synced(true);
    }

    let admin_addr: SocketAddr = cfg.admin_listen_addr.parse()?;
    let readiness_admin = readiness.clone();
    let admin_metrics = m.admin.clone();
    let admin_task = ugwc_k8s::spawn_with_trace_context(crate::admin::serve(
        admin_addr,
        admin_sd,
        readiness_admin,
        admin_metrics,
    ));

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

    let cache_xds: Arc<dyn XdsSnapshotSource> = cache.clone();
    let revision_tx = Some(cache.revision_sender());
    let xds_server_metrics = m.xds.server.clone();
    let xds_task = ugwc_k8s::spawn_with_trace_context(async move {
        if let Err(e) = serve_xds(xds_cfg, cache_xds, revision_tx, xds_sd, xds_server_metrics).await {
            tracing::error!(error = %e, "xds server exited");
        }
    });

    let gateway_state_metrics = m.gateway_state.clone();

    if cfg.kube_enable {
        let fp_k = fp.clone();
        let cache_k = cache.clone();
        let client = match ugwc_k8s::try_default_instrumented(m.kube_client.clone()).await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "kube client unavailable — disable UGWC_KUBE_ENABLE or fix kubeconfig");
                let (noop_tx, _, noop_metrics) =
                    ugwc_k8s::new_reconcile_queue(1, ugwc_k8s::WorkqueueMetrics::noop());
                wait_shutdown(
                    shutdown,
                    readiness,
                    noop_tx,
                    noop_metrics,
                    None,
                    admin_task,
                    xds_task,
                    None,
                )
                .await;
                return Ok(());
            }
        };

        let (leader, lease_handle) = if cfg.leader_election {
            let ns =
                std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_string());
            let identity = std::env::var("HOSTNAME")
                .unwrap_or_else(|_| format!("ugwc-{}", std::process::id()));
            let lease_sd = shutdown.child_token();
            let (state, handle) = crate::leader_election::spawn_lease_election(
                client.clone(),
                ns,
                identity,
                lease_sd,
            );
            (state, Some(handle))
        } else {
            metrics::set_leader_active(true);
            (LeaderState::leader_always(), None)
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
        let impl_profiles = cfg.implementation_profiles.clone();
        let dataplane_enable = cfg.dataplane_enable;
        let control_plane_xds = cfg
            .control_plane_xds_bootstrap_addr
            .clone()
            .unwrap_or_default();
        let xds_insecure = cfg.xds_insecure;
        let xds_wire = cfg.xds_wire.clone();
        let reconcile_concurrency = cfg.reconcile_concurrency;
        let status_patch_concurrency = cfg.status_patch_concurrency;
        let status_cache: std::sync::Arc<
            std::sync::Mutex<std::collections::HashMap<String, String>>,
        > = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let active_crds: ugwc_k8s::ActiveCrds = Arc::new(std::sync::RwLock::new(
            std::collections::HashSet::new(),
        ));
        let active_crds_reconcile = active_crds.clone();
        let status_cache_k = status_cache.clone();
        let gateway_events = ugwc_k8s::GatewayEvents::new(client.clone());
        let (reconcile_tx, work_queue, queue_metrics) =
            ugwc_k8s::new_reconcile_queue(256, m.workqueue.clone());
        let sync_gate = Arc::new(ugwc_k8s::InitialSyncGate::new(Some(
            Readiness::on_cache_synced_callback(readiness.clone()),
        )));
        let (shared_stores, always_on_writers) = ugwc_k8s::build_shared_stores();
        let gateway_index = Arc::new(ugwc_k8s::GatewayIndex::new(m.index.clone()));
        let gateway_index_ctrl = gateway_index.clone();
        let reconcile_tx_shutdown = reconcile_tx.clone();
        // A follower reconciles and patches status but skips dataplane
        // mutations. Enqueue every known gateway when this replica wins the
        // lease so provisioning is not lost to the pre-leadership pass.
        let resync_tx = reconcile_tx.clone();
        let resync_stores = shared_stores.clone();
        let mut leader_rx = leader.subscribe();
        let resync_sd = shutdown.child_token();
        ugwc_k8s::spawn_with_trace_context(async move {
            while !resync_sd.is_cancelled() {
                tokio::select! {
                    _ = resync_sd.cancelled() => break,
                    changed = leader_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if !*leader_rx.borrow() {
                            continue;
                        }
                        for gw in resync_stores.list_gateways() {
                            let Some(name) = gw.metadata.name.clone() else {
                                continue;
                            };
                            let ns = gw.metadata.namespace.clone().unwrap_or_default();
                            if resync_tx.send(ns, name).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });
        let kube_task = ugwc_k8s::spawn_with_trace_context(async move {
            let stores_rfn = shared_stores.clone();
            let rfn: ugwc_k8s::ReconcileFn = Arc::new(move |ns: String, name: String| {
                let fp_k = fp_k.clone();
                let cache_k = cache_k.clone();
                let leader_k = leader_k.clone();
                let client_inner = reconcile_client.clone();
                let primary_controller = primary_controller.clone();
                let status_cache = status_cache_k.clone();
                let impl_profiles = impl_profiles.clone();
                let dataplane_enable = dataplane_enable;
                let control_plane_xds = control_plane_xds.clone();
                let xds_insecure = xds_insecure;
                let xds_wire = xds_wire.clone();
                let active_crds = active_crds_reconcile.clone();
                let controller_name = primary_controller.clone();
                let gateway_events = gateway_events.clone();
                let gateway_state_metrics = gateway_state_metrics.clone();
                let stores = stores_rfn.clone();
                let gateway_index = gateway_index.clone();
                let status_patch_concurrency = status_patch_concurrency;
                Box::pin(async move {
                    let reconcile_started = Instant::now();
                    let outcome = crate::reconcile::reconcile_gateway(
                        crate::reconcile::ReconcileCtx {
                            fp: fp_k,
                            cache: cache_k,
                            leader: leader_k,
                            client: client_inner,
                            primary_controller,
                            status_cache,
                            impl_profiles,
                            dataplane_enable,
                            control_plane_xds,
                            xds_insecure,
                            xds_wire,
                            active_crds,
                            events: Some(gateway_events.clone()),
                            gateway_state_metrics,
                            stores,
                            gateway_index,
                            status_patch_concurrency,
                        },
                        ns,
                        name,
                    )
                    .await;
                    metrics::record_reconcile_run(&controller_name, outcome.is_ok());
                    metrics::observe_reconcile_duration_seconds(
                        &controller_name,
                        reconcile_started.elapsed().as_secs_f64(),
                    );
                    outcome
                })
            });
            ugwc_k8s::run_gateway_controller(
                watch_client,
                kube_sd,
                debounce,
                reconcile_tx,
                work_queue,
                rfn,
                reconcile_concurrency,
                controller_names.clone(),
                active_crds,
                Some(sync_gate),
                shared_stores,
                always_on_writers,
                gateway_index_ctrl,
            )
            .await;
        });
        wait_shutdown(
            shutdown,
            readiness,
            reconcile_tx_shutdown,
            queue_metrics,
            lease_handle,
            admin_task,
            xds_task,
            Some(kube_task),
        )
        .await;
    } else {
        if cfg.leader_election {
            warn!("UGWC_LEADER_ELECTION requires UGWC_KUBE_ENABLE — running as single leader");
        }
        metrics::set_leader_active(true);
        let (noop_tx, _, noop_metrics) =
            ugwc_k8s::new_reconcile_queue(1, ugwc_k8s::WorkqueueMetrics::noop());
        wait_shutdown(
            shutdown,
            readiness,
            noop_tx,
            noop_metrics,
            None,
            admin_task,
            xds_task,
            None,
        )
        .await;
    }

    Ok(())
}

/// Seconds to wait for in-flight reconciles during graceful shutdown (must be < Helm terminationGracePeriodSeconds).
pub const SHUTDOWN_DRAIN_SECS: u64 = 30;
/// Per-task join timeout after cancel (admin, xds, kube).
pub const SHUTDOWN_JOIN_SECS: u64 = 10;

async fn wait_shutdown(
    shutdown: tokio_util::sync::CancellationToken,
    readiness: Arc<Readiness>,
    reconcile_tx: ugwc_k8s::ReconcileSender,
    queue_metrics: ugwc_k8s::ReconcileQueueMetrics,
    lease_handle: Option<Arc<LeaseElectionHandle>>,
    admin: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    xds: tokio::task::JoinHandle<()>,
    kube: Option<tokio::task::JoinHandle<()>>,
) {
    wait_for_shutdown_signal().await;
    info!("signal received — draining");
    readiness.set_cache_synced(false);
    reconcile_tx.close();
    let drain_deadline = std::time::Duration::from_secs(SHUTDOWN_DRAIN_SECS);
    let drain_start = Instant::now();
    while queue_metrics.in_flight() > 0 && drain_start.elapsed() < drain_deadline {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if queue_metrics.in_flight() > 0 {
        warn!(
            in_flight = queue_metrics.in_flight(),
            "shutdown drain timeout with reconciles still in flight"
        );
    }
    if let Some(handle) = lease_handle {
        handle.release().await;
    }
    shutdown.cancel();
    let join = std::time::Duration::from_secs(SHUTDOWN_JOIN_SECS);
    let _ = tokio::time::timeout(join, admin).await;
    let _ = tokio::time::timeout(join, xds).await;
    if let Some(k) = kube {
        let _ = tokio::time::timeout(join, k).await;
    }
    crate::otlp::flush();
    crate::otlp::shutdown_traces();
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term =
            signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    }
}
