//! Per-gateway reconcile with structured tracing spans.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use kube::api::Api;
use kube::Client;
use tracing::{instrument, warn, Span};
use ugwc_core::ir::IrScopeId;
use ugwc_core::publish::plan_publish;
use ugwc_core::{
    grpc_route_resolved_refs_row, http_route_resolved_refs_row, tcp_route_resolved_refs_row,
    tls_route_resolved_refs_row, udp_route_resolved_refs_row, CompileInput, CompileOutcome,
    FailurePolicy, IrSnapshot, PublishAction, PublishPlan,
};
use ugwc_k8s::resources::{Gateway, ListenerSet};
use ugwc_k8s::SharedStores;
use ugwc_xds::{MultiScopeSnapshot, XdsSnapshotSource};

use crate::gateway_state_metrics::GatewayStateMetrics;
use crate::implementation_profiles::{self, DataPlaneKind};
use crate::leader::LeaderState;
use crate::metrics;

/// Context for a single gateway reconcile invocation.
pub struct ReconcileCtx {
    pub fp: FailurePolicy,
    pub cache: Arc<MultiScopeSnapshot>,
    pub leader: LeaderState,
    pub client: Client,
    pub primary_controller: String,
    pub status_cache: Arc<Mutex<HashMap<String, String>>>,
    pub impl_profiles: std::collections::HashMap<String, crate::implementation_profiles::ImplementationProfile>,
    pub dataplane_enable: bool,
    pub control_plane_xds: String,
    pub xds_insecure: bool,
    pub xds_wire: String,
    pub active_crds: ugwc_k8s::ActiveCrds,
    pub events: Option<ugwc_k8s::GatewayEvents>,
    pub gateway_state_metrics: GatewayStateMetrics,
    pub stores: Arc<SharedStores>,
    pub gateway_index: Arc<ugwc_k8s::GatewayIndex>,
    pub status_patch_concurrency: usize,
}

#[instrument(
    name = "reconcile_gateway",
    skip_all,
    fields(
        k8s.resource.namespace = %ns,
        k8s.resource.name = %name,
        k8s.resource.generation = tracing::field::Empty,
    )
)]
pub async fn reconcile_gateway(ctx: ReconcileCtx, ns: String, name: String) -> Result<(), String> {
    let latest = match ctx.stores.get_gateway(&ns, &name) {
        Some(gw) => gw,
        None => {
            return reconcile_gateway_gone(&ctx, &ns, &name).await;
        }
    };
    let api: Api<Gateway> = Api::namespaced(ctx.client.clone(), &ns);
    Span::current().record("k8s.resource.generation", latest.metadata.generation);

                    let t_load = Instant::now();
                    let cluster =
                        match ugwc_k8s::load_cluster_snapshot(&ctx.stores, &ns, &name, &ctx.active_crds) {
                            Ok(c) => {
                                let refs = ugwc_k8s::refs_from_snapshot(&ctx.stores, &c);
                                ctx.gateway_index.update_gateway(&ns, &name, refs);
                                Some(c)
                            }
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
                    let co = do_compile(&ctx.fp, &input)?;
                    metrics::observe_compile_seconds(t_compile.elapsed().as_secs_f64());
                    metrics::set_compile_output_size(
                        co.snapshot.listeners.len(),
                        co.snapshot.routes.len(),
                        co.snapshot.backends.len(),
                    );
                    metrics::record_ungranted_references(&co.report);

                    if co.report.has_errors() {
                        metrics::record_compile_error();
                    }
                    if co.report.has_blocking_errors() {
                        if let Some(ref ev) = ctx.events {
                            ev.publish_warning(
                                &latest,
                                ugwc_k8s::REASON_COMPILE_BLOCKED,
                                "Gateway compile has blocking errors; see status conditions",
                                None,
                            )
                            .await;
                        }
                    }

                    if let Some(ref ev) = ctx.events {
                        if let Some(ref snap) = input.cluster {
                            let tls_errors: Vec<&str> = snap
                                .effective_listeners
                                .iter()
                                .filter_map(|l| l.tls_resolve_error.as_deref())
                                .collect();
                            if !tls_errors.is_empty() {
                                let msg = format!(
                                    "{} listener(s) with TLS errors: {}",
                                    tls_errors.len(),
                                    tls_errors.join("; ")
                                );
                                ev.publish_warning(
                                    &latest,
                                    ugwc_k8s::REASON_TLS_MATERIAL_MISSING,
                                    &msg,
                                    None,
                                )
                                .await;
                            }
                        }
                    }

                    let programmed_ok =
                        !co.report.has_blocking_errors() && !co.snapshot.meta.has_errors;
                    if let Some(ref snap) = input.cluster {
                        ctx.gateway_state_metrics.record_from_reconcile(snap, programmed_ok);
                    }

                    if let Err(e) = ugwc_k8s::apply_gateway_reconciled_status(
                        &ctx.client,
                        &api,
                        &latest,
                        &co,
                        input.cluster.as_ref(),
                        Some(ctx.status_cache.clone()),
                    )
                    .await
                    {
                        warn!(error = %e, %ns, %name, "gateway status patch");
                    }

                    if let Some(ref snap) = input.cluster {
                        apply_attached_resource_statuses(
                            &ctx,
                            snap,
                            &co,
                            programmed_ok,
                            &ns,
                            &name,
                        )
                        .await;
                    }

                    if !ctx.leader.is_leader() {
                        metrics::record_publish_skipped();
                        return Ok(());
                    }

                    let prev = ctx.cache.previous_for_scope(&co.snapshot.scope);
                    let plan = plan_publish(prev.as_deref(), &co.snapshot, &ctx.fp);
                    do_publish(&ctx.cache, &co, &plan);

                    let controller_name = input
                        .cluster
                        .as_ref()
                        .and_then(|c| c.gateway_class.as_ref())
                        .map(|gc| gc.controller_name.as_str());
                    let profile = implementation_profiles::resolve_for_controller(
                        &ctx.impl_profiles,
                        controller_name,
                    );

                    match &profile.data_plane {
                        DataPlaneKind::External => {}
                        DataPlaneKind::SharedDeployment => {
                            ugwc_k8s::reconcile_shared_deployment_stub(&name);
                        }
                        DataPlaneKind::PerGatewayPraxis => {
                            if !ctx.dataplane_enable {
                                ugwc_k8s::warn_per_gateway_without_enable(&name);
                            } else if programmed_ok {
                                let Some(img) = profile.proxy_image.as_deref() else {
                                    warn!(%name, "per_gateway_praxis missing proxy_image");
                                    metrics::record_dataplane_error();
                                    if let Some(ref ev) = ctx.events {
                                        ev.publish_warning(
                                            &latest,
                                            ugwc_k8s::REASON_DATAPLANE_MISCONFIGURED,
                                            "per_gateway_praxis profile missing proxy_image",
                                            None,
                                        )
                                        .await;
                                    }
                                    return Ok(());
                                };
                                if ctx.control_plane_xds.trim().is_empty() {
                                    warn!(%name, "dataplane enabled but bootstrap xDS addr empty");
                                    metrics::record_dataplane_error();
                                    if let Some(ref ev) = ctx.events {
                                        ev.publish_warning(
                                            &latest,
                                            ugwc_k8s::REASON_DATAPLANE_MISCONFIGURED,
                                            "UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR is empty",
                                            None,
                                        )
                                        .await;
                                    }
                                    return Ok(());
                                }
                                let scope = format!("{ns}/{name}");
                                let args = ugwc_k8s::PraxisProvisionArgs {
                                    proxy_image: img.to_string(),
                                    xds_bootstrap_addr: ugwc_k8s::praxis_istiod_dial_url(
                                        &ctx.control_plane_xds,
                                        ctx.xds_insecure,
                                    ),
                                    xds_insecure: ctx.xds_insecure,
                                    xds_wire: ctx.xds_wire.clone(),
                                    gateway_scope: scope,
                                    gateway_class_name: latest.spec.gateway_class_name.clone(),
                                };
                                let listener_ports = input
                                    .cluster
                                    .as_ref()
                                    .map(ugwc_k8s::collect_effective_listener_ports);
                                match ugwc_k8s::reconcile_per_gateway_praxis(
                                    &ctx.client,
                                    &latest,
                                    &args,
                                    listener_ports.as_deref(),
                                )
                                .await
                                {
                                    Ok(()) => metrics::record_dataplane_apply(),
                                    Err(e) => {
                                        warn!(error = %e, %ns, %name, "dataplane provision");
                                        metrics::record_dataplane_error();
                                        if let Some(ref ev) = ctx.events {
                                            ev.publish_warning(
                                                &latest,
                                                ugwc_k8s::REASON_PROVISION_FAILED,
                                                &format!("dataplane provision failed: {e}"),
                                                None,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                        }
                    }

    Ok(())
}

#[instrument(name = "compile", skip_all)]
async fn reconcile_gateway_gone(ctx: &ReconcileCtx, ns: &str, name: &str) -> Result<(), String> {
    let scope = IrScopeId::new(ns, name);
    let empty = IrSnapshot::empty_for_scope(scope.clone());
    if ctx.leader.is_leader() {
        ctx.cache.store_scoped(empty);
        let _ = ctx.cache.remove_scoped(&scope);
        metrics::record_publish_applied();
    }
    ctx.gateway_state_metrics.clear_gateway(ns, name);
    ctx.gateway_index.remove_gateway(ns, name);
    if ctx.dataplane_enable {
        if let Err(e) = ugwc_k8s::deprovision_per_gateway_praxis(&ctx.client, ns, name).await {
            warn!(error = %e, %ns, %name, "dataplane deprovision after gateway delete");
            metrics::record_dataplane_error();
        }
    }
    Ok(())
}

fn do_compile(fp: &FailurePolicy, input: &CompileInput) -> Result<CompileOutcome, String> {
    ugwc_core::compile(fp, input).map_err(|e| e.to_string())
}

async fn apply_attached_resource_statuses(
    ctx: &ReconcileCtx,
    snap: &ugwc_core::ClusterSnapshot,
    co: &CompileOutcome,
    programmed_ok: bool,
    gateway_ns: &str,
    gateway_name: &str,
) {
    let concurrency = ctx.status_patch_concurrency;
    let status_cache = Some(ctx.status_cache.clone());
    let t_status = Instant::now();

    let patch_count = snap.http_routes.len()
        + snap.grpc_routes.len()
        + snap.tls_routes.len()
        + snap.tcp_routes.len()
        + snap.udp_routes.len()
        + snap.listener_sets.len()
        + snap.rejected_listener_sets.len()
        + snap.backend_tls_policy_outcomes.len()
        + snap.referenced_inference_pools().len();

    let mut futs: Vec<
        std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
    > = Vec::new();

    futs.extend(snap.http_routes.iter().map(|r| {
        let (rs, rr, rm) = http_route_resolved_refs_row(&co.report, r);
        Box::pin(async {
            let route_name = r.name.clone();
            if let Err(e) = ugwc_k8s::apply_http_route_merged_parent_status(
                ugwc_k8s::HttpRouteMergedParentStatusArgs {
                    client: &ctx.client,
                    route_ns: &r.namespace,
                    route_name: &r.name,
                    controller_name: &ctx.primary_controller,
                    route_generation: r.generation,
                    programmed_ok,
                    resolved_refs_status: rs,
                    resolved_refs_reason: rr,
                    resolved_refs_message: rm,
                    reconciled_gateway_ns: gateway_ns,
                    reconciled_gateway_name: gateway_name,
                    snapshot: snap,
                    compile_report: &co.report,
                    status_cache: status_cache.clone(),
                },
            )
            .await
            {
                warn!(error = %e, route = %route_name, "httproute status patch");
            }
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.grpc_routes.iter().map(|r| {
        let (rs, rr, rm) = grpc_route_resolved_refs_row(&co.report, r);
        Box::pin(async {
            let route_name = r.name.clone();
            if let Err(e) = ugwc_k8s::apply_grpc_route_merged_parent_status(
                ugwc_k8s::GrpcRouteMergedParentStatusArgs {
                    client: &ctx.client,
                    route_ns: &r.namespace,
                    route_name: &r.name,
                    controller_name: &ctx.primary_controller,
                    route_generation: r.generation,
                    programmed_ok,
                    resolved_refs_status: rs,
                    resolved_refs_reason: rr,
                    resolved_refs_message: rm,
                    reconciled_gateway_ns: gateway_ns,
                    reconciled_gateway_name: gateway_name,
                    snapshot: snap,
                    compile_report: &co.report,
                    status_cache: status_cache.clone(),
                },
            )
            .await
            {
                warn!(error = %e, route = %route_name, "grpcroute status patch");
            }
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.tls_routes.iter().map(|r| {
        let (rs, rr, rm) = tls_route_resolved_refs_row(&co.report, r);
        Box::pin(async {
            let route_name = r.name.clone();
            if let Err(e) = ugwc_k8s::apply_tls_route_merged_parent_status(
                ugwc_k8s::TlsRouteMergedParentStatusArgs {
                    client: &ctx.client,
                    route_ns: &r.namespace,
                    route_name: &r.name,
                    controller_name: &ctx.primary_controller,
                    route_generation: r.generation,
                    programmed_ok,
                    resolved_refs_status: rs,
                    resolved_refs_reason: rr,
                    resolved_refs_message: rm,
                    reconciled_gateway_ns: gateway_ns,
                    reconciled_gateway_name: gateway_name,
                    snapshot: snap,
                    compile_report: &co.report,
                    status_cache: status_cache.clone(),
                },
            )
            .await
            {
                warn!(error = %e, route = %route_name, "tlsroute status patch");
            }
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.tcp_routes.iter().map(|r| {
        let (rs, rr, rm) = tcp_route_resolved_refs_row(&co.report, r);
        Box::pin(async {
            let route_name = r.name.clone();
            if let Err(e) = ugwc_k8s::apply_tcp_route_merged_parent_status(
                ugwc_k8s::TcpRouteMergedParentStatusArgs {
                    client: &ctx.client,
                    route_ns: &r.namespace,
                    route_name: &r.name,
                    controller_name: &ctx.primary_controller,
                    route_generation: r.generation,
                    programmed_ok,
                    resolved_refs_status: rs,
                    resolved_refs_reason: rr,
                    resolved_refs_message: rm,
                    reconciled_gateway_ns: gateway_ns,
                    reconciled_gateway_name: gateway_name,
                    snapshot: snap,
                    compile_report: &co.report,
                    status_cache: status_cache.clone(),
                },
            )
            .await
            {
                warn!(error = %e, route = %route_name, "tcproute status patch");
            }
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.udp_routes.iter().map(|r| {
        let (rs, rr, rm) = udp_route_resolved_refs_row(&co.report, r);
        Box::pin(async {
            let route_name = r.name.clone();
            if let Err(e) = ugwc_k8s::apply_udp_route_merged_parent_status(
                ugwc_k8s::UdpRouteMergedParentStatusArgs {
                    client: &ctx.client,
                    route_ns: &r.namespace,
                    route_name: &r.name,
                    controller_name: &ctx.primary_controller,
                    route_generation: r.generation,
                    programmed_ok,
                    resolved_refs_status: rs,
                    resolved_refs_reason: rr,
                    resolved_refs_message: rm,
                    reconciled_gateway_ns: gateway_ns,
                    reconciled_gateway_name: gateway_name,
                    snapshot: snap,
                    compile_report: &co.report,
                    status_cache: status_cache.clone(),
                },
            )
            .await
            {
                warn!(error = %e, route = %route_name, "udproute status patch");
            }
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.listener_sets.iter().map(|ls_snap| {
        let ls_ns = ls_snap.namespace.clone();
        let ls_name = ls_snap.name.clone();
        let cache = status_cache.clone();
        Box::pin(patch_listenerset_reconciled(
            ctx,
            snap,
            co,
            programmed_ok,
            ls_ns,
            ls_name,
            cache,
        )) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    futs.extend(snap.rejected_listener_sets.iter().map(|rej| {
        let ls_ns = rej.namespace.clone();
        let ls_name = rej.name.clone();
        let reason = rej.reason.clone();
        let cache = status_cache.clone();
        Box::pin(patch_listenerset_rejected(
            ctx,
            ls_ns,
            ls_name,
            reason,
            cache,
        )) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
    }));

    ugwc_k8s::status::run_bounded(concurrency, futs).await;

    if let Err(e) = ugwc_k8s::apply_backend_tls_policies_for_gateway(
        &ctx.client,
        gateway_ns,
        gateway_name,
        &ctx.primary_controller,
        snap,
        status_cache.clone(),
        concurrency,
    )
    .await
    {
        warn!(error = %e, "backend TLS policy status patch");
    }

    if let Err(e) = ugwc_k8s::apply_inference_pools_for_gateway(
        &ctx.client,
        gateway_ns,
        gateway_name,
        &ctx.primary_controller,
        snap,
        programmed_ok,
        &co.report,
        status_cache,
        concurrency,
    )
    .await
    {
        warn!(error = %e, "inferencepool status patch");
    }

    metrics::record_status_patch_count(patch_count);
    metrics::observe_status_patch_duration_seconds(t_status.elapsed().as_secs_f64());
}

async fn patch_listenerset_reconciled(
    ctx: &ReconcileCtx,
    snap: &ugwc_core::ClusterSnapshot,
    co: &CompileOutcome,
    programmed_ok: bool,
    ls_ns: String,
    ls_name: String,
    status_cache: Option<std::sync::Arc<std::sync::Mutex<HashMap<String, String>>>>,
) {
    let ls_api: Api<ListenerSet> = Api::namespaced(ctx.client.clone(), &ls_ns);
    match ls_api.get(&ls_name).await {
        Ok(ls) => {
            if let Err(e) = ugwc_k8s::apply_listenerset_reconciled_status(
                &ctx.client,
                &ls,
                snap,
                programmed_ok,
                co,
                status_cache,
            )
            .await
            {
                warn!(error = %e, listenerset = %ls_name, "listenerset status patch");
            }
        }
        Err(e) => warn!(error = %e, listenerset = %ls_name, "get listenerset for status"),
    }
}

async fn patch_listenerset_rejected(
    ctx: &ReconcileCtx,
    ls_ns: String,
    ls_name: String,
    reason: String,
    status_cache: Option<std::sync::Arc<std::sync::Mutex<HashMap<String, String>>>>,
) {
    let ls_api: Api<ListenerSet> = Api::namespaced(ctx.client.clone(), &ls_ns);
    match ls_api.get(&ls_name).await {
        Ok(ls) => {
            if let Err(e) = ugwc_k8s::apply_listenerset_rejected_status(
                &ctx.client,
                &ls,
                &reason,
                status_cache,
            )
            .await
            {
                warn!(
                    error = %e,
                    listenerset = %ls_name,
                    "rejected listenerset status patch"
                );
            }
        }
        Err(e) => warn!(
            error = %e,
            listenerset = %ls_name,
            "get rejected listenerset for status"
        ),
    }
}

#[instrument(name = "publish", skip_all, fields(action = ?plan.action))]
fn do_publish(cache: &MultiScopeSnapshot, co: &CompileOutcome, plan: &PublishPlan) {
    match plan.action {
        PublishAction::Skip => metrics::record_publish_skipped(),
        PublishAction::FullReplace => {
            cache.store_scoped(co.snapshot.clone());
            metrics::record_publish_applied();
        }
        PublishAction::Delta => {}
        _ => {}
    }
}
