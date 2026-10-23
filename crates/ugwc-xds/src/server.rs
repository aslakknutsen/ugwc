//! `AggregatedDiscoveryService` gRPC server (ADS).

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prost::Message;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn, Instrument};
use ugwc_core::IrSnapshot;

use crate::cache::XdsSnapshotSource;
use xds_api::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::{
    AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
};
use xds_api::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};
use xds_api::pb::google::protobuf::Any;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct XdsDisconnectLabels {
    pub reason: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NoLabels;

impl prometheus_client::encoding::EncodeLabelSet for NoLabels {
    fn encode(
        &self,
        _encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}

/// xDS server metric handles.
#[derive(Clone)]
pub struct XdsMetrics {
    pub streams_active: Gauge,
    pub stream_disconnects: Family<XdsDisconnectLabels, Counter>,
    pub push_bytes: Counter,
    pub push_resources: Gauge,
    pub push_duration: Family<NoLabels, Histogram>,
}

/// Control-plane behaviour for plaintext vs TLS + explicit insecure prototype flag.
#[derive(Clone, Debug)]
pub struct XdsSecurityConfig {
    pub allow_insecure: bool,
    pub tls_cert_path: Option<std::path::PathBuf>,
    pub tls_key_path: Option<std::path::PathBuf>,
    pub tls_ca_path: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug)]
pub struct XdsServerConfig {
    pub listen_addr: SocketAddr,
    pub security: XdsSecurityConfig,
    /// When false, DeltaAggregatedResources is rejected (plan §7 — SotW before Delta).
    pub allow_delta: bool,
    /// `gwxds` (Praxis `istio.gwxds.Resource`) or `envoy` (RDS-only slice).
    pub xds_wire: String,
}

struct AdsService {
    cache: Arc<dyn XdsSnapshotSource>,
    allow_delta: bool,
    xds_wire: String,
    /// When set, ADS streams subscribe for pushes after `store_scoped` bumps the watch.
    revision_tx: Option<watch::Sender<u64>>,
    metrics: XdsMetrics,
}

#[tonic::async_trait]
impl AggregatedDiscoveryService for AdsService {
    type StreamAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, Status>> + Send>>;
    type DeltaAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, Status>> + Send>>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let mut inbound = request.into_inner();
        info!(
            ads.method = "StreamAggregatedResources",
            "xds stream opened"
        );
        self.metrics.streams_active.inc();

        let cache = self.cache.clone();
        let wire = self.xds_wire.clone();
        let revision_tx = self.revision_tx.clone();
        let metrics = self.metrics.clone();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            let mut snapshot_rx = revision_tx.map(|t| t.subscribe());
            let mut node_id_key: Option<String> = None;
            let mut last_type_url = String::new();

            if let Some(ref mut snap_rx) = snapshot_rx {
                let mut last_sent_revision: Option<String> = None;
                loop {
                    tokio::select! {
                        msg = inbound.message() => {
                            match msg {
                                Ok(Some(req)) => {
                                    if let Some(n) = node_id_from_request(&req) {
                                        node_id_key = Some(n.to_string());
                                    }
                                    let node_key = node_id_key.as_deref();
                                    let snap = cache.resolve_for_discovery(node_key);
                                    if discovery_is_ack(&req, snap.as_ref()) {
                                        continue;
                                    }
                                    if !req.type_url.is_empty() {
                                        last_type_url.clone_from(&req.type_url);
                                    }
                                    let type_url = last_type_url.as_str();
                                    if send_discovery_response(snap.as_ref(), &wire, type_url, &tx, &metrics)
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    last_sent_revision = Some(snap.revision_token());
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    warn!(error = %e, "ads request stream");
                                    break;
                                }
                            }
                        }
                        ch = snap_rx.changed() => {
                            if ch.is_err() {
                                break;
                            }
                            snap_rx.borrow_and_update();
                            let Some(ref nk) = node_id_key else {
                                continue;
                            };
                            if send_snapshot_if_revision_changed(
                                &cache,
                                &wire,
                                last_type_url.as_str(),
                                Some(nk.as_str()),
                                &mut last_sent_revision,
                                &tx,
                                &metrics,
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            } else {
                loop {
                    match inbound.message().await {
                        Ok(Some(req)) => {
                            if let Some(n) = node_id_from_request(&req) {
                                node_id_key = Some(n.to_string());
                            }
                            let node_key = node_id_key.as_deref();
                            let snap = cache.resolve_for_discovery(node_key);
                            if discovery_is_ack(&req, snap.as_ref()) {
                                continue;
                            }
                            if !req.type_url.is_empty() {
                                last_type_url.clone_from(&req.type_url);
                            }
                            let type_url = last_type_url.as_str();
                            if send_discovery_response(snap.as_ref(), &wire, type_url, &tx, &metrics)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            warn!(error = %e, "ads request stream");
                            break;
                        }
                    }
                }
            }

            metrics.streams_active.dec();
            metrics
                .stream_disconnects
                .get_or_create(&XdsDisconnectLabels {
                    reason: "stream_end".to_string(),
                })
                .inc();
            info!(
                ads.method = "StreamAggregatedResources",
                "xds stream closed"
            );
        }.instrument(tracing::Span::current()));

        let out = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(out) as Self::StreamAggregatedResourcesStream
        ))
    }

    async fn delta_aggregated_resources(
        &self,
        _request: Request<Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaAggregatedResourcesStream>, Status> {
        if !self.allow_delta {
            return Err(Status::unimplemented(
                "Delta gated until SotW suite passes (set UGWC_XDS_DELTA=true)",
            ));
        }
        warn!("delta aggregated resources — stub empty stream");
        let (_tx, rx) = mpsc::channel::<Result<DeltaDiscoveryResponse, Status>>(1);
        let out = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(out) as Self::DeltaAggregatedResourcesStream
        ))
    }
}

fn node_id_from_request(req: &DiscoveryRequest) -> Option<&str> {
    req.node
        .as_ref()
        .map(|n| n.id.as_str())
        .filter(|s| !s.is_empty())
}

fn discovery_is_ack(req: &DiscoveryRequest, snap: &IrSnapshot) -> bool {
    req.error_detail.is_none()
        && !req.version_info.is_empty()
        && req.version_info == snap.revision_token()
}

async fn send_discovery_response(
    snap: &IrSnapshot,
    wire: &str,
    type_url: &str,
    tx: &mpsc::Sender<Result<DiscoveryResponse, Status>>,
    metrics: &XdsMetrics,
) -> Result<(), ()> {
    let push_started = std::time::Instant::now();
    let resp = discovery_response(snap, wire, type_url);
    metrics.push_bytes.inc_by(resp.encoded_len() as u64);
    metrics.push_resources.set(resp.resources.len() as i64);
    let out = if tx.send(Ok(resp)).await.is_err() {
        Err(())
    } else {
        Ok(())
    };
    metrics
        .push_duration
        .get_or_create(&NoLabels)
        .observe(push_started.elapsed().as_secs_f64());
    out
}

async fn send_snapshot_if_revision_changed(
    cache: &Arc<dyn XdsSnapshotSource>,
    wire: &str,
    type_url: &str,
    node_key: Option<&str>,
    last_sent_revision: &mut Option<String>,
    tx: &mpsc::Sender<Result<DiscoveryResponse, Status>>,
    metrics: &XdsMetrics,
) -> Result<(), ()> {
    let snap = cache.resolve_for_discovery(node_key);
    let rev = snap.revision_token();
    if last_sent_revision.as_deref() == Some(rev.as_str()) {
        return Ok(());
    }
    send_discovery_response(snap.as_ref(), wire, type_url, tx, metrics).await?;
    *last_sent_revision = Some(rev);
    Ok(())
}

fn discovery_response(ir: &IrSnapshot, xds_wire: &str, req_type_url: &str) -> DiscoveryResponse {
    let use_gwxds =
        xds_wire.eq_ignore_ascii_case("gwxds") || req_type_url.contains("istio.gwxds.Resource");
    if use_gwxds {
        let resources: Vec<Any> = ugwc_gwxds::encode_ir(ir)
            .into_iter()
            .map(|r| Any {
                type_url: ugwc_gwxds::GW_TYPE_URL.to_string(),
                value: r.encode_to_vec(),
            })
            .collect();
        return DiscoveryResponse {
            version_info: ir.revision_token(),
            resources,
            type_url: ugwc_gwxds::GW_TYPE_URL.to_string(),
            ..Default::default()
        };
    }

    let wire = crate::encode::encode_sotw(ir);
    DiscoveryResponse {
        version_info: wire.version.clone(),
        resources: wire
            .listeners
            .iter()
            .chain(wire.routes.iter())
            .chain(wire.clusters.iter())
            .chain(wire.endpoints.iter())
            .cloned()
            .collect(),
        type_url: String::new(),
        ..Default::default()
    }
}

/// Errors from xDS server startup (TLS config or transport).
#[derive(Debug, thiserror::Error)]
pub enum XdsServeError {
    #[error("xDS TLS configuration: {0}")]
    TlsConfig(String),
    #[error("xDS transport: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Run until `shutdown` triggers or the server errors.
pub async fn serve(
    cfg: XdsServerConfig,
    cache: Arc<dyn XdsSnapshotSource>,
    revision_tx: Option<watch::Sender<u64>>,
    shutdown: tokio_util::sync::CancellationToken,
    metrics: XdsMetrics,
) -> Result<(), XdsServeError> {
    let svc = AdsService {
        cache,
        allow_delta: cfg.allow_delta,
        xds_wire: cfg.xds_wire.clone(),
        revision_tx,
        metrics,
    };

    let ads = AggregatedDiscoveryServiceServer::new(svc);

    if cfg.security.allow_insecure {
        warn!("xDS listening in **insecure (plaintext)** mode — prototype only");
        Server::builder()
            .add_service(ads)
            .serve_with_shutdown(cfg.listen_addr, async move {
                shutdown.cancelled().await;
            })
            .await?;
    } else {
        let cert_path = cfg.security.tls_cert_path.as_ref().ok_or_else(|| {
            XdsServeError::TlsConfig(
                "UGWC_XDS_TLS_CERT_PATH required when insecure mode is disabled".into(),
            )
        })?;
        let key_path = cfg.security.tls_key_path.as_ref().ok_or_else(|| {
            XdsServeError::TlsConfig(
                "UGWC_XDS_TLS_KEY_PATH required when insecure mode is disabled".into(),
            )
        })?;

        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| XdsServeError::TlsConfig(format!("read {}: {e}", cert_path.display())))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|e| XdsServeError::TlsConfig(format!("read {}: {e}", key_path.display())))?;

        let identity = Identity::from_pem(&cert_pem, &key_pem);
        let mut tls_config = ServerTlsConfig::new().identity(identity);

        if let Some(ref ca_path) = cfg.security.tls_ca_path {
            let ca_pem = std::fs::read(ca_path).map_err(|e| {
                XdsServeError::TlsConfig(format!("read CA {}: {e}", ca_path.display()))
            })?;
            tls_config = tls_config.client_ca_root(Certificate::from_pem(&ca_pem));
            info!("xDS TLS: mTLS enabled (client CA loaded)");
        }

        info!("xDS listening with TLS");
        Server::builder()
            .tls_config(tls_config)?
            .add_service(ads)
            .serve_with_shutdown(cfg.listen_addr, async move {
                shutdown.cancelled().await;
            })
            .await?;
    }
    Ok(())
}

/// Best-effort graceful drain (plan Phase 2C / 8).
pub async fn shutdown_gracefully(
    token: tokio_util::sync::CancellationToken,
    join: tokio::task::JoinHandle<Result<(), XdsServeError>>,
    timeout: std::time::Duration,
) {
    token.cancel();
    let _ = tokio::time::timeout(timeout, join).await;
}
