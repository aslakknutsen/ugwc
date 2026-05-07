//! `AggregatedDiscoveryService` gRPC server (ADS).

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};
use ugwc_core::IrSnapshot;

use crate::cache::SnapshotCache;
use xds_api::pb::envoy::service::discovery::v3::aggregated_discovery_service_server::{
    AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
};
use xds_api::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
};

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
}

struct AdsService<C: SnapshotCache + 'static> {
    cache: Arc<C>,
    allow_delta: bool,
}

#[tonic::async_trait]
impl<C: SnapshotCache + 'static> AggregatedDiscoveryService for AdsService<C> {
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

        let cache = self.cache.clone();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(_req)) => {
                        let snap = cache.current();
                        let resp = discovery_response_for_snapshot(&snap);
                        if tx.send(Ok(resp)).await.is_err() {
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
            info!(
                ads.method = "StreamAggregatedResources",
                "xds stream closed"
            );
        });

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

fn discovery_response_for_snapshot(ir: &IrSnapshot) -> DiscoveryResponse {
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

/// Run until `shutdown` triggers or the server errors.
pub async fn serve<C: SnapshotCache + 'static>(
    cfg: XdsServerConfig,
    cache: Arc<C>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), tonic::transport::Error> {
    if cfg.security.allow_insecure {
        warn!("xDS listening in **insecure (plaintext)** mode — prototype only");
    } else {
        for path in [
            &cfg.security.tls_cert_path,
            &cfg.security.tls_key_path,
            &cfg.security.tls_ca_path,
        ]
        .into_iter()
        .flatten()
        {
            if !path.exists() {
                warn!(
                    ?path,
                    "TLS material path missing — handshake will fail when wired"
                );
            }
        }
    }

    let svc = AdsService {
        cache,
        allow_delta: cfg.allow_delta,
    };

    let ads = AggregatedDiscoveryServiceServer::new(svc);

    Server::builder()
        .add_service(ads)
        .serve_with_shutdown(cfg.listen_addr, async move {
            shutdown.cancelled().await;
        })
        .await
}

/// Best-effort graceful drain (plan Phase 2C / 8).
pub async fn shutdown_gracefully(
    token: tokio_util::sync::CancellationToken,
    join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    timeout: std::time::Duration,
) {
    token.cancel();
    let _ = tokio::time::timeout(timeout, join).await;
}
