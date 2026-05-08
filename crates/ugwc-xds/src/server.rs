//! `AggregatedDiscoveryService` gRPC server (ADS).

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use prost::Message;
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
use xds_api::pb::google::protobuf::Any;

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

struct AdsService<C: SnapshotCache + 'static> {
    cache: Arc<C>,
    allow_delta: bool,
    xds_wire: String,
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
        let wire = self.xds_wire.clone();
        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(req)) => {
                        let snap = cache.current();
                        let resp = discovery_response(&snap, &wire, req.type_url.as_str());
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
        xds_wire: cfg.xds_wire.clone(),
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
