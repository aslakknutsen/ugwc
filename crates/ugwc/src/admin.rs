//! Admin HTTP (`/healthz`, `/readyz`, `/metrics`).

use std::time::Instant;

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use once_cell::sync::Lazy;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

static STARTED: Lazy<Instant> = Lazy::new(Instant::now);

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz() -> StatusCode {
    let _ = STARTED.elapsed();
    StatusCode::OK
}

async fn metrics_prom(
    axum::extract::State(h): axum::extract::State<PrometheusHandle>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        h.render(),
    )
}

/// Spawn admin server; cancel via token.
pub async fn serve(
    listen: std::net::SocketAddr,
    shutdown: CancellationToken,
    prom: PrometheusHandle,
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_prom).with_state(prom));

    let listener = TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
