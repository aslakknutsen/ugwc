//! Admin HTTP (`/healthz`, `/readyz`, `/metrics`).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::readiness::Readiness;

#[derive(Clone)]
struct AdminState {
    prom: PrometheusHandle,
    readiness: Arc<Readiness>,
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(st): State<AdminState>) -> StatusCode {
    if st.readiness.is_cache_synced() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_prom(
    State(st): State<AdminState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let mut body = st.prom.render();
    if let Ok(extra) = crate::metrics::render_process_metrics() {
        if !extra.is_empty() {
            if !body.is_empty() && !body.ends_with('\n') {
                body.push('\n');
            }
            body.push_str(&extra);
        }
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

/// Spawn admin server; cancel via token.
pub async fn serve(
    listen: std::net::SocketAddr,
    shutdown: CancellationToken,
    prom: PrometheusHandle,
    readiness: Arc<Readiness>,
) -> Result<(), std::io::Error> {
    let state = AdminState { prom, readiness };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_prom))
        .with_state(state);

    let listener = TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
