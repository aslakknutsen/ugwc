//! Admin HTTP (`/healthz`, `/readyz`, `/metrics`, `PUT /log-level`).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::logging;
use crate::readiness::Readiness;

#[derive(Clone)]
struct AdminState {
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

async fn metrics_prom() -> Result<
    ([(axum::http::header::HeaderName, &'static str); 1], String),
    StatusCode,
> {
    let body = crate::metrics::render().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

/// Body: plain-text `RUST_LOG` directive (e.g. `info,kube=warn,ugwc=debug`).
async fn put_log_level(body: Bytes) -> StatusCode {
    let directive = match std::str::from_utf8(&body) {
        Ok(s) => s.trim(),
        Err(_) => return StatusCode::BAD_REQUEST,
    };
    if directive.is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    let Some(handle) = logging::filter_handle() else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    match logging::set_log_filter(handle, directive) {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

/// Spawn admin server; cancel via token.
pub async fn serve(
    listen: std::net::SocketAddr,
    shutdown: CancellationToken,
    readiness: Arc<Readiness>,
) -> Result<(), std::io::Error> {
    let state = AdminState { readiness };
    let routes = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_prom))
        .route("/log-level", put(put_log_level))
        .with_state(state);

    let app = Router::new().nest("/", routes).layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(TimeoutLayer::new(Duration::from_secs(30))),
    );

    let listener = TcpListener::bind(listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
