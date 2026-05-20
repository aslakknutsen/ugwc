//! Prometheus histograms for Kubernetes API requests (`ugwc_rest_client_request_duration_seconds`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::sync::Mutex;

use http::{Request, Response};
use kube::client::ClientBuilder;
use kube::{Client, Config};
use metrics::histogram;
use tower::{Layer, Service};

/// Build a [`Client`] with REST request duration metrics on the HTTP stack.
pub async fn try_default_instrumented() -> Result<Client, kube::Error> {
    let config = Config::infer()
        .await
        .map_err(|e| kube::Error::Service(format!("config infer: {e}").into()))?;
    instrumented_client(config).await
}

/// Build a [`Client`] from `config` with REST metrics.
pub async fn instrumented_client(config: Config) -> Result<Client, kube::Error> {
    let builder = ClientBuilder::try_from(config)?;
    Ok(builder.with_layer(&RestMetricsLayer).build())
}

#[derive(Clone, Copy, Default)]
pub struct RestMetricsLayer;

impl<S> Layer<S> for RestMetricsLayer {
    type Service = RestMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RestMetricsService {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Clone)]
pub struct RestMetricsService<S> {
    inner: Arc<Mutex<S>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RestMetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Send + 'static,
    S::Future: Send,
    S::Error: Send,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Lock is taken per request in `call`; readiness is delegated to the inner service there.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let verb = req.method().as_str().to_string();
        let resource = k8s_resource_from_path(req.uri().path()).to_string();
        let started = Instant::now();
        let inner = self.inner.clone();
        Box::pin(async move {
            let result = inner.lock().await.call(req).await;
            let code = result
                .as_ref()
                .ok()
                .map(|r| r.status().as_u16().to_string())
                .unwrap_or_else(|| "0".to_string());
            histogram!(
                "ugwc_rest_client_request_duration_seconds",
                "verb" => verb,
                "resource" => resource,
                "code" => code
            )
            .record(started.elapsed().as_secs_f64());
            result
        })
    }
}

fn k8s_resource_from_path(path: &str) -> &'static str {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if let Some(i) = segments.iter().position(|&s| s == "namespaces") {
        if let Some(r) = segments.get(i + 2) {
            return normalize_resource(r);
        }
    }
    if segments.first() == Some(&"api") {
        if let Some(r) = segments.get(1) {
            return normalize_resource(r);
        }
    }
    if segments.first() == Some(&"apis") {
        if let Some(r) = segments.get(3) {
            return normalize_resource(r);
        }
    }
    "unknown"
}

fn normalize_resource(name: &str) -> &'static str {
    match name {
        "gateways" => "gateways",
        "gatewayclasses" => "gatewayclasses",
        "httproutes" => "httproutes",
        "grpcroutes" => "grpcroutes",
        "tlsroutes" => "tlsroutes",
        "tcproutes" => "tcproutes",
        "udproutes" => "udproutes",
        "listenersets" => "listenersets",
        "referencegrants" => "referencegrants",
        "backendtlspolicies" => "backendtlspolicies",
        "leases" => "leases",
        "services" => "services",
        "secrets" => "secrets",
        "configmaps" => "configmaps",
        "endpointslices" => "endpointslices",
        "customresourcedefinitions" => "customresourcedefinitions",
        "inferencepools" => "inferencepools",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_namespaced_gateway_path() {
        let p = "/apis/gateway.networking.k8s.io/v1/namespaces/ns/gateways/gw";
        assert_eq!(k8s_resource_from_path(p), "gateways");
    }

    #[test]
    fn parses_lease_path() {
        let p = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases/ugwc-leader";
        assert_eq!(k8s_resource_from_path(p), "leases");
    }
}
