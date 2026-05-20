//! Optional OTLP export. Pull scrape via `/metrics` remains the default SLO path.

#[cfg(any(feature = "otlp-metrics", feature = "otlp-traces"))]
mod imp {
    use opentelemetry::global;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    #[cfg(feature = "otlp-metrics")]
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    #[cfg(feature = "otlp-metrics")]
    use std::time::Duration;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::runtime::Tokio;
    use opentelemetry_sdk::trace::TracerProvider;
    use opentelemetry_sdk::Resource;
    use tracing::warn;

    #[cfg(feature = "otlp-metrics")]
    static METRICS_PROVIDER: std::sync::OnceLock<SdkMeterProvider> = std::sync::OnceLock::new();

    #[cfg(feature = "otlp-metrics")]
    pub fn init_metrics(endpoint: &str, interval: Duration) -> Result<(), String> {
        if METRICS_PROVIDER.get().is_some() {
            return Ok(());
        }
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| e.to_string())?;
        let reader = PeriodicReader::builder(exporter, Tokio)
            .with_interval(interval)
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(otlp_resource())
            .build();
        global::set_meter_provider(provider.clone());
        let _ = METRICS_PROVIDER.set(provider);
        Ok(())
    }

    #[cfg(feature = "otlp-traces")]
    static TRACER_PROVIDER: std::sync::OnceLock<TracerProvider> = std::sync::OnceLock::new();

    #[cfg(feature = "otlp-traces")]
    pub fn init_traces(endpoint: &str) -> Result<(), String> {
        if TRACER_PROVIDER.get().is_some() {
            return Ok(());
        }
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| e.to_string())?;
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, Tokio)
            .with_resource(otlp_resource())
            .build();
        global::set_tracer_provider(provider.clone());
        let _ = TRACER_PROVIDER.set(provider);
        Ok(())
    }

    fn otlp_resource() -> Resource {
        Resource::new([
            KeyValue::new("service.name", "ugwc"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new(
                "deployment.environment",
                std::env::var("UGWC_RUN_PROFILE").unwrap_or_else(|_| "unknown".into()),
            ),
        ])
    }

    pub fn flush() {
        #[cfg(feature = "otlp-metrics")]
        if let Some(p) = METRICS_PROVIDER.get() {
            if let Err(e) = p.force_flush() {
                warn!(error = %e, "otlp metrics flush");
            }
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "otlp metrics shutdown");
            }
        }
        #[cfg(feature = "otlp-traces")]
        shutdown_traces();
    }

    #[cfg(feature = "otlp-traces")]
    pub fn shutdown_traces() {
        if let Some(p) = TRACER_PROVIDER.get() {
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "otlp traces shutdown");
            }
        }
    }

    #[cfg(feature = "otlp-traces")]
    pub fn tracer_provider() -> Option<&'static TracerProvider> {
        TRACER_PROVIDER.get()
    }
}

#[cfg(feature = "otlp-metrics")]
pub use imp::init_metrics;

#[cfg(not(feature = "otlp-metrics"))]
pub fn init_metrics(_endpoint: &str, _interval: std::time::Duration) -> Result<(), String> {
    Ok(())
}

#[cfg(any(feature = "otlp-metrics", feature = "otlp-traces"))]
pub use imp::flush;

#[cfg(not(any(feature = "otlp-metrics", feature = "otlp-traces")))]
pub fn flush() {}

#[cfg(feature = "otlp-traces")]
pub use imp::{init_traces, shutdown_traces};

#[cfg(feature = "otlp-traces")]
pub use imp::tracer_provider;

#[cfg(not(feature = "otlp-traces"))]
pub fn init_traces(_endpoint: &str) -> Result<(), String> {
    Ok(())
}

#[cfg(not(feature = "otlp-traces"))]
pub fn shutdown_traces() {}
