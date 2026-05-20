//! Optional OTLP metrics push (`otlp-metrics` feature). Pull scrape via `/metrics` remains default.

#[cfg(feature = "otlp-metrics")]
mod imp {
    use std::time::Duration;

    use opentelemetry::global;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::runtime::Tokio;
    use opentelemetry_sdk::Resource;
    use tracing::warn;

    static PROVIDER: std::sync::OnceLock<SdkMeterProvider> = std::sync::OnceLock::new();

    pub fn init(endpoint: &str, interval: Duration) -> Result<(), String> {
        if PROVIDER.get().is_some() {
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
            .with_resource(Resource::empty())
            .build();
        global::set_meter_provider(provider.clone());
        let _ = PROVIDER.set(provider);
        Ok(())
    }

    pub fn flush() {
        if let Some(p) = PROVIDER.get() {
            if let Err(e) = p.force_flush() {
                warn!(error = %e, "otlp metrics flush");
            }
            if let Err(e) = p.shutdown() {
                warn!(error = %e, "otlp metrics shutdown");
            }
        }
    }
}

#[cfg(feature = "otlp-metrics")]
pub use imp::{flush, init};

#[cfg(not(feature = "otlp-metrics"))]
pub fn init(_endpoint: &str, _interval: std::time::Duration) -> Result<(), String> {
    Ok(())
}

#[cfg(not(feature = "otlp-metrics"))]
pub fn flush() {}
