//! `tracing` + `tracing-subscriber` bootstrap.

use std::sync::OnceLock;

use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;
use tracing_subscriber::Registry;

/// Handle for runtime log-level changes (admin `PUT /log-level`).
pub type LogFilterHandle = reload::Handle<EnvFilter, Registry>;

static FILTER_HANDLE: OnceLock<LogFilterHandle> = OnceLock::new();

const DEFAULT_FILTER: &str = "info,kube=warn";

/// Store the handle returned from [`init`] for admin runtime reload.
pub fn store_filter_handle(handle: LogFilterHandle) {
    let _ = FILTER_HANDLE.set(handle);
}

/// Access the global filter handle (after [`init`]).
#[must_use]
pub fn filter_handle() -> Option<&'static LogFilterHandle> {
    FILTER_HANDLE.get()
}

fn default_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
}

/// Initialize global subscriber; returns reload handle for dynamic filters.
pub fn init(json: bool, #[allow(unused_variables)] otlp_endpoint: Option<&str>) -> LogFilterHandle {
    #[cfg(feature = "otlp-traces")]
    let otlp_traces = otlp_endpoint.is_some_and(|s| !s.trim().is_empty());
    #[cfg(feature = "otlp-traces")]
    if otlp_traces {
        if let Some(ep) = otlp_endpoint {
            if let Err(e) = crate::otlp::init_traces(ep) {
                eprintln!("otlp traces init failed: {e}");
            }
        }
        use opentelemetry::trace::TracerProvider as _;
        let provider = crate::otlp::tracer_provider().expect("tracer provider after init_traces");
        let tracer = provider.tracer("ugwc");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let filter = default_filter();
        let (env_filter_layer, reload_handle) = reload::Layer::new(filter);
        let fmt_layer = if json {
            tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .flatten_event(true)
                .with_span_events(FmtSpan::CLOSE)
                .boxed()
        } else {
            tracing_subscriber::fmt::layer().compact().boxed()
        };
        tracing_subscriber::registry()
            .with(env_filter_layer)
            .with(otel_layer)
            .with(fmt_layer)
            .init();
        return reload_handle;
    }

    let filter = default_filter();
    let (env_filter_layer, reload_handle) = reload::Layer::new(filter);

    if json {
        tracing_subscriber::registry()
            .with(env_filter_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true)
                    .flatten_event(true)
                    .with_span_events(FmtSpan::CLOSE),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter_layer)
            .with(tracing_subscriber::fmt::layer().compact())
            .init();
    }

    reload_handle
}

/// Apply a new `RUST_LOG`-style filter directive at runtime.
pub fn set_log_filter(handle: &LogFilterHandle, directive: &str) -> Result<(), String> {
    handle
        .modify(|filter| *filter = EnvFilter::new(directive))
        .map_err(|e| e.to_string())
}

#[allow(unused_imports)]
pub use ugwc_core::redact::redact_field;
