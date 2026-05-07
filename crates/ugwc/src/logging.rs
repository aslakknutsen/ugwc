//! `tracing` + `tracing-subscriber` bootstrap.

use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

pub fn init(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if json {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_span_events(FmtSpan::CLOSE),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .init();
    }
}

/// Skip logging raw bearer tokens or PEM-looking bodies in field values.
#[must_use]
#[allow(dead_code)] // Called from integration tests / future secret log fields; keep API stable.
fn redact_field(input: &str) -> String {
    if input.contains("-----BEGIN") {
        return "<redacted-pem>".into();
    }
    if input.len() > 20
        && input
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return format!("<redacted:{}>", input.len());
    }
    input.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_redacted() {
        let s = redact_field("-----BEGIN RSA PRIVATE KEY-----\nMIIF");
        assert_eq!(s, "<redacted-pem>");
    }
}
