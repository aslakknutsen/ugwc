use metrics::{atomics::AtomicU64, GaugeFn};

#[derive(Debug, Default)]
pub struct MetricsGauge {
    pub(super) inner: prometheus_client::metrics::gauge::Gauge<f64, AtomicU64>,
}

impl GaugeFn for MetricsGauge {
    fn increment(&self, value: f64) {
        self.inner.inc_by(value);
    }

    fn decrement(&self, value: f64) {
        self.inner.dec_by(value);
    }

    fn set(&self, value: f64) {
        self.inner.set(value);
    }
}
