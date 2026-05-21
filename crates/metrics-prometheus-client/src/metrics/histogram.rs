use metrics::HistogramFn;

const HIST_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug)]
pub struct MetricsHistogram {
    pub(super) inner: prometheus_client::metrics::histogram::Histogram,
}

impl Default for MetricsHistogram {
    fn default() -> Self {
        // We currently hardcode the buckets.
        Self {
            inner: prometheus_client::metrics::histogram::Histogram::new(HIST_BUCKETS.into_iter()),
        }
    }
}

impl HistogramFn for MetricsHistogram {
    fn record(&self, value: f64) {
        self.inner.observe(value);
    }
}
