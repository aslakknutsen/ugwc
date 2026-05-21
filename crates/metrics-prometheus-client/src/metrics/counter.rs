use std::sync::atomic::Ordering;

use metrics::CounterFn;

#[derive(Debug, Default)]
pub struct MetricsCounter {
    pub(super) inner: prometheus_client::metrics::counter::Counter,
}

impl CounterFn for MetricsCounter {
    fn increment(&self, value: u64) {
        self.inner.inc_by(value);
    }

    fn absolute(&self, value: u64) {
        self.inner.inner().fetch_max(value, Ordering::Relaxed);
    }
}
