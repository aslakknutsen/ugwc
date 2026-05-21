use prometheus_client::{
    encoding::{EncodeMetric, MetricEncoder},
    metrics::MetricType,
};

pub use self::{counter::*, gauge::*, histogram::*};
use std::{fmt, sync::Arc};

mod counter;
mod gauge;
mod histogram;

#[derive(Debug)]
pub enum Metric {
    Counter(Arc<MetricsCounter>),
    Gauge(Arc<MetricsGauge>),
    Histogram(Arc<MetricsHistogram>),
}

impl EncodeMetric for Metric {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), fmt::Error> {
        match self {
            Metric::Counter(counter) => counter.inner.encode(encoder),
            Metric::Gauge(gauge) => gauge.inner.encode(encoder),
            Metric::Histogram(hist) => hist.inner.encode(encoder),
        }
    }

    fn metric_type(&self) -> MetricType {
        match self {
            Metric::Counter(_) => MetricType::Counter,
            Metric::Gauge(_) => MetricType::Gauge,
            Metric::Histogram(_) => MetricType::Histogram,
        }
    }
}
