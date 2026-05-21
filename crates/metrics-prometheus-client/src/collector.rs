use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::MetricType,
    registry::Unit as PrometheusUnit,
};

use crate::{metrics::*, utils::*};

pub type Map<K, V> = Arc<RwLock<HashMap<K, V>>>;
pub type Label = (String, String);

#[derive(Debug, Default)]
struct Descriptor {
    help: String,
    unit: Option<PrometheusUnit>,
}

impl Descriptor {
    fn new(help: String, unit: Option<Unit>) -> Self {
        Self {
            help,
            unit: unit.map(convert_unit_to_prometheus),
        }
    }
}

/// This module provides compatibility with the `metrics` crate.
/// It registers as a `Collector` with the `prometheus_client` crate
/// and implements the `Recorder` trait of the `metrics` crate.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    metrics: Map<KeyName, Vec<(Vec<Label>, Metric)>>,
    descriptors: Map<KeyName, Descriptor>,
}

impl Clone for MetricsCollector {
    fn clone(&self) -> Self {
        Self {
            metrics: Arc::clone(&self.metrics),
            descriptors: Arc::clone(&self.descriptors),
        }
    }
}

impl Collector for MetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), fmt::Error> {
        for (key_name, metrics) in self.metrics.read().unwrap_or_else(|e| e.into_inner()).iter() {
            // Find descriptor for the metric.
            let descriptors = self.descriptors.read().unwrap_or_else(|e| e.into_inner());
            let (help, unit) = descriptors
                .get(key_name)
                .map(|d| (d.help.as_str(), d.unit.as_ref()))
                .unwrap_or_else(|| ("", None));

            // Gather statistics about the metrics.
            if metrics.is_empty() {
                continue;
            }
            let metric_type = metrics[0].1.metric_type();
            // If there is more than one entry, this is always true.
            let has_labels = !metrics[0].0.is_empty();

            // Encode descriptor and metric.
            let export_name = match metric_type {
                MetricType::Counter => prometheus_counter_base_name(key_name.as_str()),
                _ => key_name.as_str(),
            };
            let mut descriptor_encoder =
                encoder.encode_descriptor(export_name, help, unit, metric_type)?;

            // Encode metrics for this key.
            // If labels are present, encode the metric as a family.
            if has_labels {
                for (labels, metric) in metrics {
                    let metric_encoder = descriptor_encoder.encode_family(labels)?;
                    metric.encode(metric_encoder)?;
                }
            } else {
                let metric = &metrics[0].1;
                metric.encode(descriptor_encoder)?;
            }
        }
        Ok(())
    }
}

impl MetricsCollector {
    fn register(&self, key: &Key, metric: Metric) {
        let (key_name, labels) = key.clone().into_parts();
        let labels = convert_labels_to_prometheus(labels);

        let mut metrics = self.metrics.write().unwrap_or_else(|e| e.into_inner());
        let entry = metrics.entry(key_name).or_default();

        if let Some((_, existing)) = entry.iter().find(|(l, _)| l == &labels) {
            debug_assert_eq!(
                existing.metric_type().as_str(),
                metric.metric_type().as_str(),
                "re-registering metric {:?} with different type",
                key
            );
            return;
        }
        if !entry.is_empty()
            && entry[0].1.metric_type().as_str() != metric.metric_type().as_str()
        {
            debug_assert!(
                false,
                "registering metric {:?} with different type than existing series",
                key
            );
            return;
        }
        entry.push((labels, metric));
    }

    fn describe(&self, key: &KeyName, unit: Option<Unit>, description: SharedString) {
        let _ = self
            .descriptors
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.clone())
            .or_insert_with(|| Descriptor::new(description.into_owned(), unit));
    }
}

impl Recorder for MetricsCollector {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(&key, unit, description)
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(&key, unit, description)
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(&key, unit, description)
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        if let Some(Metric::Counter(c)) = self.lookup(key) {
            return Counter::from_arc(c);
        }
        let counter = Arc::new(MetricsCounter::default());
        self.register(key, Metric::Counter(Arc::clone(&counter)));
        Counter::from_arc(counter)
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        if let Some(Metric::Gauge(g)) = self.lookup(key) {
            return Gauge::from_arc(g);
        }
        let gauge = Arc::new(MetricsGauge::default());
        self.register(key, Metric::Gauge(Arc::clone(&gauge)));
        Gauge::from_arc(gauge)
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        if let Some(Metric::Histogram(h)) = self.lookup(key) {
            return Histogram::from_arc(h);
        }
        let hist = Arc::new(MetricsHistogram::default());
        self.register(key, Metric::Histogram(Arc::clone(&hist)));
        Histogram::from_arc(hist)
    }
}

impl MetricsCollector {
    fn lookup(&self, key: &Key) -> Option<Metric> {
        let (key_name, labels) = key.clone().into_parts();
        let labels = convert_labels_to_prometheus(labels);
        let metrics = self.metrics.read().unwrap_or_else(|e| e.into_inner());
        let entry = metrics.get(&key_name)?;
        let (_, metric) = entry.iter().find(|(l, _)| l == &labels)?;
        match metric {
            Metric::Counter(c) => Some(Metric::Counter(Arc::clone(c))),
            Metric::Gauge(g) => Some(Metric::Gauge(Arc::clone(g))),
            Metric::Histogram(h) => Some(Metric::Histogram(Arc::clone(h))),
        }
    }
}
