use metrics::{Label, Unit};
use prometheus_client::registry::Unit as PrometheusUnit;

pub(crate) fn convert_labels_to_prometheus(labels: Vec<Label>) -> Vec<(String, String)> {
    labels
        .into_iter()
        .map(Label::into_parts)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

pub(crate) fn convert_unit_to_prometheus(unit: Unit) -> PrometheusUnit {
    match unit {
        Unit::Count => PrometheusUnit::Other("count".to_string()),
        Unit::Percent => PrometheusUnit::Other("percent".to_string()),
        Unit::Seconds => PrometheusUnit::Other("seconds".to_string()),
        Unit::Milliseconds => PrometheusUnit::Other("milliseconds".to_string()),
        Unit::Microseconds => PrometheusUnit::Other("microseconds".to_string()),
        Unit::Nanoseconds => PrometheusUnit::Other("nanoseconds".to_string()),
        Unit::Tebibytes => PrometheusUnit::Other("tebibytes".to_string()),
        Unit::Gibibytes => PrometheusUnit::Other("gibibytes".to_string()),
        Unit::Mebibytes => PrometheusUnit::Other("mebibytes".to_string()),
        Unit::Kibibytes => PrometheusUnit::Other("kibibytes".to_string()),
        Unit::Bytes => PrometheusUnit::Bytes,
        Unit::TerabitsPerSecond => PrometheusUnit::Other("terabits per second".to_string()),
        Unit::GigabitsPerSecond => PrometheusUnit::Other("gigabits per second".to_string()),
        Unit::MegabitsPerSecond => PrometheusUnit::Other("megabits per second".to_string()),
        Unit::KilobitsPerSecond => PrometheusUnit::Other("kilobits per second".to_string()),
        Unit::BitsPerSecond => PrometheusUnit::Other("bits per second".to_string()),
        Unit::CountPerSecond => PrometheusUnit::Other("count per second".to_string()),
    }
}
