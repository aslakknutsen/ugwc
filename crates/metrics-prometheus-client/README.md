# metrics-prometheus-client
Creating compatibility between `metrics` and `prometheus_client`.

## Usage

```rust
use metrics_prometheus_client::install;
use prometheus_client::{encoding::text::encode, registry::Registry};

let collector = install();

// Use `metrics` crate interfaces.
metrics::counter!("count", "whose" => "mine", "kind" => "owned").increment(1);

let mut registry = Registry::default();
registry.register_collector(Box::new(collector));

let mut report = String::new();
encode(&mut report, &registry).unwrap();

assert_eq!(
    report.trim(),
    "# HELP count \n# TYPE count counter\ncount_total{whose=\"mine\",kind=\"owned\"} 1\n# EOF"
        .trim(),
);
```
