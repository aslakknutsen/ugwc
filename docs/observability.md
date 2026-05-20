# Observability

ugwc exposes Prometheus metrics on the admin HTTP server. Gateway API **resource state**
(gauges such as `gatewayapi_gateway_status`) is expected from a separate
[gateway-api-state-metrics](https://github.com/Kuadrant/gateway-api-state-metrics) deployment
or kube-state-metrics custom-resource-state — not duplicated inside the controller binary.

## Scraping

| Method | When to use |
|--------|-------------|
| Helm `serviceMonitor.enabled=true` | Clusters with Prometheus Operator |
| `prometheusAnnotations=true` | Annotation-based scrapers without Operator |
| Direct | `curl http://127.0.0.1:8080/metrics` (default `UGWC_ADMIN_LISTEN_ADDR`) |

## Controller-owned metrics (summary)

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_workqueue_depth` | Gauge | `queue_name` |
| `ugwc_workqueue_adds_total` | Counter | `queue_name` |
| `ugwc_workqueue_queue_duration_seconds` | Histogram | `queue_name` |
| `ugwc_reconcile_duration_seconds` | Histogram | `controller_name` |
| `ugwc_reconcile_runs_total` | Counter | `controller_name`, `status` |
| `ugwc_ungranted_reference_total` | Counter | `route_kind` |
| `ugwc_cache_synced` | Gauge | — |
| `process_*` | (process collector) | standard Prometheus process metrics |

## gateway-api-state-metrics

Install upstream exporter in the observability namespace and scrape it alongside ugwc.
Example join (adjust metric names to your mesh):

```promql
sum by (gateway_name) (
  rate(http_request_duration_seconds_sum[5m])
)
* on (gateway_name) group_left()
gatewayapi_gateway_status{status="Ready"}
```

## Ungranted references

`ugwc_ungranted_reference_total{route_kind="HTTPRoute"}` increments when compile reports
`RefNotPermitted` for a resource in the reconciled gateway snapshot. Status conditions on
routes still carry the authoritative per-parent detail; the counter is for cluster-wide alerting.

## Deferred

- `rest_client_request_duration_seconds` — requires custom kube-rs HTTP middleware
- `etcd_request_duration_seconds` — not visible from the controller process
- Tokio runtime metrics — optional with `RUSTFLAGS='--cfg tokio_unstable'` (not enabled in default builds)
- OTLP push — use when pull scraping is insufficient for your deployment model
