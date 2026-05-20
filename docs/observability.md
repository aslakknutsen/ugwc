# Observability

ugwc exposes Prometheus metrics on the admin HTTP server. Gateway API **resource state** gauges (`gatewayapi_*`) are emitted from the controller reconcile path for resources in each reconciled gateway fan-out (label `source="ugwc"`). An external [gateway-api-state-metrics](https://github.com/Kuadrant/gateway-api-state-metrics) deployment remains optional for cluster-wide views across multiple controllers.

## Scraping

| Method | When to use |
|--------|-------------|
| Helm `observability.serviceMonitor.enabled=true` (default in chart) | Clusters with Prometheus Operator |
| `prometheusAnnotations=true` | Annotation-based scrapers without Operator |
| Direct | `curl http://127.0.0.1:8080/metrics` (default `UGWC_ADMIN_LISTEN_ADDR`) |

**Dual export:** Prometheus pull via `/metrics` is the SLO source of truth. Optional OTLP metrics push (`UGWC_OTLP_ENDPOINT`, `otlp-metrics` feature) complements pull for collector-centric fleets. Admin HTTP also exposes `PUT /log-level` for runtime tracing filters — see [operations.md](operations.md).

## Controller-owned metrics

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_workqueue_depth` | Gauge | `queue_name` |
| `ugwc_workqueue_adds_total` | Counter | `queue_name` |
| `ugwc_workqueue_queue_duration_seconds` | Histogram | `queue_name` |
| `ugwc_workqueue_unfinished_work_seconds` | Gauge | `queue_name` (sum of in-flight reconcile ages) |
| `ugwc_workqueue_longest_running_processor_seconds` | Gauge | `queue_name` |
| `ugwc_workqueue_retries_total` | Counter | `queue_name` |
| `ugwc_reconcile_duration_seconds` | Histogram | `controller_name` |
| `ugwc_reconcile_runs_total` | Counter | `controller_name`, `status` |
| `ugwc_rest_client_request_duration_seconds` | Histogram | `verb`, `resource`, `code` |
| `ugwc_ungranted_reference_total` | Counter | `route_kind` |
| `ugwc_cache_synced` | Gauge | — |
| `ugwc_leader_active` | Gauge | — |
| `ugwc_xds_streams_active` | Gauge | — |
| `ugwc_xds_stream_disconnects_total` | Counter | `reason` |
| `ugwc_xds_push_duration_seconds` | Histogram | — |
| `ugwc_xds_scope_miss_total` | Counter | — |
| `process_*` | (process collector) | standard Prometheus process metrics |

## Gateway API state (`gatewayapi_*`)

Emitted per reconcile for the gateway snapshot (owned `controllerName` fan-out):

| Metric | Labels |
|--------|--------|
| `gatewayapi_gateway_created` | `namespace`, `name`, `source` |
| `gatewayapi_gateway_status` | `namespace`, `name`, `status`, `source` |
| `gatewayapi_httproute_attached_gateways` | `namespace`, `name`, `source` |
| `gatewayapi_grpcroute_attached_gateways` | … |
| `gatewayapi_tlsroute_attached_gateways` | … |
| `gatewayapi_tcproute_attached_gateways` | … |
| `gatewayapi_udproute_attached_gateways` | … |
| `gatewayapi_route_rules_total` | `namespace`, `name`, `kind`, `source` |
| `gatewayapi_referencegrant_status` | `namespace`, `name`, `validated`, `source` |

## Readiness and shutdown

- `/readyz` returns 200 only after `InitialSyncGate` completes: CRD list, Gateway list, EndpointSlice list. CRD-gated route watchers may start later by design.
- On SIGTERM: readiness fails → reconcile enqueue stops → in-flight drain (30s) → leader lease release → task cancel. See `SHUTDOWN_DRAIN_SECS` / `SHUTDOWN_JOIN_SECS` in `crates/ugwc/src/run.rs`.

## Tokio runtime metrics (optional)

Build with `--features tokio-metrics` and `RUSTFLAGS='--cfg tokio_unstable'` for `ugwc_tokio_*` gauges.

## OTLP push (optional)

Build with `--features otlp-metrics`. Set `UGWC_OTLP_ENDPOINT` and optional `UGWC_OTLP_PUSH_INTERVAL_MS`. Pull scrape remains the default for long-lived Deployments.

## OTLP traces (optional)

Build with `--features otlp-traces`. When enabled:
- All tracing spans are exported via OTLP gRPC to `UGWC_OTLP_ENDPOINT`.
- The `PUT /log-level` admin endpoint remains functional (filter reload works with the OTLP layer stack).
- Spawned tasks propagate the parent span, so trace IDs survive across `tokio::spawn` boundaries.
- Outbound Kubernetes API requests include a W3C `traceparent` header for cross-service correlation.

## Grafana dashboard

A ready-to-import dashboard is at `charts/ugwc/dashboards/ugwc-overview.json`. Panels: reconcile rate/error rate, P50/P95/P99 latency, workqueue depth, REST client latency by resource, Gateway API state, leader status.

## Recording rules

The Helm `PrometheusRule` includes recording rules for SLO burn-rate queries:
- `ugwc:reconcile_error_rate:5m`
- `ugwc:reconcile_p99_seconds:5m`

## Known limitations

- **Exemplars on reconcile histograms:** The `metrics` crate facade does not support Prometheus exemplars. Linking `ugwc_reconcile_duration_seconds` histogram samples to their active `trace_id` requires switching to native `prometheus-client` histograms directly, bypassing the facade. Until then, use JSON log `trace_id` fields and the `reconcile_gateway` span for trace correlation.
- `etcd_request_duration_seconds` — not visible from the controller process.
- Target Allocator / hybrid scrape — not bundled in-chart.

## Example alerts

See Helm `observability.prometheusRule` or [operations.md](operations.md) PromQL examples.

```promql
sum(rate(ugwc_reconcile_runs_total{status="error"}[5m]))
  / sum(rate(ugwc_reconcile_runs_total[5m]))

histogram_quantile(
  0.99,
  sum(rate(ugwc_reconcile_duration_seconds_bucket[5m])) by (le, controller_name)
)

ugwc_workqueue_depth{queue_name="gateway_reconcile"}
```
