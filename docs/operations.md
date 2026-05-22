# ugwc Operations Runbook

## Environment variables

All settings are prefixed `UGWC_` and parsed at startup via `envy`.

| Variable | Default | Description |
|----------|---------|-------------|
| `UGWC_ADMIN_LISTEN_ADDR` | `127.0.0.1:8080` | Admin HTTP (probes, metrics, log-level) |
| `UGWC_XDS_LISTEN_ADDR` | `127.0.0.1:18000` | xDS ADS gRPC server |
| `UGWC_CONTROLLER_NAMES` | `example.com/gateway-controller` | Comma-separated GatewayClass controller names |
| `UGWC_KUBE_ENABLE` | `false` | Enable Kubernetes watches and reconcile loop |
| `UGWC_XDS_INSECURE` | `false` | Allow plaintext xDS (rejected in `prod` profile) |
| `UGWC_XDS_TLS_CERT_PATH` | — | PEM certificate for xDS TLS |
| `UGWC_XDS_TLS_KEY_PATH` | — | PEM private key for xDS TLS |
| `UGWC_XDS_TLS_CA_PATH` | — | CA bundle for mutual TLS on xDS |
| `UGWC_LOG_JSON` | `false` | Structured JSON logs to stdout (set `true` in prod) |
| `UGWC_RUN_PROFILE` | `dev` | `dev` or `prod` — prod rejects insecure xDS |
| `UGWC_LEADER_ELECTION` | `false` | Lease-based leader election for HA replicas |
| `UGWC_ENDPOINT_DEBOUNCE_MS` | `200` | Coalesce EndpointSlice churn before reconciling |
| `UGWC_RECONCILE_CONCURRENCY` | `16` | Concurrent gateway reconcile workers |
| `UGWC_XDS_WIRE` | `gwxds` | Wire profile: `gwxds` (Praxis) or `envoy` |
| `UGWC_DATAPLANE_ENABLE` | `false` | Create per-gateway Praxis workloads |
| `UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR` | — | Praxis ISTIOD_ADDR bootstrap (`host:port` or URI) |
| `UGWC_OTLP_ENDPOINT` | — | OTLP endpoint (requires `otlp-metrics`/`otlp-traces` features) |
| `UGWC_OTLP_PUSH_INTERVAL_MS` | `15000` | OTLP push interval |
| `RUST_LOG` | `info,kube=warn` | `tracing` filter directive (overrides default) |

## Admin endpoints

The admin server listens on `UGWC_ADMIN_LISTEN_ADDR`.

### `GET /healthz`

Returns `200 OK` unconditionally once the process is up. Used as the Kubernetes liveness probe.

### `GET /readyz`

Returns `200 OK` after initial informer/cache sync completes. Returns `503` before that and during graceful shutdown drain. Used as the Kubernetes readiness probe.

### `GET /metrics`

Prometheus text exposition (`text/plain; version=0.0.4`). All `ugwc_*` and `gatewayapi_*` metrics.

### `PUT /log-level`

Change log filter at runtime without a restart. Body is a plain-text `RUST_LOG`-style directive.

```bash
# raise ugwc to debug, keep kube at warn
curl -X PUT http://localhost:8080/log-level \
  -d 'debug,kube=warn'

# reset to defaults
curl -X PUT http://localhost:8080/log-level \
  -d 'info,kube=warn'
```

## Kubernetes Events

ugwc publishes Warning events on the Gateway object for operator-visible failures. The ClusterRole in the Helm chart grants `events` create/patch/update on both `""` and `events.k8s.io` API groups.

| Reason | When |
|--------|------|
| `CompileBlocked` | Gateway compile has blocking errors (invalid refs, conflicted listeners) |
| `ProvisionFailed` | Per-gateway dataplane provisioning failed |
| `TlsMaterialMissing` | One or more HTTPS listeners have certificate errors |
| `DataplaneMisconfigured` | Missing proxy_image or empty bootstrap address |

View events with `kubectl describe gateway <name>` or `kubectl get events --field-selector involvedObject.name=<name>`.

## Metrics reference

### Control plane

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_reconcile_runs_total` | Counter | `controller_name`, `status` (`success`/`error`) |
| `ugwc_reconcile_duration_seconds` | Histogram | `controller_name` (+ `trace_id` exemplar when OTel active) |
| `ugwc_workqueue_depth` | Gauge | `queue_name` |
| `ugwc_workqueue_adds_total` | Counter | `queue_name` |
| `ugwc_workqueue_queue_duration_seconds` | Histogram | `queue_name` |
| `ugwc_workqueue_unfinished_work_seconds` | Gauge | `queue_name` |
| `ugwc_workqueue_longest_running_processor_seconds` | Gauge | `queue_name` |
| `ugwc_workqueue_retries_total` | Counter | `queue_name` |
| `ugwc_rest_client_request_duration_seconds` | Histogram | `verb`, `resource`, `code` |
| `ugwc_admin_request_duration_seconds` | Histogram | `method`, `status` |

### Domain

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_compile_duration_seconds` | Histogram | — |
| `ugwc_compile_errors_total` | Counter | — |
| `ugwc_snapshot_load_duration_seconds` | Histogram | — |
| `ugwc_publish_applied_total` | Counter | — |
| `ugwc_publish_skipped_total` | Counter | — |
| `ugwc_dataplane_apply_total` | Counter | — |
| `ugwc_dataplane_error_total` | Counter | — |
| `ugwc_ungranted_reference_total` | Counter | `route_kind` |
| `ugwc_leader_active` | Gauge | — |
| `ugwc_cache_synced` | Gauge | — |

### Gateway API state

| Metric | Type | Labels |
|--------|------|--------|
| `gatewayapi_gateway_created` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_gateway_status` | Gauge | `namespace`, `name`, `status`, `source` |
| `gatewayapi_httproute_attached_gateways` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_grpcroute_attached_gateways` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_tlsroute_attached_gateways` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_tcproute_attached_gateways` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_udproute_attached_gateways` | Gauge | `namespace`, `name`, `source` |
| `gatewayapi_route_rules_total` | Gauge | `namespace`, `name`, `kind`, `source` |
| `gatewayapi_referencegrant_status` | Gauge | `namespace`, `name`, `validated`, `source` |
| `gatewayapi_inferencepool_status` | Gauge | `namespace`, `name`, `source` |

### Runtime (optional features)

| Metric | Type | Feature |
|--------|------|---------|
| `ugwc_tokio_workers_count` | Gauge | `tokio-metrics` + `tokio_unstable` |
| `ugwc_tokio_total_busy_duration_seconds` | Gauge | `tokio-metrics` |
| `ugwc_tokio_total_polls_count` | Gauge | `tokio-metrics` |
| `ugwc_tokio_total_noop_count` | Gauge | `tokio-metrics` |
| `process_*` | various | always (via `metrics-process`) |

## Common PromQL queries

Reconcile error rate over 5 minutes:

```promql
sum(rate(ugwc_reconcile_runs_total{status="error"}[5m]))
  / sum(rate(ugwc_reconcile_runs_total[5m]))
```

Reconcile P99 latency:

```promql
histogram_quantile(0.99,
  sum(rate(ugwc_reconcile_duration_seconds_bucket[5m])) by (le, controller_name))
```

Workqueue wait P99:

```promql
histogram_quantile(0.99,
  sum(rate(ugwc_workqueue_queue_duration_seconds_bucket[5m])) by (le))
```

K8s API latency by resource:

```promql
histogram_quantile(0.99,
  sum(rate(ugwc_rest_client_request_duration_seconds_bucket[5m])) by (le, resource))
```

Gateways not programmed:

```promql
gatewayapi_gateway_status{status="NotProgrammed", source="ugwc"}
```

## Correlating logs, traces, and metrics

When built with `otlp-traces`, ugwc attaches `trace_id` to:

1. **Structured logs** — the `otel.trace_id` field appears on spans in JSON output. Filter in Loki/Elasticsearch by `trace_id`.
2. **Histogram exemplars** — `ugwc_reconcile_duration_seconds` carries `trace_id` as an exemplar label. In Grafana, enable "Exemplars" on the histogram panel and configure the Tempo/Jaeger datasource link.
3. **Outbound K8s API calls** — the `traceparent` header is injected on every kube-client HTTP request (via `otel-context` feature), so downstream spans appear in the same trace.

Correlation flow: start from a slow reconcile in the histogram panel, click the exemplar to jump to the trace in Tempo, then use the trace_id to find the structured log lines.

## Graceful shutdown

On SIGTERM or Ctrl-C:

1. `/readyz` returns 503
2. Reconcile queue stops accepting new items
3. In-flight reconciles drain (up to `SHUTDOWN_DRAIN_SECS` = 30s)
4. Leader lease released (fast failover)
5. Cancel admin, xDS, and kube tasks (join timeout 10s each)
6. Flush OTLP metrics and traces
7. Exit 0

`terminationGracePeriodSeconds` in the Helm chart is 60s, which exceeds the 30s drain + 10s join budget.

## Gateway API conformance (local kind)

Prerequisites and defaults: [README.md](../README.md#gateway-api-conformance-local-kind). Script: `hack/setup.sh`.

```bash
./hack/setup.sh
./hack/conformance.sh
```

## Local kind observability (optional)

By default, `hack/setup.sh` does **not** install a metrics backend. The ugwc Helm chart still creates a `ServiceMonitor` and `PrometheusRule`; they are inactive until Prometheus Operator exists.

Enable a slim **kube-prometheus-stack** (Prometheus + Grafana) for local scraping:

```bash
./hack/setup.sh --install-prometheus
# or: INSTALL_PROMETHEUS=1 ./hack/setup.sh
```

Environment overrides:

| Variable | Default | Description |
|----------|---------|-------------|
| `INSTALL_PROMETHEUS` | `0` | Set `1` to install the monitoring stack |
| `PROMETHEUS_NAMESPACE` | `monitoring` | Namespace for kube-prometheus-stack |
| `PROMETHEUS_RELEASE` | `kube-prometheus` | Helm release name |
| `PROMETHEUS_CHART_VERSION` | `67.11.0` | Pinned chart version |

Values file: `hack/prometheus-values.yaml` (disables Alertmanager, node-exporter, kube-state-metrics, and default cluster rules to reduce kind footprint).

After setup, the script prints port-forward commands. Verify scraping:

```bash
# Prometheus UI (after port-forward to 9090)
curl -sG 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=ugwc_reconcile_runs_total'
```

Grafana imports the **ugwc Controller Overview** dashboard from `hack/grafana-ugwc-dashboard.yaml` (UID `ugwc-overview`, sourced from `charts/ugwc/dashboards/ugwc-overview.json`).

Metrics scraped include all `ugwc_*` and `gatewayapi_*` series documented in [Metrics reference](#metrics-reference) above.
