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
| `UGWC_STATUS_PATCH_CONCURRENCY` | `16` | Concurrent status patches per gateway reconcile |
| `UGWC_XDS_WIRE` | `gwxds` | Wire profile: `gwxds` (Praxis) or `envoy` |
| `UGWC_IMPLEMENTATION_PROFILES_PATH` | — | JSON profile overrides keyed by `GatewayClass.spec.controllerName` (`data_plane`, `output`, `proxy_image`, …) |
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
| `PraxisConfigUnsupported` | A Gateway feature cannot be represented in native Praxis config (conflicting CORS policies, method/query-param match, mixed-TLS weighted backends, …); the emitted config may be broader than requested |
| `PraxisConfigFailed` | Encoding or applying the native Praxis ConfigMap/Secret failed |

View events with `kubectl describe gateway <name>` or `kubectl get events --field-selector involvedObject.name=<name>`.

## Metrics reference

### Control plane

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_reconcile_runs_total` | Counter | `controller_name`, `status` (`success`/`error`) |
| `ugwc_reconcile_duration_seconds` | Histogram | `controller_name` (+ `trace_id` exemplar when OTel active) |
| `ugwc_workqueue_depth` | Gauge | `queue_name` |
| `ugwc_workqueue_in_flight` | Gauge | `queue_name` |
| `ugwc_workqueue_adds_total` | Counter | `queue_name` |
| `ugwc_workqueue_dedup_total` | Counter | `queue_name` |
| `ugwc_workqueue_queue_duration_seconds` | Histogram | `queue_name` |
| `ugwc_workqueue_unfinished_work_seconds` | Gauge | `queue_name` |
| `ugwc_workqueue_longest_running_processor_seconds` | Gauge | `queue_name` |
| `ugwc_workqueue_retries_total` | Counter | `queue_name` |
| `ugwc_index_fanout_size` | Histogram | `event_kind` |
| `ugwc_index_gateways_indexed` | Gauge | — |
| `ugwc_rest_client_request_duration_seconds` | Histogram | `verb`, `resource`, `code` |
| `ugwc_admin_request_duration_seconds` | Histogram | `method`, `status` |

### Domain

| Metric | Type | Labels |
|--------|------|--------|
| `ugwc_compile_duration_seconds` | Histogram | — |
| `ugwc_compile_errors_total` | Counter | — |
| `ugwc_snapshot_load_duration_seconds` | Histogram | — |
| `ugwc_status_patch_duration_seconds` | Histogram | — |
| `ugwc_status_patch_count` | Histogram | — |
| `ugwc_publish_applied_total` | Counter | — |
| `ugwc_publish_skipped_total` | Counter | — |
| `ugwc_dataplane_apply_total` | Counter | — |
| `ugwc_dataplane_error_total` | Counter | — |
| `ugwc_praxis_config_encode_seconds` | Histogram | — |
| `ugwc_praxis_config_apply` | Counter | — |
| `ugwc_praxis_config_error` | Counter | — |
| `ugwc_praxis_config_unsupported` | Counter | `feature` |
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
| `process_*` | various | always (via `kubert-prometheus-process`) |

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

Dedup rate (skipped duplicate enqueues):

```promql
rate(ugwc_workqueue_dedup_total{queue_name="gateway_reconcile"}[5m])
```

EndpointSlice fan-out P99 (should be ~1 after index warm-up):

```promql
histogram_quantile(0.99,
  sum(rate(ugwc_index_fanout_size_bucket{event_kind="endpointslice"}[5m])) by (le))
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

## Praxis native config output

The built-in `ugwc.io/default-controller` profile uses `output: praxis_configmap`, so
per-gateway Praxis deployments receive native YAML instead of dialing xDS. For a
Gateway `ns/<gateway>`:

| Object | Content |
|--------|---------|
| ConfigMap `ugwc-dp-<gateway>` | `data["praxis.yaml"]` |
| Secret `ugwc-dp-<gateway>-tls` | `listener-<name>.crt/.key` and `backend-<cluster>.ca.pem/.crt/.key`; created for every configmap-mode gateway (empty when the snapshot has no TLS material) |
| Deployment `ugwc-dp-<gateway>` | mounts `/etc/praxis` and `/etc/praxis-tls`, runs `praxis -c /etc/praxis/praxis.yaml` |

Inspect and debug:

```bash
kubectl get configmap,secret -n <ns> \
  -l gateway.ugwc.io/managed=ugwc-per-gateway
kubectl get configmap ugwc-dp-<gateway> -n <ns> \
  -o jsonpath='{.data.praxis\.yaml}'
kubectl logs deploy/ugwc-dp-<gateway> -n <ns>
kubectl get events -n <ns> --field-selector reason=PraxisConfigUnsupported
```

Behavior and limits:

- The built-in profile pins `ghcr.io/praxis-proxy/praxis:0.7.5`. The native config
  schema (including `insecure_options.allow_private_endpoints`) is newer than the old
  `quay.io/aslakknutsen/praxis:latest` build; do not downgrade the image without
  regenerating/patching the encoder.
- The ConfigMap is written only when the compile is programmed; a failed compile keeps
  the last good file, so the proxy keeps serving the previous config.
- Praxis requires at least one listener. When a Gateway has no routable listener (for
  example a TLS-passthrough listener with no TLSRoutes yet), the encoder emits a no-op
  HTTP listener on port `15000` so the proxy starts and becomes Ready. No Gateway
  listener is bound to that port.
- The encoder sets `admin.metrics_address` to `0.0.0.0:15021`, matching the Deployment
  probes (`/healthy`, `/ready`) and metrics scraping; the `PRAXIS_ADMIN_ADDR`
  environment variable is not read by Praxis.
- Static config changes (routes, listeners, filters) and TLS material changes update
  the pod template's `gateway.ugwc.io/config-revision` annotation, which rolls the
  proxy so they take effect promptly instead of waiting for kubelet ConfigMap/Secret
  propagation (typically about a minute). The revision deliberately excludes endpoint
  lists.
- Endpoint-only changes are hot reloaded from the mounted ConfigMap by the Praxis file
  watcher; kubelet sync is eventual, typically within a minute.
- Gateway listeners that share a port are merged into one Praxis listener: their
  routing tables are combined (effective route hostnames already include the
  listener-hostname intersection) and TLS-terminating listeners contribute one
  certificate each, selected by SNI.
- Weighted `backendRefs` are merged into one Praxis cluster with endpoint weights
  scaled by the LCM of endpoint counts, preserving the requested aggregate split.
  Weights are then reduced by their GCD so the round-robin cycle stays short.
- **Backend mTLS client certificates** are loaded by the cluster TLS config, which has
  no file watcher. Rotating one updates the Secret and the config revision, so the
  proxy Deployment rolls; Praxis does not reload these files in place.
- Route-scoped CORS is emitted as one conditional CORS filter per route match. Because
  Praxis treats conditions on security filters as a bypass risk, the generated config
  sets `insecure_options.skip_pipeline_checks.conditional_security: true` whenever a
  scoped CORS filter is present.
- Praxis 0.7.5 rejects `allowCredentials` combined with a wildcard origin, method, or
  header; Gateway API expects the proxy to echo the request origin in that case. The
  encoder reports `CorsCredentialsWildcard` and disables credentials for that policy
  instead of emitting a config Praxis refuses to load.
- `RequestRedirect` with `ReplacePrefixMatch` is reported as `RedirectPathPrefix` and
  keeps the original path: Praxis 0.7.5's redirect `Location` template only substitutes
  the whole request path, so a prefix cannot be stripped or replaced. `ReplaceFullPath`
  redirects work normally.
- Praxis 0.7.5's redirect filter accepts only 301/302/307/308. A 303 redirect is served
  by a `static_response` with status 303 and a static `Location`. The location uses the
  `ReplaceFullPath` value when present, otherwise the matched route path (or `/`); it
  cannot echo an unspecified request host or a request path suffix. The encoder reports
  `RedirectStatus303Location` so callers can see the gap.
- Praxis 0.7.5's upstream connection pool is keyed by address, SNI, client certificate,
  and verify flags, but not by the trust anchors. If two clusters dial the same address
  with the same SNI but trust different CAs, they could share a pooled connection and
  skip verification. The encoder detects that collision and sets `idle_timeout_ms: 1`
  on the affected clusters so connections are not kept for reuse; those backends pay
  a new TLS handshake per request.
- Praxis has no separate `backendRequest` deadline. A rule's `backendRequest` timeout is
  enforced as the tighter whole-request timeout (with `request` taking the minimum when
  both are set); a `0s` value disables only that field.
- Unsupported mappings (for example method/query-parameter matching or conflicting CORS
  policies) are surfaced as `PraxisConfigUnsupported` events and the
  `ugwc_praxis_config_unsupported{feature}` metric rather than being silently dropped.
- To keep xDS for a class, add a profile file entry with `"output": "xds"` and point
  `UGWC_IMPLEMENTATION_PROFILES_PATH` at it.

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
