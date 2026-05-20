# Metrics compliance checklist

Maps [reference.md](../.cursor/skills/gateway-api-metrics-audit/reference.md) domains to ugwc implementation.

| Domain | Status | Implementation |
|--------|--------|----------------|
| 1 Delivery | Compliant | Pull `/metrics`; Helm `observability.serviceMonitor` (default on); optional `otlp-metrics` / `otlp-traces` |
| 2 Control plane | Compliant | `ugwc_workqueue_*`, `ugwc_reconcile_*`, `ugwc_rest_client_*`; `etcd_*` N/A |
| 3 Gateway state | Compliant | In-controller `gatewayapi_*` including `gatewayapi_gateway_created`; `ugwc_ungranted_reference_total` |
| 4 Rust instrumentation | Compliant | `metrics` + `metrics-prometheus-client` → `prometheus-client` registry |
| 5 Runtime / admin | Compliant | `process_*`; xDS gauges in `ugwc-xds`; Tokio via `tokio-metrics` in release image; admin `/log-level` + Tower trace/timeout |
| 6 K8s ops | Compliant | `InitialSyncGate`, ordered shutdown, probes, `terminationGracePeriodSeconds` |
| 7 Documentation | Compliant | [observability.md](observability.md), [operations.md](operations.md), this file |

## Maturity: L4 (metrics)

Logging/tracing L4 requires building with `--features otlp-traces` and setting `UGWC_OTLP_ENDPOINT`.

## Environment

| Variable | Purpose |
|----------|---------|
| `UGWC_ADMIN_LISTEN_ADDR` | Admin HTTP (metrics, health, log-level) |
| `UGWC_OTLP_ENDPOINT` | OTLP push (requires `otlp-metrics` and/or `otlp-traces` features) |
| `UGWC_OTLP_PUSH_INTERVAL_MS` | OTLP metrics flush interval (default 15000) |

## Shutdown timing

Code constants in `crates/ugwc/src/run.rs`: `SHUTDOWN_DRAIN_SECS=30`, `SHUTDOWN_JOIN_SECS=10`. Helm `terminationGracePeriodSeconds` should stay greater than their sum (default chart: 60).
