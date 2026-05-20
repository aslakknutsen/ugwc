# Metrics compliance checklist (L4)

Maps [reference.md](../.cursor/skills/gateway-api-metrics-audit/reference.md) domains to ugwc implementation.

| Domain | Status | Implementation |
|--------|--------|----------------|
| 1 Delivery | Compliant | Pull `/metrics`; Helm `observability.serviceMonitor`; optional `otlp-metrics` feature |
| 2 Control plane | Compliant | `ugwc_workqueue_*`, `ugwc_reconcile_*`, `ugwc_rest_client_*`; `etcd_*` N/A |
| 3 Gateway state | Compliant | In-controller `gatewayapi_*` with `source="ugwc"`; `ugwc_ungranted_reference_total` |
| 4 Rust instrumentation | Compliant | `metrics` + `metrics-prometheus-client` → `prometheus-client` registry |
| 5 Runtime / admin | Partial | `process_*` via legacy collector; Tokio via feature `tokio-metrics`; xDS gauges |
| 6 K8s ops | Compliant | `InitialSyncGate`, ordered shutdown, probes, `terminationGracePeriodSeconds` |
| 7 Documentation | Compliant | [observability.md](observability.md), this file |

## Maturity: L4

For ugwc-owned Gateway API reconciliation. Multi-controller fleets may still deploy an external `gatewayapi_*` exporter for non-owned objects.

## Environment

| Variable | Purpose |
|----------|---------|
| `UGWC_ADMIN_LISTEN_ADDR` | Admin HTTP (metrics, health) |
| `UGWC_OTLP_ENDPOINT` | OTLP push (requires `--features otlp-metrics` build) |
| `UGWC_OTLP_PUSH_INTERVAL_MS` | OTLP flush interval (default 15000) |

## Shutdown timing

Code constants in `crates/ugwc/src/run.rs`: `SHUTDOWN_DRAIN_SECS=30`, `SHUTDOWN_JOIN_SECS=10`. Helm `terminationGracePeriodSeconds` should stay greater than their sum (default chart: 60).
