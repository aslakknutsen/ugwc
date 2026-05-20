# ugwc operations

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `UGWC_ADMIN_LISTEN_ADDR` | `127.0.0.1:8080` | Admin HTTP (`/healthz`, `/readyz`, `/metrics`, `/log-level`) |
| `UGWC_LOG_JSON` | `false` (Helm: `true`) | Structured JSON logs to stdout |
| `RUST_LOG` | `info,kube=warn` | Tracing filter (overrides built-in default when set) |
| `UGWC_OTLP_ENDPOINT` | — | OTLP collector URL (requires `otlp-metrics` / `otlp-traces` build features) |
| `UGWC_OTLP_PUSH_INTERVAL_MS` | `15000` | OTLP metrics push interval |
| `UGWC_RUN_PROFILE` | `dev` | `prod` rejects plaintext xDS |
| `UGWC_KUBE_ENABLE` | `false` | Enable Kubernetes watches |
| `UGWC_LEADER_ELECTION` | `false` | Lease-based HA |

See [observability.md](observability.md) for metrics and scrape configuration.

## Runtime log level

Without restart:

```bash
curl -X PUT http://127.0.0.1:8080/log-level -d 'info,kube=warn,ugwc=debug'
```

Returns `204` on success, `400` on invalid directive.

## Kubernetes Events (reason codes)

| Reason | Type | When |
|--------|------|------|
| `CompileBlocked` | Warning | Compile report has blocking errors |
| `ProvisionFailed` | Warning | Per-gateway Praxis provision failed |
| `TlsMaterialMissing` | Warning | Backend TLS material could not be resolved |
| `DataplaneMisconfigured` | Warning | Dataplane enabled but profile/bootstrap incomplete |

## Correlation (logs ↔ metrics ↔ traces)

- Reconcile logs: span `reconcile_gateway` with `k8s.resource.namespace`, `k8s.resource.name`, `k8s.resource.generation`.
- Metrics: `ugwc_reconcile_duration_seconds`, `ugwc_reconcile_runs_total` labeled by `controller_name`.
- When OTLP traces are enabled (`--features otlp-traces`), JSON logs include trace/span IDs. Spawned tasks inherit the parent span, and outbound kube API requests carry a W3C `traceparent` header for end-to-end distributed tracing. Exemplars on reconcile histograms are a known limitation (see [observability.md](observability.md#known-limitations)).
