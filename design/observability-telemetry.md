# Observability and Telemetry — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Related:** [`component-design.md`](component-design.md), [`scale-performance.md`](scale-performance.md), [`output-distribution.md`](output-distribution.md)

---

## 1. Goals

1. **Operators** can detect reconcile backlog, IR build failures, and **distribution** health (per enabled backend)  
2. **Users** understand **whether config is accepted** via Gateway API **status** (not only metrics)  
3. **Performance regressions** are visible before clusters hit limits  

---

## 2. Metrics (recommended baseline)

| Area | Metric (conceptual) | Notes |
|------|---------------------|--------|
| Reconcile | duration histogram, success/error counters | Labels: trigger kind (low cardinality) |
| IR | compile duration, invalid-ref counter | Separate “semantic error” from panic-level failure |
| **Distribution** | publish duration / success per **backend id**; queue depth if applicable | Low-cardinality `backend` label |
| xDS (if enabled) | connected streams, disconnect reasons | Heartbeat or gRPC metrics |
| **Delta / incremental** | bytes or objects pushed per publish | Per backend; validates fan-out mitigation |
| Watch | reflector lag / resync count | From client-go/kube-rs equivalents |
| Leader | is_leader gauge | Avoid double-active ambiguity |

**Naming:** Prefer OpenTelemetry semantic conventions where applicable; prefix with project name for uniqueness.

---

## 3. Tracing

- Span **reconcile** → **compile IR** → **publish** (per backend) → **encode** where useful  
- Propagate **trace context** on outbound gRPC/HTTP **if** downstream supports it  

Keep cardinality low on span attributes (gateway namespace/name OK at debug sampling only).

---

## 4. Logging

- Structured logs (JSON) with **controller release**, **leader id**, **resource version** when handling errors  
- **Never** log Secret contents or full certs  

Levels:

- **Error:** publish failures, unrecoverable API errors  
- **Warn:** repeated semantic validation failures, subscription anomalies  
- **Info:** leader transitions, major config generation milestones (rate-limit per minute)  

---

## 5. User-facing observability (Kubernetes status)

Required surfaces:

- Gateway / Route **Accepted / Resolved** conditions  
- Policy **ancestor status** so attachment is discoverable without reading IR  

Define **stable condition reasons** (machine-parseable) in a small reference table — operators bake alerts on these.

---

## 6. Dashboards and alerts (operations)

Suggested alerts:

| Signal | Condition |
|--------|-----------|
| Reconcile P99 | Sustained elevation vs baseline |
| IR errors | Spike in invalid refs |
| **Distribution** failures | Spike in publish errors for any `backend` |
| xDS disconnects (if used) | Burst correlated with deploys vs pathology |
| Leader churn | Frequent elections |

---

## 7. Debugging tooling

- `/healthz` and `/readyz` separating API connectivity vs leader + reflector health  
- Optional **pprof** or diagnostic endpoint **disabled by default** in production  

---

## 8. EPP / Inference path

EPP is external; controller observability covers **configuring** ext_proc correctly:

- Metric: pools with **invalid EPP reference**  
- Status on parent objects pointing at **misconfigured extensionRef**  

Runtime inference latency belongs to **proxy + EPP** dashboards, not the controller alone.

---

## 9. Open items

- Trace sampling defaults for large clusters  
- Whether to expose Prometheus scrape vs OTLP-only  

Align with platform observability stack when known.
