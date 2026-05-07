# Scale and Performance — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Related:** [`controller-architecture.md`](controller-architecture.md), [`component-design.md`](component-design.md), [`output-distribution.md`](output-distribution.md)

---

## 1. Threat model (performance)

Primary enemies:

1. **Fan-out:** N **consumers** × full config push on small changes  
2. **Reconcile storms:** Endpoint churn, status feedback loops  
3. **Memory:** Storing full specs for unbounded Pods when only labels/metadata matter  

---

## 2. Incremental distribution (not xDS–only)

**Principle:** Prefer **smallest necessary push** for each backend.

| Backend style | Incremental mechanism |
|---------------|-------------------------|
| **Delta xDS** | Protocol-level deltas vs prior snapshot; stable resource names |
| **Kubernetes apply** | Server-side apply / three-way merge; minimize object churn |
| **HTTP pull** | ETag or resource version; full body replace when cheap enough |
| **Blob / file** | Content-addressed artifacts; consumers pull only when hash changes |

**Requirement (where applicable):** Snapshot store (or backend-local cache) retains **previous** snapshot or hash so incremental backends avoid full fan-out on tiny changes.

**Verification:** Benchmark **per backend**: e.g. one route change with **P** xDS streams vs one ConfigMap write vs pull client poll interval.

---

## 3. kube-rs stream optimizations

Apply selectively per resource type:

| Technique | Use when |
|-----------|----------|
| Field selectors | Single-instance or scoped controllers |
| Label selectors | Multi-tenant or scoped GatewayClass |
| Metadata watchers | Large backend sets where spec is unnecessary |
| Debouncing / requeue | Rapid sequential updates (pod churn, status thrash) |

**Risk:** Over-aggressive selectors **hide** resources — document required labels for supported deployments.

---

## 4. Translation CPU

**Strategies:**

- **IR aligned to domain concepts** — backends own IR→wire mapping; avoid leaking one vendor’s structs into IR unless multiple backends truly share them  
- **Single compile per reconcile** — never per-connection recomputation of IR  
- **Incremental DAG (future):** only if profiling proves full rebuild is hot; complexity trade-off  

---

## 5. Backpressure and bounds

Define explicit limits:

| Queue | Behavior when full |
|-------|---------------------|
| Reconcile queue | Drop coalesce / requeue with jitter |
| Status write queue | Aggregate patches or shed lower-priority updates |
| **Distribution** outbound | Per-backend: stream buffers, apply queue depth, API QPS to apiserver |

Document chosen behavior — silent lag vs client disconnect / apply failure signals matter for **data plane** correctness.

---

## 6. Horizontal scaling reality

**Leader-elected control plane:** horizontal replica count **does not** multiply translation throughput; only HA.

If throughput exceeds single leader:

- Shard by **GatewayClass** / **namespace** / **cluster** (multiple controllers), **or**  
- Split **read-heavy** paths (e.g. dedicated endpoint publishers for one backend) — architectural change  

State assumptions in the index when product picks a lane.

---

## 7. Target sizing (fill during load test)

| Metric | Target | Measurement method |
|--------|--------|---------------------|
| Max Gateways | *TBD* | Soak test |
| Max Routes | *TBD* | Soak test |
| Max proxies | *TBD* | **Per output backend** (streams, watchers, pull clients) |
| P99 reconcile | *TBD* | Histogram |
| Memory ceiling | *TBD* | RSS under worst-case CRD count |

---

## 8. Rust-specific notes

- Avoid allocation spikes in hot translation loops (reuse buffers where safe)  
- Prefer structured clones only when IR versioning demands it  

Avoid quoting ultra-specific CPU/memory claims from research until reproduced on **this** codebase and workload.
