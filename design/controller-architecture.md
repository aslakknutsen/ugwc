# Controller Architecture — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Background:** [`research.md`](research.md)  
**Distribution detail:** [`output-distribution.md`](output-distribution.md)  
**Dataplane lifecycle & xDS scope:** [`dataplane-provisioning.md`](dataplane-provisioning.md)

---

## 1. Problem statement

The control plane must merge **many Kubernetes objects** (Gateway API core, policies, Services/Endpoints, Inference Extension types) into **one deterministic configuration** for **many remote data planes**, without:

1. Propagating partially resolved state after crashes or races  
2. Recomputing full relational graphs **per connected proxy**  
3. Becoming the scalability bottleneck (“fan-out”) as routes and **consumers** of config grow  

---

## 1a. Multiple outputs (GatewayClass–driven)

**Decision:** The compiled **IR** is canonical; **how** it reaches a data plane is chosen per **GatewayClass** (and optional `parametersRef` / controller config). Different classes may target different proxy families: one class may use **xDS streaming**, another **Kubernetes API writes**, another **pull-based HTTP**, and so on.

**Implication:** “Controller” in the logical sense includes a **distribution layer** of one or more **output backends**, each implementing encode + deliver for its protocol. See [`output-distribution.md`](output-distribution.md) for selection rules, backend interface, and failure isolation.

**Non-goal:** Forcing every proxy to speak xDS. xDS remains a **first-class** backend where Envoy-compatible or proxyless gRPC clients are in use.

---

## 2. Core pattern: Resolved Bare API (internal representation)

**Decision:** Introduce a strict **internal representation (IR)** — Kubernetes-agnostic routing/policy snapshot — between API-server churn and **any** output backend that distributes config to data planes.

**Properties:**

| Property | Requirement |
|----------|-------------|
| Deterministic | Same cluster inputs → same IR (modulo explicit nondeterminism called out) |
| Complete or rejected | Broken refs produce **status conditions**, not silent drops |
| Immutable snapshots | Translation publishes whole snapshots; backends that support **incremental** delivery compute deltas vs prior locally |

**Translation order (dependency-safe):** namespace/gateway identity → listeners → routes → policies → backend addresses (aligned with research; refine if Gateway API+ adds stronger ordering constraints).

---

## 3. Data flow

```
API Server (multi-CRD watches)
        │
        ▼
 Reflector stores (kube-rs)
        │
        ▼
 Idempotent reconcile: read stores → build DAG → IR snapshot
        │
        ├──► Status patches (Gateway, Route, Policy, …)
        │
        ├──► Optional dataplane provisioner (profile-gated): Deployment/Service
        │    per Gateway or external/shared strategies — see dataplane-provisioning.md
        │
        ▼
 Output coordinator: route snapshot to backend(s) per GatewayClass / profile
        │
        ├──► xDS (streams, scoped or global node id) ──► Envoy / Praxis / proxyless gRPC …
        ├──► K8s writer      ──► ConfigMaps / CRDs / …
        └──► … other backends (pull API, object store, bus, …)
```

**Mapper rule:** Secondary resources (e.g. HTTPRoute) enqueue **parent** `ObjectRef`s so reconcile stays idempotent — re-read full relevant state each pass.

---

## 4. High availability

**Decision (default):** **Active–standby** via Kubernetes **Lease** leadership.

- **Leader:** runs watchers, translation loop, and **active distribution** (streaming servers, in-process publishers, or handoff to sidecars — product-defined)  
- **Standby:** watches lease only; on takeover, rebuild IR from API state; **streaming** clients reconnect or fail over per backend design  

**Graceful shutdown:** Stop scheduling work, flush safe points, drain **open distribution sessions** (e.g. gRPC xDS streams) before exit (SIGTERM path).

**Open point:** Whether data planes must support dual endpoints or DNS failover during leader change — **per backend** (xDS vs pull vs K8s watch).

---

## 5. Multi-version APIs

**Decision:** Versioned Rust modules → **canonical IR** via trait mapping at reflector boundary; merged CRDs at install time.

**Escalation path:** Conversion webhook for incompatible schema moves (HTTP endpoint on leader only, or separate webhook Deployment — decide to avoid coupling webhook HA with controller HA).

---

## 6. Inference Extension (Gateway API+)

**InferencePool** (`inference.networking.k8s.io/v1`) is the supported extension resource end-to-end in ugwc today: pod **`selector`** (same namespace), **`targetPorts`**, **`appProtocol`** (`http` vs `kubernetes.io/h2c`), and **`endpointPickerRef`** (typically a **`Service`** + port for the Endpoint Picker / EPP). Routes reference pools via **`backendRefs`**; IR and [`gwxds`](../crates/ugwc-gwxds/proto/gwxds.proto) carry pool identity, EPP dial parameters, and backend TLS material when policies apply.

The **Endpoint Picker (EPP)** remains an out-of-band runtime service; ugwc configures how the dataplane reaches it (`failureMode` **FailOpen** / **FailClose** is part of pool spec), not EPP’s internal scheduling logic.

**InferenceObjective** (`inference.networking.x-k8s.io`, alpha) and request header **`x-gateway-inference-objective`** are **not** implemented in ugwc yet — see [`todo-gateway-api-inference-parity.md`](todo-gateway-api-inference-parity.md) (*Remaining work*).

---

## 7. Failure modes (minimum expectations)

| Scenario | Expected behavior |
|----------|-------------------|
| Missing parent/ref | Condition on affected objects; omit or freeze affected listeners/routes per explicit policy |
| Leader loss | Standby becomes leader; **consumers** reconnect or resync per backend semantics |
| Partial translation bug | Prefer **no publish** over corrupt snapshot if detection exists; define invariant checks |
| API server backlog | Bounded queues + backpressure (see [`scale-performance.md`](scale-performance.md)) |

---

## 8. Out of scope (for this doc)

- Backend catalog and GatewayClass mapping detail — [`output-distribution.md`](output-distribution.md)  
- Per-component APIs — [`component-design.md`](component-design.md)  
- Numeric SLOs and load tests — [`scale-performance.md`](scale-performance.md)
