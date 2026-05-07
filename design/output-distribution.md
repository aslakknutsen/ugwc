# Output and distribution — multiple protocols, one IR

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Prerequisite:** [`controller-architecture.md`](controller-architecture.md)  
**Related:** [`component-design.md`](component-design.md), [`scale-performance.md`](scale-performance.md), [`security.md`](security.md)

---

## 1. Problem

Data planes differ by product and deployment: Envoy-style proxies want **xDS**, other stacks want **Kubernetes-native config**, **REST pull**, **event streams**, vendor CRDs, or generated files. Baking **one** wire format into the core couples the control plane to a single proxy family.

**Decision:** Treat **distribution** as a pluggable layer. The **internal representation (IR)** is the single compiled truth; **output backends** encode and deliver that truth to consumers using whatever protocol each consumer supports.

**xDS** is one supported backend — not the definition of the architecture.

---

## 2. Model

```
                    ┌─────────────────────────────┐
                    │   IR snapshot (canonical)   │
                    └──────────────┬──────────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
   ┌───────────────┐       ┌───────────────┐       ┌───────────────┐
   │ xDS backend   │       │ K8s API       │       │ … future …    │
   │ (gRPC streams)│       │ writer        │       │               │
   └───────┬───────┘       └───────┬───────┘       └───────┬───────┘
           │                       │                       │
           ▼                       ▼                       ▼
     Envoy / Agentgateway     In-cluster CRDs /        Operator-defined
     / proxyless gRPC         ConfigMaps / Secrets     sinks
```

**Terms:**

| Term | Meaning |
|------|---------|
| **IR snapshot** | Immutable, Kubernetes-agnostic resolved config for a bounded scope (e.g. one Gateway, one class, or global — see sharding) |
| **Output backend** | Component that implements **encode + distribute** for one protocol or sink |
| **Distribution** | Act of moving encoded config to consumers (push stream, pull fetch, watch, reconcile write) |

---

## 3. Selecting a backend: GatewayClass and parameters

**Primary selector:** `GatewayClass` (Gateway API’s extension point for implementation-specific behavior).

**Mechanisms (use one or combine):**

1. **Controller name** — `spec.controllerName` matches this controller; each class maps to a **profile** (built-in or configured).  
2. **ParametersRef** — class points to a vendor CRD or `ConfigMap` that names the **output profile** (backend id, options, endpoints).  
3. **Static controller config** — default profile when class does not specify parameters.

**Rules to document per release:**

- What happens if **parameters are invalid** — class `Accepted=False`, no publish for that class scope.  
- Whether **one Gateway** may only ever bind to **one** output profile for its data plane (recommended default: yes, avoids split-brain).

**Open point:** Whether `Gateway` annotations may **override** class-level profile for advanced multi-tenant setups — default should be **no** unless explicitly designed.

---

## 4. Output backend interface (conceptual)

Each backend implements roughly:

1. **Scope** — which slice of IR it owns (e.g. gateways referencing class `foo`).  
2. **Subscribe / lifecycle** — start/stop when classes appear or disappear.  
3. **Publish** — given new IR snapshot (and optional previous snapshot), **encode** and **deliver**.  
4. **Capabilities** — supports delta vs full replace, streaming vs batch, max object size.  
5. **Status contribution** — optional hooks so the reconciler can set Gateway/GatewayClass conditions (“programmed”, “distribution failed: …”).

Backends must **not** re-parse Kubernetes objects for routing logic; they only consume IR.

---

## 5. Example backends (non-exhaustive)

| Backend | Consumer | Distribution pattern |
|---------|----------|----------------------|
| **xDS (ADS / Delta)** | Envoy, Agentgateway, proxyless gRPC | Long-lived gRPC, incremental updates |
| **Kubernetes write** | Custom operator, sidecar injector | Apply ConfigMap/Secret/CRD; watch errors |
| **HTTP pull** | Agents without gRPC | Signed URL or internal API; clients poll with ETag / resource version |
| **File / object store** | GitOps, VM fleets | Write atomically (temp + rename); optional object versioning |
| **Message bus** | Regional fan-out | Publish snapshot ref or payload; ordering per key |

Not all backends belong in the **leader** process (see §7).

---

## 6. IR shape vs backend shape

**Anti-pattern:** Making IR literally “Envoy xDS structs” so every other backend is a second-class translation.

**Preferred:**

- IR stays **neutral** (routes, clusters, policies as domain concepts).  
- Each backend owns **IR → wire** mapping (may be lossy; document unsupported features per profile).  
- Optional **shared sub-IR** (e.g. normalized “endpoint set”) only if multiple backends truly reuse it without pulling in Envoy-specific fields.

If a backend needs Envoy-specific detail, that mapping lives **inside the xDS backend**, not in the core IR schema.

---

## 7. Process layout: in-process vs sidecar distributor

| Layout | When |
|--------|------|
| **In-process** | Low-latency streaming (xDS), shared memory for snapshots |
| **Separate Deployment** | Heavy SDKs, different SLO, different blast radius (e.g. large batch writers) |
| **Sidecar** | Legacy constraint; avoid unless required |

The **leader** must own **authoritative publish decisions** for a scope; separate distributors should receive **versioned IR blobs** or poll an internal API, not rebuild IR independently.

---

## 8. Consistency and ordering

- **Per-scope version** — monotonic counter or content hash on each IR snapshot; every backend publish tags the same version for correlation.  
- **Failure isolation** — failure in backend A must not prevent backend B if they serve disjoint scopes (optional hardening).  
- **All-or-nothing per scope (optional)** — product choice: either every backend for that scope succeeds or none apply (harder; needs two-phase semantics).

---

## 9. Gateway API status and “programmed”

Users care that **their** Gateway is live on **their** data plane. Conditions should reflect:

- IR compiled successfully for that Gateway’s scope  
- **At least one** required output backend acknowledged delivery (or passive backends wrote successfully)  

Exact condition names should align with Gateway API implementation conventions for the implementation name you register.

---

## 10. Testing

- **Golden IR → per-backend fixtures** — expected xDS resources, expected YAML files, etc.  
- **Contract tests** per backend for error handling (partial write, stream reset).  

---

## 11. Out of scope

- Defining a new universal wire format to replace all others — IR is internal; wire formats remain backend-specific.  
- EPP runtime behavior — unchanged; still configured through IR into whichever proxy the chosen backend programs.
