# Component Design — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Prerequisite:** [`controller-architecture.md`](controller-architecture.md), [`output-distribution.md`](output-distribution.md)

---

## 1. Component boundary diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Controller process                       │
│  ┌──────────────┐   ┌────────────────┐   ┌────────────────┐ │
│  │ Watch mux    │──►│ Reconciler     │──►│ IR compiler    │ │
│  │ (reflectors) │   │ (idempotent)   │   │ (DAG + policies)│ │
│  └──────────────┘   └───────┬────────┘   └───────┬────────┘ │
│                             │                     │         │
│                             ▼                     ▼         │
│                     ┌───────────────┐     ┌───────────────┐ │
│                     │ Status writer │     │ Snapshot store │ │
│                     └───────────────┘     └───────┬───────┘ │
│                                                    │         │
│                                                    ▼         │
│                     ┌─────────────────────────────────────┐ │
│                     │ Output coordinator                  │ │
│                     │ (profile per GatewayClass)          │ │
│                     └──────────────┬──────────────────────┘ │
│          ┌─────────────────────────┼─────────────────────────┐
│          ▼                         ▼                         ▼
│   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
│   │ xDS backend  │          │ K8s writer   │          │ … plugins …  │
│   └──────────────┘          └──────────────┘          └──────────────┘
└─────────────────────────────────────────────────────────────┘
         ▲                              │
         │ Admission / conversion       │ per-backend protocols
         │ (optional separate Deployment)                     ▼
   Kubernetes API                      Data planes / operators / pull clients
```

---

## 2. Watch mux

**Responsibilities:**

- Configure **label/field selectors** and optional **metadata-only** watches per resource type  
- Merge streams into **reconcile triggers** (primary + mapped secondaries)  
- Apply **debouncing** / requeue policy for status storms  

**Non-goals:** Business logic; policy merge semantics.

---

## 3. Reconciler

**Responsibilities:**

- Single entry: given trigger `ObjectRef`, rebuild **full relevant slice** of state from stores (not incremental patch from event)  
- Invoke IR compiler with read-only view  
- Pass compiler output to snapshot store + status writer + **output coordinator**  

**GatewayClass resolution:** Reconciler (or a dedicated pass) resolves which **output profile** applies to each managed Gateway and ensures the coordinator registers/unregisters backends accordingly. Detail: [`output-distribution.md`](output-distribution.md).

**Interface sketch:**

- Input: trigger + store handles  
- Output: `Result<CompiledSnapshot, FatalError>` + **non-fatal** semantic errors → status  

---

## 4. IR compiler

**Submodules (logical):**

| Submodule | Input | Output |
|-----------|--------|--------|
| Graph builder | Gateways, listeners, attachment targets | Resolved attachment graph |
| Route resolver | HTTPRoutes, GRPCRoutes, … | Normalized route rules |
| Policy engine | Policy CRDs (GEP-713) | Merged effective policy per attachment point |
| Backend resolver | Services, EndpointSlices, InferencePool | Endpoint sets + pool metadata (neutral naming; **not** “EDS-only”) |
| Inference | InferencePool, InferenceModel, … | EPP refs, model weights, criticality tags |

**Policy precedence:** Implement GEP-713 override/default, tie-breakers (timestamp, name). Document any deliberate deviations.

---

## 5. Snapshot store

**Responsibilities:**

- Hold **current** and optionally **previous** snapshot for backends that implement **incremental** delivery (e.g. Delta xDS)  
- Thread-safe handoff to **output coordinator** and registered backends  
- Optional sequence numbers / content hashes for cross-backend correlation  

**Open point:** Whether snapshots are global one-shot or **sharded by GatewayClass / tenant** — affects memory, delta scope, and how many backends receive each publish.

---

## 6. Output coordinator and backends

**Coordinator responsibilities:**

- Resolve **profile** from `GatewayClass` (+ `parametersRef` / static config)  
- Maintain a **registry** of backends (built-in + optional dynamic plugins if ever)  
- On new IR snapshot: invoke **each** active backend for the relevant **scope** with read-only snapshot handles  
- Aggregate **publish errors** for status conditions  

**Backend responsibilities (per implementation):**

- **Encode** IR slice → wire format or API objects  
- **Distribute** (stream, apply, upload, enqueue)  
- Advertise **capabilities**: streaming, delta, idempotent replace, max payload  

**xDS backend (one implementation):**

- gRPC for LDS/RDS/CDS/EDS (ADS multiplexing as required)  
- **Delta** responses when supported by clients  
- Track per-stream interest (resource-level subscriptions in Delta mode)  
- **Dependencies:** `rust-control-plane` + `tonic` where applicable; validate crate versions during implementation  

**Other backends** may run out-of-process ([`output-distribution.md`](output-distribution.md) §7); the coordinator then uses a narrow API (versioned blob + metadata) instead of shared memory.

---

## 7. Status writer

**Responsibilities:**

- Patch **Gateway API** conditions on Gateways, Routes, etc.  
- **PolicyAncestorStatus** and related policy reporting for discoverability  
- Rate-limit status writes if needed (coordination with [`scale-performance.md`](scale-performance.md))  

---

## 8. Webhooks (optional components)

| Webhook | When separate | Notes |
|---------|----------------|-------|
| Conversion | Breaking CRD storage transitions | Leader coupling vs dedicated Deployment |
| Validating | Enterprise constraints beyond CRD schema | Fail-closed vs warn |

---

## 9. Configuration surface

Static config (flags / ConfigMap) should cover:

- Watched API groups/kinds  
- **Output profiles** — default backend id, allowed backends, class → profile overrides  
- **Per-backend** endpoints: xDS listen addr/TLS, HTTP pull signing keys, K8s write rate limits, …  
- Leader election identity  
- Feature gates (Inference Extension on/off, experimental routes)  

---

## 10. Testing hooks

- **Golden IR**: fixture YAML → expected IR snapshot  
- **Per-backend contract tests**: e.g. xDS resource names + hashes; K8s object golden files  
- **Policy merge vectors**: small CRD sets with conflicting attachments  

Cross-reference [`observability-telemetry.md`](observability-telemetry.md) for validation metrics.
