# Security — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Related:** [`component-design.md`](component-design.md), [`output-distribution.md`](output-distribution.md)

---

## 1. Principles

1. **Least privilege** for ServiceAccount RBAC  
2. **No silent trust expansion** across namespaces (policy attachment rules exist for a reason)  
3. **Mutual authentication** for **distribution surfaces** where the threat model requires it (xDS, HTTP admin, …)  
4. **Minimal image** — distroless/scratch-style bases where feasible  

---

## 2. Kubernetes RBAC

**Baseline intent:**

| Capability | Scope |
|------------|--------|
| get/list/watch | Managed CRDs + core types needed for resolution (Services, EndpointSlices, Namespaces, …) |
| update/patch **status** | Managed CRDs only |
| leases | Leader election namespace |
| **No** blanket write on workloads unless strictly required |
| create/update/patch **non-status** | Only if an **output backend** applies config via API (e.g. ConfigMap); scope to object types and namespaces agreed for that profile |

Review against actual informer list **and** each enabled output backend — RBAC drift is a recurring audit failure.

---

## 3. Policy attachment and trust boundaries

**BackendTLSPolicy-style constraints** (from research): enforce **same-namespace** rules for trust-sensitive refs where the spec demands it.

**Inherited policies:** merging must not allow child-owned routes to escalate privileges beyond platform policy — treat overrides vs defaults consistently with GEP-713.

---

## 4. Distribution endpoints (all backends)

Every path that exposes or applies **compiled config** is in scope: gRPC xDS, HTTP pull APIs, webhook callbacks, writes to ConfigMaps/Secrets in other namespaces, message buses, etc.

**Risks (typical):**

- **Unauthorized fetch or pull** → traffic hijack, MITM, or stale config replay  
- **Unauthorized write** → arbitrary routing or credential injection in cluster  
- **DoS** via expensive handshakes, huge payloads, or apiserver apply storms  

**Mitigations (pick per backend and deployment):**

- mTLS or signed tokens between controller and consumers  
- NetworkPolicy / egress restrictions on listener ports  
- Rate limits on new streams, pull QPS, and **Kubernetes write** throughput  
- Separate credentials per GatewayClass / tenant where required  

Document **what ships default** vs **platform responsibility** for **each** enabled backend.

---

## 5. Webhooks

Conversion / validating webhooks:

- TLS for webhook server  
- Timeout and failure policy (`Fail` vs `Ignore`) explicit  
- No cluster-admin for webhook Deployment  

---

## 6. Secrets handling

- Prefer **reference-only** in IR (Secret names + namespace), not embedding secret bytes in snapshots unless unavoidable  
- If snapshots contain certs/keys, define **redaction** in logs and debug dumps  

---

## 7. Supply chain

- Pin crate versions; SBOM generation in CI  
- Static linking / minimal base images per deployment doc  

---

## 8. Open decisions

1. **Defaults per backend:** e.g. in-cluster xDS plaintext vs mTLS; whether HTTP pull is disabled unless explicitly configured  
2. **Multi-cluster:** whether IR ever aggregates remote clusters (huge blast radius)  

Record outcomes in this file when decided.
