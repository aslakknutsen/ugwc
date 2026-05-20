# Security — Gateway API+ Control Plane

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Related:** [`component-design.md`](component-design.md), [`output-distribution.md`](output-distribution.md)

---

## 1. Principles

1. **Least privilege** for ServiceAccount RBAC  
2. **No silent trust expansion** across namespaces (policy attachment rules exist for a reason)  
3. **Mutual authentication** for **distribution surfaces** where the threat model requires it (xDS, HTTP admin, …)  
4. **Minimal image** — distroless/scratch-style bases where feasible  

### 1.1 Threat model (v0 — Phase 9 track)

| Actor | Asset | Primary risk | Mitigation direction |
|-------|--------|--------------|----------------------|
| Cluster insider / compromised workload | xDS stream to Envoy | Push forged LDS/RDS/CDS | Identity at transport layer + narrow RBAC for dataplane clients (see §2) |
| Cluster/network attacker | gRPC xDS on the data plane network | MITM, replays | **TLS/mTLS** on xDS in non-`dev` profiles; `run_profile=prod` rejects plaintext (see implementation) |
| Operator misconfiguration | Management plane process | Privileged credentials, leaked secrets | Redact secrets in logs; **dev** vs **prod** config profile |

Plaintext xDS remains **prototype-only** and must not be the implicit production default.

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

### 2.1 Secret watch justification

The controller watches `core/v1 Secret` resources (cluster-wide `get/list/watch`) because:
- HTTPS listeners require resolved PEM for xDS push to the dataplane
- BackendTLSPolicy client certificates similarly need PEM at compile time

This deviates from the zero-secret RBAC model described in external standards.
Mitigations:
- PEM private keys use `Zeroizing<Vec<u8>>` (zeroed on drop)
- Log redaction via `redact_field` catches PEM in tracing output
- IR serialization skips key fields (`#[serde(skip)]`)

If a deployment does not use HTTPS listeners or BackendTLSPolicy client certs,
the `secrets` rule can be removed from the ClusterRole manually.

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

### 4.1 Admin HTTP endpoint

The admin port (`/healthz`, `/readyz`, `/metrics`) binds to `0.0.0.0` by default.
This is required for kubelet liveness/readiness probes (which connect from the node,
not from within the pod).

**Platform responsibility:** deploy a `NetworkPolicy` that restricts ingress to the
admin port to the cluster monitoring stack and kubelet CIDR only. The chart does not
ship a default NetworkPolicy because node CIDR ranges are cluster-specific.

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
