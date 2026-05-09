# Gateway API + Inference Extension parity tracker

**Goal:** Stay aligned with **Gateway API** (standard + selected experimental CRDs) and **Gateway API Inference Extension** end-to-end: watch → `ClusterSnapshot` → compile → IR/xDS → status.

**How to use:** Each `- [ ]` under **Remaining work** is sized for a **single** follow-up issue or PR. Check it off here when that issue closes. Broader themes are grouped; avoid folding unrelated work into one checkbox.

---

## Baseline — what the code does today

Primary crates: `crates/ugwc-k8s` (watches, loader, status), `crates/ugwc-core` (snapshot, compile, IR), `crates/ugwc-gwxds` (proto / encode).

| Layer | Implemented |
| ----- | ----------- |
| **Watches** | `Gateway`, `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `TCPRoute`, `UDPRoute`, `ListenerSet`, `ReferenceGrant`, `BackendTLSPolicy`, `ConfigMap` (BackendTLS CA refs), `EndpointSlice`, `Secret`, `GatewayClass`, `InferencePool` |
| **Snapshot** | Merged listeners (`Gateway` + `ListenerSet` when `spec.allowedListeners` permits); HTTP / GRPC / TLS / TCP / UDP routes; `Service` + `InferencePool` backends; `EndpointSlice` for Services; cluster `ReferenceGrant` list; `BackendTLSPolicy` → `backend_tls_effective` (+ outcomes for status) |
| **Compile / IR** | HTTP + GRPC + TLS + TCP + UDP routes into IR; ReferenceGrant enforcement on cross-namespace parents/backends/secrets; Backend TLS on **Service** backends; InferencePool → IR |
| **Status** | Gateway/listeners; HTTPRoute / GRPCRoute / TLSRoute / TCPRoute / UDPRoute parents; ListenerSet; BackendTLSPolicy ancestors; InferencePool parents |
| **xDS** | `gwxds.proto`: GRPC routes, TLS/TCP/UDP route bits, listener `protocol` / `allowed_routes`, `Backend.tls` / `Backend.inference_pool`, EPP Service host/port |

**Explicitly not implemented:** `InferenceObjective` (and related `inference.networking.x-k8s.io` CRDs), BackendTLS targets beyond **Service**, arbitrary HTTP/GRPC extended filters (many surfaces still return `UnsupportedValue` / reject).

**`UDPRoute`:** experimental Gateway API CRD `gateway.networking.k8s.io/v1alpha2` — same bundle requirement as TCPRoute; TCPRoute-shaped pipeline (snapshot → compile → status → `gwxds` `Protocol::UDP` / `Route.udp_route`). First rule only; UDP-vs-UDP precedence mirrors TCP; UDP+L7 same-port conflicts merged like TCP+L7.

**TCPRoute:** experimental Gateway API CRD `gateway.networking.k8s.io/v1alpha2` — cluster must install the experimental bundle. First attached rule only (same as TLS path today); TCP-vs-TCP precedence on one listener uses older `creationTimestamp` then `(namespace,name)`. InferencePool backends follow TLS-style resolution where wired.

ListenerSet + GRPCRoute + TLSRoute assume Gateway API CRDs consistent with **standard channel v1.5.x** where noted in upstream (e.g. ListenerSet on standard).

---

## Implemented (parity doc only — no new issues needed)

Use this as the “done” ledger; keep **Remaining work** for gaps only.

### Watch graph

- [x] **`ReferenceGrant`** — cluster-wide watch; enqueue affected gateways (grant namespace + route namespaces from `spec.from`; fallback when `from` empty).
- [x] **`GRPCRoute`** — watch; map to parent `Gateway` / attached `ListenerSet`; reconcile key from parent.
- [x] **`TLSRoute`** — watch; map + snapshot + compile + status (passthrough / SNI routing per implementation).
- [x] **`TCPRoute`** (`gateway.networking.k8s.io/v1alpha2`) — watch; map + snapshot + compile + status + gwxds (`Protocol::TCP`, `Route.tcp_route`); requires experimental Gateway API CRDs.
- [x] **`UDPRoute`** (`gateway.networking.k8s.io/v1alpha2`) — watch; map + snapshot + compile + status + gwxds (`Protocol::UDP`, `Route.udp_route`); requires experimental Gateway API CRDs.
- [x] **`BackendTLSPolicy`** — cluster-wide watch; `ConfigMap` watch for CA bundles; fan-out to gateways using targeted **Service** backends only.
- [x] **`ListenerSet`** — watch; merge when `Gateway.spec.allowedListeners` permits (**standard v1.5.x** shape).
- [x] **`InferencePool`** (`inference.networking.k8s.io/v1`) — watch; fan-out via routes with pool `backendRefs`.

### Snapshot & topology

- [x] ListenerSet merge fields (`listener_sets`, `effective_listeners`, allowedListeners snapshot).
- [x] GRPC + TLS + TCP + UDP routes in snapshot alongside HTTP.
- [x] ReferenceGrant list + enforcement surfaces (parents, Service/InferencePool backends, listener TLS secrets; narrowed `to.group` rules as coded).
- [x] `backend_tls_effective` + policy outcomes for status writers.
- [x] InferencePool spec snapshot for referenced pools.
- [x] Service backends → EndpointSlice; InferencePool backends → no slice-derived endpoints (EPP).

### Compile / IR / publish

- [x] GRPC + TLS + TCP + UDP + HTTP normalization into IR (core match/filter paths; extended behavior varies).
- [x] ReferenceGrant → compile report + route parent conditions (`RefNotPermitted`, etc.).
- [x] Backend TLS → `BackendIr.backend_tls` / invalid CA handling.
- [x] InferencePool → IR + `Backend.inference_pool` + cross-namespace grant checks.

### Status & xDS

- [x] GRPCRoute / TLSRoute / TCPRoute / UDPRoute parent status merge patterns (with Gateway + ListenerSet parents).
- [x] InferencePool `status.parents` (`Accepted`, `ResolvedRefs`, …).
- [x] BackendTLSPolicy `status.ancestors` (`Accepted`, `ResolvedRefs`, `Conflicted`).
- [x] gwxds encode for GRPC/TLS/TCP/UDP/listener allowed routes/backend TLS/inference backends.

---

## Remaining work (one checkbox ≈ one issue)

### Inference extension (beyond InferencePool)

- [ ] **`InferenceObjective`** — watch (`inference.networking.x-k8s.io` / version pinned with matrix) + snapshot if needed + IR or header contract documentation for dataplane (`x-gateway-inference-objective`).
- [ ] **InferencePool `Exported` / multi-cluster** — decide in/out; if in, reconcile status + any IR fields.

### Backend TLS expansion

- [ ] **`BackendTLSPolicy` non-Service targets** — e.g. other backend kinds Gateway API allows; snapshot resolution + compile + status fan-out rules.

### HTTP / GRPC API completeness

- [ ] **Audit `UnsupportedValue` / unsupported filter paths** — produce a matrix doc (Gateway API field → ugwc support level) or shrink gap intentionally.
- [ ] **TLS listener modes** — close gaps called out in compile path (e.g. TLS terminate + TLSRoute interactions if still partial).

### Controller hygiene (verify or improve)

- [ ] **Document or enforce reconcile ordering** — grant/route/policy events enqueue the same gateway keys as Gateway/ListenerSet changes (audit `controller.rs` + document).
- [ ] **EndpointSlice debouncing** — confirm sufficiency under slice churn; optional smarter debounce / sharding (`design/TODO-channel-experimental-handling.md`).

### Versioning & operator docs

- [ ] **Pin Gateway API bundle version(s)** in repo docs (standard vs experimental CRDs explicitly listed).
- [ ] **Pin Inference Extension release** (`manifests.yaml` URL or version) for `InferencePool` v1.
- [ ] **Compatibility table** — ugwc release ↔ Gateway API bundle ↔ Inference Extension ↔ optional Envoy/Praxis version.

### Testing

- [ ] **`ugwc-core` fixtures** — snapshot + compile cases for GRPCRoute, TLSRoute, ListenerSet merge, InferencePool backend, BackendTLS precedence (extend beyond partial BackendTLS coverage); **UDPRoute** has baseline TCP-mirrored unit tests — extend as needed.
- [ ] **Integration: envtest/kind** — ReferenceGrant cross-namespace parent; GRPCRoute attach; BackendTLSPolicy; InferencePool backendRef (unignore or replace `reference_grant_kube.rs` stub).
- [ ] **Optional:** run or document subset of upstream Gateway API / inference conformance when a suitable dataplane is pinned.

### Documentation

- [ ] **`docs/architecture.md`** — watcher diagram + full resource list matching `controller.rs` module comment.
- [ ] **Operator guide** — CRD install order, required feature gates (if any), flags, minimal GatewayClass + Inference CRD prerequisites.

---

## Open questions

1. **Gateway API minor pin** — Single supported bundle vs “best effort” on older CRDs; ListenerSet already implies **≥ v1.5 standard** for that feature.
2. **InferenceObjective** — Product requirement vs optional; API lives under **`inference.networking.x-k8s.io`** (alpha) separate from `InferencePool` v1.
3. **Shard model** — Single reconciler listing all gateways vs namespace/class sharding for watch fan-out at scale.
4. **UDPRoute / UDP listeners** — ugwc treats UDP+L7 same-port like TCP+L7 (listeners conflicted); reconfirm against upstream Gateway distinctness if the spec evolves.

---

## Suggested sequencing (remaining only)

1. Version pin + compatibility table (unblocks support expectations).
2. InferenceObjective after pools are validated in integration tests.
3. BackendTLS non-Service targets if customer backends require it.
