# TODO: Gateway API (extended) + Inference Extension parity

**Goal:** Track latest **Gateway API** (standard + extended channel semantics where relevant) and **Gateway API Inference Extension** so the controller observes, loads into `ClusterSnapshot`, compiles, publishes IR/xDS, and writes status for all resource kinds required for parity—not only `HTTPRoute` + `Service`.

**Baseline (current code):** Watches `Gateway`, `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `ListenerSet`, `ReferenceGrant`, `BackendTLSPolicy`, `ConfigMap` (when referenced as BackendTLS CA bundles), `EndpointSlice` (debounced full-gateway fan-out), `GatewayClass` (owned classes), `InferencePool` (routes → gateways). Snapshot loads merged listeners (`Gateway` + attached `ListenerSet` when `allowedListeners` permits), HTTP / GRPC / TLS routes attached to either parent kind, `Service` + `InferencePool` backends (pool spec + EPP `Service`), resolved upstream TLS rows from `BackendTLSPolicy`, `EndpointSlice`s, relevant `ReferenceGrant`s, `GatewayClass`. `InferenceObjective` and broader policy APIs remain tracked below but are not implemented.

---

## 1. Watch graph and reconcile keys

For each new resource type, define:

- [ ] **Watch** (cluster-wide vs namespaced) and **mapper → `(gateway_ns, gateway_name)`** (or shard key if multi-gateway scopes are introduced).
- [ ] **Ordering:** grant/route/policy changes must enqueue the same gateways as listener/GW changes.
- [ ] **Debouncing:** extend or replace EndpointSlice-only debounce where global churn is unacceptable.

Concrete additions:

- [ ] **`ReferenceGrant`** (`gateway.networking.k8s.io`) — reconcile when cross-namespace refs depend on grants; map affected namespaces/routes/gateways (may require an index of refs → gateways).
- [x] **`GRPCRoute`** — parent refs to `Gateway`; same attachment rules as `HTTPRoute`; reconcile key from parent Gateway.
- [ ] **`TLSRoute`** / **`TCPRoute`** / **`UDPRoute`** (as applicable to supported protocols) — watch + map + snapshot fields.
- [x] **`BackendTLSPolicy`** (`gateway.networking.k8s.io/v1`) — cluster-wide watch + namespaced `ConfigMap` watch when policies reference CA `ConfigMap`s; fan-out to gateways whose attached routes use a targeted `Service` backend. **Scope:** `Service` targets only (extended core); not arbitrary backend kinds.
- [x] **`ListenerSet`** — watch + merge into listener resolution (**Gateway API standard channel v1.5.x**; `Gateway.spec.allowedListeners` required for attachment).
- [x] **`InferencePool`** (`inference.networking.k8s.io/v1`) — watch; map to Gateway via routes referencing the pool in `backendRefs`.
- [ ] **`InferenceObjective`** (if shipped as a CRD in the target version) — watch + tie to routes/pools per spec.

---

## 2. Cluster snapshot (`ClusterSnapshot` / loader)

Extend types and `load_cluster_snapshot` (or successor) to include:

- [x] **ListenerSet merge + effective listeners** on `ClusterSnapshot` (`listener_sets`, `effective_listeners`, `allowedListeners` snapshot fields).
- [x] **Routes:** `GRPCRoute` (and TLS/TCP/UDP as scoped).
- [ ] **Grants:** relevant `ReferenceGrant` documents for allowed cross-namespace reference pairs touching that Gateway’s scope (partial: HTTPRoute + GRPCRoute Service refs).
- [x] **TLS:** resolved `BackendTLSPolicy` → `ClusterSnapshot.backend_tls_effective` (`ns/name:port` → `BackendTlsIr`) plus `backend_tls_policy_outcomes` for status.
- [x] **Inference:** `InferencePool` specs (selector, `targetPorts`, `appProtocol`, `endpointPickerRef`, failure modes) for pools referenced by routes; optional **InferenceObjective** if part of routing (still open).

Ensure:

- [x] **Backend refs:** **`InferencePool`** in addition to `Service` (HTTPRoute / GRPCRoute / TLSRoute).
- [x] **Endpoint topology:** `Service` backends use `EndpointSlice`; `InferencePool` backends omit slice-derived endpoints here (EPP selects endpoints per inference extension).

---

## 3. Core compile / IR

- [x] **Normalize** GRPC (and other L4/L7) routes into IR alongside HTTP (core filters / exact matches; extended filters tracked separately).
- [ ] **Enforce ReferenceGrant** semantics in compile reports (conditions / errors when refs are illegal) (partial for Service backends from HTTPRoute/GRPCRoute).
- [x] **Backend TLS:** map winning policy per backend → `BackendIr.backend_tls`; invalid CA → compile error / route `ResolvedRefs` (`InvalidCACertificateRef`).
- [x] **Inference:** map `InferencePool` → IR + `gwxds` `Backend.inference_pool` (`endpointPickerRef`, failure modes); ReferenceGrant for cross-namespace pool refs.
- [ ] **InferenceObjective:** if request-scoped objectives affect routing or filters, thread through IR and the data plane contract.

---

## 4. Status writers

- [x] Extend Gateway / route / parent status patches for **GRPCRoute** and other route kinds per Gateway API status patterns (GRPCRoute parent merge + supported kinds; other kinds still open).
- [x] **InferencePool** `status.parents` / conditions (`Accepted`, `ResolvedRefs`) per reconciled Gateway + controller identity.
- [x] **BackendTLSPolicy** — `status.ancestors` per reconciled Gateway + controller identity (`Accepted`, `ResolvedRefs`, `Conflicted` when duplicate coverage loses precedence).

---

## 5. xDS / gwxds output

- [x] Ensure published IR matches proto expectations in `gwxds.proto` (`Route.grpc_route`, `RouteMatch.grpc_*`, listener `allowed_routes`; `Backend.tls` / `BackendTLS` from `BackendIr.backend_tls`; `Backend.inference_pool` when IR carries inference backends).
- [x] Verify Envoy/Praxis dataplane contract for **ext_proc / EPP** when InferencePool is active — top-level `Backend.host`/`port`/`dial_port` mirror resolved EPP Service DNS + port (`encode_ir`).

---

## 6. Versioning and CRD matrix

- [ ] Pin supported **Gateway API** bundle version(s) (e.g. v1.3.x) and document **standard vs extended** channel assumptions.
- [ ] Pin **Inference Extension** API version (`inference.networking.k8s.io/v1`) and CRD install bundle.
  - Install the upstream Inference Extension CRDs before using `InferencePool` objects and HTTPRoute `backendRefs` to them; see [Inference Extension guides](https://gateway-api-inference-extension.sigs.k8s.io/guides/).
- [ ] Add a compatibility table: ugwc version ↔ Gateway API version ↔ Inference Extension version.

---

## 7. Testing and conformance

- [ ] Fixture tests for new snapshot + compile inputs (pure `ugwc-core`). **Partial:** BackendTLS via `backend_tls_effective` → `BackendIr` / compile errors (`from_cluster`); loader section/precedence helpers (`ugwc-k8s`).
- [ ] Integration tests with envtest/kind: ReferenceGrant cross-namespace, GRPCRoute attach, BackendTLSPolicy, InferencePool backend ref. **Partial:** BackendTLSPolicy covered in unit tests only so far.
- [ ] Optional: upstream **Gateway API conformance** and **inference extension** conformance subsets when implementations exist.

---

## 8. Documentation

- [ ] Update `docs/architecture.md` watcher diagram and `design/` controller docs to list the full resource set. **Partial:** `docs/architecture.md` documents BackendTLSPolicy / CA `ConfigMap` watches and data path.
- [ ] Operator-facing doc: required CRDs, install order, and flags.

---

## Dependencies / ordering (suggested)

1. **ReferenceGrant** + cross-namespace ref resolution (blocks correct behavior on shared clusters).
2. **GRPCRoute** + snapshot + compile (large surface; can parallelize with grants).
3. ~~**BackendTLSPolicy**~~ — implemented (`Service` targets, snapshot + IR + gwxds + status + watches); envtest/kind still optional (§7).
4. ~~**InferencePool**~~ — watch + loader + compile + xDS + pool status (depends on non-Service `backendRefs`).
5. **InferenceObjective** (if applicable) after pools are end-to-end.
6. ~~**ListenerSet**~~ — implemented against Gateway API **standard v1.5.x** (`allowedListeners`, merge order, status).

---

## Open questions (resolve before calling parity “complete”)

1. Exact **Gateway API release** to standardize on (CRD set differs by minor version). **Partial:** `ListenerSet` support assumes **standard channel v1.5.x** CRDs.
2. Whether **InferenceObjective** is a CRD in that release or documented-only.
3. **Shard model:** single reconciler listing all gateways vs namespace/class sharding for watch fan-out at scale.
