# TODO: Gateway API (extended) + Inference Extension parity

**Goal:** Track latest **Gateway API** (standard + extended channel semantics where relevant) and **Gateway API Inference Extension** so the controller observes, loads into `ClusterSnapshot`, compiles, publishes IR/xDS, and writes status for all resource kinds required for parity—not only `HTTPRoute` + `Service`.

**Baseline (current code):** Watches `Gateway`, `HTTPRoute`, `GRPCRoute` (parents may be `Gateway` or `ListenerSet`), `ListenerSet`, `ReferenceGrant`, `EndpointSlice` (debounced full-gateway fan-out), `GatewayClass` (owned classes). Snapshot loads merged listeners (`Gateway` + attached `ListenerSet` when `allowedListeners` permits), HTTP and GRPC routes attached to either parent kind, `Service` backends, `EndpointSlice`s, relevant `ReferenceGrant`s, `GatewayClass`. Policy APIs, non-Service backend kinds, and `InferencePool` are out of scope for this note.

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
- [ ] **`BackendTLSPolicy`** — watch; resolve targets that reference `Service` / `HTTPRoute` / backends used by owned Gateways; reconcile affected gateways.
- [x] **`ListenerSet`** — watch + merge into listener resolution (**Gateway API standard channel v1.5.x**; `Gateway.spec.allowedListeners` required for attachment).
- [ ] **`InferencePool`** (`inference.networking.k8s.io/v1`) — watch; map to Gateway via `HTTPRoute` / `backendRefs` and/or parent/status relationships per spec.
- [ ] **`InferenceObjective`** (if shipped as a CRD in the target version) — watch + tie to routes/pools per spec.

---

## 2. Cluster snapshot (`ClusterSnapshot` / loader)

Extend types and `load_cluster_snapshot` (or successor) to include:

- [x] **ListenerSet merge + effective listeners** on `ClusterSnapshot` (`listener_sets`, `effective_listeners`, `allowedListeners` snapshot fields).
- [x] **Routes:** `GRPCRoute` (and TLS/TCP/UDP as scoped).
- [ ] **Grants:** relevant `ReferenceGrant` documents for allowed cross-namespace reference pairs touching that Gateway’s scope (partial: HTTPRoute + GRPCRoute Service refs).
- [ ] **TLS:** resolved `BackendTLSPolicy` blobs (or normalized TLS IR inputs) for backends that require upstream TLS.
- [ ] **Inference:** `InferencePool` specs (selector, `targetPorts`, `appProtocol`, `endpointPickerRef`, failure modes) for pools referenced by routes; optional **InferenceObjective** if part of routing.

Ensure:

- [ ] **Backend refs:** support `kind` / `group` beyond default `Service` — at minimum **`InferencePool`** for inference parity.
- [ ] **Endpoint topology:** for `Service` backends, keep the `EndpointSlice` path; for `InferencePool`, define how endpoints are discovered (pods via selector + ports, coordination with EPP—align with inference extension docs).

---

## 3. Core compile / IR

- [x] **Normalize** GRPC (and other L4/L7) routes into IR alongside HTTP (core filters / exact matches; extended filters tracked separately).
- [ ] **Enforce ReferenceGrant** semantics in compile reports (conditions / errors when refs are illegal) (partial for Service backends from HTTPRoute/GRPCRoute).
- [ ] **Backend TLS:** map `BackendTLSPolicy` → IR/TLS fields used by publish backends.
- [ ] **Inference:** map `InferencePool` → IR (cluster/upstream + ext_proc/EPP wiring per `ugwc-gwxds` / design); handle `endpointPickerRef` and failure modes.
- [ ] **InferenceObjective:** if request-scoped objectives affect routing or filters, thread through IR and the data plane contract.

---

## 4. Status writers

- [x] Extend Gateway / route / parent status patches for **GRPCRoute** and other route kinds per Gateway API status patterns (GRPCRoute parent merge + supported kinds; other kinds still open).
- [ ] **InferencePool** `status.parents` / conditions (`Accepted`, `ResolvedRefs`, …) per inference extension spec.
- [ ] **BackendTLSPolicy** status if this controller owns or participates in those conditions.

---

## 5. xDS / gwxds output

- [x] Ensure published IR matches proto expectations in `gwxds.proto` (`Route.grpc_route`, `RouteMatch.grpc_*`, listener `allowed_routes`; `InferencePool`, `BackendTLS`, etc. still future work).
- [ ] Verify Envoy/Praxis dataplane contract for **ext_proc / EPP** when InferencePool is active.

---

## 6. Versioning and CRD matrix

- [ ] Pin supported **Gateway API** bundle version(s) (e.g. v1.3.x) and document **standard vs extended** channel assumptions.
- [ ] Pin **Inference Extension** API version (`inference.networking.k8s.io/v1`) and CRD install bundle.
- [ ] Add a compatibility table: ugwc version ↔ Gateway API version ↔ Inference Extension version.

---

## 7. Testing and conformance

- [ ] Fixture tests for new snapshot + compile inputs (pure `ugwc-core`).
- [ ] Integration tests with envtest/kind: ReferenceGrant cross-namespace, GRPCRoute attach, BackendTLSPolicy, InferencePool backend ref.
- [ ] Optional: upstream **Gateway API conformance** and **inference extension** conformance subsets when implementations exist.

---

## 8. Documentation

- [ ] Update `docs/architecture.md` watcher diagram and `design/` controller docs to list the full resource set.
- [ ] Operator-facing doc: required CRDs, install order, and flags.

---

## Dependencies / ordering (suggested)

1. **ReferenceGrant** + cross-namespace ref resolution (blocks correct behavior on shared clusters).
2. **GRPCRoute** + snapshot + compile (large surface; can parallelize with grants).
3. **BackendTLSPolicy** (depends on stable backend resolution).
4. **InferencePool** watch + loader + compile + xDS path (depends on non-Service `backendRefs`).
5. **InferenceObjective** (if applicable) after pools are end-to-end.
6. ~~**ListenerSet**~~ — implemented against Gateway API **standard v1.5.x** (`allowedListeners`, merge order, status).

---

## Open questions (resolve before calling parity “complete”)

1. Exact **Gateway API release** to standardize on (CRD set differs by minor version). **Partial:** `ListenerSet` support assumes **standard channel v1.5.x** CRDs.
2. Whether **InferenceObjective** is a CRD in that release or documented-only.
3. **Shard model:** single reconciler listing all gateways vs namespace/class sharding for watch fan-out at scale.
