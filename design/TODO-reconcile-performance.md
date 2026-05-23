# TODO: Reconcile performance — store-backed concurrent reconciliation

**Purpose:** Track phased work to replace the serial, API-server-backed reconcile loop with a store-backed, concurrent, deduplicating architecture.  
**Related:** [`scale-performance.md`](scale-performance.md), [`controller-architecture.md`](controller-architecture.md)  
**Operator docs:** [Reconcile pipeline](../docs/architecture.md#reconcile-pipeline)  
**Target:** 1000+ Gateways, 10000+ Routes, multi-tenant shared control plane.

---

## Problem statement

The current reconcile path has five compounding bottlenecks:

1. **Serial reconcile loop.** `controller.rs` processes one gateway at a time (`recv → await reconcile → loop`). Latency grows linearly: gateway N waits for 0..N-1 to finish.

2. **No informer store.** `load_cluster_snapshot` issues ~12+ live API server calls per reconcile (cluster-wide list of HTTPRoutes, EndpointSlices, ReferenceGrants, etc.). At 1000 gateways that's 12,000+ API round trips per pass — most returning 10,000+ objects that get filtered down to a handful.

3. **No queue deduplication.** The `mpsc::channel(256)` has no set semantics. If a gateway key is enqueued 5 times before it's dequeued, it reconciles 5 times with identical results.

4. **EndpointSlice fan-out.** Any EndpointSlice event calls `gateway_keys_all` (a live `Gateway::list`) and enqueues every gateway. Combined with serial processing, one endpoint change blocks the queue for the full pass duration.

5. **Sequential status writes.** Each reconcile patches gateway status, then iterates every attached route writing individual status patches — all awaited one at a time.

Measured on 50 namespaces / 50 gateways / 100 routes:

| Kind | Avg latency |
|------|-------------|
| Gateway | 22.3s |
| HTTPRoute | 10.7s |

These numbers scale roughly linearly with gateway count.

---

## Phase 1 — Reflector stores (eliminate API server reads during reconcile) ✅

**Goal:** Per-reconcile `load_cluster_snapshot` reads from local memory instead of hitting the API server.

### What changes

- **New module `crates/ugwc-k8s/src/stores.rs`** — define `SharedStores` struct holding a `kube::runtime::reflector::Store<K>` for each watched resource type (Gateway, GatewayClass, HTTPRoute, GRPCRoute, TLSRoute, TCPRoute, UDPRoute, Service, EndpointSlice, Secret, ConfigMap, ReferenceGrant, ListenerSet, BackendTLSPolicy, InferencePool, Namespace).

- **`crates/ugwc-k8s/src/watch_stream.rs`** — add `reflector_full_watch_stream` and `reflector_meta_watch_stream` variants that pipe the watcher through `kube::runtime::reflector()` and return `(Store<K>, impl Stream<Item = WatchItem<K>>)`.

- **`crates/ugwc-k8s/src/controller.rs`** — construct reflectors at startup, build `SharedStores`, pass to reconcile context.

- **`crates/ugwc-k8s/src/gateway_watchers.rs`** — each `spawn_*_watcher` function takes a `reflector::Writer` and returns a `Store<K>` reader. The existing event → reconcile-key logic continues to consume the post-reflector stream.

- **`crates/ugwc-k8s/src/loader/mod.rs`** — rewrite `load_cluster_snapshot` to accept `&SharedStores` instead of `&Client`. Replace every `Api::list().await` with `store.state()` (returns `Vec<Arc<K>>`). Replace every `Api::get().await` with a store lookup by namespace/name. The function becomes synchronous (no `async`).

- **`crates/ugwc/src/reconcile.rs`** — `ReconcileCtx` gains a `SharedStores` field. `reconcile_gateway` calls the new store-backed loader. The `Client` is still needed for status writes and dataplane provisioning, but not for reading state.

- **`crates/ugwc/src/run.rs`** — wire `SharedStores` into `ReconcileCtx`.

### What stays the same

- Reconcile loop stays serial (phase 3).
- Queue stays `mpsc` (phase 2).
- EndpointSlice fan-out stays broad (phase 4).
- Status writes stay sequential (phase 5).

### Verification

- Run perftest with 50 gateways / 100 routes. Per-reconcile time should drop from hundreds of ms to single-digit ms.
- `ugwc_snapshot_load_seconds` histogram should show sub-ms p99.
- API server audit log should show zero list/get calls from ugwc during steady-state reconcile (only watch streams and status patches).

### Notes

- The initial sync gate (`InitialSyncGate`) currently does a standalone `Gateway::list` and `EndpointSlice::list` for readiness. With reflector stores, readiness can be gated on the reflector's `InitDone` event instead, eliminating those extra list calls.
- `load_cluster_snapshot` currently takes `&Client` at 5 call sites (reconcile, plus test helpers). The test helpers can use store-backed snapshots too, or keep a separate `load_cluster_snapshot_from_api` for integration tests.
- Secrets and ConfigMaps are watched but their full content is needed (TLS certs, CA bundles). These must be full-object reflectors, not metadata-only.

---

## Phase 2 — Deduplicating workqueue (eliminate redundant reconciles) ✅

**Goal:** A gateway key queued N times before processing results in 1 reconcile, not N.

### What changes

- **New module `crates/ugwc-k8s/src/dedup_queue.rs`** (or replace internals of `reconcile_queue.rs`) — implement a set-guarded queue with three states per key:
  - **pending:** in the queue, not yet picked up by a worker.
  - **in-flight:** currently being reconciled by a worker.
  - **dirty:** re-enqueued while in-flight; will be moved back to pending when the current reconcile finishes.

  `enqueue(ns, name)`: if pending → no-op; if in-flight → mark dirty; else → add to pending set, notify.  
  `dequeue()`: pop from pending, move to in-flight.  
  `done(key)`: remove from in-flight; if dirty, move to pending.

- **`crates/ugwc-k8s/src/reconcile_queue.rs`** — preserve the existing `WorkqueueMetrics` and `ReconcileQueueMetrics` interfaces. `DedupWorkQueue` wraps or replaces the inner `mpsc`. The `depth` gauge now reflects `pending.len()`.

- **`crates/ugwc-k8s/src/controller.rs`** — use `DedupWorkQueue` instead of `ReconcileQueue`.

- **`crates/ugwc-k8s/src/watch_enqueue.rs`** — `ReconcileSender::send` calls `enqueue` on the dedup queue.

### Verification

- Perftest: total reconcile count should be close to `number_of_gateways`, not `number_of_events`.
- Add `ugwc_workqueue_dedup_total` counter for skipped duplicates.
- Startup: with 50 gateways, watch initial sync fires ~50 gateway events + ~100 route events. Without dedup that's 150+ reconciles. With dedup it should be ~50.

---

## Phase 3 — Concurrent reconcile workers (eliminate serial bottleneck) ✅

**Goal:** Multiple gateways reconcile in parallel. No two workers process the same gateway simultaneously.

### What changes

- **`crates/ugwc-k8s/src/controller.rs`** — replace the single `loop { recv → reconcile }` with a `JoinSet` of N worker tasks, each pulling from the `DedupWorkQueue`. The per-key mutual exclusion comes from the dedup queue's in-flight set (phase 2): a key can't be dequeued while it's in-flight.

- **`crates/ugwc/src/run.rs`** — add `UGWC_RECONCILE_CONCURRENCY` env var (default 16). Pass to controller.

- **`crates/ugwc-k8s/src/reconcile_queue.rs`** — `in_flight` gauge now reflects actual concurrent in-flight count. `longest_running_processor_seconds` tracks per-worker max.

### Verification

- Perftest with 50 gateways: wall-clock time should scale as `~N * T / workers` instead of `N * T`.
- With 16 workers and store-backed reconcile (~5ms each), 50 gateways should complete in <1s.
- `ugwc_reconcile_in_flight` gauge should show up to `concurrency` concurrent reconciles.

### Notes

- Status writes still use `Client`, which is `Clone` and internally multiplexed — safe for concurrent use.
- The `status_cache` (`Arc<Mutex<HashMap>>`) is per-controller, not per-gateway. Concurrent reconciles writing different gateway statuses are fine since they use different keys. If two reconciles for the same gateway could run concurrently, the status cache could conflict — but phase 2's dedup queue prevents that.

---

## Phase 4 — Reverse index for targeted fan-out (eliminate thundering herd) ✅

**Goal:** An EndpointSlice/Secret/ConfigMap change only enqueues the gateways that actually reference the affected service/secret.

### What changes

- **`crates/ugwc-k8s/src/gateway_index.rs`** — reverse indexes:
  - `service (ns, name) → HashSet<(gateway_ns, gateway_name)>`
  - `secret (ns, name) → HashSet<(gateway_ns, gateway_name)>`
  - `configmap (ns, name) → HashSet<(gateway_ns, gateway_name)>`

  Index updated incrementally after each gateway reconcile from `ClusterSnapshot` refs.

- **`crates/ugwc-k8s/src/controller.rs`** — EndpointSlice/Secret/ConfigMap watchers use `GatewayIndex::resolve_*_fanout`.

- **`crates/ugwc-k8s/src/watch_enqueue.rs`** — store-backed fallback helpers (`all_gateway_keys_from_stores`, etc.) during startup before index is fully populated.

- **`crates/ugwc/src/reconcile.rs`** — after snapshot load, `index.update_gateway`; on delete, `index.remove_gateway`.

### Verification

- Perftest (`hack/perftest`): 50 gateways each with unique Service backend; `-trigger-endpoint` patches one EndpointSlice.
- `ugwc_index_fanout_size{event_kind="endpointslice"}` should observe `1`, not 50.
- Startup: broad fan-out until all gateways indexed once; then targeted.

---

## Phase 5 — Concurrent status writes (reduce per-reconcile wall time) ✅

**Goal:** Route status patches within a single reconcile run in parallel instead of sequentially.

### What changes

- **`crates/ugwc-k8s/src/status/parallel.rs`** — `run_bounded` helper using `buffer_unordered` with a concurrency cap.
- **`crates/ugwc/src/reconcile.rs`** — `apply_attached_resource_statuses` runs route and ListenerSet status patches in parallel after gateway status; BackendTLSPolicy and InferencePool delegate to parallelized ugwc-k8s helpers.
- **`UGWC_STATUS_PATCH_CONCURRENCY`** env var (default 16) caps concurrent patches per reconcile.

### Verification

- Perftest with 50 gateways / 100 routes: per-reconcile duration should drop proportionally to the number of routes per gateway.
- `ugwc_reconcile_duration_seconds` and `ugwc_status_patch_duration_seconds` histogram p99 should show the improvement.

### Notes

- Status cache checks (`status_cache`) remain correct under concurrent writes. Each route status patch uses a distinct cache key (`route_ns/route_name`).
- The Gateway's own status patch still happens before route status patches (it sets `Programmed` which routes may depend on for their condition).

---

## Summary

| Phase | Change | Primary metric impact |
|-------|--------|-----------------------|
| 1 | Reflector stores | Per-reconcile time: ~500ms → ~5ms |
| 2 | Dedup workqueue | Reconcile count: N*events → N*gateways |
| 3 | Concurrent workers | Wall-clock: O(N*T) → O(N*T/W) |
| 4 | Reverse index | Fan-out: O(all gateways) → O(affected) |
| 5 | Concurrent status | Per-reconcile time: further ~3-5x reduction |

After all phases, target numbers for 1000 gateways / 10000 routes:

| Metric | Current (extrapolated) | Target |
|--------|------------------------|--------|
| P99 reconcile latency | minutes | < 500ms |
| Full pass wall-clock | 10+ minutes | < 5s |
| API server calls per reconcile | ~12 lists | 0 (reads from store) |
| API server calls per status pass | 11,000 patches | 11,000 patches (unchanged, but concurrent) |

---

## Verification checklist

Manual validation against the targets above. Record results in your runbook or PR — none are automated in CI yet.

### Setup

```bash
cd hack/perftest && go build -o perftest .
./perftest -namespaces 50 -gateways 1 -routes 2 -services -trigger-endpoint
```

Ensure ugwc is running with `UGWC_KUBE_ENABLE=true` and scrape `/metrics`.

### Metrics to check

| Check | PromQL or metric | Expected |
|-------|------------------|----------|
| Snapshot load p99 | `histogram_quantile(0.99, sum(rate(ugwc_snapshot_load_duration_seconds_bucket[5m])) by (le))` | Sub-ms to low-ms after warm-up |
| Dedup at startup | `rate(ugwc_workqueue_dedup_total{queue_name="gateway_reconcile"}[5m])` | Non-zero during initial sync |
| Concurrent workers | `ugwc_workqueue_in_flight{queue_name="gateway_reconcile"}` | Up to `UGWC_RECONCILE_CONCURRENCY` under load |
| Endpoint fan-out | `histogram_quantile(0.99, sum(rate(ugwc_index_fanout_size_bucket{event_kind="endpointslice"}[5m])) by (le))` | ~1 after index warm-up (with `-trigger-endpoint`) |
| Status patch parallelism | `histogram_quantile(0.99, sum(rate(ugwc_status_patch_duration_seconds_bucket[5m])) by (le))` | Lower than pre-parallel baseline at same route count |
| Reconcile wall-clock | `histogram_quantile(0.99, sum(rate(ugwc_reconcile_duration_seconds_bucket[5m])) by (le, controller_name))` | 50 gateways with 16 workers: well under pre-change baseline |

### API audit (optional)

During steady-state reconcile (no object changes), audit logs should show **no** list/get from ugwc except watch streams and status patches.
