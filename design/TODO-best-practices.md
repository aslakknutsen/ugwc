# TODO: Gateway API control plane best practices

**Purpose:** Track gaps against *Architectural Standards and Engineering Best Practices for Kubernetes Gateway API Control Planes in Rust* (internal audit, 2026).  
**Related:** [`scale-performance.md`](scale-performance.md), [`security.md`](security.md), [`../docs/architecture.md`](../docs/architecture.md), [`../docs/operations.md`](../docs/operations.md).

---

## Executive summary

| Area | Grade | Notes |
|------|-------|--------|
| Workspace / boundaries | Green | Split crates, `[workspace.dependencies]`. |
| Clean architecture / IR | Amber | Core has no kube/xDS deps; `from_cluster` is very large. |
| Reconciliation / API citizenship | Amber | SSA on status/provision; full watches; queue behavior vs design doc. |
| IR compiler / determinism | Amber | Content hash exists but uses `Debug` in `revision_for`. |
| xDS / gRPC | Amber | ADS SotW; delta stub; weak ACK/nonce story. |
| Async / HTTP | Amber | No `spawn_blocking` for compile; admin Axum without Tower layers. |
| Serde / secrets / errors | Amber | Config not `deny_unknown_fields`; reconcile errors as strings. |
| Observability | Amber | Sparse `instrument`; no OTel; readiness is shallow. |
| Supply chain / CI | Red | `deny.toml` exists; CI does not run deny/audit. |
| Build / perf | Amber | Metrics for compile/load; no profiling story in repo. |
| HA / webhooks / sharding | Red | Leader election stub; no webhooks/sharding in code. |

---

## Prioritized TODOs

1. **Leader election** — `UGWC_LEADER_ELECTION=true` uses `LeaderState::follower_stub()` so `is_leader()` is always false and publish/status/dataplane work gated on leader is skipped. Either implement Lease-based election and `watch` channel updates, or document that the flag must remain `false` until implemented (`crates/ugwc/src/run.rs`, `crates/ugwc/src/leader.rs`, `docs/operations.md`).

2. **Doc vs code: reconcile queue** — `design/scale-performance.md` §5 describes drop/coalesce when the queue is full; code uses `mpsc::channel(256)` with awaitable `send`, so watchers block under pressure. Align implementation with documented policy or revise the design table (`crates/ugwc-k8s/src/controller.rs`).

3. **Deterministic revision token** — Replace `format!("{:?}", …)` in `revision_for` with a canonical serialization (explicit struct walk, stable-ordered serde, or protobuf) so hashes do not depend on `Debug` stability (`crates/ugwc-core/src/compile.rs`).

4. **xDS ACK/NACK** — For generic Envoy clients, implement `version_info` / **nonce** correlation on `StreamAggregatedResources` (and eventual Delta) per xDS spec (`crates/ugwc-xds/src/server.rs`).

5. **Graceful shutdown on Kubernetes** — `wait_shutdown` only listens for `ctrl_c()`. Add SIGTERM handling (and optionally SIGHUP) so pod preStop / kube termination drains admin, xDS, and controller tasks (`crates/ugwc/src/run.rs`).

6. **CI supply chain** — Run `cargo deny check` (lockfile policy already in `deny.toml`) and preferably `cargo audit` (or `deny` advisories) in `.github/workflows/ci.yml`; document any intentional gaps.

7. **CPU-heavy compile off runtime** — Measure compile time; if significant, move `ugwc_core::compile` behind `tokio::task::spawn_blocking` with a bounded pool or document why it stays inline (`crates/ugwc/src/run.rs`).

8. **Watch scale** — Evaluate metadata-only watches, field selectors, or label selectors per resource type where full specs are unnecessary; document required labels if selectors are introduced (`crates/ugwc-k8s/src/controller.rs`, `design/scale-performance.md` §3).

9. **Admin HTTP hardening** — Add Tower-style layers where appropriate: request timeouts, optional rate limiting on admin, structured request tracing (`crates/ugwc/src/admin.rs`; deps already in workspace).

10. **Observability** — Add `tracing::instrument` with gateway `namespace`/`name` on hot paths (reconcile, snapshot load, status apply); consider xDS metrics (stream count, NACKs, scope misses beyond `ugwc_xds_scope_miss_total`).

11. **Sharding / multi-instance** — If horizontal scale beyond HA is a goal, design namespace- or label-based shard filters so each replica’s watchers match its shard (not present today).

12. **Admission / conversion webhooks** — Either implement with TLS, timeout, and least privilege per `design/security.md`, or keep **N/A** with an explicit scope line in `docs/architecture.md` (no webhook binary today).

---

## Quick wins

- Document **`UGWC_LEADER_ELECTION`** as non-functional until Lease wiring lands (one paragraph in `docs/operations.md` prevents production misconfiguration).

- Add **`cargo deny check`** to CI to match `Makefile` `deny` target and `deny.toml`.

- Wire **SIGTERM** alongside ctrl-c for drain behavior.

- Add **`#[instrument]`** on `run_gateway_controller` reconcile invocation or wrapper with structured fields for gateway identity.

---

## Checklist (Met / Partial / Missing / N/A)

Rationale is one line each; primary symbols/paths in the last column.

| # | Item | Status | Rationale | Evidence |
|---|------|--------|-----------|----------|
| 1 | Cargo workspace + logical split | Met | Members map to domain, k8s, xDS, binary. | `Cargo.toml`, `docs/architecture.md` |
| 2 | `[workspace.dependencies]` | Met | Centralized tokio, kube, serde, etc. | Root `Cargo.toml` |
| 3 | Flat `crates/` layout | Met | Five members under `crates/`. | Repository layout |
| 4 | Central IR (not raw K8s everywhere) | Met | `IrSnapshot`, `ListenerIr`, routes, backends in core. | `crates/ugwc-core/src/ir.rs` |
| 5 | Dependency direction (adapters) | Met | Core crate has no kube/tonic deps. | `crates/ugwc-core/Cargo.toml` |
| 6 | Documented data flow | Met | Architecture diagrams and sequences. | `docs/architecture.md` |
| 7 | Reconciler idempotency | Partial | Recompute from API each time; queue can backlog same key; sends block. | `crates/ugwc/src/run.rs`, `crates/ugwc-k8s/src/controller.rs` |
| 8 | Server-Side Apply for owned writes | Met | `PatchParams::apply("ugwc")` on status and provision templates. | `crates/ugwc-k8s/src/status.rs`, `provision/mod.rs` |
| 9 | Partial failure resumable | Partial | SSA helps; no explicit checkpoint beyond re-reconcile. | Same as #8 |
| 10 | Scale hygiene (metadata/field/selectors) | Partial | Default full-object watches cluster-wide. | `crates/ugwc-k8s/src/controller.rs` |
| 11 | Burst / rate awareness | Partial | Endpoint debounce only; no APF/QPS doc for SA. | `controller.rs`, `docs/operations.md` |
| 12 | Streaming list (K8s 1.33+) | N/A | Pin is v1_30; feature not claimed. | `Cargo.toml` `k8s-openapi` |
| 13 | Compile vs graph walk separation | Partial | Modular but `from_cluster` is a large hotspot. | `from_cluster::ir_from_cluster` |
| 14 | Determinism (snapshot → xDS hash) | Partial | SHA256 revision uses `Debug` strings in hash input. | `compile::revision_for` |
| 15 | Closed vs open extension | Partial | Enums where appropriate; no general plugin trait model. | `crates/ugwc-core/src/ir.rs` |
| 16 | Transport choice documented | Met | ADS SotW; Delta gated in server and config. | `docs/architecture.md`, `server.rs` |
| 17 | ADS for cross-type consistency | Met | `AggregatedDiscoveryService` implementation. | `crates/ugwc-xds/src/server.rs` |
| 18 | IR → stream bridge, bounded, lag policy | Partial | `ArcSwap` cache; per-stream `mpsc(8)`; no coalesce-to-latest on lag. | `cache.rs`, `server.rs` |
| 19 | `version_info` / nonce / ACK | Missing | Response uses defaults for nonce; no ACK loop. | `server.rs` |
| 20 | Backpressure visible | Partial | Bounded channels; reconcile backpressure blocks producers. | `controller.rs`, `server.rs` |
| 21 | CPU-heavy work off runtime | Missing | `compile` runs in async reconcile closure. | `crates/ugwc/src/run.rs` |
| 22 | `controller::Config` / concurrency | Missing | Custom single-consumer `mpsc` loop. | `crates/ugwc-k8s/src/controller.rs` |
| 23 | Management HTTP + Tower | Met | Trace + timeout layers; `PUT /log-level`. | `crates/ugwc/src/admin.rs` |
| 24 | Graceful shutdown | Met | SIGTERM/ctrl-c, drain, OTLP flush. | `run.rs`, `ugwc-xds/server.rs` |
| 25 | `serde` strictness policy | Partial | Env `Config` without `deny_unknown_fields`. | `crates/ugwc/src/config.rs` |
| 26 | Secrets handling | Partial | PEM as `Vec<u8>`; no `zeroize`; redact helper mostly unused. | `ir.rs`, `logging.rs` |
| 27 | Structured domain errors | Partial | `thiserror` in core/config; reconcile uses `String`. | `compile.rs`, `run.rs` |
| 28 | `tracing` on hot paths | Met | `reconcile_gateway` span + controller entry. | `reconcile.rs`, `controller.rs` |
| 29 | JSON log option | Met | `log_json` / subscriber JSON layer (Helm default true). | `logging.rs`, `config.rs` |
| 30 | Distributed tracing | Partial | OTLP traces via `otlp-traces` feature + `UGWC_OTLP_ENDPOINT`. | `logging.rs`, `otlp.rs` |
| 31 | `/metrics`, health, xDS health | Met | Prometheus + probes; xDS metrics; generation watch filters. | `admin.rs`, `metrics.rs`, `ugwc-xds/server.rs`, `watch_stream.rs` |
| 32 | `cargo audit` in CI | Missing | CI is fmt, clippy, test, build. | `.github/workflows/ci.yml` |
| 33 | `cargo-deny` in CI | Partial | `deny.toml` + `make deny`; not in CI. | `deny.toml`, `Makefile` |
| 34 | `cargo vet` or waiver | Missing | No vet config or explicit waiver doc. | — |
| 35 | Container / SBOM | Partial | Distroless image; no `cargo auditable` story. | `Dockerfile` |
| 36 | Faster linker documented | N/A | Not documented in repo. | — |
| 37 | Workspace feature unification | N/A | No hakari / doc for duplicate dep pain. | — |
| 38 | Profiling story | Partial | Histogram metrics; no flamegraph/timings scripts. | `metrics.rs`, `run.rs` |
| 39 | Admission webhooks | N/A | Out of scope in code; design mentions future. | `design/security.md` |
| 40 | Sharding | Missing | No label/namespace shard on watchers. | `controller.rs` |
| 41 | Leader election | Missing | Stub; flag true ⇒ permanent follower. | `leader.rs`, `run.rs` |

---

## Known contradictions (fix doc or code)

1. **`docs/operations.md`** suggests leader-gated behavior when `UGWC_LEADER_ELECTION` is enabled; implementation is a non-leader stub that skips gated work.

2. **`design/scale-performance.md` §5** describes reconcile queue drop/coalesce when full; implementation uses blocking `send` on a bounded queue.

---

## Assumptions used in the audit

1. Scope is this repository’s `ugwc` process and crates, not external dataplane builds.  
2. Kubernetes / Gateway API versions follow repo pins (e.g. `k8s-openapi` v1_30, Gateway API v1.5.x per docs).  
3. Delta xDS and full leader election are explicitly incomplete where code comments state so.

---

## Entry points reference (for future edits)

- Binary: `crates/ugwc/src/main.rs` → `run::run` (`crates/ugwc/src/run.rs`).  
- Controller graph: `ugwc_k8s::run_gateway_controller` (`crates/ugwc-k8s/src/controller.rs`).  
- Core pipeline: `ugwc_core::compile`, `plan_publish` (`crates/ugwc-core/src/compile.rs`, `publish.rs`).  
- xDS: `ugwc_xds::serve` (`crates/ugwc-xds/src/server.rs`).
