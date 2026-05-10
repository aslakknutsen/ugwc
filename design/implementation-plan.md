# Implementation plan — Gateway API+ control plane

**Status:** Large parts of this phased plan are **landed** in the Rust workspace (controller, multi-route compile, ListenerSet, BackendTLS, InferencePool, etc.). For **what the binary does today** and remaining gaps, prefer **[`todo-gateway-api-inference-parity.md`](todo-gateway-api-inference-parity.md)** (baseline table) and **[`docs/architecture.md`](../docs/architecture.md)** / **[`docs/operations.md`](../docs/operations.md)**. The sections below remain useful for historical ordering, merge points, and parallel-work discipline.

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Architecture:** [`controller-architecture.md`](controller-architecture.md), [`component-design.md`](component-design.md), [`output-distribution.md`](output-distribution.md)

This document orders work from **project scaffolding** to **running conformance tests**, marks **parallel lanes**, and calls out **merge points** where outputs from parallel work must integrate before proceeding.

---

## Assumptions (v1 baseline)

1. **Scope:** Kubernetes Gateway API, Gateway API **Inference Extension**, and **Policy APIs** as a deliberately enumerated list (close the list before locking RBAC and compiler surface).
2. **Distribution v1:** **xDS** only in the first shipping path; other backends remain extension points per [`output-distribution.md`](output-distribution.md).
3. **Profile / class mapping (initial):** **Controller-level configuration** (environment variables and static flags). Minimum: which `GatewayClass.spec.controllerName` values this controller accepts. Richer `parametersRef` can arrive later without blocking scaffolding.
4. **Failure handling:** A **FailurePolicy** extension point (data + registry; pluggable behavior later) so publish vs partial-compile decisions are not hard-coded once.
5. **HA / streaming:** **Reconnect** on leader change or stream reset; no multi-active xDS requirement for v1.
6. **Security:** A **written security model** is required before production; **plaintext xDS is acceptable only for prototype phases** and must not become the implicit production default.

---

## How to read phases

- **Depends on:** hard prerequisites.
- **Parallel with:** safe concurrent work until the next **Merge point**.
- **Outputs:** artifacts the next phase consumes (types, behavior, CI contracts).
- **Affects downstream:** explicit blast-radius notes.

### Implementation stack (v0 — Rust)

**Workspace:** Cargo workspace at the repository root (`Cargo.toml` `[workspace]`), `resolver = "2"`, Rust edition **2021** (or newer if CI pins a toolchain). Pin compiler with **`rust-toolchain.toml`** or CI matrix when reproducibility requires it.

**Crate mapping** (names mirror the design docs; optional `ugwc-policy` as a workspace member when policy code dominates size):

| Design name | Crate / path | Responsibility |
|-------------|----------------|----------------|
| Binary `ugwc` | `crates/ugwc` (`src/main.rs`) | CLI entry, wiring `kube-runtime` / tonic / admin HTTP / leader election |
| `ugwc-core` | `crates/ugwc-core` | IR (`IrSnapshot`, route/listener/backend types), `CompileReport`, **FailurePolicy** registry and data |
| `ugwc-k8s` | `crates/ugwc-k8s` | `kube` / `kube-runtime` watches, status writers, Gateway API typed refs |
| `ugwc-xds` | `crates/ugwc-xds` | gRPC (tonic), ADS/snapshot encoding, **no** Kubernetes imports in the hot encode path if cycles threaten — orchestration lives in `ugwc` or a thin `ugwc-orchestrate` crate added at Merge A if needed |
| Optional `ugwc-policy` | `crates/ugwc-policy` | Policy CRD watches and merge into IR when warranted |

**Async / runtime:** Tokio **multi-thread** runtime (`#[tokio::main]` or explicit `Runtime` in tests). Long-running services (**xDS**, **kube** controllers) are async; **pure compile** functions in `ugwc-core` stay sync unless profiling forces otherwise. Use **`tracing`** (with **`tracing-subscriber`**) for structured logs; avoid logging Secret material (redaction helpers in `ugwc` or `ugwc-core`).

**Key traits / seams (evolve with phases):**

| Concept | Where | Notes |
|---------|--------|--------|
| `CompileInput` → `IrSnapshot` | `ugwc-core` | Pure functions + `CompileReport`; kube types stay out of core tests (fixtures) |
| `PublishPlan` / publish pipeline | `ugwc` or `ugwc-core` | After Merge A; consumes `prev` / `next` snapshot + FailurePolicy |
| xDS snapshot / encoder | `ugwc-xds` | IR-neutral wire mapping per [`output-distribution.md`](output-distribution.md) — **no full Envoy structs in IR** |

**Parallel execution tip:** After Phase 1, lanes **2A / 2B / 2C** may use **separate git worktrees** (see each phase’s **Parallelism** subsection). Phases **0–1**, **Merge A**, and many integration gates are **single-lane** unless explicitly called out.

---

## Phase 0 — Scaffold and engineering baseline

**Goal:** Workspace compiles; `cargo test` / `clippy` / `fmt` run in CI; release binary and OCI image build.

**Work (summary)**

- Cargo **workspace** with binary crate `ugwc` and library crates `ugwc-core`, `ugwc-k8s`, `ugwc-xds` (optional later: `ugwc-policy`).
- Tooling: `rustfmt`, `clippy`, `cargo deny` or `cargo audit` policy, `Makefile` or `just` recipes mirroring CI.
- CI: format check, clippy (`-D warnings` once stable), unit tests, `--release` binary artifact, Docker image when the runner allows.
- Embed build metadata: semver from `CARGO_PKG_VERSION`, git SHA via `vergen` / `built` or build script env.

**Outputs:** Green CI, reproducible artifact, stable crate names.

**Affects downstream:** Crate renames are costly—lock names in this phase.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| None | Phase 1 |

**Optional git worktrees:** Not recommended—single lane.

**Must stay serial:** Entire phase before Phase 1.

### Sub-phases

#### 0.1 — Cargo workspace and crate graph

**Dependencies:** None.

**Tasks**

1. Add root `Cargo.toml` with `[workspace]`, `members = ["crates/ugwc", "crates/ugwc-core", "crates/ugwc-k8s", "crates/ugwc-xds"]`, `resolver = "2"`.
2. Add `rust-toolchain.toml` (channel `stable` or team-pinned) when reproducibility is required.
3. Create `crates/*` with `[package]` metadata, `edition = "2021"`, shared `repository` / `license` fields.
4. Wire path dependencies: `ugwc` depends on the three libraries (even if unused in stub) to validate the graph.

**Deliverables:** `cargo check --workspace` succeeds; no orphan crates.

**Rust specifics:** Workspace-only features live in root `[workspace.dependencies]` when versions repeat; keep `ugwc-core` free of `kube` / `tonic` until later phases pull them in.

**Acceptance criteria**

- [ ] `cargo check --workspace` passes locally and in CI.
- [ ] Crate names match the table in **Implementation stack (v0 — Rust)**.

#### 0.2 — Binary entrypoint and build metadata

**Dependencies:** 0.1.

**Tasks**

1. `crates/ugwc/src/main.rs`: minimal process (`clap` or `argh` for CLI); `--version` prints semver + optional git SHA.
2. Optional `build.rs` + `vergen` / `built`, or CI-injected `env!` for commit; document the contract in crate docs.
3. Stub startup message or exit `0` when no subcommand (until Phase 1).

**Deliverables:** `cargo run -p ugwc -- --version` shows version metadata.

**Rust specifics:** Prefer `clap` with derive for future subcommands; keep MSRV policy aligned with `kube` / `tonic` transitive requirements.

**Acceptance criteria**

- [ ] Binary builds in `debug` and `--release`.
- [ ] Version output is deterministic in CI (SHA from env or git).

#### 0.3 — Library stubs and module boundaries

**Dependencies:** 0.1.

**Tasks**

1. `ugwc-core`: `lib.rs` + `lib_stub` / `compile` modules as placeholders; crate-level doc comment for IR scope (v0 global vs per–`GatewayClass` TBD).
2. `ugwc-k8s`: empty `lib` exporting a trivial symbol or `#[doc(hidden)]` placeholder until Phase 2A adds `kube`.
3. `ugwc-xds`: placeholder for gRPC server types (no network bind in 0.3 if undesired—may stay empty `lib`).

**Deliverables:** `cargo test -p ugwc-core` can run zero tests or a single `#[test] fn placeholder() {}`.

**Rust specifics:** Reserve module paths (`core::snapshot`, `k8s::watchers`, `xds::server`) in docs even if files appear later.

**Acceptance criteria**

- [ ] Each library crate compiles standalone and as a dependency of `ugwc`.
- [ ] No circular dependencies between crates (DAG: `ugwc` → libs).

#### 0.4 — Local and CI quality gates

**Dependencies:** 0.1–0.3.

**Tasks**

1. `Makefile` or `justfile`: `fmt`, `clippy`, `test`, `build`, optional `ci` aggregate.
2. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` in CI.
3. Document `cargo deny` / `cargo audit` in `design/` or `CONTRIBUTING` when policy exists; add minimal `deny.toml` if using `cargo deny`.

**Deliverables:** Contributors can run one command for local parity with CI.

**Rust specifics:** `clippy` pedantic is optional; start with `-D warnings` on default lints.

**Acceptance criteria**

- [ ] CI fails on formatting drift and clippy warnings (per policy).

#### 0.5 — CI workflow and artifacts

**Dependencies:** 0.4.

**Tasks**

1. GitHub Actions / equivalent: checkout, cache `~/.cargo` + `target`, run fmt/clippy/test, upload `ugwc` binary as artifact (optional).
2. Optional cross-build matrix if release targets multiple arch/OS.

**Deliverables:** Green PR checks without cluster credentials.

**Rust specifics:** Use `actions-rust-lang` or `dtolnay/rust-toolchain@stable` + cache action; split `cargo test` from `cargo build --release` if time-bound.

**Acceptance criteria**

- [ ] Every PR runs fmt, clippy, test on the workspace.

#### 0.6 — OCI image (multi-stage)

**Dependencies:** 0.2, 0.4.

**Tasks**

1. `Dockerfile`: builder stage `FROM rust:1.x` or `cargo-chef` / `docker buildx` cache; runtime `FROM gcr.io/distroless/cc-debian12` or `alpine` + glibc compatibility note.
2. Copy `target/release/ugwc` into runtime image; `ENTRYPOINT` with non-root user where feasible.
3. CI job builds image when Docker available; otherwise document skip.

**Deliverables:** `docker build -t ugwc:dev .` produces a runnable image (may only print help/version until later phases).

**Rust specifics:** Static `musl` linking is optional—document if dynamic glibc in distroless.

**Acceptance criteria**

- [ ] Image build documented; CI or Makefile target exists.

#### Phase 0 — Roll-up checklist

**Dependencies:** None for the phase overall; internal order 0.1 → 0.6.

**Acceptance criteria**

- [ ] `cargo test --workspace` passes; CI mirrors local recipes.
- [ ] Crate and module names stable for downstream phases.
- [ ] New contributor runs documented targets without cluster credentials.

---

## Phase 1 — Runtime shell: config, logging, health, security hooks

**Goal:** Process starts, validates configuration, serves probes, emits structured logs.

**Work (summary)**

- **Controller configuration** (env-first via `figment` / `serde` + `envy` pattern or custom `from_env`): HTTP admin bind, xDS bind, accepted `controllerName` list, watch scope, **optional TLS paths** for xDS (no-op until Phase 9).
- **FailurePolicy** scaffolding in `ugwc-core`: types and registry (`OnPublishError`, `OnPartialCompile` or agreed keys); defaults = **do not** push broken snapshots unless explicitly allowed.
- **`tracing`** + structured fields; redaction helper for Secret-like values.

**Outputs:** Validated `Config`, health routes (`GET /healthz`, `GET /readyz`).

**Affects downstream:** All crates consume `Config` from `ugwc` wiring or a small `ugwc-config` module—semantic changes break operators.

**Dependencies:** Phase 0.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| None | Parallel window 1 (2A/2B/2C) |

**Optional git worktrees:** N/A (single lane).

**Must stay serial:** Complete Phase 1 before forking 2A/2B/2C.

### Sub-phases

#### 1.1 — `Config` type and validation

**Dependencies:** Phase 0.

**Tasks**

1. Define `Config` in `ugwc` or `ugwc-core` (prefer **`ugwc`** or dedicated `config.rs` to avoid `core` ↔ binary cycles): env keys documented; explicit defaults.
2. Implement `validate() -> Result<(), ConfigError>`: bind addresses parseable, controller names non-empty list, mutual exclusion rules for TLS vs insecure xDS flags.
3. Unit tests for invalid combinations.

**Deliverables:** `Config` struct + `Display`/`Debug` that never prints raw TLS key material.

**Rust specifics:** `serde`, `figment` (`Figment::from(Env::prefixed("UGWC_"))`), or `config` crate; use `thiserror` for errors.

**Acceptance criteria**

- [ ] Bin exits non-zero on invalid env; errors are actionable strings.

#### 1.2 — Admin HTTP server (health)

**Dependencies:** 1.1.

**Tasks**

1. **`axum`** or **`warp`** (or `hyper` directly): bind admin addr from `Config`.
2. `GET /healthz` → `200 OK` when server task running.
3. `GET /readyz` → stub `200` until kube/xDS deps exist; document future semantics.

**Deliverables:** Async server task spawned under Tokio alongside future long-lived services.

**Rust specifics:** `tokio::spawn` server graceful shutdown via `CancellationToken` (or tokio-util) in later phases; for 1.2, ctrl-c acceptable.

**Acceptance criteria**

- [ ] Probes return expected codes from an integration test or local curl.

#### 1.3 — Structured logging and redaction

**Dependencies:** 1.1.

**Tasks**

1. Initialize `tracing_subscriber` with JSON or compact format; log level from `RUST_LOG` / config.
2. Redaction: wrapper for `tracing` fields that skips or hashes patterns matching Secret refs or sensitive keys.

**Deliverables:** Startup log line with non-secret effective config snapshot.

**Rust specifics:** `tracing`, `tracing-subscriber`; optional `secrecy` crate for newtypes around tokens.

**Acceptance criteria**

- [ ] No bearer token or PEM body in logs in fixture tests.

#### 1.4 — TLS path fields (dormant)

**Dependencies:** 1.1.

**Tasks**

1. Optional `PathBuf` fields for xDS server cert/key/CA on `Config`; validation = paths exist when set (or defer read to Phase 9).
2. Document “prototype plaintext only with explicit env” per project security assumptions.

**Deliverables:** Types compile; Phase 9 wires loading.

**Rust specifics:** Use `std::path::PathBuf`; avoid reading files at config parse if validation requires syscalls—team choice.

**Acceptance criteria**

- [ ] Config documents TLS vs insecure mutual exclusion.

#### 1.5 — FailurePolicy registry (types only)

**Dependencies:** 1.1 (`ugwc-core`).

**Tasks**

1. Enums / structs for policy dimensions; `Default` impls = conservative (no partial publish).
2. Registry or static `FailurePolicy::default()` consumed later at compile/publish boundaries.

**Deliverables:** `ugwc-core` exports types; `ugwc` holds runtime-selected policy from env (optional).

**Rust specifics:** `enum` + `strum` for stringly env parse; unit tests for defaults.

**Acceptance criteria**

- [ ] Defaults documented in crate docs; types used in compile stubs after Merge A.

#### Phase 1 — Roll-up checklist

**Acceptance criteria**

- [ ] Binary starts with valid env; fails fast on invalid config.
- [ ] Probes respond; logs structured without secret bodies.
- [ ] FailurePolicy types compile and are registered (behavior stubbed until Merge A).

## Parallel window 1 — Lanes 2A, 2B, 2C (until Merge A)

After Phase 1, three lanes may proceed **in parallel** until **Merge A**:

| Lane | ID | Focus | Suggested worktree path | Suggested branch name |
|------|-----|-------|-------------------------|------------------------|
| Kubernetes spine | **2A** | Clients, watches, minimal reconcile | `../ugwc-lane-2a` | `lane/2a-kube` |
| IR core | **2B** | `IrSnapshot` types, trivial compile | `../ugwc-lane-2b` | `lane/2b-ir` |
| xDS skeleton | **2C** | gRPC server, empty / dummy resources | `../ugwc-lane-2c` | `lane/2c-xds` |

**Create worktrees (from primary clone, after Phase 1 on `main`):**

```bash
git fetch origin
git worktree add -b lane/2a-kube ../ugwc-lane-2a origin/main
git worktree add -b lane/2b-ir ../ugwc-lane-2b origin/main
git worktree add -b lane/2c-xds ../ugwc-lane-2c origin/main
```

**Merge / integration rules**

- Rebase all three onto the same **Phase 1** baseline **before Merge A**.
- **API contract freeze:** agree on `IrSnapshot` shape, `CompileReport`, xDS “current snapshot” handle, and `Config` fields that cross crates—track via short **design note** or PR checklist.
- **Must stay serial:** **Merge A** itself; no Phase 3 until Merge A completes.

---

### Phase 2A — Kubernetes client and first watch

**Goal:** In-cluster (or kubeconfig dev): list/watch one Gateway API resource reliably.

**Depends on:** Phase 1.

**Parallel with:** 2B, 2C.

**Outputs:** Stable watch + resync behavior, observable in logs.

**Affects downstream:** **`kube` + `kube-derive` + `kube-runtime`** vs lower-level client is the main reversible cost—prefer `kube` ecosystem for Rust.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| 2B, 2C | Merge A |

**Optional git worktrees:** `../ugwc-lane-2a` on `lane/2a-kube` (see table above).

**Must stay serial:** First merge with 2B/2C at **Merge A** (resolve `Arc<Config>` / client lifetimes).

### Sub-phases

#### 2A.1 — Dependencies and Gateway API types

**Dependencies:** Phase 1.

**Tasks**

1. Add `kube`, `kube-runtime`, `k8s-openapi` with feature flags for `gateway_api` / correct K8s version feature.
2. Pin `k8s-openapi` **one** Kubernetes minor per workspace (document in workspace `README` section in this file only—not a new repo README).

**Deliverables:** `cargo check -p ugwc-k8s` with API features enabled.

**Rust specifics:** Use `kube::Client::try_default()` for local dev; service account in-cluster.

**Acceptance criteria**

- [ ] `GatewayClass`, `Gateway`, `HTTPRoute` Rust types resolve to `gateway.networking.k8s.io/v1`.

#### 2A.2 — Controller / reflector skeleton

**Dependencies:** 2A.1.

**Tasks**

1. **`Controller`** from `kube-runtime` over one resource (start with `Gateway` or `GatewayClass`).
2. Resync period and backoff policy documented (see [`scale-performance.md`](scale-performance.md) §3).
3. Log `resourceVersion` + namespaced name on reconcile.

**Deliverables:** `ugwc-k8s::run_controller()`-style entry (exact name TBD) invoked from `ugwc` binary behind feature flag or config.

**Rust specifics:** `Api` + `Informer` or `Controller` + `watcher`; use `tracing` spans per reconcile.

**Acceptance criteria**

- [ ] In kind / k3s / env-test harness: events logged; watch errors not silent.

#### Phase 2A — Roll-up checklist

**Dependencies:** Phase 1.

**Acceptance criteria**

- [ ] In dev cluster or integration test: watch receives events; reconcile logs stable keys.
- [ ] Resync and errors surfaced in logs/metrics stubs without tight loops.

### Phase 2B — IR v0: types and trivial compile

**Goal:** Frozen **IR module** boundaries in `ugwc-core` and a mandatory **`ir_version`** field.

**Depends on:** Phase 1 (config contracts); tests use fixtures only—**no `kube` imports in `ugwc-core` unit tests**.

**Parallel with:** 2A, 2C.

**Outputs:** `IrSnapshot`, `CompileReport`, trivial `compile()` for empty/single-error cases.

**Affects downstream:** Structural blast radius for xDS, status, conformance—invest here deliberately.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| 2A, 2C | Merge A |

**Optional git worktrees:** `../ugwc-lane-2b` on `lane/2b-ir`.

**Must stay serial:** Public IR types must be merged and reviewed before Encode work in Phase 7 assumes names.

### Sub-phases

#### 2B.1 — Scope decision and envelope

**Dependencies:** Phase 1.

**Tasks**

1. Document v0 **scope** (cluster-wide vs per–`GatewayClass`) in `crates/ugwc-core/src/lib.rs` module docs **and** update the “Living document” section when changed.
2. Define `IrScopeId` (newtype `Uuid`, string, or enum) + rationale for xDS sharding (per [`output-distribution.md`](output-distribution.md) §8 **per-scope version**).

**Deliverables:** ADR-style paragraph in crate docs.

**Rust specifics:** `non_exhaustive` enums where extension is likely; `semver::Version` for `ir_version` or embedded `u32` schema version—pick one and encode in every snapshot.

**Acceptance criteria**

- [ ] Scope choice explicit; linked from this plan’s Living document checklist.

#### 2B.2 — `IrSnapshot` and domain placeholders

**Dependencies:** 2B.1.

**Tasks**

1. `IrSnapshot { ir_version, revision: ContentHashOrMonotonic, scope: IrScopeId, … }`.
2. Placeholder structs: `ListenerIr`, `RouteRuleIr`, `BackendIr` with minimal `#[derive]` (`Clone`, `Debug`, `PartialEq`); avoid serde until needed.

**Deliverables:** Types compile; `Debug` does not dump secrets (none yet).

**Rust specifics:** Prefer **`im`** / **`rpds`** later for persistent structures—start with `Vec` + `Arc` for v0.

**Acceptance criteria**

- [ ] `IrSnapshot` always carries `ir_version` and scope.

#### 2B.3 — `compile()` and `CompileReport`

**Dependencies:** 2B.2, FailurePolicy types from Phase 1.5.

**Tasks**

1. `CompileInput` stub (fixtures only): start with empty struct / explicit test builders.
2. `fn compile(&FailurePolicy, input: &CompileInput) -> Result<IrSnapshot, CoreError>` **or** `(IrSnapshot, CompileReport)` per project error-model—pick **one** style and document.
3. `CompileReport` holds `Vec<ObjectRefErr>` with stable object refs (`Group`, `Kind`, `Namespace`, `Name`).

**Deliverables:** Table-driven tests: empty input → empty IR; invalid ref → populated report.

**Rust specifics:** `thiserror` for errors; pure functions under `#[cfg(test)]` heavy coverage.

**Acceptance criteria**

- [ ] Tests pass without cluster; IR version always set on success path.
- [ ] FailurePolicy stub influences whether partial snapshots are emitted on errors (documented).

#### Phase 2B — Roll-up checklist

**Acceptance criteria**

- [ ] `cargo test -p ugwc-core` is hermetic (no network).

---

### Phase 2C — xDS gRPC skeleton

**Goal:** Data plane clients can open **ADS / discovery** streams; server returns minimal empty resources without panicking.

**Depends on:** Phase 1 (`Config` carries bind addr + TLS / insecure flags).

**Parallel with:** 2A, 2B.

**Outputs:** Runnable xDS server surface in `ugwc-xds`.

**Affects downstream:** ADS muxing and resource naming—align early with Envoy / Agentgateway expectations.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| 2A, 2B | Merge A |

**Optional git worktrees:** `../ugwc-lane-2c` on `lane/2c-xds`.

**Must stay serial:** Snapshot holder ownership merges at Merge A.

### Sub-phases

#### 2C.1 — xDS protos and tonic server shell

**Dependencies:** Phase 1.

**Tasks**

1. Add **`tonic`**, **`prost`**; vendor or build **`envoy-api-rs` / `xds-api`** (or pin `data-plane-api` protos)—record choice in workspace `Cargo.toml` comments.
2. Register **Aggregated Discovery Service** (ADS) service with minimal implementations returning empty `DiscoveryResponse` bodies.
3. Listen on `Config.xds_listen_addr`.

**Deliverables:** `ugwc-xds::serve(cfg: XdsServerConfig) -> Result<(), Error>` async entry.

**Rust specifics:** Single Tokio runtime in `ugwc` binary; avoid blocking in service handlers.

**Acceptance criteria**

- [ ] `grpcurl` / Envoy connects without panic; logs stream open/close at `info`.

#### 2C.2 — Security mode and lifecycle

**Dependencies:** 2C.1.

**Tasks**

1. Plaintext **only** if `UGWC_XDS_INSECURE=true` (or config mirror)—**warn** once at startup.
2. TLS: load paths from Phase 1 when present (stub file read or defer to Phase 9)—at minimum **validate paths exist** when insecure is false.
3. SIGTERM / SIGINT: join server with timeout (`tokio::time::timeout`); document drain limitations.

**Deliverables:** Documented constants for max streams / idle (see [`scale-performance.md`](scale-performance.md) §5).

**Rust specifics:** `tower` layers for connection limits later; start with `tcp_nodelay` + doc’d backlog.

**Acceptance criteria**

- [ ] Insecure mode explicit in structured logs; server binds configurable address.

#### Phase 2C — Roll-up checklist

**Acceptance criteria**

- [ ] Envoy or grpcurl opens ADS stream; connection behavior documented.
- [ ] Resource naming placeholders logged (for later alignment with Phase 7).

---

### Merge A — Integrate 2A + 2B + 2C

**Stop parallel lanes here — integrate on one branch before Phase 3.**

**Goal:** One binary: **watch** → **reconcile** → **trivial `IrSnapshot`** → **xDS layer** holds `ArcSwap<IrSnapshot>` (or channel) for the “current” publishable snapshot.

**Depends on:** 2A, 2B, 2C each reaches roll-up; **rebase** all lanes onto the same Phase 1 baseline.

**Outputs:** End-to-end tick with no real routing; publish gating + FailurePolicy wiring stubs.

**Affects downstream:** Encodes ownership boundaries for Phase 7 (IR owner vs xDS encoder).

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Nothing — integration only | Phase 3 |

**Optional git worktrees:** Close `lane/2a-kube`, `lane/2b-ir`, `lane/2c-xds` or merge via PR into **`integration/merge-a`** on primary worktree.

**Must stay serial:** All of Merge A before **any** Phase 3 work.

### Sub-phases

#### Merge A.1 — Publish trait and `PublishPlan`

**Dependencies:** 2B (`IrSnapshot`), 1.5 (`FailurePolicy`).

**Tasks**

1. Define `PublishPlanner` trait or `fn plan_publish(prev: Option<&IrSnapshot>, next: &IrSnapshot, pol: &FailurePolicy) -> PublishPlan` in `ugwc-core` or `ugwc` (avoid cycles).
2. `PublishPlan` describes whether xDS should push / skip / full replace (v0 may only have “replace snapshot id”).

**Deliverables:** Unit tests for planner with fixture IR.

**Rust specifics:** `Arc<IrSnapshot>` for shared read-only views; writers hold `ArcSwap` or `RwLock`—benchmark later per scale doc.

**Acceptance criteria**

- [ ] Compile error path returns **skip publish** under default FailurePolicy.

#### Merge A.2 — Orchestrated reconcile loop

**Dependencies:** Merge A.1, 2A.2, 2C.1.

**Tasks**

1. On reconcile event: build `CompileInput` stub from watched object metadata (may be empty).
2. Call `compile` → on success, install new snapshot into **`ugwc-xds` SnapshotCache** interface.
3. On compile error: log + optional **status** patch stub in `ugwc-k8s`.

**Deliverables:** `crates/ugwc/src/run.rs` (or similar) wires tasks; if cycle risk, add `crates/ugwc-orchestrate` as a thin crate.

**Rust specifics:** `tokio::select!` between shutdown and controller stream; propagate `tracing` span IDs.

**Acceptance criteria**

- [ ] Integration or kind: synthetic event advances snapshot version/hash observable in xDS logs.

#### Merge A.3 — Optional status stub

**Dependencies:** Merge A.2.

**Tasks**

1. Minimal `Gateway` or `GatewayClass` condition **`Programmed=False`** when compile fails (or `Ready=Unknown`).
2. Ensure kube patch does not retry hot-loop on permanent errors (`kube-runtime` backoff).

**Deliverables:** Observable condition in `kubectl describe`.

**Rust specifics:** `kube::Api::patch_status` with proper `PatchParams`.

**Acceptance criteria**

- [ ] Single binary runs watch + compile stub + xDS stub together in CI or kind.

---

## Phase 3 — Multi-resource watches and trigger graph

**Goal:** Secondary objects enqueue **primary** reconciles (e.g. `HTTPRoute` → parent `Gateway`).

**Depends on:** Merge A.

**Outputs:** Deterministic fan-in; bounded queues under EndpointSlice churn (see [`scale-performance.md`](scale-performance.md) §3, §5).

**Affects downstream:** Mapper bugs → flaky conformance.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 4 **test fixtures only** once mapper signatures stable | Phase 4 compile depth |
| Policy watches **only if** compiler ignores policy CRDs until Phase 5 | Policy-aware compile (Phase 5) |

**Optional git worktrees:** `../ugwc-phase3-mappers` on `phase/3-mappers` vs `../ugwc-phase4-ir` on `phase/4-ir-http` — **only after** Merge A; rebase often onto shared `ugwc-core` API commits.

**Must stay serial:** Mapper graph before Phase 4 e2e; **EndpointSlice** watch before endpoint resolution tests.

### Sub-phases

#### 3.1 — Multi-resource `Controller` graph

**Dependencies:** Merge A.

**Tasks**

1. Register watches for `GatewayClass`, `Gateway`, `HTTPRoute`, **`Service`**, **`EndpointSlice`**.
2. Use **`kube-runtime`** mapper / owners pattern to enqueue a **Gateway-sized** reconcile key.
3. Log enqueued key + cause (`spec` update vs related resource).

**Deliverables:** `ugwc-k8s::mappers` with unit-testable pure functions.

**Rust specifics:** Mappers are **sync** + deterministic; avoid I/O here.

**Acceptance criteria**

- [ ] Parent ref changes reroute work to the correct Gateway key in unit tests.

#### 3.2 — Debounce / rate limits

**Dependencies:** 3.1.

**Tasks**

1. Debounce high-churn sources (`EndpointSlice`) per [`scale-performance.md`](scale-performance.md) §3.
2. Document bounded queue policy (coalesce vs drop) — ties to Phase 10 metrics later.

**Deliverables:** Tunable `Config` fields (durations, max queue depth if exposed).

**Rust specifics:** `Controller::rate_limits` / custom backoff from `kube_runtime`.

**Acceptance criteria**

- [ ] Soak harness: rapid endpoint updates do not grow reconciles without bound.

#### Phase 3 — Roll-up checklist

**Acceptance criteria**

- [ ] Route change triggers parent Gateway reconcile deterministically.
- [ ] Endpoint churn coalesced per documented policy.

---

## Phase 4 — Real IR: HTTP routing happy path (policies optional)

**Goal:** `Gateway` + `HTTPRoute` + `Service` + `EndpointSlice` → consistent **`IrSnapshot`** and status.

**Depends on:** Phase 3.

**Outputs:** Working L7 IR; golden fixtures.

**Affects downstream:** xDS resource names (Phase 7); IR stays **neutral** per [`output-distribution.md`](output-distribution.md) §6.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Golden YAML in `crates/ugwc-core/tests/fixtures/` | Policy merge (Phase 5) once IR extension points stable |
| Phase 7 snapshot goldens **for the same HTTP slice** once IR stable | Broader encoder work |

**Optional git worktrees:** `../ugwc-phase4-ir` on `phase/4-ir-http`.

**Must stay serial:** Status write rules vs future policy attachment if conflicts appear—coordinate with Phase 5 owner.

### Sub-phases

#### 4.1 — HTTPRoute → IR normalization

**Dependencies:** Phase 3.

**Tasks**

1. Normalize matches (host, path, headers, query); preserve **filter chain order**.
2. Emit listener + route tables for HTTP listeners on `Gateway`.

**Deliverables:** `ugwc-core` compiler module + golden tests.

**Rust specifics:** Pure functions; use `tracing` spans around compile stages from `ugwc`.

**Acceptance criteria**

- [ ] Goldens stable across runs (sorted slices deterministic).

#### 4.2 — Backend resolution

**Dependencies:** 4.1.

**Tasks**

1. Resolve Services + **ready** endpoints from `EndpointSlice`; enforce ref grants / namespaces.
2. Encode endpoints into IR without leaking k8s-specific structs into long-lived IR if avoidable (use domain structs).

**Deliverables:** Compile errors with stable reasons for **RefNotPermitted**, **BackendNotFound**, etc.

**Rust specifics:** Watch allocation churn (§8 scale doc)—reuse `Vec` capacity where safe in hot paths.

**Acceptance criteria**

- [ ] Missing backend → status reason + `CompileReport` entry.

#### 4.3 — Route/Gateway status

**Dependencies:** 4.1–4.2.

**Tasks**

1. Patch `Accepted`, `ResolvedRefs`, `Programmed` with machine-parseable **Reason** constants.
2. Aggregate multi-parent routes if applicable.

**Deliverables:** `ugwc-k8s::status` helpers.

**Rust specifics:** Use SSA or patch with proper **field manager** strings for server-side apply.

**Acceptance criteria**

- [ ] kind / integration proves HTTP happy path (proxy may wait for Phase 7 for full dataplane proof).

#### Phase 4 — Roll-up checklist

**Acceptance criteria**

- [ ] Manifest-level e2e in kind or scripted integration.
- [ ] Errors observable on Route/Gateway status.

---

## Phase 5 — Policy attachment and selected Policy CRDs

**Goal:** GEP-713-style attachment merged into IR; conflicts reported and surfaced on status.

**Depends on:** Phase 4.

**Outputs:** Policy-decorated IR + **enumerated v1 policy kinds**.

**Affects downstream:** xDS filters (Phase 7); RBAC (Phase 9).

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 10 metric scaffolding (names only) | — |
| Per-policy branches | **Merge** attachment engine before RC |

**Optional git worktrees:** `../ugwc-phase5-policy-<kind>` per policy family; keep list **serial** in design review.

**Must stay serial:** **Policy kind list lock** before Phase 9 RBAC generation.

### Sub-phases

#### 5.1 — Freeze v1 policy list

**Dependencies:** Phase 4.

**Tasks**

1. Close the CRD/RBAC list; update **Living document** section.
2. Sketch RBAC rules (`ClusterRole`/`Role`) aligned with watches.

**Deliverables:** Policy matrix in this doc + `ugwc-policy` crate docs.

**Rust specifics:** Parsing policy kinds via `strum` / `serde`.

**Acceptance criteria**

- [ ] Supported vs unsupported kinds explicit.

#### 5.2 — Attachment + merge engine

**Dependencies:** 5.1.

**Tasks**

1. Direct + inherited attachment; precedence table in code comments + tests.
2. Extend IR (`PolicyOverlayIr` or per-route bundles); avoid Envoy structs in IR.

**Deliverables:** Conflicts → `CompileReport` + Route/Gateway conditions.

**Rust specifics:** Deterministic sort orders for tie-breakers.

**Acceptance criteria**

- [ ] Each supported kind: unit + one integration vector minimum.

#### 5.3 — Policy status

**Dependencies:** 5.2.

**Tasks**

1. Report influenced objects on policy CRDs (where API allows).
2. Update skip matrix on deferrals.

**Deliverables:** Status helpers in `ugwc-k8s`.

**Acceptance criteria**

- [ ] Unsupported matrix kept current.

#### Phase 5 — Roll-up checklist

**Acceptance criteria**

- [ ] Policy-decorated IR compiles; conflicts visible in status and reports.

---

## Phase 6 — Inference Extension in watches and IR

**Goal:** Inference CRDs compile into IR fields for **EPP / ext_proc** (weights, criticality, pool selectors).

**Depends on:** Phase 4 minimum; **coordinate Phase 5** for policy-on-inference attachment.

**Outputs:** Inference-capable IR extensions.

**Affects downstream:** Phase 7 listener filters; Phase 13 verification.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Mock EPP servers in tests (`tonic`/`hyper`) | Phase 13 |
| Phase 7 encoders focused on inference slice | Generalized Delta work |

**Optional git worktrees:** `../ugwc-phase6-inference` on `phase/6-inference`.

**Must stay serial:** Single **API version pin** for Inference CRDs in `k8s-openapi` / `kube`.

### Sub-phases

#### 6.1 — Watches + validation

**Dependencies:** Phase 3 patterns.

**Tasks**

1. Add watches for `InferencePool` / `InferenceModel` (versions per upstream).
2. Surface invalid specs via `CompileReport`.

**Deliverables:** `ugwc-k8s::inference`.

**Rust specifics:** Optional `--features inference` on workspace if dependency weight warrants.

**Acceptance criteria**

- [ ] Watch streams healthy; parse errors identifiable in logs.

#### 6.2 — IR encoding

**Dependencies:** 6.1, Phase 4 IR.

**Tasks**

1. Map `extensionRef` → EPP metadata + **criticality** in IR.
2. Document pool endpoint deferral to EDS semantics.

**Deliverables:** Fixtures + unit tests (no kube in `ugwc-core` tests).

**Acceptance criteria**

- [ ] Phase 7 can encode EPP fields without importing `kube`.

#### Phase 6 — Roll-up checklist

**Acceptance criteria**

- [ ] IR fields reviewed with Phase 7 owner; documented in design index.

---

## Phase 7 — xDS encoding from IR

**Goal:** Valid **LDS/RDS/CDS/EDS** (SotW first); **Delta** gated. Constraints: [`output-distribution.md`](output-distribution.md) **per-scope version**; [`scale-performance.md`](scale-performance.md) **retain prior snapshot/hash** for incremental backends.

**Depends on:** Phase 4+ IR; Phases 5–6 as features demand.

**Outputs:** Envoy-compatible snapshots; `ugwc-xds` goldens.

**Affects downstream:** Conformance, proxy upgrades.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 10 metrics on publish path | Prod hardening narrative |
| Phase 12 suite harness (without full signal) | Release gating |

**Optional git worktrees:** `../ugwc-phase7-xds` on `phase/7-xds-encode`.

**Must stay serial:** **SotW golden tests green** before enabling Delta in CI; **global resource naming** review before multiple data planes consume xDS.

### Sub-phases

#### 7.1 — SotW encode

**Dependencies:** Phase 2C server, Phase 4 IR.

**Tasks**

1. `encode_sotw(ir: &IrSnapshot) -> XdsSnapshot` with stable names (`route`, `cluster`, `listener`).
2. Golden JSON tests (no cluster).

**Deliverables:** `ugwc-xds::encode` only depends on `ugwc-core` IR types.

**Rust specifics:** Avoid cloning large maps; align with §8 scale notes.

**Acceptance criteria**

- [ ] `/config_dump` or goldens validate L7 demo.

#### 7.2 — Snapshot cache + prev hash

**Dependencies:** 7.1.

**Tasks**

1. Store **previous** snapshot hash / struct for incremental push ([`scale-performance.md`](scale-performance.md) §2).
2. Document eviction / retention.

**Deliverables:** `SnapshotCache` API used by publish pipeline.

**Acceptance criteria**

- [ ] Version strings correlate with IR per-scope revision.

#### 7.3 — Delta (flagged)

**Dependencies:** 7.1–7.2.

**Tasks**

1. Optional Delta ADS when client + server capable; fallback SotW.
2. CI flag `UGWC_XDS_DELTA` (example) default **off**.

**Acceptance criteria**

- [ ] Delta remains off until SotW suite passes.

#### Phase 7 — Roll-up checklist

**Acceptance criteria**

- [ ] Snapshot goldens + optional live Envoy dump prove HTTP path.
- [ ] Delta explicitly feature-flagged.

---

## Phase 8 — High availability: leader election and publish gating

**Goal:** Multiple replicas safe: **one leader** publishes IR/xDS; followers idle or read-only; proxies **reconnect** after failover ([`scale-performance.md`](scale-performance.md) §6: HA ≠ horizontal throughput).

**Depends on:** Merge A; full value once Phase 7 publishes real config.

**Outputs:** HA runbook + Lease integration.

**Affects downstream:** Multi-replica conformance stability.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 9 **docs** draft | TLS defaults must follow threat model |
| Chaos tests in dedicated CI job | Must not block dev CI until feature-ready |

**Optional git worktrees:** `../ugwc-phase8-ha` on `phase/8-leader`.

**Must stay serial:** Leader gating of **publish** before running multi-replica conformance in Phase 12.

### Sub-phases

#### 8.1 — Lease election

**Dependencies:** Merge A.

**Tasks**

1. Watch `coordination.k8s.io/v1` **Lease**; become leader using `kube` lease helpers or `kube-runtime` patterns.
2. Wire **non-leader** behavior: pause watches vs drift-safe read-only (document choice).

**Deliverables:** `LeaderState` handle in `ugwc` run loop.

**Rust specifics:** `tokio::sync::watch` or channels to gate publisher task.

**Acceptance criteria**

- [ ] Exactly one active publisher in steady state (test with 2 replicas).

#### 8.2 — Publish gating + shutdown

**Dependencies:** 8.1, Phase 7 publish path.

**Tasks**

1. Only leader runs compile→publish path; followers skip xDS updates.
2. SIGTERM: cancel context → **drain** xDS (`tonic` graceful) with timeout → exit.

**Deliverables:** Documented client reconnect behavior to Service VIP.

**Rust specifics:** Share shutdown token across admin HTTP, kube controller, xDS server.

**Acceptance criteria**

- [ ] Failover test: leader loss → new leader resumes publish within bounded time.

#### Phase 8 — Roll-up checklist

**Acceptance criteria**

- [ ] Two-replica deployment tests pass; proxies survive leader bounce via reconnect.

---

## Phase 9 — Security model and hardening (production path)

**Goal:** Documented threat model; **TLS/mTLS** on xDS for real deployments; RBAC matches watches/writes.

**Depends on:** Phases 7–8 surfaces.

**Outputs:** [`security.md`](security.md) updates; hardened defaults.

**Affects downstream:** Phase 11 packaging; audits.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| External security review (human) | Release candidate |
| Phase 11 manifest drafting | Production install story |

**Optional git worktrees:** `../ugwc-phase9-sec` on `phase/9-security` (RBAC + TLS).

**Must stay serial:** **Threat model** approval before flipping defaults away from plaintext prototype.

### Sub-phases

#### 9.1 — Threat model + config profiles

**Dependencies:** Phase 1 TLS fields.

**Tasks**

1. Author threat model: who talks to xDS, expected identities, network boundaries ([`security.md`](security.md)).
2. Define **profile** enum: `dev` vs `prod` — reject `insecure + prod` combinations at startup.

**Deliverables:** Design doc PR + `Config` validation tests.

**Rust specifics:** `figment` / custom validation returning `anyhow::Error` or typed `ConfigError`.

**Acceptance criteria**

- [ ] Insecure xDS impossible without explicit prototype profile + warnings.

#### 9.2 — TLS/mTLS implementation

**Dependencies:** 9.1, Phase 2C server.

**Tasks**

1. Load certs using Rustls (`tonic` + tls feature) or `tokio-rustls` stack; document cipher/TLS version policy.
2. Optional client cert verification for mTLS.

**Deliverables:** Helm/Kustomize volume mounts in Phase 11 reference these paths.

**Acceptance criteria**

- [ ] Integration test with generated certs (small PKI fixture) proves handshake.

#### 9.3 — RBAC tightening

**Dependencies:** Phases 3–6 watch set frozen.

**Tasks**

1. Generate least-privilege `Role`/`ClusterRole` for ServiceAccount actually used.
2. Split roles if webhooks / writers appear later.

**Deliverables:** YAML in `deploy/` reviewed against watch verbs.

**Acceptance criteria**

- [ ] RBAC manifest matches `kube::Api` usage list in code (script or audit checklist).

#### Phase 9 — Roll-up checklist

**Acceptance criteria**

- [ ] No implicit plaintext for non-prototype profiles; RBAC reviewed.

---

## Phase 10 — Observability

**Goal:** Operators can separate **watch lag** vs **compile failure** vs **publish/xDS failure** ([`observability-telemetry.md`](observability-telemetry.md)).

**Depends on:** Merge A; spikes after Phase 7 publish path.

**Outputs:** Metrics + tracing sufficient for conformance triage.

**Affects downstream:** SLOs, dashboards.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 5 policy work **if** IR extension points versioned / feature-flagged | RC quality bar |
| Phase 12 harness | — |

**Optional git worktrees:** `../ugwc-phase10-otel` on `phase/10-obs`.

**Must stay serial:** **Metric name registry** — align names before dashboard freeze ([`observability-telemetry.md`](observability-telemetry.md)).

### Sub-phases

#### 10.1 — Metrics

**Dependencies:** Merge A (reconcile loop), Phase 8 (leader).

**Tasks**

1. Export counters/histograms: reconcile latency, compile errors by kind/GVK, publish latency, xDS streams, leader gauge.
2. Use **`metrics`** + `metrics-exporter-prometheus` or OpenTelemetry metrics SDK (pick one stack for the repo).

**Deliverables:** `/metrics` on admin HTTP (or separate bind) guarded in prod.

**Rust specifics:** Avoid high-cardinality labels (`resource name` only if bounded).

**Acceptance criteria**

- [ ] Scripted scenario distinguishes lag vs compile vs publish via metric queries.

#### 10.2 — Tracing

**Dependencies:** 10.1.

**Tasks**

1. `tracing` spans covering reconcile → compile → publish; link **snapshot version** attribute.
2. Optional OTLP export behind env.

**Deliverables:** Trace context propagated across `kube-runtime` + `tonic` via `tracing-opentelemetry`.

**Acceptance criteria**

- [ ] Single trace shows end-to-end pipeline for a synthetic reconcile.

#### Phase 10 — Roll-up checklist

**Acceptance criteria**

- [ ] Observability doc updated with metric names and exemplar dashboards (if any).

---

## Phase 11 — Packaging and install

**Goal:** Another engineer installs controllers + CRDs via **Helm** or **Kustomize** without tribal knowledge.

**Depends on:** Phases 8–9 for realistic probes/TLS/SecurityContext.

**Outputs:** `deploy/` or `charts/ugwc`; user-facing env reference.

**Affects downstream:** Phase 12 installs use these artifacts.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Docs-only edits on install paths | RC declaration |
| Phase 12 CI wiring **using** these manifests | Conformance job green |

**Optional git worktrees:** `../ugwc-phase11-deploy` on `phase/11-packaging`.

**Must stay serial:** Chart values must match **Phase 1 `Config` env keys** before public release.

### Sub-phases

#### 11.1 — Choose Helm vs Kustomize

**Dependencies:** Phase 1 probe ports known.

**Tasks**

1. Pick primary packaging; optional second-class path if maintenance allows.
2. Deployment: ServiceAccount + RBAC from Phase 9, **admin** Service + **xDS** Service ports, probes, resource limits.

**Deliverables:** Versioned chart or `kustomize build` instructions.

**Acceptance criteria**

- [ ] Smoke: fresh kind cluster installs from documented steps.

#### 11.2 — User-facing contract

**Dependencies:** 11.1.

**Tasks**

1. Document required CRD installs, `GatewayClass` example (`controllerName`), env reference.
2. Upgrade notes: CRD skew, leader failover, proxy reconnect.

**Acceptance criteria**

- [ ] Install README section lives **only** in existing design docs if no repo README (per project policy).

#### Phase 11 — Roll-up checklist

**Acceptance criteria**

- [ ] Repeatable install validated by non-author engineer (buddy test).

---

## Phase 12 — Gateway API conformance

**Goal:** Upstream **Gateway API** conformance suite runs against this implementation with a published report artifact.

**Depends on:** Phases 4–7 for signal; **Phases 5–8** reduce noise to “known gaps” vs infra flakes.

**Outputs:** CI job + skip list contract.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 0–1 **kind** scaffolding | Full suite execution |
| Inference / policy side efforts **if** skip list handles them | RC gating |

**Optional git worktrees:** `../ugwc-phase12-conformance` on `phase/12-conformance` (upstream suite + wrappers).

**Must stay serial:** **Skip list approval** with PM/TL before declaring “passing with known limitations.”

### Sub-phases

#### 12.1 — Pin upstream version

**Dependencies:** None (can start early).

**Tasks**

1. Submodule or version pin for `gateway-api` / conformance image tag; document in `Makefile` / CI env.

**Deliverables:** Reproducible `make conformance` target.

**Acceptance criteria**

- [ ] Local or CI can checkout identical suite revision.

#### 12.2 — CI automation

**Dependencies:** Phase 11 install artifacts, Phases 4+ features.

**Tasks**

1. kind (or k3d) cluster, install CRDs, deploy **ugwc**, install `GatewayClass`, run upstream test entrypoint.
2. Upload junit/html/text artifacts in CI.

**Rust specifics:** Go/Rust test harness interoperability via **shell out** to upstream Go suite is acceptable; Rust-native reimplementation **not** required.

**Acceptance criteria**

- [ ] CI publishes pass/fail + artifact; reruns deterministic modulo known flakes.

#### 12.3 — Skip matrix governance

**Dependencies:** 12.2.

**Tasks**

1. Maintain `conformance-skips.yaml` (example name) listing skip + issue link + reason.
2. Tie skips to Phase owners for remediation.

**Acceptance criteria**

- [ ] Every skip documented; no silent failures.

#### Phase 12 — Roll-up checklist

**Acceptance criteria**

- [ ] Quality gate reads green/skip policy before release tags.

---

## Phase 13 — Inference Extension verification

**Goal:** Inference behavior guarded by CI (upstream suite if exists; else bespoke e2e).

**Depends on:** Phase 6 IR + Phase 7 xDS effects.

**Outputs:** CI signal distinct from Phase 12.

### Parallelism

| Concurrent with | Serial after |
|-----------------|--------------|
| Phase 12 if resources allow | — |

**Optional git worktrees:** `../ugwc-phase13-infer-verify` on `phase/13-inference-verify`.

**Must stay serial:** Agreement on **mock EPP** interface vs real sidecar—document once to avoid duplicated harnesses.

### Sub-phases

#### 13.1 — Discover upstream tests

**Dependencies:** Phase 6.

**Tasks**

1. If Inference Extension ships conformance tests, wrap like Phase 12; else plan bespoke.

**Deliverables:** Decision recorded in design index.

#### 13.2 — Bespoke e2e (if needed)

**Dependencies:** Phase 7.

**Tasks**

1. kind + InferencePool/Model manifests + **mock EPP** (Rust test service) + Envoy validating headers/routes.

**Acceptance criteria**

- [ ] At least one CI case covers pool/model → programmed path **or** documented skip with issue.

#### Phase 13 — Roll-up checklist

**Acceptance criteria**

- [ ] Inference release confidence articulated in CHANGELOG / release notes (existing process).

---

## Parallelization cheat sheet

| When | What can run in parallel | Suggested worktrees / branches |
|------|---------------------------|--------------------------------|
| After Phase 1 | **2A** kube (`../ugwc-lane-2a`, `lane/2a-kube`), **2B** IR (`../ugwc-lane-2b`, `lane/2b-ir`), **2C** xDS (`../ugwc-lane-2c`, `lane/2c-xds`) | See **Parallel window 1** |
| After Merge A | Phase **3** mappers (`../ugwc-phase3-mappers`) vs Phase **4** IR depth (`../ugwc-phase4-ir`); Phase **7** goldens once IR stable for a slice | Rebase frequently; share `ugwc-core` API |
| Mid-project | **10** observability vs **5** policy kinds **only if** IR extension points are versioned or feature-flagged | `../ugwc-phase10-otel`, `../ugwc-phase5-policy-*` |
| Conformance | Cluster scaffolding in Phase 0–1; **full Gateway suite** after **4–7** solid | `../ugwc-phase12-conformance` |
| HA / security | Phase **8** leader (`../ugwc-phase8-ha`) vs Phase **9** docs early; **merge** before RC | Serial on threat-model approval |

**Stay serial:** Merge A integration; policy kind list lock before RBAC blast radius; SotW before Delta; threat model before prod TLS defaults.

## Dependency chain (outputs → consumers)

1. **IR snapshot scope and versioning** → xDS sharding, resource naming, status aggregation, HA behavior.
2. **Watch mappers** → conformance stability and perceived “lag”.
3. **Enumerated policy kinds** → RBAC, compiler, xDS “supported features” matrix.
4. **xDS naming and ADS semantics** → golden tests and proxy upgrades.
5. **FailurePolicy defaults** → user-visible status and safety during partial compile.

---

## Living document

Update this file when:

- v1 policy list or CRD scope changes,
- IR snapshot scope changes,
- conformance skip list changes,
- security defaults move out of prototype.

When code exists, link concrete packages in [`control-plane-design-index.md`](control-plane-design-index.md) under “Related code”.

**Praxis / gwxds:** Configure the Praxis data-plane xDS client to use ugwc's ADS listen address (the same operational slot as Istio's `ISTIOD_ADDR`). Praxis subscribes for `type.googleapis.com/istio.gwxds.Resource`; run ugwc with `UGWC_XDS_WIRE=gwxds` (the default) so ADS returns prost-encoded `istio.gwxds.Resource` batches rather than the optional Envoy RDS wire.

**Implementation snapshot (workspace):** Cargo workspace + CI (`Makefile`, `.github/workflows`) are live; crate graph `ugwc` / `ugwc-core` / `ugwc-k8s` / `ugwc-xds`. Full multi-worktree lane simulation was **not** used for this drop — changes landed sequentially on one branch while respecting the documented serial gates (Phase 0→1 before 2A/2B/2C, Merge A before Phase 3+, **SotW before Delta**, threat-model text before prod TLS defaults).
