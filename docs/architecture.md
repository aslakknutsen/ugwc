# Architecture

## Workspace

Rust workspace (`Cargo.toml` `[workspace]`) with these members:

| Crate | Role |
|-------|------|
| **ugwc** | Binary: CLI, config from environment, async runtime wiring (admin HTTP, xDS server, optional Kubernetes controller), leader/metrics hooks. |
| **ugwc-core** | Pure pipeline: cluster snapshot types, HTTP/Gateway-oriented IR, `compile`, `plan_publish`, `FailurePolicy` — **no** Kubernetes or xDS imports as the integration seam. |
| **ugwc-k8s** | `kube` / `kube-runtime`: watches, mappers from API objects into core inputs, status updates. |
| **ugwc-xds** | gRPC (tonic): ADS-style serving from a shared IR snapshot cache; security and wire options configured by the binary. |
| **ugwc-gwxds** | Encodes ugwc IR to `istio.gwxds` protobuf (build-time `prost`); used for Praxis-oriented distribution paths. |

## Gateway API compatibility

Target **Gateway API** bundle: **v1.5.x standard channel** for core v1 resources (see [Gateway API releases](https://github.com/kubernetes-sigs/gateway-api/releases)). Install e.g. `standard-install.yaml` for the pinned minor.

- **Standard channel:** `Gateway`, `GatewayClass`, `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `ListenerSet`, `ReferenceGrant`, and related v1 types ugwc reconciles (HTTPS listeners terminating TLS via `listener.tls.certificateRefs` → `Secret` with `tls.crt` / `tls.key`; optional `Gateway.spec.addresses`; `Gateway.spec.infrastructure.labels` merged into per-gateway Praxis workload labels).
- **Experimental Gateway API CRDs:** **`TCPRoute`** and **`UDPRoute`** (`gateway.networking.k8s.io/v1alpha2`) require the **experimental** install bundle (`experimental-install.yaml` or equivalent). If those CRDs are missing, ugwc logs and skips listing those types.
- **Backend TLS:** extended-channel installs must include **`BackendTLSPolicy`** **`gateway.networking.k8s.io/v1`** when using upstream TLS to Services / InferencePools per policy.
- **Inference Extension:** install [`inference.networking.k8s.io`](https://gateway-api-inference-extension.sigs.k8s.io/guides/) CRDs when using **`InferencePool`** backends.

Operator install order and env summaries: [operations.md](operations.md).

**ReferenceGrant:** Cross-namespace references require grants in the **referenced resource namespace** — attachment of routes from namespace A to a `Gateway` or attached `ListenerSet` in namespace B, `Service` / `InferencePool` backends, and cross-namespace TLS `Secret` refs on listeners are enforced during snapshot load / compile. The `ReferenceGrant` watcher enqueues gateways in the grant’s namespace plus gateways referenced by routes living in each `spec.from[].namespace` (cluster-wide route listing remains the fallback when `from` is empty).

**Backend TLS:** `BackendTLSPolicy` objects are listed for namespaces of backend resources needed by attached routes (see below). Validation material is resolved at snapshot load time and attached on the compiled IR; **Service** backends use snapshot keys `namespace/name:servicePort`, **InferencePool** backends use `inferencepool/namespace/name:targetPort`. `ugwc-gwxds` maps this to `Backend.tls` (including optional client certificate material for backend mTLS). Conflicting policies or bad refs surface on policy **ancestor** status toward each reconciled `Gateway` and as **`ResolvedRefs`** errors on affected routes when compilation fails closed.

**BackendTLSPolicy limitations (operator-facing):**

1. **Targets:** Core path is **`Service`** `targetRefs` in the **same namespace** as the policy. **`InferencePool`** targets are also supported when those pools are referenced as backends on routes for this Gateway. Other `targetRef` kinds are ignored until explicitly implemented.
2. **CA / trust:** **`caCertificateRefs`** — `ConfigMap` (`ca.crt`) and **`Secret`** (`ca.crt`, or `tls.crt` as a PEM certificate bundle) in the policy namespace, or **cross-namespace** when a **`ReferenceGrant`** in the CA resource namespace allows `from: kind BackendTLSPolicy, namespace: <policy ns>` to `to: kind ConfigMap|Secret` (name optional/wildcard per grant rules). **`wellKnownCACertificates: System`** is supported; other well-known values are rejected unless added explicitly.
3. **Client certificate:** Optional **`clientCertificateRefs`** on the policy (`Secret` with `tls.crt` / `tls.key`, same-namespace or cross-namespace via **`ReferenceGrant`** with `to: Secret`) populate upstream mTLS identity in IR/xDS.
4. **Route coverage:** Backend Services referenced only from **HTTPRoute**/**GRPCRoute** rule **`filters`** (**RequestMirror**, **ExternalAuth**) or nested **`backendRefs[].filters`** are included in snapshot loading and in **`BackendTLSPolicy` → Gateway reconcile** fan-out (same as rule-level `backendRefs`).
5. **`spec.options`:** Not forwarded to the dataplane yet.

## Compatibility pins

Best-effort reference for support expectations (CI and manual tests track Gateway API **v1.5.x** standard + experimental where coded). Exact pins should converge with release notes over time.

| Axis | Pin / note |
|------|------------|
| **ugwc** | Workspace version (e.g. `0.1.0` in root `Cargo.toml`) or `main` at a given commit — treat as development until release tagging is formalized. |
| **Gateway API** | Standard **v1.5.x** for ListenerSet and core v1 routes; **experimental** bundle for **TCPRoute** / **UDPRoute** (`v1alpha2`). |
| **Inference Extension** | Release **`manifests.yaml`** from [gateway-api-inference-extension releases](https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases) when using **InferencePool** — pin a tag in your fleet docs. |
| **Dataplane** | Implementation-specific (e.g. Praxis / Envoy build that speaks `gwxds`); not pinned in-repo. |

## Diagrams

### Logical components

Process boundary is the **`ugwc` binary**. It hosts long-lived Tokio tasks; **ugwc-core** has no async runtime of its own — it is invoked synchronously from those tasks.

```mermaid
flowchart TB
    subgraph clients["Data plane & ops"]
        DP["xDS clients\n(Envoy / agents)"]
        Prom["Prometheus / metrics scraper"]
        AdminC["Admin HTTP client"]
    end

    subgraph ugwc_bin["ugwc binary"]
        Admin["admin::serve\n(metrics + health)"]
        XdsSrv["ugwc_xds::serve\ngRPC ADS"]
        Cache["MultiScopeSnapshot\n(global + per-scope)"]
        Ctrl["run_gateway_controller\n(optional)"]
        Recon["Reconcile closure\nper Gateway ns/name"]
    end

    subgraph core["ugwc-core (sync)"]
        Compile["compile"]
        Plan["plan_publish"]
        FP["FailurePolicy"]
    end

    subgraph k8s_crate["ugwc-k8s"]
        Watch["Watchers:\nGateway, HTTPRoute, GRPCRoute, TLSRoute,\nTCPRoute, UDPRoute, ListenerSet,\nReferenceGrant, BackendTLSPolicy,\nConfigMap + Secret (TLS/BackendTLS),\nInferencePool, EndpointSlice, GatewayClass"]
        Load["load_cluster_snapshot"]
        Status["apply_*_status"]
    end

    K8sAPI["Kubernetes API"]

    DP <-->|"gRPC xDS"| XdsSrv
    Prom --> Admin
    AdminC --> Admin
    XdsSrv --> Cache
    Watch --> K8sAPI
    Load --> K8sAPI
    Status --> K8sAPI
    Ctrl --> Watch
    Ctrl -->|"mpsc (ns, name)"| Recon
    Recon --> Load
    Recon --> Compile
    Recon --> Plan
    Compile --> FP
    Plan --> FP
    Plan -->|"FullReplace"| Cache
    Recon --> Status
```

### Crate dependency graph

```mermaid
flowchart LR
    bin["ugwc\n(binary)"]
    core["ugwc-core"]
    k8s["ugwc-k8s"]
    xds["ugwc-xds"]
    gwx["ugwc-gwxds"]

    bin --> core
    bin --> k8s
    bin --> xds
    k8s --> core
    xds --> core
    xds --> gwx
    gwx --> core
```

`ugwc-gwxds` implements IR → `istio.gwxds` protobuf encoding; the **`ugwc` binary pulls it in transitively** via `ugwc-xds`.

### Runtime: startup and concurrent tasks

```mermaid
flowchart TD
    Start([main / run]) --> Config[Load Config from env]
    Config --> Metrics[metrics::install]
    Metrics --> Leader[LeaderState:\nelection stub or always-leader]
    Leader --> InitCompile["compile(CompileInput\ndefault / empty gateway)"]
    InitCompile --> CacheCreate["MultiScopeSnapshot::new\n(global stub IrSnapshot)"]
    CacheCreate --> ShutdownTok["CancellationToken::new"]
    ShutdownTok --> SpawnAdmin[spawn admin_task]
    ShutdownTok --> SpawnXds[spawn xds_task]
    SpawnXds --> CacheClone["xDS uses XdsSnapshotSource\n(node.id → scope)"]
    SpawnAdmin --> KubeCheck{kube_enable?}
    KubeCheck -->|no| WaitOnly[wait_shutdown:\nadmin + xds]
    KubeCheck -->|yes| KubeClient[kube::Client::try_default]
    KubeClient -->|fail| WaitOnly
    KubeClient -->|ok| SpawnKube[spawn kube_task:\nrun_gateway_controller + ReconcileFn]
    SpawnKube --> WaitAll[wait_shutdown:\nadmin + xds + kube]
    WaitOnly --> Sig[ctrl_c]
    WaitAll --> Sig
    Sig --> Drain[cancel tokens,\njoin with timeout]
```

### Kubernetes controller: watch fan-in

Several parallel watchers enqueue the same reconcile key `(namespace, gateway_name)`. A single consumer loop `recv`s keys and invokes the shared **`ReconcileFn`**.

```mermaid
flowchart LR
    subgraph watchers["Parallel watcher tasks"]
        W1["Gateway watcher\n→ (ns, name)"]
        W2["HTTPRoute / GRPCRoute / TLSRoute /\nTCPRoute / UDPRoute\n→ parent Gateway keys"]
        W3["ListenerSet / ReferenceGrant /\nBackendTLSPolicy / ConfigMap /\nSecret / InferencePool\n→ affected gateways\n(ReferenceGrant: grant ns\n+ route namespaces\nfrom spec.from)"]
        W4["EndpointSlice watcher\n→ debounce → list all Gateways\n→ enqueue each"]
        W5["GatewayClass watcher\n→ owned class → list Gateways\nof that class → enqueue"]
    end

    Q["mpsc channel\n256 capacity"]
    Loop["Main loop:\nreconcile(ns, name)"]

    W1 --> Q
    W2 --> Q
    W3 --> Q
    W4 --> Q
    W5 --> Q
    Q --> Loop
```

### Reconcile sequence (per Gateway)

This matches the closure wired in `ugwc::run`: load snapshot → compile → plan → status patches → optional cache update.

```mermaid
sequenceDiagram
    participant Loop as controller main loop
    participant API as Kubernetes API
    participant Load as ugwc_k8s::load_cluster_snapshot
    participant Core as ugwc_core::compile
    participant Plan as plan_publish
    participant Cache as MultiScopeSnapshot
    participant St as status writers
    participant Xds as xDS server tasks

    Loop->>API: GET Gateway (latest)
    Loop->>Load: load_cluster_snapshot(client, ns, name)
    Load->>API: list/get related objects
    Load-->>Loop: ClusterSnapshot or None
    Loop->>Core: compile(FailurePolicy, CompileInput)
    Core-->>Loop: CompileOutcome (report + IrSnapshot)
    alt not leader
        Loop-->>Loop: skip publish status provision
    else leader
        Loop->>Cache: previous_for_scope(next.scope)
        Loop->>Plan: plan_publish(prev, next, policy)
        Plan-->>Loop: PublishPlan (action)
        Loop->>St: apply_gateway_reconciled_status
        Loop->>St: merged parent status (HTTP GRPC TLS TCP UDP routes)
        Loop->>St: apply_listenerset_reconciled_status
        Loop->>St: apply_backend_tls_policies_for_gateway
        Loop->>St: apply_inference_pools_for_gateway
        St->>API: PATCH status subresources
        alt PublishAction::FullReplace
            Loop->>Cache: store_scoped(new IrSnapshot)
        end
        Note over Loop: optional dataplane provision (profile)
    end
    Note over Cache,Xds: xDS resolves node.id to per-scope or global snapshot
```

### Compile → publish → cache → xDS (data flow)

```mermaid
flowchart LR
    subgraph input["Inputs"]
        GW["Gateway + routes +\nendpoints in API"]
    end

    subgraph pipeline["Core pipeline"]
        CI["CompileInput\n(cluster + reconcile_gateway)"]
        C["compile →\nIrSnapshot + CompileReport"]
        PP["plan_publish\n(prev, next, FailurePolicy)"]
    end

    subgraph actions["PublishAction"]
        Skip["Skip"]
        Full["FullReplace"]
        Delta["Delta\n(stub / future)"]
    end

    Store["MultiScopeSnapshot::store_scoped"]
    Read["Snapshot readers\n(xDS encode path)"]

    GW --> CI
    CI --> C
    C --> PP
    PP --> Skip
    PP --> Full
    PP --> Delta
    Full --> Store
    Store --> Read
```

## Runtime shape

At a high level:

1. **Startup** — Load config, initialize logging/metrics, optionally elect leader via Kubernetes `Lease` (or assume leader when election is off). `/readyz` stays **503** until the CRD supervisor completes its initial list and starts watchers (`ugwc_cache_synced=1`).
2. **IR cache** — `MultiScopeSnapshot`: a global snapshot plus per-gateway keys; xDS selects by `node.id`.
3. **Compile loop** — Core `compile` consumes `CompileInput` (from fixtures, tests, or cluster-derived snapshots) and produces `CompileOutcome` + snapshot.
4. **Publish planning** — `plan_publish` compares previous and next snapshots with `FailurePolicy` to decide actions (full replace, skip, etc.).
5. **Kubernetes path** (when enabled) — Controller reconciles Gateway (and related) objects, feeds the compiler, updates shared cache and status.
6. **xDS path** — Server streams encoded resources from the cache to connected proxies.

Admin and Prometheus endpoints run alongside these services for health and observability. On shutdown (SIGTERM/Ctrl-C), readiness clears first, in-flight reconciles drain (up to ~30s), then watches and servers stop. See [observability.md](observability.md).

## Layering rules

- **ugwc-core** stays the place for deterministic, testable transforms: IR types, compile reports, publish plans.
- **ugwc-k8s** translates API machinery into what core expects; it does not reimplement routing semantics that belong in core.
- **ugwc-xds** maps IR to xDS wire messages; Envoy-specific structs are not the IR itself.

## Related material

Design rationale and phased delivery: [design.md](design.md), [operations.md](operations.md), [observability.md](observability.md), and [`../design/`](../design/) for longer-form notes.
