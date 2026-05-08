# Design

## Problem framing

Data planes differ: Envoy-style proxies want **xDS**; others may want Kubernetes-native writes, pull APIs, or vendor CRDs. Coupling the control plane to one wire format limits portability.

**Approach:** One **internal representation (IR)** is the compiled truth after resolving Gateway API (and related) objects. **Output backends** encode and deliver that snapshot; xDS is the primary v1 backend, not the definition of the domain model.

## Core concepts

1. **IR snapshot** — Immutable, Kubernetes-agnostic configuration for a bounded scope (e.g. gateways under a class or shard), versioned for compatibility (`IR_VERSION` in core).
2. **Compile** — `CompileInput` → `CompileOutcome` with a `CompileReport`: pure logic, fixture-testable without a live cluster.
3. **Publish plan** — `plan_publish` takes prior and next snapshots plus `FailurePolicy` and yields `PublishAction`s so the runtime does not hard-code “always push” / “fail closed” behavior in one place.
4. **Failure policy** — Extension point for how partial compile failures interact with publishing (registry + data in core; behavior can grow over time).

## Kubernetes integration

- **GatewayClass** and controller configuration define which gateways this instance owns (see implementation plan for v1 defaults: controller name, env-based profile).
- **Status** — Reconciler contributes conditions that reflect compile and distribution health where supported (`ugwc-k8s`).

## Security and operations

- xDS may be TLS-terminated or insecure for prototypes; production expects an explicit security model (TLS, authn/z), not accidental plaintext defaults.
- Metrics and structured logging (`tracing`) are first-class; secrets must not appear in logs.

## Relation to `design/`

The repository root **`design/`** directory holds the working design set: implementation plan, output-distribution model, controller architecture, etc. This **`docs/`** tree is a shorter, stable-facing summary; when they disagree, **`design/`** may be ahead of the code or describe future phases — verify against the crates.

## See also

- [architecture.md](architecture.md) — crate map and runtime flow.
- [main.md](main.md) — index.
