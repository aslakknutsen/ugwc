# Operations

Install prerequisites, cluster CRDs, and minimal controller configuration for **ugwc**.

## CRD install order

Install bundles in dependency order for your feature set:

1. **Gateway API — standard channel** — Required for `Gateway`, `GatewayClass`, `HTTPRoute`, `GRPCRoute`, `TLSRoute`, `ListenerSet`, `ReferenceGrant`, etc. Use a **v1.5.x** [`standard-install.yaml`](https://github.com/kubernetes-sigs/gateway-api/releases) (see [Gateway API getting started](https://gateway-api.sigs.k8s.io/guides/getting-started/)).

2. **Gateway API — experimental channel** — Required only if you use **`TCPRoute`** or **`UDPRoute`** (`gateway.networking.k8s.io/v1alpha2`). Apply [`experimental-install.yaml`](https://github.com/kubernetes-sigs/gateway-api/releases) from the same release line (or a bundle that includes those CRDs). If these CRDs are absent, ugwc logs and skips listing those types.

3. **`BackendTLSPolicy`** — Included in experimental / extended Gateway API bundles per upstream packaging; required when using upstream TLS settings from `BackendTLSPolicy` to **Service** or **InferencePool** backends (`gateway.networking.k8s.io/v1`).

4. **Inference Extension** — Required when routes reference **`InferencePool`** backends (`inference.networking.k8s.io/v1`). Install [`manifests.yaml`](https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases) from a [pinned release tag](https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases) (for CI/scripts, resolving latest stable tag is fine; fleets should pin).

Optional alpha CRDs under **`inference.networking.x-k8s.io`** (e.g. **InferenceObjective**) are **not** consumed by ugwc today — see [`todo-gateway-api-inference-parity.md`](../design/todo-gateway-api-inference-parity.md).

## Controller prerequisites

- **`GatewayClass.spec.controllerName`** must appear in **`UGWC_CONTROLLER_NAMES`** (comma-separated list). Gateways using other classes are ignored.
- **Kubernetes reconciliation** requires **`UGWC_KUBE_ENABLE=true`** (see env table below).
- **Optional per-gateway Praxis provisioning** uses implementation profiles and **`UGWC_DATAPLANE_ENABLE`**; see [`dataplane-provisioning.md`](../design/dataplane-provisioning.md).

## Environment variables (essentials)

Configuration uses the **`UGWC_`** prefix (`serde` snake_case → upper snake env names). Inspect the effective config after exporting variables:

```bash
cargo run -p ugwc -- --dump-config
```

| Variable | Purpose |
|----------|---------|
| `UGWC_CONTROLLER_NAMES` | Comma-separated `controllerName` values this instance owns (required non-empty). |
| `UGWC_KUBE_ENABLE` | Enable cluster watches and reconciliation (`true` / default false in many dev setups). |
| `UGWC_ADMIN_LISTEN_ADDR` | Admin HTTP (metrics, health), default `127.0.0.1:8080`. |
| `UGWC_XDS_LISTEN_ADDR` | gRPC xDS bind address, default `127.0.0.1:18000`. |
| `UGWC_XDS_INSECURE` | TLS off for xDS (disallowed with `UGWC_RUN_PROFILE=prod`). |
| `UGWC_XDS_WIRE` | `gwxds` (Praxis) or `envoy` wire encoding. |
| `UGWC_IMPLEMENTATION_PROFILES_PATH` | JSON file: map `controllerName` → dataplane strategy / subscription defaults / `proxy_image`. |
| `UGWC_DATAPLANE_ENABLE` | Apply **PerGatewayPraxis** workloads when profile requests it; requires bootstrap addr when needed. |
| `UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR` | Host:port provisioned proxies use to dial xDS. |
| `UGWC_ENDPOINT_DEBOUNCE_MS` | Coalesce **EndpointSlice** churn before cluster-wide gateway enqueue (default `200`). |
| `UGWC_LEADER_ELECTION` | Leader-only publish / status / dataplane apply when enabled. |

Full schema and validation live in [`crates/ugwc/src/config.rs`](../crates/ugwc/src/config.rs).

## Compatibility pins

See [architecture.md — Compatibility pins](architecture.md#compatibility-pins).

## See also

- [architecture.md](architecture.md) — Watch graph, reconcile flow, BackendTLS limitations.
- [design.md](design.md) — IR and publish model.
