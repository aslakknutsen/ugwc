# Example: standard Gateway API + BackendTLSPolicy + TLS

This walks through the sample under [`examples/gateway-standard-plus-tls/`](../examples/gateway-standard-plus-tls/) (kustomize bundle).

## Prerequisites

1. **Gateway API — standard channel (v1.5.x)** — Install [`standard-install.yaml`](https://github.com/kubernetes-sigs/gateway-api/releases) so you have `Gateway`, `GatewayClass`, `HTTPRoute`, `GRPCRoute`, `ListenerSet`, `ReferenceGrant`, etc.

2. **`BackendTLSPolicy` v1** — Must be present (`gateway.networking.k8s.io/v1`). Upstream packages this with the **experimental** or **extended** Gateway API install for a given release line; apply the bundle that includes `BackendTLSPolicy` for the same minor as your standard install. See [operations.md — CRD install order](operations.md#crd-install-order) §3.

This sample does **not** use `TCPRoute`, `UDPRoute`, or `InferencePool`.

## Configure ugwc

1. Set **`GatewayClass.spec.controllerName`** in `01-gatewayclass.yaml` to a value you own, e.g. `example.com/ugwc-sample`, and export:

   ```bash
   export UGWC_CONTROLLER_NAMES=example.com/ugwc-sample
   ```

2. Enable reconciliation and (for local debugging) plaintext xDS:

   ```bash
   export UGWC_KUBE_ENABLE=true
   export UGWC_XDS_INSECURE=true
   export UGWC_XDS_LISTEN_ADDR=127.0.0.1:18000
   export UGWC_XDS_WIRE=envoy   # or gwxds
   ```

3. Run ugwc (from repo root):

   ```bash
   cargo run -p ugwc
   ```

## Apply the sample

```bash
kubectl apply -k examples/gateway-standard-plus-tls/
```

Watch ugwc logs for skipped CRD list errors; resolve by installing the missing CRD bundle.

## xDS node id (scoped snapshots)

The xDS server resolves a stream by `DiscoveryRequest.node.id` (see [`MultiScopeSnapshot::resolve_for_discovery`](../crates/ugwc-xds/src/cache.rs)): empty, `global`, or `*` uses the global snapshot; otherwise the id is treated as a scope key, typically **`namespace/name`** of the reconciled `Gateway` matching [`IrScopeId::new`](../crates/ugwc-core/src/ir.rs) (`ugwc-sample-gateway/demo-gateway` for this sample).

## xDS dump CLI (debugging)

`ugwc xds-dump` opens **ADS** `StreamAggregatedResources` against a **plaintext** `http://` endpoint (matches typical `UGWC_XDS_INSECURE=true`).

Examples:

```bash
# Envoy-style wire (empty type_url on the request; server returns concatenated L/R/C/E resources)
cargo run -p ugwc -- xds-dump --addr http://127.0.0.1:18000 --wire envoy --node-id ugwc-sample-gateway/demo-gateway

# Praxis gwxds wire (request type_url istio.gwxds.Resource)
cargo run -p ugwc -- xds-dump --addr http://127.0.0.1:18000 --wire gwxds --node-id ugwc-sample-gateway/demo-gateway

# Same, but decode each `Any` as `istio.gwxds.Resource` and print Debug (full message, including PEM fields)
cargo run -p ugwc -- xds-dump --addr http://127.0.0.1:18000 --wire gwxds --node-id ugwc-sample-gateway/demo-gateway --decode
```

Flags: `--iterations N` (default 3) sends that many `DiscoveryRequest` messages; today’s server responds with one `DiscoveryResponse` per request. `--type-url` overrides `--wire`’s default `type_url`.

**Limitations:** The server does not implement full ADS ACK/nonce behaviour yet ([`design/TODO-best-practices.md`](../design/TODO-best-practices.md)); this command is for **inspecting** `version_info` and packed `Any` sizes, not for validating a production Envoy client.

## Manual verification checklist

1. **Apply order** — `kubectl apply -k` applies `kustomization.yaml` resources in list order; namespaces and `GatewayClass` precede the `Gateway`.

2. **Gateway** — `demo-gateway` in `ugwc-sample-gateway` has HTTP + HTTPS listeners; HTTPS cert is in `ugwc-sample-certs` with a `ReferenceGrant` in the **Secret’s namespace**.

3. **Routes** — `HTTPRoute` / `GRPCRoute` in `ugwc-sample-routes` attach to the remote `Gateway`; `ReferenceGrant` objects live in the **Gateway namespace** (parent) and **Service namespace** (backend), per Gateway API rules.

4. **Backend TLS** — `BackendTLSPolicy` in `ugwc-sample-workloads` references the `backend-ca` `ConfigMap` in the same namespace. Gateway API v1.5 `caCertificateRefs` are namespace-local, so no `ReferenceGrant` is involved.

5. **xDS** — After reconcile publishes an IR snapshot for that gateway scope, `ugwc xds-dump --node-id ugwc-sample-gateway/demo-gateway` prints non-zero `resources` when `UGWC_XDS_WIRE` matches the `--wire` hint (or use `--type-url` explicitly).

## Teardown

```bash
kubectl delete -k examples/gateway-standard-plus-tls/
```
