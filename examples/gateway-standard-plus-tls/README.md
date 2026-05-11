# Sample: Gateway + ListenerSet + cross-namespace TLS and BackendTLSPolicy

Multi-namespace manifests intended to exercise ugwc’s cluster snapshot path: `Gateway` (addresses, infrastructure, `allowedListeners`), `ListenerSet` merge, HTTPS listener `certificateRefs` with `ReferenceGrant`, `HTTPRoute` / `GRPCRoute` with cross-namespace parents and backends, `BackendTLSPolicy` with CA in a `ConfigMap` in another namespace, plus `Service` and `EndpointSlice`.

Apply:

```bash
kubectl apply -k examples/gateway-standard-plus-tls/
```

Before applying, edit [`01-gatewayclass.yaml`](01-gatewayclass.yaml) so `spec.controllerName` matches **`UGWC_CONTROLLER_NAMES`** for your ugwc process.

TLS material in this directory is a **development-only** self-signed pair (same cert reused as BackendTLS CA). Regenerate with:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout tls.key -out tls.crt -days 3650 \
  -subj "/CN=demo.example.com" -addext "subjectAltName=DNS:demo.example.com"
```

`EndpointSlice` uses TEST-NET address `203.0.113.10` for control-plane debugging only; it does not need reachable pods.

See [docs/example-gateway-standard-plus-tls.md](../../docs/example-gateway-standard-plus-tls.md) for CRD prerequisites, environment variables, and verification (including `ugwc xds-dump`).
