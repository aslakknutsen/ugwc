# ugwc

Gateway API–oriented control plane in Rust: reconcile cluster objects into an internal **IR** snapshot, plan publishes, and serve data planes (notably **xDS**).

**Documentation:** start at [docs/main.md](docs/main.md) (architecture, design, operations). Intro and goals: [docs/README.md](docs/README.md). CRD install and **`UGWC_*`** variables: [docs/operations.md](docs/operations.md).

## Repository layout

- `crates/` — workspace members: `ugwc` (binary), `ugwc-core`, `ugwc-k8s`, `ugwc-xds`, `ugwc-gwxds`
- `design/` — detailed design and implementation plan drafts
- `docs/` — short stable-facing docs

## Quick start

```bash
cargo build -p ugwc --release
cargo run -p ugwc -- --help
```

Configuration is environment-driven; use `cargo run -p ugwc -- --dump-config` after setting variables to inspect effective config.

Clusters must install the **experimental** Gateway API CRD bundle if you use route kinds such as **TCPRoute** or **UDPRoute** (`gateway.networking.k8s.io/v1alpha2`). If a CRD is missing, ugwc lists fail softly (logged) for that resource type.

## Helm

Chart: [`charts/ugwc`](charts/ugwc). Install Gateway API (and optional experimental) CRDs first — see [docs/operations.md](docs/operations.md).

Render manifests (smoke check):

```bash
make helm-template
```

Install or upgrade into a namespace (defaults: release `ugwc`, namespace `ugwc-system`):

```bash
make docker-build   # or: podman + make CONTAINER_ENGINE=podman docker-build
make helm-install
```

Remove the release (uses `HELM_RELEASE` / `HELM_NAMESPACE`, same as install):

```bash
make helm-uninstall
```

Override image, controller ownership, and namespace as needed:

```bash
helm upgrade --install ugwc ./charts/ugwc --namespace ugwc-system --create-namespace \
  --set image.repository=your.registry/ugwc \
  --set image.tag=v0.1.0 \
  --set ugwc.controllerNames=your.company/gateway-controller
```

`values.yaml` documents further `UGWC_*`-related toggles (e.g. `ugwc.xdsInsecure`, `ugwc.dataplaneEnable`).

## License

Apache-2.0 — see [Cargo.toml](Cargo.toml).
