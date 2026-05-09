# ugwc

Gateway API–oriented control plane in Rust: reconcile cluster objects into an internal **IR** snapshot, plan publishes, and serve data planes (notably **xDS**).

**Documentation:** start at [docs/main.md](docs/main.md) (architecture, design, and links to deeper notes in [`design/`](design/)). Intro and goals: [docs/README.md](docs/README.md).

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

## License

Apache-2.0 — see [Cargo.toml](Cargo.toml).
