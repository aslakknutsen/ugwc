# ugwc documentation

**ugwc** is a Gateway API–oriented control plane: it watches cluster state, compiles a Kubernetes-agnostic intermediate representation (IR), plans publishes, and serves configuration to data planes (notably xDS).

## Goals

1. **Single source of truth in IR** — routing and backends are resolved into a snapshot; wire formats are backends, not the core model.
2. **Clean crate boundaries** — pure compile/publish logic stays free of Kubernetes and gRPC where possible; integration lives in the binary and adapter crates.
3. **Extensible distribution** — v1 emphasizes xDS; additional output backends follow the same “encode + distribute” seam.
4. **Operable process** — admin/metrics, configurable xDS security, optional Kubernetes reconciliation behind flags.

Full map of topics: **[main.md](main.md)**.
