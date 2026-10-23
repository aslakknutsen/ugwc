# Dataplane provisioning and subscription scope

**Parent index:** [`control-plane-design-index.md`](control-plane-design-index.md)  
**Related:** [`output-distribution.md`](output-distribution.md), [`controller-architecture.md`](controller-architecture.md)

---

## 1. Problem

Some deployments need the control plane to **create and bootstrap** data plane workloads (proxies) from `Gateway` objects. Others keep dataplanes **external**, share one deployment across **multiple** gateways, or use patterns not yet enumerated. The system must treat these as **explicit strategies**, not implicit behavior.

Separately, each client connecting for configuration (e.g. xDS) must subscribe to a **scope** of IR: **all**, **some** (subset), or **one** gateway. A proxy provisioned **per** `Gateway` must receive **only** that gateway’s compiled IR, not cluster-wide config.

---

## 2. Unit of configuration: implementation profile

Each owned `GatewayClass.spec.controllerName` resolves (via static config today; `parametersRef` later) to an **implementation profile** with four axes:

| Axis | Meaning |
|------|--------|
| **Admission** | Class `controllerName` matches this controller instance (`UGWC_CONTROLLER_NAMES`). |
| **Data plane strategy** | **External** — no workload mutations. **PerGatewayPraxis** — one `Gateway` ⇒ one Deployment (+ Service) with bootstrap env. **SharedDeployment** — *stub*: multiple gateways → one deployment; **grouping key not decided** (see §7). |
| **Publish protocol** | **`output`** on the profile: `praxis_configmap` (built-in default; render native Praxis YAML into a per-gateway ConfigMap + TLS Secret mounted into the provisioned Deployment) or `xds` (wire selected by `UGWC_XDS_WIRE`, `gwxds` vs `envoy`). Orthogonal to provisioning but tied to what the proxy binary speaks. |
| **Subscription scope (default)** | **All** — client uses global xDS node id (`global` / `*`). **OneGateway** — client node id = `namespace/name` of the `Gateway`; server serves only that scope’s snapshot. **Some** — reserved for pool/shard; not implemented in v1. |

**Pairing rule:** `PerGatewayPraxis` implies **OneGateway** subscription for the provisioned workload unless overridden.

---

## 3. Leader and HA

Mutating Kubernetes workloads (provisioner) follows the same **leader-only** rule as publishing scoped snapshots: non-leaders skip apply.

---

## 4. Scoped xDS publish path

The server maintains:

- A **global** snapshot (startup stub and **All** subscribers).
- A **per-scope** map keyed by `IrScopeId` string (`namespace/gateway-name`).

Reconcile updates **only** the entry for the gateway being compiled when `PublishAction::FullReplace` applies. xDS **DiscoveryRequest** carries **node.id**; resolution:

- Missing, empty, `global`, or `*` → global snapshot.
- Otherwise → per-scope snapshot; if **missing**, return an **empty** IR for that scope and increment a **scope miss** metric (possible misconfiguration or race). **Do not** fall back to another tenant’s snapshot.

**Security:** Without transport identity binding node id to the caller, a client could spoof another scope. Production requires **mTLS / authn** on xDS and mapping identity → allowed scopes (out of scope for this doc’s v1 code path).

---

## 5. Praxis bootstrap (v1)

Provisioned pods are configured according to the profile's `output`:

**`praxis_configmap` (built-in default)**

- **Config** — the controller encodes the gateway's IR to Praxis native YAML and
  writes it to ConfigMap `ugwc-dp-<gateway>` (key `praxis.yaml`). The Deployment mounts
  it at `/etc/praxis` and runs with `-c /etc/praxis/praxis.yaml`.
- **TLS material** — listener certificates and backend mTLS CA/client material are
  written to Secret `ugwc-dp-<gateway>-tls`, mounted at `/etc/praxis-tls`.
- **No xDS dial address** — `UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR` is not required and
  no xDS bootstrap environment is set.
- Static config and TLS material changes roll the Deployment through the
  `gateway.ugwc.io/config-revision` annotation; endpoint-only changes are hot reloaded
  from the mounted ConfigMap (see
  [`docs/operations.md`](../docs/operations.md#praxis-native-config-output)).

**`xds`**

- **xDS dial address** — `UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR` (host:port reachable
  from the pod; typically a `Service` fronting ugwc).
- **Scope / node id** — `UGWC_XDS_NODE_ID` = `namespace/name` (must match compile
  `IrScopeId`).
- **TLS** — mirror `UGWC_XDS_INSECURE` and related TLS paths where the proxy supports
  them.

**Proxy image** comes from the profile JSON (`proxy_image`).

---

## 6. Status

Gateway listener conditions remain the primary “programmed” signal. Today
`programmed_ok` derives from compile results only, and status is patched before the
publish/provision step. Output-backend failures and unsupported mappings are surfaced
as **Events** (`PraxisConfigFailed`, `PraxisConfigUnsupported`) and **Prometheus
metrics** (`ugwc_praxis_config_*`), not yet as Gateway conditions.

Follow-up: return an `OutputOutcome` (delivered / failed / unsupported) from the output
step and feed it into Gateway status once the generic mechanism exists (tracked in
[`todo-gateway-api-inference-parity.md`](todo-gateway-api-inference-parity.md)).

---

## 7. Shared deployment (stub)

**SharedDeployment** is recognized in profile JSON but **does not** create or update workloads until a **grouping key** is defined (e.g. label, `parametersRef`, or pool name). Today it behaves like **External** for apply purposes and is logged as unimplemented.

---

## 8. Non-goals (v1)

- Parsing rich `parametersRef` on `GatewayClass`.
- Automatic proxy image upgrades.
- Multi-cluster provisioning.
- Merging many gateways into one IR for **All** (global remains non-federated).

---

## 9. Example profile file

Path: `UGWC_IMPLEMENTATION_PROFILES_PATH` (JSON).

```json
{
  "profiles": {
    "example.com/gateway-controller": {
      "data_plane": "per_gateway_praxis",
      "output": "xds",
      "subscription_default": "one_gateway",
      "proxy_image": "ghcr.io/praxis-proxy/praxis:0.7.5"
    },
    "ugwc.io/praxis-config": {
      "data_plane": "per_gateway_praxis",
      "output": "praxis_configmap",
      "subscription_default": "one_gateway",
      "proxy_image": "ghcr.io/praxis-proxy/praxis:0.7.5"
    }
  }
}
```

`output` defaults to `xds` when omitted from a profile entry. The built-in
`ugwc.io/default-controller` profile uses `praxis_configmap`. `praxis_configmap`
requires `per_gateway_praxis` and does not require
`UGWC_CONTROL_PLANE_XDS_BOOTSTRAP_ADDR` (the proxy reads a mounted config file instead
of dialing xDS). `xds` with `per_gateway_praxis` still requires the bootstrap address.

Controllers listed in `UGWC_CONTROLLER_NAMES` but omitted from this file default to **External** + **All**.
