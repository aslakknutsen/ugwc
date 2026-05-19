# TODO: HTTPRoute extended standard features

**Purpose:** Track implementation status and plan for Gateway API HTTPRoute
extended (standard-channel) conformance features.  
**Spec:** gateway-api v1.5.1 `pkg/features/httproute.go`  
**Related:** [`TODO-channel-experimental-handling.md`](TODO-channel-experimental-handling.md)

---

## Current state (updated 2026-05-20)

The conformance runner declares 25 supported features and passes all 459 subtests
with 0 failures. Most HTTPRoute extended features are implemented and passing.

Supported features:
`Gateway,HTTPRoute,ReferenceGrant,TLSRoute,GRPCRoute,ListenerSet,BackendTLSPolicy,HTTPRouteCORS,HTTPRouteCORSAllowCredentialsBehavior,HTTPRouteMethodMatching,HTTPRouteQueryParamMatching,HTTPRouteSchemeRedirect,HTTPRoutePortRedirect,HTTPRouteHostRewrite,HTTPRouteNamedRouteRule,HTTPRoutePathRedirect,HTTPRoute303RedirectStatusCode,HTTPRoute307RedirectStatusCode,HTTPRoute308RedirectStatusCode,HTTPRouteResponseHeaderModification,HTTPRoutePathRewrite,HTTPRouteRequestTimeout,HTTPRouteBackendTimeout,HTTPRouteListenerPortMatching,HTTPRouteParentRefPort`

---

## Feature matrix

Features are grouped by the component that needs work. Status key:

- **done** — code exists in ugwc + praxis, conformance test passes
- **not started** — no implementation in either component

### Matching & structural

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteQueryParamMatching | done | standard | |
| HTTPRouteMethodMatching | done | standard | |
| HTTPRouteDestinationPortMatching | not started | experimental | Lower priority — experimental channel |
| HTTPRouteParentRefPort | done | standard | |
| HTTPRouteNamedRouteRule | done | standard | |
| HTTPRouteListenerPortMatching | done | standard | |

### Request header / response header modification

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| RequestHeaderModifier | done | standard (core) | |
| ResponseHeaderModifier | done | standard | |
| BackendRequestHeaderModifier | not started | standard | Per-backendRef header modifier |

### Redirect

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| RequestRedirect (host + status) | done | standard (core) | |
| HTTPRouteSchemeRedirect | done | standard | |
| HTTPRoutePortRedirect | done | standard | |
| HTTPRoutePathRedirect | done | standard | |
| HTTPRoute303RedirectStatusCode | done | standard | |
| HTTPRoute307RedirectStatusCode | done | standard | |
| HTTPRoute308RedirectStatusCode | done | standard | |
| HTTPRouteRedirectPortAndScheme | not started | standard | Skipped — requires `GatewayPort8080` feature |

### URL rewrite

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteHostRewrite | done | standard | |
| HTTPRoutePathRewrite | done | standard | |

### Request mirror

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteRequestMirror | not started | standard | Fork request to mirror backend; praxis needs a mirror filter |
| HTTPRouteRequestMultipleMirrors | not started | standard | Multiple mirror filters per rule |
| HTTPRouteRequestPercentageMirror | not started | standard | Percentage-based mirroring |

### Timeouts

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteRequestTimeout | done | standard | |
| HTTPRouteBackendTimeout | done | standard | |

### Backend protocol

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteBackendProtocolH2C | not started | standard | Connect to backend via cleartext HTTP/2 (appProtocol `kubernetes.io/h2c`) |
| HTTPRouteBackendProtocolWebSocket | not started | standard | WebSocket upgrade support (appProtocol `kubernetes.io/ws`) |

### Inline filters (v1.5+)

| Feature | Status | Channel | Notes |
|---------|--------|---------|-------|
| HTTPRouteCORS | done | standard | |
| HTTPRouteCORSAllowCredentialsBehavior | done | standard | |

---

## Remaining work

Only 6 HTTPRoute extended features remain unimplemented (5 standard, 1 experimental):

| Feature | Channel | Effort | Notes |
|---------|---------|--------|-------|
| HTTPRouteBackendRequestHeaderModification | standard | Small | Per-backendRef header modifier — compile.rs filter addition |
| HTTPRouteRequestMirror | standard | Medium | Praxis needs a mirror filter |
| HTTPRouteRequestMultipleMirrors | standard | Medium | Builds on mirror support |
| HTTPRouteRequestPercentageMirror | standard | Medium | Percentage-based mirroring (depends on mirror) |
| HTTPRouteBackendProtocolH2C | standard | Medium | Praxis upstream connection changes |
| HTTPRouteBackendProtocolWebSocket | standard | Medium | Praxis WebSocket upgrade handling |
| HTTPRouteDestinationPortMatching | experimental | Low priority | Experimental channel — not needed for standard conformance |

### Suggested priority

1. **Backend request header modifier** — smallest standard gap (2 tests unlocked)
2. **Request mirror** — unlocks 2 tests; MultipleMirrors and PercentageMirror build on it (3 more)
3. **Backend protocol H2C + WebSocket** — praxis upstream work; 1 test each

---

## Adding a feature to the conformance run

1. Implement the feature in ugwc (IR, xDS encoding) and praxis (translate, filter).
2. Add the feature name to `SUPPORTED_FEATURES` in `hack/conformance.sh`.
3. Run `make conformance` and verify the new tests pass.
4. Features can also be enabled per-run:
   `CONFORMANCE_SUPPORTED_FEATURES=...,NewFeature ./hack/conformance.sh`
