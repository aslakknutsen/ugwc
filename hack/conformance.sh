#!/usr/bin/env bash
# Run upstream Gateway API conformance tests against the current kube context.
#
# Prerequisites:
#   - Go toolchain
#   - A cluster with Gateway API CRDs matching the suite revision
#   - GatewayClass installed (name passed via GATEWAY_CLASS_NAME)
#   - ugwc deployed as the implementation controller for that class
#
# Environment:
#   GATEWAY_API_ROOT   — path to a checkout of kubernetes-sigs/gateway-api (required)
#   GATEWAY_CLASS_NAME — GatewayClass under test (default: gateway-conformance)
#   SKIP_TESTS         — comma-separated ShortName values (see upstream suite)
#   EXTRA_CONFORMANCE_ARGS — additional args after "--args"

set -euo pipefail

: "${GATEWAY_API_ROOT:?set GATEWAY_API_ROOT to a kubernetes-sigs/gateway-api checkout}"

GATEWAY_CLASS_NAME="${GATEWAY_CLASS_NAME:-gateway-conformance}"
SKIP_TESTS="${SKIP_TESTS:-}"

cd "${GATEWAY_API_ROOT}"

# shellcheck disable=SC2086
exec go test ./conformance -count=1 -timeout=120m -v \
  --args \
  -gateway-class="${GATEWAY_CLASS_NAME}" \
  -all-features=true \
  -conformance-profiles=GATEWAY-HTTP \
  -skip-tests="${SKIP_TESTS}" \
  ${EXTRA_CONFORMANCE_ARGS:-}
