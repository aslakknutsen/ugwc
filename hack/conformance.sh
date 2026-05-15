#!/usr/bin/env bash
# Runs Kubernetes Gateway API conformance tests against a cluster (typically kind
# from hack/setup.sh). Shallow-clones kubernetes-sigs/gateway-api at GATEWAY_API_REF
# unless GATEWAY_API_SRC is set.
#
# Prerequisites: git, go, kubectl; cluster provisioned (./hack/setup.sh).
#
# Usage:
#   ./hack/conformance.sh
#   ./hack/conformance.sh --debug --run-test=HTTPRouteInvalidCrossNamespaceParentRef
#   ./hack/conformance.sh --single-test HTTPRouteSimpleSameNamespace
#   CONFORMANCE_SUPPORTED_FEATURES=Gateway,HTTPRoute,GRPCRoute ./hack/conformance.sh
#
# Defaults (shared with hack/setup.sh — see hack/conformance_defaults.sh):
#   UGWC_CONFORMANCE_GATEWAY_CLASS   GatewayClass metadata.name
#   UGWC_GATEWAY_CONTROLLER_NAME     spec.controllerName (setup applies the class)
#
# Environment:
#   CLUSTER_NAME                   kind cluster name (default: ugwc-dev)
#   GATEWAY_CLASS                  overrides GatewayClass name (default: UGWC_CONFORMANCE_GATEWAY_CLASS)
#   GATEWAY_API_REF                git tag or branch (default: from conformance_defaults.sh)
#   GATEWAY_API_SRC                if set, use this checkout instead of cloning
#   GATEWAY_API_REPO               clone URL (default: upstream gateway-api)
#   GATEWAY_API_CACHE              parent directory for shallow clone
#   CONFORMANCE_SUPPORTED_FEATURES comma list for --supported-features (default: Gateway,HTTPRoute)
#   GO_TEST_FLAGS                  extra go test flags (default: -timeout=3h -count=1 -v)
#
# Additional arguments are forwarded to the conformance suite (e.g. --debug).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/conformance_defaults.sh"

info()  { echo "==> $*"; }
warn()  { echo "WARN: $*" >&2; }
fatal() { echo "ERROR: $*" >&2; exit 1; }

CLUSTER_NAME="${CLUSTER_NAME:-ugwc-dev}"
GATEWAY_CLASS="${GATEWAY_CLASS:-$UGWC_CONFORMANCE_GATEWAY_CLASS}"
GATEWAY_API_REPO="${GATEWAY_API_REPO:-https://github.com/kubernetes-sigs/gateway-api.git}"
CACHE_ROOT="${GATEWAY_API_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/ugwc-gateway-api-conformance}"
CLONE_DIR="${CACHE_ROOT}/gateway-api"
if [[ -z "${GO_TEST_FLAGS:-}" ]]; then
  GO_TEST_FLAGS="-timeout=3h -count=1 -v"
fi
SUPPORTED_FEATURES="${CONFORMANCE_SUPPORTED_FEATURES:-Gateway,HTTPRoute,ReferenceGrant,TLSRoute,GRPCRoute,ListenerSet}"
GO_TEST_RUN="${GO_TEST_RUN:-TestConformance}"
SINGLE_TEST="${CONFORMANCE_SINGLE_TEST:-}"
FORWARD_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --single-test)
      [[ $# -ge 2 ]] || fatal "--single-test requires a value"
      SINGLE_TEST="$2"
      shift 2
      ;;
    --single-test=*)
      SINGLE_TEST="${1#*=}"
      shift
      ;;
    *)
      FORWARD_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -n "$SINGLE_TEST" ]]; then
  GO_TEST_RUN="TestConformance/${SINGLE_TEST}$"
  FORWARD_ARGS+=(--run-test="$SINGLE_TEST")
fi

require() {
  for cmd in "$@"; do
    command -v "$cmd" &>/dev/null || fatal "required tool not found: $cmd"
  done
}

require git go kubectl

CTX="kind-${CLUSTER_NAME}"
kubectl config use-context "$CTX" &>/dev/null || fatal "kubectl context '$CTX' not found (run ./hack/setup.sh first)"

if ! kubectl get gatewayclass "$GATEWAY_CLASS" &>/dev/null; then
  warn "GatewayClass '$GATEWAY_CLASS' not found — tests will fail until it exists (run ./hack/setup.sh)"
fi

ensure_gateway_api_src() {
  if [[ -n "${GATEWAY_API_SRC:-}" ]]; then
    [[ -d "$GATEWAY_API_SRC" ]] || fatal "GATEWAY_API_SRC is not a directory: $GATEWAY_API_SRC"
    GW_SRC="$(cd "$GATEWAY_API_SRC" && pwd)"
    info "Using GATEWAY_API_SRC=$GW_SRC"
    return
  fi

  mkdir -p "$CACHE_ROOT"

  if [[ ! -d "$CLONE_DIR/.git" ]]; then
    info "Shallow-cloning gateway-api ${GATEWAY_API_REF} into ${CLONE_DIR}"
    git clone --depth 1 --branch "${GATEWAY_API_REF}" "$GATEWAY_API_REPO" "$CLONE_DIR"
  elif ! git -C "$CLONE_DIR" checkout -q "${GATEWAY_API_REF}" 2>/dev/null; then
    warn "Could not checkout ${GATEWAY_API_REF} in existing clone (shallow history); re-cloning"
    rm -rf "$CLONE_DIR"
    git clone --depth 1 --branch "${GATEWAY_API_REF}" "$GATEWAY_API_REPO" "$CLONE_DIR"
  else
    info "Using cached gateway-api at ${CLONE_DIR} (ref ${GATEWAY_API_REF})"
  fi

  GW_SRC="$(cd "$CLONE_DIR" && pwd)"
}

ensure_gateway_api_src

[[ -d "$GW_SRC/conformance" ]] || fatal "missing conformance/ in $GW_SRC (unexpected gateway-api layout)"

info "Running Gateway API conformance (ref=${GATEWAY_API_REF}, gateway-class=${GATEWAY_CLASS})"
info "go test -run regex: ${GO_TEST_RUN}"

cd "$GW_SRC"
# shellcheck disable=SC2086
exec go test ${GO_TEST_FLAGS} -run "${GO_TEST_RUN}" ./conformance \
  -args \
  --gateway-class="${GATEWAY_CLASS}" \
  --supported-features="${SUPPORTED_FEATURES}" \
  "${FORWARD_ARGS[@]}"
