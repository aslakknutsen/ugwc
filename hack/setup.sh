#!/usr/bin/env bash
# Provisions a kind cluster with MetalLB, Gateway API CRDs, ugwc (Helm), and a
# GatewayClass for running hack/conformance.sh.
#
# Prerequisites: kind, kubectl, helm, python3; docker or podman (CONTAINER_ENGINE).
# For image build: same engine as Makefile.
#
# Usage:
#   ./hack/setup.sh               # build image, load into kind, helm install
#   ./hack/setup.sh --skip-build  # reuse existing local image (must match IMAGE_REPO:IMAGE_TAG)
#
# Environment (see also hack/conformance_defaults.sh):
#   CLUSTER_NAME            kind cluster name (default: ugwc-dev)
#   CONTAINER_ENGINE        docker or podman (default: docker)
#   IMAGE_REPO / IMAGE_TAG  image tag for build + Helm (defaults: ugwc / dev)
#   HELM_RELEASE / HELM_NAMESPACE  (defaults: ugwc / ugwc-system)
#   METALLB_VERSION         (default: v0.14.9)
#   KIND_DOCKER_NETWORK     container network for kind (default: kind)
#   METALLB_IP_POOL         optional "start-end" override for L2 pool
#   INSTALL_INFERENCE_CRDS  set to 0 to skip Gateway API inference extension CRDs
#   GATEWAY_API_REF         Gateway API CRD bundle version (default from conformance_defaults.sh)
#
set -euo pipefail

info()  { echo "==> $*"; }
warn()  { echo "WARN: $*" >&2; }
fatal() { echo "ERROR: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/conformance_defaults.sh"

CLUSTER_NAME="${CLUSTER_NAME:-ugwc-dev}"
CONTAINER_ENGINE="${CONTAINER_ENGINE:-docker}"
IMAGE_REPO="${IMAGE_REPO:-ugwc}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
HELM_RELEASE="${HELM_RELEASE:-ugwc}"
HELM_NAMESPACE="${HELM_NAMESPACE:-ugwc-system}"
METALLB_VERSION="${METALLB_VERSION:-v0.14.9}"
KIND_DOCKER_NETWORK="${KIND_DOCKER_NETWORK:-kind}"
INSTALL_INFERENCE_CRDS="${INSTALL_INFERENCE_CRDS:-1}"
UGWC_GIT_COMMIT="${UGWC_GIT_COMMIT:-$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)}"

SKIP_BUILD=false
for arg in "$@"; do
  [[ "$arg" == "--skip-build" ]] && SKIP_BUILD=true
done

require() {
  for cmd in "$@"; do
    command -v "$cmd" &>/dev/null || fatal "required tool not found: $cmd"
  done
}

kind_bridge_ipv4_subnet() {
  "$CONTAINER_ENGINE" network inspect "$KIND_DOCKER_NETWORK" \
    --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' 2>/dev/null \
    | grep -E '^([0-9]{1,3}\.){3}[0-9]{1,3}/' | head -1 || true
}

metallb_pool_range_from_subnet() {
  local subnet="$1"
  python3 - "$subnet" <<'PY'
import ipaddress
import sys

net = ipaddress.ip_network(sys.argv[1].strip(), strict=False)
if net.version != 4:
    sys.exit("MetalLB pool auto-detection supports IPv4 container subnets only")
hosts = list(net.hosts())
if len(hosts) < 4:
    sys.exit("subnet too small for MetalLB pool")
n = min(50, len(hosts) - 1)
print(f"{hosts[-n]}-{hosts[-1]}")
PY
}

require kind kubectl helm python3 "$CONTAINER_ENGINE"

if [[ "$SKIP_BUILD" == false ]]; then
  require git
fi

info "Creating kind cluster '$CLUSTER_NAME'"
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  info "Cluster already exists, skipping create"
else
  kind create cluster --name "$CLUSTER_NAME"
fi

kubectl config use-context "kind-${CLUSTER_NAME}"

METALLB_MANIFEST="https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"
info "Installing MetalLB ${METALLB_VERSION}"
kubectl apply -f "$METALLB_MANIFEST"
kubectl rollout status -n metallb-system deployment/controller --timeout=180s
kubectl rollout status -n metallb-system daemonset/speaker --timeout=180s

SUBNET="$(kind_bridge_ipv4_subnet)"
[[ -n "$SUBNET" ]] || fatal "could not read IPv4 subnet from network '$KIND_DOCKER_NETWORK' (set KIND_DOCKER_NETWORK or METALLB_IP_POOL)"

if [[ -n "${METALLB_IP_POOL:-}" ]]; then
  POOL_RANGE="$METALLB_IP_POOL"
  info "Using METALLB_IP_POOL=$POOL_RANGE"
else
  POOL_RANGE="$(metallb_pool_range_from_subnet "$SUBNET")" || fatal "could not compute MetalLB pool from subnet $SUBNET (set METALLB_IP_POOL manually)"
  info "MetalLB L2 pool ${POOL_RANGE} (from subnet ${SUBNET})"
fi

kubectl apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
    - ${POOL_RANGE}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kind-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - kind-pool
EOF

info "Installing Gateway API CRDs ${GATEWAY_API_REF}"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_REF}/standard-install.yaml"

if [[ "$INSTALL_INFERENCE_CRDS" != "0" ]]; then
  INFERENCE_VERSION="${INFERENCE_VERSION:-v1.4.0}"
  info "Installing Gateway API Inference Extension CRDs ${INFERENCE_VERSION}"
  kubectl apply --server-side -f \
    "https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/${INFERENCE_VERSION}/manifests.yaml"
else
  info "Skipping inference extension CRDs (INSTALL_INFERENCE_CRDS=0)"
fi

FULL_IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"
if [[ "$SKIP_BUILD" == false ]]; then
  info "Building ${FULL_IMAGE} (${CONTAINER_ENGINE})"
  (
    cd "$REPO_ROOT"
    "$CONTAINER_ENGINE" build -f Containerfile \
      --build-arg "UGWC_GIT_COMMIT=${UGWC_GIT_COMMIT}" \
      -t "$FULL_IMAGE" .
  )
else
  info "Skipping image build (--skip-build); ensure ${FULL_IMAGE} exists locally"
fi

info "Loading image into kind cluster"
kind load docker-image "$FULL_IMAGE" --name "$CLUSTER_NAME"

info "Installing ugwc via Helm (${HELM_RELEASE} into ${HELM_NAMESPACE})"
helm upgrade --install "$HELM_RELEASE" "$REPO_ROOT/charts/ugwc" \
  --namespace "$HELM_NAMESPACE" \
  --create-namespace \
  --set "image.repository=${IMAGE_REPO}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set "image.pullPolicy=Never" \
  --set "ugwc.controllerNames=${UGWC_GATEWAY_CONTROLLER_NAME}" \
  --wait --timeout=5m

info "Applying GatewayClass ${UGWC_CONFORMANCE_GATEWAY_CLASS} (${UGWC_GATEWAY_CONTROLLER_NAME})"
kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: ${UGWC_CONFORMANCE_GATEWAY_CLASS}
spec:
  controllerName: ${UGWC_GATEWAY_CONTROLLER_NAME}
EOF

cat <<EOF

Setup complete. Context: kind-${CLUSTER_NAME}

Run Gateway API conformance:

  ./hack/conformance.sh
  # or: make conformance

EOF
