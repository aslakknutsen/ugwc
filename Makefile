# Local parity with CI — see design/implementation-plan.md Phase 0.4
.PHONY: fmt fmt-check clippy test build release ci check deny docker-build image-build container-build \
	helm-lint helm-template helm-install helm-upgrade helm-uninstall hack-setup conformance _ensure_protoc_tmp

# prost-build runs protoc, which writes under $TMPDIR; a full /tmp breaks release builds.
# Override with your own path: `make release TMPDIR=/var/tmp` (or export TMPDIR).
TMPDIR ?= $(CURDIR)/.tmp
export TMPDIR

CONTAINER_ENGINE ?= docker
IMAGE_REPO ?= ugwc
IMAGE_TAG ?= dev
UGWC_GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
CONTAINERFILE ?= Containerfile

CHART_PATH ?= charts/ugwc
HELM_RELEASE ?= ugwc
HELM_NAMESPACE ?= ugwc-system

_ensure_protoc_tmp:
	@mkdir -p "$(TMPDIR)"

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy: _ensure_protoc_tmp
	cargo clippy --workspace --all-targets -- -D warnings

test: _ensure_protoc_tmp
	UGWC_XDS_INSECURE=true UGWC_RUN_PROFILE=dev cargo test --workspace

build: _ensure_protoc_tmp
	cargo build --workspace

release: _ensure_protoc_tmp
	cargo build -p ugwc --release

check: fmt-check clippy test

ci: fmt-check clippy test release

deny:
	cargo deny check

docker-build image-build container-build:
	$(CONTAINER_ENGINE) build -f "$(CONTAINERFILE)" \
		--build-arg "UGWC_GIT_COMMIT=$(UGWC_GIT_COMMIT)" \
		-t "$(IMAGE_REPO):$(IMAGE_TAG)" .

helm-lint:
	helm lint "$(CHART_PATH)"

helm-template:
	helm template "$(HELM_RELEASE)" "$(CHART_PATH)" --namespace "$(HELM_NAMESPACE)"

helm-install:
	helm upgrade --install "$(HELM_RELEASE)" "$(CHART_PATH)" --namespace "$(HELM_NAMESPACE)" --create-namespace

helm-upgrade: helm-install

helm-uninstall:
	helm uninstall "$(HELM_RELEASE)" --namespace "$(HELM_NAMESPACE)"

# kind + MetalLB + CRDs + ugwc + GatewayClass — see docs/operations.md
hack-setup:
	./hack/setup.sh

# Requires Go, git, kubectl; cluster from hack/setup.sh — see hack/conformance.sh
conformance:
	./hack/conformance.sh
