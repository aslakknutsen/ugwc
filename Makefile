# Local parity with CI — see design/implementation-plan.md Phase 0.4
.PHONY: fmt fmt-check clippy test build release ci check deny docker-build conformance _ensure_protoc_tmp

# prost-build runs protoc, which writes under $TMPDIR; a full /tmp breaks release builds.
# Override with your own path: `make release TMPDIR=/var/tmp` (or export TMPDIR).
TMPDIR ?= $(CURDIR)/.tmp
export TMPDIR

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

docker-build:
	docker build -t ugwc:dev .

# Requires GATEWAY_API_ROOT, Go, and a conforming cluster — see hack/conformance.sh
conformance:
	./hack/conformance.sh
