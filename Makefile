# Local parity with CI — see design/implementation-plan.md Phase 0.4
.PHONY: fmt fmt-check clippy test build release ci check deny docker-build conformance

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	UGWC_XDS_INSECURE=true UGWC_RUN_PROFILE=dev cargo test --workspace

build:
	cargo build --workspace

release:
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
