# Multi-stage OCI image — RHEL 8 UBI (rust-toolset module) builder, RHEL 9 UBI Micro runtime.
# See design/implementation-plan.md Phase 0.6.
FROM registry.access.redhat.com/ubi8/ubi:8.10 AS builder
USER 0
WORKDIR /src

# Rust via AppStream module (rust-toolset); OpenSSL headers for crates that probe pkg-config.
RUN dnf -y module install rust-toolset \
    && dnf install -y --setopt=install_weak_deps=0 openssl-devel pkg-config protobuf-compiler \
    && dnf clean all

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

ARG UGWC_GIT_COMMIT=unknown
ENV UGWC_GIT_COMMIT=${UGWC_GIT_COMMIT}

RUN cargo build -p ugwc --release --locked

# CA bundle for TLS clients (kube API, xDS) — ubi-micro has no package manager.
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5 AS certs
RUN microdnf install -y ca-certificates \
    && microdnf clean all \
    && rm -rf /var/cache/yum /var/lib/rpm/*

FROM registry.access.redhat.com/ubi9/ubi-micro:9.5

COPY --from=certs /etc/pki /etc/pki
COPY --from=builder /src/target/release/ugwc /usr/local/bin/ugwc

# Arbitrary non-root UID (no passwd entry required).
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ugwc"]
