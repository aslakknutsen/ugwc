# syntax=docker/dockerfile:1

# Multi-stage OCI image — RHEL 8 UBI (rust-toolset module) builder, RHEL 9 UBI Micro runtime.
# See design/implementation-plan.md Phase 0.6.
FROM registry.access.redhat.com/ubi8/ubi:8.10 AS builder
USER 0
WORKDIR /src

# Rust via AppStream module (rust-toolset); OpenSSL headers for crates that probe pkg-config.
RUN dnf -y module install rust-toolset \
    && dnf install -y --setopt=install_weak_deps=0 openssl-devel pkg-config protobuf-compiler \
    && dnf clean all

# ------------------------------------------------------------------------------
# Cache build: manifests + protos + stubs, then cargo build (deps in BuildKit cache).
# https://shaneutt.com/blog/rust-fast-small-docker-image-builds/
# ------------------------------------------------------------------------------

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ugwc/Cargo.toml crates/ugwc/Cargo.toml
COPY crates/ugwc/build.rs crates/ugwc/build.rs
COPY crates/ugwc-core/Cargo.toml crates/ugwc-core/Cargo.toml
COPY crates/ugwc-gwxds/Cargo.toml crates/ugwc-gwxds/Cargo.toml
COPY crates/ugwc-gwxds/build.rs crates/ugwc-gwxds/build.rs
COPY crates/ugwc-gwxds/proto crates/ugwc-gwxds/proto
COPY crates/ugwc-k8s/Cargo.toml crates/ugwc-k8s/Cargo.toml
COPY crates/ugwc-xds/Cargo.toml crates/ugwc-xds/Cargo.toml

ARG UGWC_GIT_COMMIT=unknown
ENV UGWC_GIT_COMMIT=${UGWC_GIT_COMMIT}

RUN mkdir -p crates/ugwc-core/src \
    crates/ugwc-gwxds/src \
    crates/ugwc-k8s/src \
    crates/ugwc-xds/src \
    crates/ugwc/src \
    && echo '//! stub' > crates/ugwc-core/src/lib.rs \
    && echo '//! stub' > crates/ugwc-gwxds/src/lib.rs \
    && echo '//! stub' > crates/ugwc-k8s/src/lib.rs \
    && echo '//! stub' > crates/ugwc-xds/src/lib.rs \
    && printf '%s\n' '//! stub' 'fn main() {}' > crates/ugwc/src/main.rs

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p ugwc

COPY crates/ugwc-core/src crates/ugwc-core/src
COPY crates/ugwc-gwxds/src crates/ugwc-gwxds/src
COPY crates/ugwc-k8s/src crates/ugwc-k8s/src
COPY crates/ugwc-k8s/templates crates/ugwc-k8s/templates
COPY crates/ugwc-xds/src crates/ugwc-xds/src
COPY crates/ugwc/src crates/ugwc/src

RUN find crates/ugwc-core/src crates/ugwc-gwxds/src \
    crates/ugwc-k8s/src crates/ugwc-xds/src crates/ugwc/src \
    -name '*.rs' -exec touch {} +

# Copy binary out of cache-mounted target so the next stage can COPY it.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p ugwc \
    && install -m 0555 target/release/ugwc /usr/local/bin/ugwc

# CA bundle for TLS clients (kube API, xDS) — ubi-micro has no package manager.
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.5 AS certs
RUN microdnf install -y ca-certificates \
    && microdnf clean all \
    && rm -rf /var/cache/yum /var/lib/rpm/*

FROM registry.access.redhat.com/ubi9/ubi-micro:9.5

COPY --from=certs /etc/pki /etc/pki
COPY --from=builder /usr/local/bin/ugwc /usr/local/bin/ugwc

# Arbitrary non-root UID (no passwd entry required).
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ugwc"]
