# Multi-stage OCI image — design/implementation-plan.md Phase 0.6
# Dynamic glibc (Distroless debian12) matches rust:bookworm linker.
FROM rust:1.85-bookworm AS builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
ARG UGWC_GIT_COMMIT=unknown
ENV UGWC_GIT_COMMIT=${UGWC_GIT_COMMIT}
RUN cargo build -p ugwc --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/ugwc /usr/local/bin/ugwc
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/ugwc"]
