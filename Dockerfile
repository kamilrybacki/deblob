# syntax=docker/dockerfile:1
#
# Multi-stage build for the `deblob` binary (Task 20).
#
# NOT BUILT IN THIS SANDBOX (disk/time constraints) — written correctly
# against the workspace's actual dependencies but unverified by an actual
# `docker build`. Verify in CI or a real Docker host before relying on it.
#
# No secrets or environment values are baked into either stage: every
# secret (DEBLOB_API_TOKEN, DEBLOB_REDIS_URL, DEBLOB_KAFKA_BROKERS, the
# optional DEBLOB_KAFKA_SASL_* vars) is read exclusively at container
# runtime from the process environment — see crates/deblob/src/config.rs.
# The non-secret deblob.toml is likewise NOT copied into the image; mount
# it at runtime (e.g. a k8s ConfigMap) so the same image serves every
# environment.

########################################
# Builder
########################################
# A transitive dependency (cpufeatures 0.3.x, via sha2/rdkafka) requires
# Cargo's `edition2024` feature, stabilized in Rust 1.85 — so the builder
# toolchain must be >= 1.85 even though the workspace's own MSRV is older.
FROM rust:1.86-bookworm AS builder

# rdkafka is built with the `cmake-build` + `ssl` features (see
# crates/deblob-kafka/Cargo.toml and crates/deblob/Cargo.toml), which
# compiles librdkafka from vendored source rather than linking a
# system package. That needs:
#   - cmake, build-essential (the C/C++ toolchain cmake drives)
#   - pkg-config + libssl-dev (the `ssl` feature, OpenSSL bindings)
#   - libsasl2-dev (SASL PLAIN/SCRAM auth support, wired up via
#     DEBLOB_KAFKA_SASL_* env vars at runtime)
#   - zlib1g-dev, libzstd-dev, liblz4-dev (compression codecs librdkafka
#     can negotiate with brokers)
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        build-essential \
        pkg-config \
        libssl-dev \
        libsasl2-dev \
        zlib1g-dev \
        libzstd-dev \
        liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the whole workspace — Cargo.lock is committed and must be used
# as-is (no floating dependency resolution in the image build).
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# --locked: fail the build rather than silently re-resolve
# dependencies if Cargo.lock and Cargo.toml ever drift apart.
# Build both the server binary (`deblob`) and the benchmark client
# (`deblob-bench`) — one image serves the Deblob Deployment (ENTRYPOINT
# deblob) and the in-cluster benchmark Job (command deblob-bench).
RUN cargo build --release --locked --package deblob --package deblob-bench

########################################
# Runtime
########################################
# bookworm-slim runtime. rdkafka dynamically links zlib/openssl/sasl/lz4/zstd
# (the `ssl` + compression features) — distroless cc ships only glibc+libgcc,
# so the binary failed at load with `libz.so.1: cannot open shared object`.
# slim + the runtime shared libs is the robust fix; a dedicated non-root user
# keeps the container from running as root.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        zlib1g \
        libsasl2-2 \
        liblz4-1 \
        libzstd1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 nonroot \
    && useradd -u 65532 -g 65532 -M -s /usr/sbin/nologin nonroot

COPY --from=builder /build/target/release/deblob /usr/local/bin/deblob
COPY --from=builder /build/target/release/deblob-bench /usr/local/bin/deblob-bench

WORKDIR /app

# The management API's default listen port (deblob.example.toml's
# [management].addr = "127.0.0.1:9615"). Bind to 0.0.0.0 via
# DEBLOB_MANAGEMENT_ADDR (or the config file) for this to be reachable
# from outside the container.
EXPOSE 9615

USER nonroot

ENTRYPOINT ["/usr/local/bin/deblob"]
# No default CMD args: --config defaults to "deblob.toml", resolved
# relative to WORKDIR (/app) — mount a real config file to /app/deblob.toml
# (e.g. a k8s ConfigMap volume), or pass --config /path/to/file.toml
# explicitly as a container arg.
