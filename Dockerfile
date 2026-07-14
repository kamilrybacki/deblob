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
# rust-version in Cargo.toml's [workspace.package] pins MSRV to 1.80.
FROM rust:1.80-bookworm AS builder

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
RUN cargo build --release --locked --package deblob

########################################
# Runtime
########################################
# distroless's cc-debian12 base ships glibc + libgcc (rdkafka/openssl are
# dynamically linked) but no shell, package manager, or other attack
# surface beyond that. The :nonroot tag also pre-creates and switches to
# a non-root uid/gid (65532), so no separate `useradd` step is needed.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /build/target/release/deblob /usr/local/bin/deblob

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
