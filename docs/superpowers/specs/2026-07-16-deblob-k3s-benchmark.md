# Deblob k3s Benchmark — Design Specification

- **Date:** 2026-07-16
- **Status:** Draft
- **Goal:** Establish a real-infrastructure performance baseline for the deterministic core + P2-D (semantic fingerprint + similarity) before committing to P3 (SLM go-live) or P4, by deploying Deblob to the k3s cluster and driving controlled + real-world JSON streams through it.
- **Scope:** Deployment packaging + an in-cluster benchmark harness + a report. NO product code changes to the shipped crates (bench is additive). NO SLM (P3) — the SLM lane needs a live model endpoint and is out of scope; this baselines what is BUILT and self-contained.

## 1. Decisions (locked)

- **Placement:** everything runs on k3s **worker nodes** (`lw-c1`/`lw-c2`/`lw-c3`), with node affinity + anti-affinity to keep it OFF `lw-main` (the edge node the user wants free).
- **Broker:** **Redpanda** single-broker (Kafka-API compatible, one binary, no ZooKeeper/KRaft, ~1–2 GB). Deblob's `rdkafka` client talks to it unchanged.
- **Vault:** a **dedicated AOF + `noeviction` Redis** (the vault refuses non-persistent Redis unless `--unsafe-volatile`; do NOT reuse `shared-redis`).
- **Lifecycle:** **ephemeral** — a `deblob-bench` namespace deployed via argocd, benchmarked, then **torn down**. Redeployable from the chart anytime.
- **Datasets:** a synthetic generator + real-world corpora (GitHub webhook events, K8s events, CloudEvents) embedded as fixtures.

## 2. Architecture

```
                         k3s worker nodes (lw-c1/c2/c3)  — namespace: deblob-bench
producer Job ──kafka──▶ ingest topic ──▶ Deblob relay (serve()) ──▶ tagged topic ──▶ measurer
   (deblob-bench)                              │  │
                                              │  └─▶ discovery topic ─▶ cold lane
                                              ▼
                                        Redis vault (AOF)
                                              ▲
                          mgmt API (bearer) ──┘  ← bench prober (candidates/promote/semantic/neighbors)
   in-cluster Prometheus ── scrapes ──▶ Deblob /metrics ; kubectl top / Grafana ── pod CPU/mem
```

- Deblob runs the SAME `serve()` wiring the binary uses (relay + discovery consumer + cold lane + mgmt API), configured for Redpanda + the dedicated Redis.
- The bench harness is a NEW crate `deblob-bench` (a binary), packaged into the same image (or a sibling image) and run as a k8s Job on a worker.

## 3. Components

### 3.1 `deblob-bench` crate (new, additive — no product change)
- **Synthetic generator:** parameterized JSON stream — `distinct_schemas` (10/100/1k/10k), `optional_field_churn`, `drift_rate`, `malformed_pct`, `payload_bytes`, `count`. Deterministic (seeded) so runs are reproducible.
- **Real-world fixtures:** a handful of GitHub webhook event bodies, K8s event objects, and CloudEvents envelopes embedded as JSON fixtures; a mixed-stream mode.
- **Producer:** `rdkafka` FutureProducer → the ingest topic at a target rate (or max-throughput).
- **Measurer:** a `read_committed` consumer on the tagged topic → per-message end-to-end latency (produce ts → tag ts via a header/timestamp) + throughput; histograms → p50/p95/p99.
- **Mgmt prober:** times candidate listing, promotion, `PUT /semantic` annotation, and `GET /semantic-neighbors` at varying vault sizes.
- **Reporter:** emits a machine-readable JSON result + a human summary; the controller renders the final report.

### 3.2 Deblob packaging
- **Image:** build the existing `Dockerfile` (multi-stage, distroless) — UNVERIFIED to date; verifying the build is Task 1 (highest risk: rdkafka's vendored librdkafka needs cmake + libssl/sasl in the builder). Push to **ghcr.io** (private) → `imagePullSecret: ghcr-pull` (already used in the homelab).
- **Chart `charts/deblob`:** Deployment (Deblob) + Service (mgmt + ingest ports) + ConfigMap (`deblob.toml`) + the sops-encrypted secret env (`DEBLOB_API_TOKEN` etc.) + worker nodeAffinity/anti-affinity + resource requests/limits. Redpanda + the vault Redis as sub-charts or sibling manifests in the same namespace.
- **Config:** `deblob.toml` for the bench (relay topics, cold-lane, `[slm].enabled=false`, `[http_proxy].enabled=false`); secrets env-only via sops.

### 3.3 GitOps
- An argocd `Application` for `deblob-bench` (or `kubectl apply` of the rendered chart for a truly ephemeral run — decide in the plan). Namespace-scoped so teardown is `argocd delete` / `kubectl delete ns deblob-bench`.

## 4. Benchmark scenarios

1. **Hot-path throughput/latency sweep** — fixed stream, vary `distinct_schemas` ∈ {10,100,1k,10k} and `payload_bytes` ∈ {small, medium, large}; report msgs/s + tag latency p50/p95/p99.
2. **Malformed/quarantine** — vary `malformed_pct`; confirm quarantine path cost + that malformed never blocks the hot path.
3. **Cold-lane discovery** — a stream of novel shapes; measure candidate-creation rate + time-to-promotable; promote via the mgmt API and measure post-promotion resolve.
4. **P2-D semantic** — annotate N schemas via the governance API; measure annotation throughput + digest cost; then `GET /semantic-neighbors` latency vs vault size (100/1k/10k annotated schemas).
5. **Resilience** — kill the Redis pod mid-stream; confirm `unresolved` tagging (never `cand_`) + recovery timing when it returns.
6. **Resource** — per-pod CPU/mem (Prometheus) under sustained load; Redis vault memory growth vs schema count.
7. **Real-world mix** — the GitHub/K8s/CloudEvents corpora through the full pipeline (realistic shape distribution).

## 5. Metrics collected

Deblob `/metrics` (match/candidate/unresolved/quarantine rates, cold-lane lag, index size, tag latency p50/p99, Redis/AOF health, promotions) scraped by the in-cluster Prometheus; end-to-end latency + throughput from the measurer; per-pod CPU/mem + Redis memory from Prometheus/Grafana; mgmt-API operation latencies from the prober.

## 6. Deliverable

A **benchmark report** (visual Artifact + the raw JSON): the scenario results, the scaling curves (throughput/latency vs schema count + payload size), the P2-D digest + neighbor-query costs, resource envelope, and a plain-language read on whether the deterministic core + P2-D are performant enough to proceed to P3/P4 — plus any bottleneck found. Then **teardown**.

## 7. Non-goals

- No SLM (P3) — needs a model endpoint; separate.
- No changes to shipped product crates (bench is additive; if the bench reveals a product bug, that's a separate fix).
- No permanent standing deployment (ephemeral, torn down).
- Not a load-test-to-failure / chaos suite beyond the one resilience scenario (P1 already has crash-consistency tests).

## 8. Risks

- **Image build unverified** — rdkafka vendored build in Docker may need toolchain fixes; verify FIRST. Local build only (no git remote → no CI path); disk/time cost on the workstation.
- **ghcr push creds** — need a PAT with `packages:write`; reuse the homelab GitHub token.
- **Worker capacity** — verify `kubectl top nodes` headroom before applying Redpanda; size requests/limits conservatively; respect the cluster-intervention-restraint rule (no force-sync/rollout if the cluster is saturated).
- **Redpanda ↔ rdkafka** — verify the client negotiates (Redpanda is Kafka-API compatible but confirm the exact API version / compression the relay uses).
