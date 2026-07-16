# Deblob k3s Benchmark Implementation Plan

> **For agentic workers:** code tasks use superpowers:subagent-driven-development; infra tasks (image build, chart, deploy) are controller-driven with cellarette/helm/argocd. Steps use checkbox (`- [ ]`).

**Goal:** Deploy Deblob to k3s workers + benchmark the deterministic core + P2-D against synthetic and real-world JSON streams, then report and tear down.

**Spec:** `docs/superpowers/specs/2026-07-16-deblob-k3s-benchmark.md`.

## Global constraints

- All workloads on worker nodes (`lw-c1/c2/c3`), OFF `lw-main` (node affinity + anti-affinity). Conservative resource requests/limits.
- Respect cluster-intervention-restraint: verify `kubectl top nodes` headroom before applying; never force-sync/rollout on a saturated cluster.
- No product-crate changes (bench is additive: a new `deblob-bench` crate + a chart). Secrets env-only via sops. Ephemeral — tear down after.
- `[slm].enabled=false`, `[http_proxy].enabled=false` in the bench config (baseline the deterministic core + P2-D).

## Phase 1 — De-risk prerequisites (controller)

### Task 1: Build + push the Deblob image (HIGHEST RISK — do first)
- [ ] `docker build -t ghcr.io/<owner>/deblob:bench .` — fix any rdkafka/librdkafka/toolchain issues in the Dockerfile until it builds. (Vendored librdkafka needs cmake + libssl/sasl in the builder — already in the Dockerfile; verify.)
- [ ] `docker login ghcr.io` with a `packages:write` PAT (reuse the homelab GitHub token); `docker push`.
- [ ] Confirm `imagePullSecret ghcr-pull` in the target namespace can pull it.

### Task 2: Redpanda ↔ rdkafka smoke (local, before cluster)
- [ ] Run a single-broker Redpanda container locally; point a minimal relay/producer at it; confirm the `deblob-kafka` relay produces+consumes through Redpanda (API version / compression negotiate). If a config tweak is needed (e.g. `api.version.request`), capture it for the chart.

## Phase 2 — Bench harness (`deblob-bench`, subagent-driven)

### Task 3: `deblob-bench` crate + synthetic generator + real fixtures
- [ ] New `crates/deblob-bench` (excluded from the product build if it pulls heavy deps, else a workspace member). Seeded synthetic JSON generator with the §3.1 knobs; embedded GitHub/K8s/CloudEvents fixtures; a mixed-stream mode. Unit tests: determinism (same seed → same stream), the knobs actually vary the output (distinct-schema count, malformed %).

### Task 4: producer + measurer
- [ ] `rdkafka` producer → ingest topic at a target/max rate; a `read_committed` measurer on the tagged topic computing end-to-end latency (produce→tag) + throughput, into HDR-style histograms (p50/p95/p99). Test against a local Docker Redpanda+Redis + `serve()` (mirror `e2e_it.rs`): a fixed stream yields a throughput + latency summary.

### Task 5: mgmt-API prober + scenario driver
- [ ] Times candidate list/promote, `PUT /semantic` annotation, `GET /semantic-neighbors` vs vault size; a driver that runs the §4 scenarios from config and emits a machine-readable result JSON. Test the prober against the local stack.

### Task 6: reporter
- [ ] Aggregate scenario results → a JSON report + a human summary (the controller renders the Artifact). Test the aggregation on fixed inputs.

## Phase 3 — Deploy (controller: helm + argocd + cellarette)

### Task 7: `charts/deblob` + Redpanda + vault Redis
- [ ] `charts/deblob` (in `~/Code/helm`): Deblob Deployment/Service/ConfigMap(`deblob.toml`)/sops-secret, worker affinity + anti-affinity, resource requests/limits; Redpanda single-broker (chart or manifest, AOF/persistence for the bench is optional — ephemeral topics ok); the dedicated AOF Redis vault. A ServiceMonitor/scrape annotation for `/metrics`.
- [ ] `helm template` + `kubeconform`/lint clean.

### Task 8: deploy to `deblob-bench` namespace
- [ ] Verify `kubectl top nodes` headroom. Create the namespace + `ghcr-pull` secret + sops secret. Apply the chart (argocd app or `kubectl apply` for a clean ephemeral run). Confirm Deblob `/readyz` green, Redpanda + Redis healthy, all pods on workers (not lw-main).

## Phase 4 — Run + report + teardown

### Task 9: run scenarios
- [ ] Run the §4 scenarios as k8s Jobs (bench image, on workers); collect the result JSON + scrape Prometheus for per-pod CPU/mem + Deblob internal rates over each run window.

### Task 10: benchmark report + teardown
- [ ] Render the benchmark report (Artifact: scaling curves, latency percentiles, P2-D costs, resource envelope, go/no-go read for P3/P4). Tear down: `kubectl delete ns deblob-bench` (or argocd delete). Confirm nothing left on the cluster.

## Task order

1 → 2 (de-risk) → 3 → 4 → 5 → 6 (harness, verified locally) → 7 → 8 (deploy) → 9 → 10. Phase 2 can proceed in parallel with Phase 1 once Task 1 is underway. Do NOT deploy (Phase 3) until the harness works locally against Docker Redpanda+Redis.
