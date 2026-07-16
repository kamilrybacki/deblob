# Deblob k3s comparative-experiment deploy

Ephemeral stack for `docs/superpowers/specs/2026-07-16-deblob-experiment.md`
(Task 5: "deploy: model endpoints + experiment Job pinned to workers
c1/c2 ... reusing the `deploy/bench` pattern"). This is manifests + a
Dockerfile target only — **no Rust logic changed by this task**.

Everything here runs on the **worker nodes** (`lw-c1`/`lw-c2`) via a
required `node-role.kubernetes.io/control-plane DoesNotExist` affinity on
every Pod-creating resource (every Deployment + the Job). The control-plane
node and `lw-main` (the edge host, not a k8s node at all) are **never
touched** — nothing in this deploy schedules there, nothing here requests a
GPU (workers are CPU-only; the arm-C fine-tune step runs remotely — see
below).

## Components (namespace `deblob-experiment`)

| File | Resource |
|------|----------|
| `00-namespace.yaml` | the `deblob-experiment` namespace |
| `05-networkpolicy.yaml` | default-deny ingress+egress, intra-ns allow, DNS allow, model-server-only internet-egress allow |
| `10-redis-vault.yaml` | AOF + noeviction Redis vault (reused verbatim from `deploy/bench/20-redis-vault.yaml` — feedback store + model registry) |
| `20-model-ollama.yaml` | Ollama Deployment + Service :11434 (Granite 3.1-MoE 1B, Qwen2.5 1.5B-Instruct) |
| `21-model-llamacpp.yaml` | llama.cpp server Deployment + Service :8080 (FunctionGemma 270M GGUF) |
| `22-model-cactus.yaml` | Cactus Deployment + Service :8081 (Needle 26M) — **placeholder image, see below** |
| `30-experiment-config.yaml` | ConfigMap: corpus/model-roster/gate/training-job config (see "current vs forward-declared") |
| `35-experiment-secret.example.yaml` | Secret template — copy, fill in, gitignored |
| `90-experiment-job.yaml` | the `deblob-experiment` runner Job |

## Deploy

```bash
# 1) namespace + network policy
kubectl apply -f deploy/experiment/00-namespace.yaml
kubectl apply -f deploy/experiment/05-networkpolicy.yaml

# 2) image pull secret (private ghcr) — same secret name/shape as deploy/bench
kubectl -n deblob-experiment create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<gh-user> --docker-password=<ghcr-PAT>

# 3) the experiment secret — copy the example, fill in real values, apply
#    (NEVER commit the filled-in file; it's gitignored)
cp deploy/experiment/35-experiment-secret.example.yaml deploy/experiment/35-experiment-secret.yaml
#   ... edit HF_TOKEN / FINE_TUNE_API_KEY in the copy ...
kubectl apply -f deploy/experiment/35-experiment-secret.yaml

# 4) redis vault + model-serving stack + config
kubectl apply -f deploy/experiment/10-redis-vault.yaml
kubectl apply -f deploy/experiment/20-model-ollama.yaml
kubectl apply -f deploy/experiment/21-model-llamacpp.yaml
kubectl apply -f deploy/experiment/22-model-cactus.yaml   # see Cactus caveat below
kubectl apply -f deploy/experiment/30-experiment-config.yaml

# 5) wait for the model servers (weight pulls take a while on first start)
kubectl -n deblob-experiment rollout status deploy/redis-vault
kubectl -n deblob-experiment rollout status deploy/ollama
kubectl -n deblob-experiment rollout status deploy/llama-cpp
# kubectl -n deblob-experiment rollout status deploy/cactus   # once the image exists

# 6) run the experiment
kubectl apply -f deploy/experiment/90-experiment-job.yaml
kubectl -n deblob-experiment wait --for=condition=complete job/deblob-experiment-run --timeout=10m
```

`kubectl apply -f deploy/experiment/` also works for steps 4-5+6 combined
(namespace/networkpolicy/secrets must exist first — see step ordering
above).

## Collecting the report

The Job's container `tee`s the Markdown report to
`/reports/experiment-report.md` (an `emptyDir`, so it's gone once the Job's
pod is deleted) alongside a snapshot of the ConfigMap it was run with:

```bash
POD=$(kubectl -n deblob-experiment get pods -l job-name=deblob-experiment-run -o name | head -1)
kubectl -n deblob-experiment logs "$POD"    # same content, streamed
kubectl -n deblob-experiment cp "${POD#pod/}:/reports/experiment-report.md" ./experiment-report.md
```

If a durable report across Job re-runs is wanted, swap the `reports`
`emptyDir` in `90-experiment-job.yaml` for a `PersistentVolumeClaim` against
the cluster's existing worker-accessible StorageClass — the Job spec
otherwise needs no change.

## Teardown

```bash
kubectl delete ns deblob-experiment
```

Nothing else on the cluster is touched — `lw-main` and the control-plane
node never see a Pod from this namespace.

## Model weight provisioning (spec §8)

No weights are baked into any image (constraint: "do NOT bake weights into
images"). Chosen approach per backend — simplest option that fits the
`deploy/bench` pattern (off-the-shelf public images, no custom Dockerfile
per service):

- **Ollama** (`20-model-ollama.yaml`): an `initContainer` runs a throwaway
  `ollama serve` long enough to `ollama pull granite3.1-moe:1b` and
  `ollama pull qwen2.5:1.5b-instruct` into a shared `emptyDir`
  (`OLLAMA_MODELS=/data/models`), then exits; the main container starts the
  real, long-lived server against that already-populated directory. Tags
  verified against `ollama.com/library/{granite3.1-moe,qwen2.5}/tags`.
- **llama.cpp** (`21-model-llamacpp.yaml`): `llama-server`'s own `-hf` flag
  pulls the GGUF straight from the Hugging Face Hub at startup into
  `$LLAMA_CACHE` (an `emptyDir`) — no `initContainer` needed. Repo verified
  via the HF Hub API: `lmstudio-community/functiongemma-270m-it-GGUF`,
  whose only file is `functiongemma-270m-it-F16.gguf` (llama-server falls
  back to a repo's sole file when its default `Q4_K_M` tag doesn't exist).
- **Cactus** (`22-model-cactus.yaml`): **unresolved, documented placeholder
  — see the concern below.**

Both the Ollama and llama.cpp Deployments carry `role: model-server` so
`05-networkpolicy.yaml`'s scoped egress rule (443 only, to
`0.0.0.0/0`) is the ONLY hole in an otherwise fully default-deny namespace;
the experiment Job and the Redis vault carry no such label and get **no**
internet egress at all, matching spec §8's "the experiment Job may reach
the model-serving Services and the Redis vault only."

## Secrets

Env-only, never baked into an image or ConfigMap:

| Key | Purpose |
|---|---|
| `HF_TOKEN` | Authenticates the `hf` CLI itself, which `HfJobsBackend` (`crates/deblob-experiment/src/continual/training_job/hf_jobs.rs`) shells out to (`hf jobs run ...`) from the experiment Job's own pod, to submit/poll arm-C's remote fine-tune job. |
| `FINE_TUNE_API_KEY` | Placeholder for a future non-HF-Jobs `TrainingBackend` (spec §8: "pluggable"). Unused today — leave as the placeholder value if you have no second backend. |

Copy `35-experiment-secret.example.yaml` to `35-experiment-secret.yaml`
(gitignored — see `/.gitignore`), fill in real values, `kubectl apply` it.
Never commit the filled-in file.

## Arm-C fine-tune is REMOTE — the worker never trains

Spec §7/§9: "no real GPU training inside the cluster ... the REMOTE
fine-tune backend ... is pluggable ... the harness treats it as 'submit
job -> receive gated quantized adapter'." Concretely: `HfJobsBackend`
builds and shells out an `hf jobs run --flavor <hardware_flavor> --secrets
<hf_token_secret_ref> ...` command from wherever
`TrainingBackendFineTuneHook::train` runs — in this deploy, that's the
`deblob-experiment` Job's own pod, on a CPU-only worker. That pod:

1. Submits the job (`hf jobs run ...`) — the actual training compute runs
   on Hugging Face's infrastructure, not this cluster.
2. Polls for completion.
3. Receives artifact **digests only** (never raw weights) — see
   `TRAINING_CHECKPOINT_KEY`/`QUANTIZED_WEIGHTS_KEY` in `training_job/mod.rs`.
4. Hands those digests to `deblob::model_registry`'s existing statistical
   gate + two-stage canary — unchanged, no new promotion logic.

`hardware_flavor` and `max_usd_ceiling`/`max_runtime_minutes` (the budget
ceiling — a spec over the ceiling is rejected **before** any backend
`submit` call, per `validate_budget` in `training_job/mod.rs`) are set in
`30-experiment-config.yaml`'s `[continual.training_job]` section.
`lw-main` and the control-plane node are never involved in any of this.

## Current vs forward-declared config

`crates/deblob-experiment/src/main.rs` (as of this task) takes no CLI
flags and always runs `RunConfig::default()` (the synthetic corpus + the
mock inferencer) — see `run.rs`'s own module docs: "real corpus ingestion
... and live model adapters ... are later tasks." This task is deploy-only
(no Rust logic change), so:

- `30-experiment-config.yaml`'s `[corpus.synthetic]` section mirrors
  today's actual hardcoded `RunConfig::default()` values (seed 42, 12
  families, 8 variants/family, 2000 bootstrap iterations) as an audit
  trail.
- Everything else in that ConfigMap (`[corpus.real]`, `[models]`,
  `[continual.training_job]`) is **forward-declared**: the contract a
  future config/CLI-loading change should read, not something today's
  binary consumes. The Job still runs correctly today — it just runs the
  synthetic/mock experiment and archives the (currently unused) config
  file alongside the report for traceability.
- `[gate]` is **never** meant to be consumed at runtime: the trust gate is
  frozen at the Rust level (`deblob::shadow::evaluate_policy` takes no
  config argument; its thresholds — `POLICY_MAX_DISTANCE = 0.15`,
  `POLICY_MIN_MARGIN = 0.10`, `POLICY_MIN_OBSERVATIONS = 20`, plus a
  rank-must-equal-1 requirement — are `const`s in
  `crates/deblob/src/shadow.rs`). That section is an audit trail of the
  pinned `deblob` dependency's actual compiled-in values, nothing more.

## Concerns / open items

1. **Cactus has no verified Linux deploy path.** No official prebuilt
   container image exists; PyPI's `cactus-compute` 2.0.1 ships only
   `manylinux_2_27_aarch64` and macOS-arm64 wheels (no x86_64 Linux
   wheel); the `cactus` CLI itself is built from source
   (`git clone ... && source ./setup && cactus build --python`, a
   dev-environment script) and its kernels are documented as "ARM NEON
   SIMD" — this looks like an ARM/mobile-first project with unconfirmed
   x86_64 server support. `22-model-cactus.yaml` ships as a documented
   placeholder (`image: ...cactus-server:TO_BE_BUILT`) — it will validate
   under `--dry-run=client` but will not actually run until someone (a)
   confirms `lw-c1`/`lw-c2`'s CPU architecture and (b) builds and pushes a
   real image, or decides to substitute the spec §5 "Needle JAX adapter —
   diagnostic only" instead.
2. **The model-weight-pull egress hole is IP-range-wide, not FQDN-scoped.**
   `allow-model-weight-pull-egress` allows `0.0.0.0/0:443` for
   `role: model-server` pods because Ollama's registry / Hugging Face
   Hub / PyPI don't publish stable CIDRs. If the cluster's CNI is Cilium
   (per `~/Code/CLAUDE.md`'s "Cilium migration gotchas" note), a
   `CiliumNetworkPolicy` FQDN rule scoped to the specific registries would
   be a tighter follow-up.
3. **`ghcr.io/kamilandrzejrybacki-inc/deblob:e1` doesn't exist yet.**
   `90-experiment-job.yaml` references a new tag because the
   `deblob-experiment` binary is new to the Dockerfile's build in this
   task — CI needs to build and push it (see the repo-root `Dockerfile`
   diff) before the Job can actually run; `--dry-run=client` validates the
   YAML shape regardless.
4. **`deblob-experiment`'s runtime image doesn't ship the corpus fixtures.**
   The Dockerfile's runtime stage copies only compiled binaries (never
   `tests/` or corpus data), so `[corpus.real]`'s fixture paths
   (`/corpus/github_archive_sample.json`, `/corpus/wikimedia_sample.json`)
   will need a second ConfigMap or PVC mounted at `/corpus` once real
   corpus-ingest wiring lands — out of scope for this deploy-only task.
