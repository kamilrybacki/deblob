# Deblob stability + robustness — Joint Research Report
run: jr-deblob-stability-231518 · 2026-07-23 · agents: Claude Code + Hermes

## Executive summary
The user reported deblob "highly unstable." Diagnosis: a **three-way OOM cascade** ~1 day
after the from-scratch wipe, as the corpus + ingestion grew into limits sized with **zero
headroom** and several **unbounded memory reservoirs**. Data was safe throughout (Redis AOF
on a PVC). Root-caused, band-aided to stable, then hardened with P0 code+config fixes. Nodes
were never the constraint (35–63% free); the problem was *bounds*, not capacity.

## The cascade `[C]`
1. **redis-vault** (source-of-truth store, `maxmemory 0` = uncapped) grew past its **512 Mi**
   container limit → **OOMKilled 11×** (~7 min lifetime).
2. **Cascade:** redis OOM → deblob readiness fails → mgmt API `Connection refused` →
   **namer-controller fails** + **relay lag 0 → 9388** (registry writes fail).
3. **deblob** itself **OOMKilled 35×** (~14 s) — startup relay-consumer prefetches the raw
   backlog past its **1 Gi** limit; the redis-driven lag made the spike worse.
4. **ollama** separately **OOMKilled 7×** — 2 concurrent 0.5B contexts + `keep_alive=-1` > 3 Gi.

## Root causes — FOUR unbounded reservoirs + two unsafe behaviors `[C+H]`
Hermes' code review was decisive: consumer prefetch was only *one* reservoir.
1. **Consumer prefetch** — no bounds → librdkafka default (~1 GiB/partition) × ~160 partitions.
2. **Producer queue** — `queue.buffering.max.kbytes` unset (default ~1 GiB).
3. **Relay batch** — flushed at 500 records, **byte-unbounded** → ~500 MiB at the 1 MiB ceiling.
4. **Limits sized at steady-state** (deblob 152 Mi, ollama 145 Mi, redis 445 Mi actual) with no
   headroom for transient spikes or corpus growth.
5. **Redis `maxmemory 0`** → cgroup OOMKill instead of graceful write-refusal.
6. **Discovery consumer committed the Kafka offset even when the Redis write FAILED** → silent
   data loss on a redis flap (same bug class as the relay's MessageSizeTooLarge abort).

## Fixes shipped (all live) `[C]`
**Immediate band-aids (held):** redis 512 Mi→2 Gi · deblob 1→3 Gi · ollama NUM_PARALLEL 2→1.

**P0 supply-side memory bounds — RSS now independent of backlog / partitions / record size:**
- **b32** — consumer prefetch bounded (`queued.max.messages.kbytes`=2 MiB/partition, fetch caps).
- **b33** — producer queue bounded (`queue.buffering.max.kbytes`=64 MiB); relay batch
  byte-bounded (`max_batch_bytes`=32 MiB); **discovery consumer holds the offset + retries on a
  transient store failure** (Kafka keeps the backlog), only skips+advances on a permanent
  malformed/bad-id error.
- **Redis** `maxmemory 1700mb` + `noeviction` (live + durable) → refuses writes (deblob freezes
  via its health-gate) at the ceiling rather than OOMKill — graceful, observable, no data loss.
- **Ollama** `OLLAMA_MAX_QUEUE=64` admission cap → SLM burst queues/rejects, never piles contexts.

**Verified:** kafka lib 20 + discovery 4 + **relay_it/chaos_it 8 integration tests pass** (batching
+ exactly-once transactional correctness intact under the byte bounds); clippy clean. Post-deploy:
deblob 1/1, **0 restarts**, **lag 0**, all pods stable.

## The demand-side design (user's idea, Hermes-validated) `[C+H]`
"Stop analyzing once high-confidence learned; pass through to the consumer." Correct architecture —
it makes deblob load proportional to **new/drifting shapes, not raw volume** (the firehose is
1.1M/day). Hermes: **settle-and-sample, never total bypass** (total loses drift detection, deblob's
whole value); static topic/source stamping unsafe for heterogeneous sources; initial op point
~**1-in-1000 + audit bursts**; feature-flag one proven homogeneous firehose source first;
persist settlement generation + expected fingerprints + sample policy; a sampled fingerprint
mismatch un-settles → resume full learning.

## Prioritized plan (Hermes) — remaining
- **P0 done:** byte bounds (all 4 reservoirs), redis refusal ceiling, discovery offset hold, ollama cap.
- **P0 remaining:** (a) **settle-and-sample** (its own feature — the load fix); (b) **leading-indicator
  alerting** — queue bytes, RSS slope, redis write-refusal, AOF rewrite, lag, drift exits, ollama
  queue (not just steady RSS).
- **P1:** persist settlement state; separate *forwarding* readiness from *registry-mutation*
  readiness; chaos tests (redis loss, 10k+ lag, max-size records, ollama overload → require bounded
  RSS, no OOM, no silent offset advance, eventual drain).
- **P2:** isolate the transactional forwarder failure domain; redis key-class accounting + 7/30-day
  growth forecasts; VPA recommendation-mode; settlement only for homogeneous routing identities.

## Conflicts & adjudication
Hermes flagged an **important causal gap**: runtime evidence never *isolated* consumer prefetch as
the dominant peak — the byte-unbounded batch + ~1 GiB producer queue could equally explain the OOM.
**Resolved by fixing all of them** (b33) rather than betting on one; RSS is now bounded regardless of
which dominated.

## Sources
Live: `kubectl describe`/`top`, `rpk group describe`, redis-cli, deblob logs. Deblob @ main (b31→b33 +
32-redis/40-ollama configs). Hermes vault: `research/Deblob-Stability-Robustness-JR-231518.md`.
librdkafka config docs; Redis admin docs; Ollama FAQ; Kubernetes VPA.

## Method note
Claude diagnosed live (OOM taxonomy, cascade, node headroom) + shipped all fixes; Hermes did the
code-review robustness plan + validated settle-and-sample. User contributed the key demand-side idea.
~15 min parallel, Hermes returned COMPLETE.
