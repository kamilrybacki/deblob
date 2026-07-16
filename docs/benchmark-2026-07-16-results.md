# Deblob k3s Benchmark Results — 2026-07-16

Deterministic core + P2-D, on k3s workers (lw-c1/lw-c2), Redpanda 1-broker + AOF Redis,
slm/http_proxy disabled. Report: docs/benchmark-report-2026-07-16.html + Artifact.

## Headline
- Hot-path tag/resolve: **0.43 ms mean** (10,732 ops, 99.9% ≤5 ms) — tagging logic is fast.
- **EOS relay: ~13.6 records/s** — one Kafka transaction PER RECORD (~73 ms/txn). THE bottleneck.
  Latency-bound: Deblob CPU ~0.2 cores idle, Redpanda ~0.1 cores idle.
- Producer → Kafka: 5,365 msgs/s (client can push fast).

## mgmt / P2-D API latency
- neighbor query 8 ms · get semantic 8.6 ms · annotate PUT 20.5 ms (mints sem_ on a real monoid-v1 schema)
  · promote 36 ms · candidates list 41 ms. All fast, Redis-backed, bypass the relay.

## Resource envelope (under load, all on workers)
- Deblob 176–245m / 40–55Mi · Redis 44m / 368Mi · Redpanda 103m / 547Mi. Idle.

## Findings
1. Relay throughput ~14 rec/s (per-record EOS transaction) — **P3 blocker**. Fix: batch N records/transaction.
2. GET /api/v1/schemas returns empty pages though schemas exist (list SCAN misses promoted keys; GET-by-id + promote work). To fix.
3. Bench measurer captured 0 (group-join vs idle-timeout); producer serial-send fixed (164→5365/s). Measurer open.
4. Deploy: Dockerfile rust 1.80→1.86 (edition2024 dep); distroless→bookworm-slim (rdkafka libz). Fixed.
5. Security: added NetworkPolicy (default-deny + intra-ns) + pod securityContext. Fixed.

## Recommendation
Batch records per Kafka transaction in deblob-kafka's relay before P3/P4 — amortises the ~73 ms commit,
should raise relay throughput ~100×, preserves exactly-once per batch. Every other layer is already fast.

## RELAY FIX RESULTS (batching + pipelining)
The benchmark's headline bottleneck (per-record EOS transaction) was fixed in two layers:
1. Transaction batching (500 records/txn) — amortises the commit. Alone: 14 -> 49 rec/s (3.5x).
   Revealed a 2nd bottleneck: the relay produced each record SERIALLY inside the txn
   (produce().await per record -> ~1000 awaited round-trips per 500-record batch -> ~10s/txn, CPU idle).
2. Intra-batch produce pipelining (send_result + await-all-before-commit) — parallelises the produces.
   Combined: **14 -> 598 rec/s (~43x)**, and Deblob CPU went from idle 30m to 1131m
   (now CPU-bound / doing real work, not commit-latency-bound).
Both preserve exactly-once (chaos suite green vs real KRaft: crash-before-commit invisible,
whole-batch reprocess, per-partition offset coverage, rebalance, duplicate-delivery).
598/s is the single-partition/single-relay figure (events.raw had 1 partition; Deblob had headroom
to its 2-core limit) -> scales further with more partitions + relay instances.
Merged to main: 96a3660 (batching), 509f1a1 (pipelining).
