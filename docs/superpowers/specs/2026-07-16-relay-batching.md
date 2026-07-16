# Relay Transaction Batching — Design Specification

- **Date:** 2026-07-16
- **Status:** Draft
- **Motivation:** The k3s benchmark (`docs/benchmark-2026-07-16-results.md`) proved the exactly-once relay commits **one Kafka transaction per record** → ~13.6 rec/s ceiling, latency-bound on the per-record commit (~73 ms), while Deblob's CPU + the tagging logic (~0.43 ms) sit idle. Batching N records per transaction amortises the commit → expected ~100× throughput, exactly-once preserved per batch.
- **Scope:** `deblob-kafka`'s relay loop + config + the chaos test suite. NO change to the tagging logic, header hygiene, quarantine, discovery, or the `HotMatcher`. NO change to the exactly-once *guarantee* — only its *granularity* (per-batch instead of per-record).

## 1. The change

Replace the per-record transaction cycle with a per-batch one:

```
loop:
  batch = accumulate up to max_batch_records records,
          OR until max_batch_linger elapses since the first record,
          OR shutdown (flush what we have, then exit)
  begin_transaction
  for each record in batch:
      run_transaction_body  (classify → produce tagged/quarantine [+ discovery])
  send_offsets_to_transaction( per-partition MAX(offset)+1 across the batch )
  commit_transaction
  (any produce/offset error → abort_transaction → whole batch reprocessed)
```

## 2. Correctness (exactly-once must hold — the load-bearing part)

- **Atomicity unchanged:** the whole batch (all produces + the offset commit + commit) is ONE transaction. A `read_committed` consumer sees nothing until commit. Crash mid-batch → the transaction is never committed → on restart the fenced consumer re-reads from the last committed offset = **the start of the batch** → the whole batch is reprocessed exactly once. Same guarantee as today, coarser unit.
- **Per-partition offsets:** a batch may span partitions (multiple raw-topic partitions on one consumer). `send_offsets_to_transaction` MUST include, for every partition touched in the batch, `MAX(offset in batch for that partition) + 1`. Track a `BTreeMap<(topic,partition), max_offset>` while accumulating.
- **Abort on any error:** if ANY record's produce (or the offset send) fails, abort the whole transaction → the whole batch is reprocessed. (A malformed record is NOT an error — it produces to quarantine as a normal part of the batch. Only a genuine produce/transport error aborts.)
- **Rebalance:** the existing `pre_rebalance` callback aborts the open transaction. With batching, an in-flight batch aborts on rebalance → its records are reprocessed after the partitions are reassigned. Correct; the `transaction_open` `AtomicBool` still guards it.
- **Ordering:** records within a partition are produced in consume order (the batch preserves recv order). Cross-partition ordering is not guaranteed today and isn't changed.

## 3. Config

Add to `[kafka]` (defaults chosen to batch by default — the whole point):
- `max_batch_records: usize` (default **500**) — flush when the batch reaches this many records.
- `max_batch_linger_ms: u64` (default **100**) — flush when this long has elapsed since the first record in the batch, even if under the count. Bounds the added latency for a partially-full batch.

`max_batch_records = 1` reproduces the exact current per-record behaviour (a documented escape hatch). Both are non-secret config; `deny_unknown_fields` + a default so existing configs still parse.

**Latency trade-off (documented):** batching adds up to `max_batch_linger_ms` to the first record of a batch. For a schema-tagger this is acceptable and configurable; the throughput gain is ~100×. The hot-path *tagging* latency (~0.43 ms) is unchanged — only the commit is amortised.

## 4. Chaos suite (the safety net — must be re-validated)

`crates/deblob-kafka/tests/chaos_it.rs` (+ `relay_it.rs`) exercise crash-consistency, exactly-once, rebalance, and duplicate-delivery against real KRaft. These are the correctness proof and MUST be updated for per-batch semantics and still pass:
- `FaultPoint::AfterProduceBeforeCommit` now fires after the BATCH's produces, before the batch commit — a crash there must leave `read_committed` seeing NOTHING from the whole batch, and a fresh relay must reprocess the whole batch exactly once (byte-identical tags).
- The exactly-once + duplicate-delivery tests must assert set-equality/exact counts over a batch that spans multiple records (and ideally multiple partitions).
- Run the batched relay through the SAME chaos scenarios; a failure here blocks the change.

## 5. Non-goals

- No change to the tagging/matcher/quarantine/discovery/header logic.
- No relaxation of exactly-once (only granularity).
- No pipelining of multiple concurrent transactions (one open transaction at a time — Kafka's transactional producer is single-transaction).
- No re-benchmark automation (a manual re-run against the k3s stack confirms the gain, out of scope for the code change).

## 6. Acceptance

- The relay batches by default; `cargo test -p deblob-kafka` incl. the Docker chaos suite is green with per-batch semantics; the exactly-once + crash-consistency + rebalance properties hold over multi-record batches; a `max_batch_records=1` config reproduces per-record behaviour. A manual re-benchmark shows relay throughput up from ~14 rec/s by ~2 orders of magnitude.
