<p align="center">
  <img src="docs/brand/readme-hero-dark.svg#gh-dark-mode-only" alt="deblob — Give every blob a permanent shape.">
  <img src="docs/brand/readme-hero-light.svg#gh-light-mode-only" alt="deblob — Give every blob a permanent shape.">
</p>

<p align="center"><strong>Continuous schema discovery and identity tagging for uncontrolled data.</strong></p>

Deblob is an open-source Rust service that assigns every message a durable
schema identity. Deterministic fingerprinting stays on the synchronous path;
sampling and semantic discovery run asynchronously; policy decides which
candidates become approved schemas.

<sub>CI · pre-release · Rust 1.80+ · Apache-2.0 — badges land with the first tagged release.</sub>

---

## What it does

Deblob sits inline on a stream. For every message it computes a canonical
structural fingerprint and attaches a permanent schema identity in transport
metadata — the payload is never modified. Shapes it has never seen are tagged
provisionally and copied to a durable discovery lane, where sampling and (later)
small language models *propose* a classification and a policy layer *decides*.

The mascot mimics and proposes; deterministic code fingerprints; policy decides.

## Install

```
cargo install deblob
```

> Deblob is in early development and is **not yet published to crates.io**.
> Today, build from source:
>
> ```
> git clone https://github.com/<owner>/deblob && cd deblob
> cargo build --release
> ```

The shortest real path — relay a Kafka topic, tagging every record with its
schema identity:

```
deblob relay kafka \
  --raw-topic events.raw \
  --tagged-topic events.tagged
```

1. **Start it** — `deblob relay kafka` consumes `events.raw`.
2. **Input** — each JSON record on the raw topic.
3. **Produces** — the same record on `events.tagged`, plus a
   `deblob-schema-id` header (`sch_…` for a known schema, `cand_…` for a new
   shape, `unresolved` if the registry is briefly unavailable).
4. **Inspect** — `deblob schema show <id>` against the schema vault.

## A 30-second proof

```
$ deblob schema show sch_7M4K2W…F9Q

family       orders.created
version      3
identity     sch_7M4K2W…F9Q
status       known
provenance   deterministic fingerprint
```

Identifiers are mid-truncated (`sch_7M4K2W…F9Q`) and every transcript stays
meaningful under `NO_COLOR`. The mascot never appears in daemon or
machine-readable output.

## What Deblob guarantees

#### Stable identity
The same canonical schema receives the same durable identity. Schema identity
does not depend on an inference model.

#### Discovery stays off the hot path
Sampling and semantic inference operate asynchronously. They cannot silently
redefine records flowing through the relay.

#### Promotion is explicit policy
Candidates are proposed; deterministic checks and policy decide whether they
become approved schemas.

## Lifecycle

<p align="center"><img src="docs/brand/lifecycle.svg" alt="Record lifecycle: fingerprint to schema identity on the hot path; asynchronous sample to candidate to policy promotion; malformed records to quarantine."></p>

- Solid amber — synchronous hot path
- Dashed blue — asynchronous discovery
- Teal square — immutable approved identity
- Red branch — malformed or quarantined

## Why Deblob?

| | Hot-path identity | Handles drift | Semantic discovery | Explicit promotion |
|---|---|---|---|---|
| Static schema registry | yes | manual | no | manual |
| Inference-only pipeline | variable | yes | yes | often implicit |
| **Deblob** | **deterministic** | **yes** | **async** | **explicit** |

Rows describe the intended design; claims will be tightened to match shipped
behaviour and benchmarks as the implementation lands.

## Architecture

```
producer → relay / fingerprint → consumer
                    │
                    └── sample → discovery → candidate vault
                                              │
                                              └── policy / promotion
```

Deterministic fingerprinting and identity resolution run per message on the
synchronous path. Unknown shapes are copied to a durable discovery lane in the
same transaction as the tagged record, so a crash can never emit a tag whose
discovery evidence was lost. Detailed documentation:

- [Design specification](docs/superpowers/specs/2026-07-14-deblob-design.md) — architecture, identity model, vault, security
- [Design book](docs/brand/design-book.md) — visual identity and voice

Planned dedicated docs: Kafka and HTTP integration, identity construction,
candidate lifecycle, compatibility policy, persistence and recovery, metrics,
security model.

## Designed for the data plane

- Deterministic identity remains independent of SLM availability.
- Explicit behaviour when persistence is unhealthy — promotions freeze, the
  relay keeps tagging (`unresolved` while the registry is unreachable).
- Stable `DBL-xxxx` error codes.
- Bounded labels in Prometheus metrics (no schema or producer IDs in labels).
- `NO_COLOR` honoured; machine-readable output carries no decorative content.
- Candidate promotion is an authenticated and audited boundary.

```
DBL-2204 AOF persistence is unhealthy; promotions are frozen.
```

## Project status

Deblob is in **early development (pre-alpha)**. The deterministic core (P1) is
under active construction; nothing is published to a package registry yet.

**Stable:** nothing is API-stable yet — everything may change before 0.1.0.

**Under active development (P1):**
- Bounded JSON parsing and canonical fingerprinting
- Immutable Redis schema vault with atomic publication
- Transactional Kafka relay (topic → tag → derived topic)
- Authenticated management API and audited promotion

**Not yet supported:**
- The semantic discovery lane and small-language-model classification (P2) —
  provider-agnostic behind an OpenAI-compatible HTTP adapter, with in-process
  llama.cpp as an opt-in
- The HTTP push reverse proxy (P2)
- Formats other than strict JSON

The CLI transcripts above show the intended interface; exact flags are being
finalised as the commands land.

## Contributing and governance

- **Documentation** — `docs/`
- **Contributing** — contribution guide to follow
- **Security** — see the design specification's security section; a
  `SECURITY.md` disclosure policy will accompany the first release
- **Compatibility policy** — schema drift and versioning rules are defined in
  the design specification
- **License** — [Apache-2.0](LICENSE)

<p align="center"><img src="docs/brand/imprintling-mark.svg" width="28" alt="deblob"></p>
