# Deblob domain glossary

Canonical vocabulary for Deblob's operational-invariant contracts. Each
` ```glossary ` block defines one entity (its `aka` field-path prefix, its
`states`, its `fields`, and its `operations`). `result` is the universal
post-state of an operation and shares the entity's fields.

These entities are deliberately narrow: they model only the *governance surface*
of Deblob (what may be promoted, consolidated, accepted, or captured), not the
full runtime. The contracts in the sibling files formalise the rules we hold
about that surface so `tauto verify` can prove they are mutually consistent and
free of dead (unsatisfiable) rules.

```glossary
entity Candidate
aka: candidate
describes: A provisionally-discovered schema shape on the cold-discovery lane,
  accumulating evidence before it may be promoted to a governed schema.
states:
  status: enum(Provisional, Promoted, Quarantined)
fields:
  sample_count: int
  age_minutes: int
  shape_settled: bool
  corroborated: bool
  domain_coherent: bool
  source: enum(Deterministic, Slm, Heuristic)
  schema_id: string
  domain: string
operations:
  discover
  autoPromote
```

```glossary
entity Umbrella
aka: umbrella
describes: A gold, human-governed contract consolidating a family of related
  schemas. Consolidation always requires explicit human (HITL) approval.
states:
  status: enum(Proposed, Active, Rejected)
fields:
  hitl_approved: bool
  domain_coherent: bool
  member_count: int
  domain: string
operations:
  consolidate
  approve
```

```glossary
entity Proposal
aka: proposal
describes: A probabilistic output of the SLM (a name or a field mapping). The
  deterministic trust gate must corroborate it before it can affect a schema.
states:
  status: enum(Draft, Accepted, Rejected)
fields:
  corroborated: bool
  confidence_ge_heuristic: bool
  invents_schema_id: bool
  on_hot_path: bool
  source: enum(Slm, Heuristic)
operations:
  propose
  accept
```

```glossary
entity Sample
aka: sample
describes: A captured example retained for drift sampling, the promotion
  evidence set, and model training. Must never carry raw payloads or PII.
states:
  status: enum(Captured, Archived, Dropped)
fields:
  contains_raw_payload: bool
  contains_pii: bool
operations:
  capture
  archive
```
