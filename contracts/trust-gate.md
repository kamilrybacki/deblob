# Trust gate

The SLM only ever runs on the cold-discovery lane (never the deterministic hot
path), and its proposal is accepted only once the deterministic gate has
corroborated it: it must not be weaker than the heuristic baseline, and it must
never invent a schema identity (the `schema_id` is supplied by structural
retrieval, not the model).

```contract
case SlmProposalOnlyOnColdLane
entity:
  Proposal
operation:
  propose
requires:
  proposal.on_hot_path == false
ensures:
  result.status == Draft
```

```contract
case SlmProposalAcceptedOnlyIfCorroborated
entity:
  Proposal
operation:
  accept
requires:
  proposal.corroborated == true
  proposal.confidence_ge_heuristic == true
  proposal.invents_schema_id == false
ensures:
  result.status == Accepted
```
