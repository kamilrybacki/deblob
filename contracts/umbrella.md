# Umbrellas

An umbrella is a gold contract: consolidating a family of schemas under one is a
durable, hard-to-reverse decision, so it is always human-in-the-loop. The model
and the deterministic pipeline may *propose* an umbrella, but only a human
approval flips it to Active — and only within a coherent domain.

```contract
case UmbrellaConsolidationNeedsHumanApproval
entity:
  Umbrella
operation:
  consolidate
requires:
  umbrella.hitl_approved == true
  umbrella.domain_coherent == true
ensures:
  result.status == Active
preserves:
  umbrella.domain
```
