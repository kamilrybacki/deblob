# Auto-promotion

A discovered candidate may only be auto-promoted to a governed schema once it has
accumulated enough settled, corroborated, in-domain evidence — and never when the
name came from the heuristic fallback (a degraded, model-unavailable run). This is
the "model proposes, deterministic code decides" rule applied to promotion: the
thresholds are deterministic and non-negotiable.

```contract
case AutoPromoteRequiresSettledCorroboratedEvidence
entity:
  Candidate
operation:
  autoPromote
requires:
  candidate.sample_count >= 50
  candidate.age_minutes >= 10
  candidate.shape_settled == true
  candidate.corroborated == true
  candidate.domain_coherent == true
  candidate.source != Heuristic
ensures:
  result.status == Promoted
preserves:
  candidate.schema_id
  candidate.domain
```
