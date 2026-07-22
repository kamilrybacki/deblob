//! Deterministic source-domain coherence gate (`jr-deblob-domain-gate-221052`).
//!
//! Structural similarity (signature.rs) measures FORM, not DOMAIN — so a
//! GPU-spot-price schema and an electricity-spot-price schema, which share
//! `cfid_price` + `cfid_region` + a timestamp, score as near neighbors even
//! though they are semantically unrelated. IDF cannot fix this (both cfids are
//! rare → high IDF), and at a small/homogeneous corpus IDF actively strips true
//! neighbors. The clean, deterministic signal is the schema's INGEST SOURCE:
//! `events.compute.runpod` vs `events.carbon.dk`. This module maps a source
//! topic to a coarse subject [`Domain`], groups domains into a few [`Cluster`]s,
//! and vetoes a neighbor pair ONLY when both sides are known and land in
//! different clusters — a proven cross-domain disjunction.
//!
//! Governance posture (mirrors `umbrella_guard.rs`): ONE-SIDED. An unknown
//! source, or a same-cluster pair, always [`DomainGate::Keep`]s — the gate can
//! only ever REMOVE a proven cross-domain false-positive, never invent a match.
//! It never mutates the exact-rational similarity score; it is a pure filter.

/// A coarse subject domain derived from an ingest source topic's namespace
/// (`events.<namespace>.<collector>`). Deliberately coarse — this is a
/// contradiction gate, not a fine taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Domain {
    Compute,
    Ai,
    Registry,
    Hardware,
    Energy,
    Carbon,
    Grid,
    Geo,
    Weather,
    Env,
    Space,
    Civic,
    KnowledgeGraph,
    Transit,
    Social,
}

/// A super-cluster of related domains. Two schemas in the SAME cluster are
/// treated as compatible (never vetoed); DIFFERENT clusters, both known, are a
/// proven-disjoint cross-domain pair. Clusters keep the gate conservative — it
/// vetoes GPU-price↔electricity-price (Tech vs Energy) while leaving
/// GPU↔ML-benchmark (both Tech) and weather↔earthquake (both Geo) intact, so a
/// genuinely single domain is never fragmented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Cluster {
    Tech,
    Energy,
    Geo,
    Civic,
    Transit,
    Social,
}

impl Domain {
    /// The super-cluster this domain belongs to.
    pub fn cluster(self) -> Cluster {
        match self {
            Domain::Compute | Domain::Ai | Domain::Registry | Domain::Hardware => Cluster::Tech,
            Domain::Energy | Domain::Carbon | Domain::Grid => Cluster::Energy,
            Domain::Geo | Domain::Weather | Domain::Env | Domain::Space => Cluster::Geo,
            Domain::Civic | Domain::KnowledgeGraph => Cluster::Civic,
            Domain::Transit => Cluster::Transit,
            Domain::Social => Cluster::Social,
        }
    }

    /// Stable lowercase slug for response/telemetry surfaces.
    pub fn slug(self) -> &'static str {
        match self {
            Domain::Compute => "compute",
            Domain::Ai => "ai",
            Domain::Registry => "registry",
            Domain::Hardware => "hardware",
            Domain::Energy => "energy",
            Domain::Carbon => "carbon",
            Domain::Grid => "grid",
            Domain::Geo => "geo",
            Domain::Weather => "weather",
            Domain::Env => "env",
            Domain::Space => "space",
            Domain::Civic => "civic",
            Domain::KnowledgeGraph => "knowledge_graph",
            Domain::Transit => "transit",
            Domain::Social => "social",
        }
    }
}

/// Maps an ingest source topic to its [`Domain`]. Topics follow
/// `events.<namespace>.<collector>` (e.g. `events.compute.runpod`); the second
/// segment is the domain namespace. Returns `None` for an unrecognized or
/// malformed source (→ the gate keeps the candidate — one-sided).
pub fn domain_of_source(source: &str) -> Option<Domain> {
    let namespace = source.strip_prefix("events.")?.split('.').next()?;
    Some(match namespace {
        "compute" => Domain::Compute,
        "ai" => Domain::Ai,
        "registry" => Domain::Registry,
        "hw" => Domain::Hardware,
        "energy" => Domain::Energy,
        "carbon" => Domain::Carbon,
        "grid" => Domain::Grid,
        "geo" => Domain::Geo,
        "weather" => Domain::Weather,
        "env" => Domain::Env,
        "space" => Domain::Space,
        "civic" => Domain::Civic,
        "kg" => Domain::KnowledgeGraph,
        "transit" => Domain::Transit,
        "firehose" => Domain::Social,
        _ => return None,
    })
}

/// The decision for one candidate pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainGate {
    /// Compatible, or insufficient evidence — the candidate is retained.
    Keep,
    /// Both sides are known and land in different clusters — a proven
    /// cross-domain disjunction. The candidate is a false-positive.
    VetoProvenDisjoint,
}

impl DomainGate {
    pub fn is_veto(self) -> bool {
        matches!(self, DomainGate::VetoProvenDisjoint)
    }

    /// Stable cause code for response/telemetry.
    pub fn cause(self) -> &'static str {
        match self {
            DomainGate::Keep => "keep",
            DomainGate::VetoProvenDisjoint => "veto_proven_disjoint",
        }
    }
}

/// The gate: veto ONLY when both domains are known and in different clusters.
/// Any unknown (either side) → `Keep` (one-sided: uncertainty never vetoes).
pub fn domain_gate(a: Option<Domain>, b: Option<Domain>) -> DomainGate {
    match (a, b) {
        (Some(x), Some(y)) if x.cluster() != y.cluster() => DomainGate::VetoProvenDisjoint,
        _ => DomainGate::Keep,
    }
}

/// Convenience: gate directly from two source topics.
pub fn domain_gate_sources(a: &str, b: &str) -> DomainGate {
    domain_gate(domain_of_source(a), domain_of_source(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_namespace_maps_to_domain_and_cluster() {
        assert_eq!(
            domain_of_source("events.compute.runpod"),
            Some(Domain::Compute)
        );
        assert_eq!(domain_of_source("events.carbon.dk"), Some(Domain::Carbon));
        assert_eq!(domain_of_source("events.ai.benchmarks"), Some(Domain::Ai));
        assert_eq!(
            domain_of_source("events.registry.pypi"),
            Some(Domain::Registry)
        );
        assert_eq!(Domain::Compute.cluster(), Cluster::Tech);
        assert_eq!(Domain::Carbon.cluster(), Cluster::Energy);
        assert_eq!(Domain::Ai.cluster(), Cluster::Tech);
        // Unknown / malformed → None → keep.
        assert_eq!(domain_of_source("events.raw"), None);
        assert_eq!(domain_of_source("not-a-topic"), None);
        assert_eq!(domain_of_source(""), None);
    }

    #[test]
    fn vetoes_the_gpu_vs_energy_cross_domain_false_positive() {
        // The motivating case: RunPod GPU pricing (Tech) vs carbon/energy
        // pricing (Energy) — structurally near-identical, semantically disjoint.
        assert_eq!(
            domain_gate_sources("events.compute.runpod", "events.carbon.dk"),
            DomainGate::VetoProvenDisjoint
        );
        assert_eq!(
            domain_gate_sources("events.compute.runpod", "events.energy.awattar"),
            DomainGate::VetoProvenDisjoint
        );
    }

    #[test]
    fn keeps_same_cluster_and_unknown_pairs() {
        // Same Tech cluster — GPU vs ML benchmark vs package registry — kept.
        assert_eq!(
            domain_gate_sources("events.compute.runpod", "events.ai.benchmarks"),
            DomainGate::Keep
        );
        assert_eq!(
            domain_gate_sources("events.compute.gpu-prices", "events.registry.pypi"),
            DomainGate::Keep
        );
        // Same Geo cluster — weather vs earthquakes vs river levels — kept.
        assert_eq!(
            domain_gate_sources("events.weather.openmeteo", "events.geo.quakes"),
            DomainGate::Keep
        );
        // Unknown source on either side → keep (one-sided).
        assert_eq!(
            domain_gate_sources("events.compute.runpod", "events.raw"),
            DomainGate::Keep
        );
    }

    #[test]
    fn gate_is_symmetric() {
        assert_eq!(
            domain_gate_sources("events.carbon.dk", "events.compute.runpod"),
            domain_gate_sources("events.compute.runpod", "events.carbon.dk"),
        );
    }
}
