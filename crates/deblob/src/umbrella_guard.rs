//! Deterministic name + value corroboration for umbrella consolidation
//! (joint design `dc-umbrella-signals-1907`, Stage 2). PURE logic — no I/O,
//! no model — computed in SHADOW: it produces evidence + cause codes for a
//! consolidated umbrella field but (this stage) never changes grouping or
//! excludes a member. Enforcement is a later, config-gated stage.
//!
//! Two signals, both one-sided by design:
//!   * **Value-bucket guard** — over each contributing leaf's coarse,
//!     OR-merged [`deblob_core::ports::value_bucket`] mask. An overlapping
//!     mask can NEVER prove compatibility (the masks are booleans, not
//!     distributions); only a *disjoint* pair among comparable units flags a
//!     `CONTRADICTORY` verdict. Different units → `NOT_COMPARABLE` (defer to
//!     the verified transform, which handles unit conversion). Missing data
//!     or below minimum support → `UNKNOWN` (never a veto).
//!   * **Name similarity** — capped POSITIVE corroboration only. Identical
//!     non-generic field names across members corroborate the existing
//!     `canonical_field_id`; name *disagreement* is never negative evidence
//!     (pure renames are a core supported case).

use serde::Serialize;

/// Default minimum numeric observations a leaf must carry before its mask may
/// participate in a `CONTRADICTORY` verdict — guards early-sample/seasonal
/// bias (a mismatch seen from a handful of rows must not block). The effective
/// value is configurable per deployment (`[umbrella].min_value_support`); this
/// is the fallback the pure-function tests use.
pub const MIN_SUPPORT: u64 = 30;

/// Generic field names that carry no identifying signal — they never
/// corroborate (an `id`/`value`/`status` match across unrelated domains is
/// meaningless).
const GENERIC_NAMES: &[&str] = &[
    "id", "value", "val", "type", "status", "data", "name", "key", "code", "kind",
];

/// The four-outcome value-bucket verdict (Hermes review): one-sided negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueVerdict {
    /// No contradiction found among comparable, well-supported leaves. NOT a
    /// positive proof of equality — overlap can't establish that.
    Compatible,
    /// A disjoint bucket pair among comparable units → exclude from
    /// auto-merge, route to HITL (shadow: recorded only).
    Contradictory,
    /// Absent profiles, <2 numeric leaves, or below `MIN_SUPPORT` → no veto.
    Unknown,
    /// Units/scale differ → defer to the verified transform, don't compare masks.
    NotComparable,
}

/// Bounded deterministic cause codes recorded for a consolidated field —
/// never SLM prose, never raw values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CauseCode {
    CfidExact,
    TypeCompatible,
    NameCorroborated,
    ValueProfileCompatible,
    ValueProfileUnknown,
    ValueProfileNotComparable,
    ValueProfileContradiction,
}

/// One member's evidence for a single consolidated umbrella field.
#[derive(Debug, Clone)]
pub struct MemberEvidence {
    /// Leaf field name (last path segment) as observed on this member.
    pub name: String,
    /// Unit code from the semantic annotation, if any (`None` = unitless).
    pub unit: Option<String>,
    /// The value-bucket bitmask for this leaf (`0` if no value profile / no
    /// numeric observations).
    pub mask: u8,
    /// Numeric observation count backing `mask` (`0` if unknown).
    pub numeric_count: u64,
    /// Whether this member had a value profile at all (distinguishes
    /// "unknown" from "known, no numbers").
    pub has_profile: bool,
}

/// The shadow evidence computed for one consolidated umbrella field.
#[derive(Debug, Clone, Serialize)]
pub struct FieldGuard {
    pub value_verdict: ValueVerdict,
    pub name_corroborated: bool,
    pub causes: Vec<CauseCode>,
}

fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Capped POSITIVE name corroboration: every member's normalized name is
/// identical AND not a generic token. Any disagreement, emptiness, or generic
/// name yields `false` — never negative evidence.
pub fn name_corroborated(names: &[String]) -> bool {
    if names.len() < 2 {
        return false;
    }
    let mut norm = names.iter().map(|n| normalize_name(n));
    let Some(first) = norm.next() else { return false };
    if first.is_empty() || GENERIC_NAMES.contains(&first.as_str()) {
        return false;
    }
    norm.all(|n| n == first)
}

/// Whether all members' units are mutually comparable: every member unitless,
/// or every member carrying the SAME unit code. A mix (some unit, some none,
/// or differing codes) is NOT comparable — bucket masks across different
/// units/scales are meaningless (cents vs dollars) and must defer to the
/// verified transform.
fn units_comparable(members: &[MemberEvidence]) -> bool {
    let first = &members[0].unit;
    members.iter().all(|m| &m.unit == first)
}

/// The value-bucket verdict over a field's members (see module docs).
/// `min_support` is the minimum numeric observations a leaf needs before its
/// mask may drive a `CONTRADICTORY` verdict.
pub fn value_verdict(members: &[MemberEvidence], min_support: u64) -> ValueVerdict {
    if members.len() < 2 {
        return ValueVerdict::Unknown;
    }
    if !units_comparable(members) {
        return ValueVerdict::NotComparable;
    }
    // Only leaves with actual numeric observations + adequate support can
    // participate; masks with no numbers behind them prove nothing.
    let numeric: Vec<&MemberEvidence> = members
        .iter()
        .filter(|m| m.has_profile && m.mask != 0 && m.numeric_count >= min_support)
        .collect();
    if numeric.len() < 2 {
        return ValueVerdict::Unknown;
    }
    // One-sided negative: any DISJOINT pair (no shared bucket) is suspicious.
    for i in 0..numeric.len() {
        for j in (i + 1)..numeric.len() {
            if numeric[i].mask & numeric[j].mask == 0 {
                return ValueVerdict::Contradictory;
            }
        }
    }
    ValueVerdict::Compatible
}

/// Full shadow evaluation for one consolidated field: the value verdict, name
/// corroboration, and the bounded cause codes that explain the decision.
/// `CFID_EXACT` + `TYPE_COMPATIBLE` are always present (that is precisely what
/// the typed grouping key already guarantees for every consolidated field).
pub fn evaluate_field(members: &[MemberEvidence], min_support: u64) -> FieldGuard {
    let verdict = value_verdict(members, min_support);
    let names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
    let corroborated = name_corroborated(&names);

    let mut causes = vec![CauseCode::CfidExact, CauseCode::TypeCompatible];
    if corroborated {
        causes.push(CauseCode::NameCorroborated);
    }
    causes.push(match verdict {
        ValueVerdict::Compatible => CauseCode::ValueProfileCompatible,
        ValueVerdict::Contradictory => CauseCode::ValueProfileContradiction,
        ValueVerdict::Unknown => CauseCode::ValueProfileUnknown,
        ValueVerdict::NotComparable => CauseCode::ValueProfileNotComparable,
    });

    FieldGuard { value_verdict: verdict, name_corroborated: corroborated, causes }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::ports::value_bucket as vb;

    fn m(name: &str, unit: Option<&str>, mask: u8, count: u64) -> MemberEvidence {
        MemberEvidence {
            name: name.to_string(),
            unit: unit.map(|u| u.to_string()),
            mask,
            numeric_count: count,
            has_profile: true,
        }
    }

    #[test]
    fn disjoint_masks_same_unit_are_contradictory() {
        // large-int vs small-ratio, both unitless, well-supported → suspicious.
        let members = vec![
            m("a", None, vb::LARGE_POSITIVE, 1000),
            m("b", None, vb::SMALL_POSITIVE, 1000),
        ];
        assert_eq!(value_verdict(&members, MIN_SUPPORT), ValueVerdict::Contradictory);
        let g = evaluate_field(&members, MIN_SUPPORT);
        assert!(g.causes.contains(&CauseCode::ValueProfileContradiction));
    }

    #[test]
    fn overlapping_masks_are_compatible_not_proven() {
        let members = vec![
            m("retailPrice", None, vb::SMALL_POSITIVE | vb::MEDIUM_POSITIVE | vb::LARGE_POSITIVE, 5000),
            m("unitPrice", None, vb::SMALL_POSITIVE | vb::MEDIUM_POSITIVE | vb::LARGE_POSITIVE, 5000),
        ];
        assert_eq!(value_verdict(&members, MIN_SUPPORT), ValueVerdict::Compatible);
    }

    #[test]
    fn different_units_defer_as_not_comparable() {
        // cents vs dollars: disjoint masks, but different units → NOT the guard's call.
        let members = vec![
            m("amount", Some("[cents]"), vb::LARGE_POSITIVE, 1000),
            m("amount", Some("USD"), vb::SMALL_POSITIVE, 1000),
        ];
        assert_eq!(value_verdict(&members, MIN_SUPPORT), ValueVerdict::NotComparable);
    }

    #[test]
    fn below_min_support_is_unknown() {
        let members = vec![
            m("a", None, vb::LARGE_POSITIVE, 5),
            m("b", None, vb::SMALL_POSITIVE, 5),
        ];
        assert_eq!(value_verdict(&members, MIN_SUPPORT), ValueVerdict::Unknown);
    }

    #[test]
    fn missing_profiles_are_unknown() {
        let members = vec![
            MemberEvidence { name: "a".into(), unit: None, mask: 0, numeric_count: 0, has_profile: false },
            MemberEvidence { name: "b".into(), unit: None, mask: 0, numeric_count: 0, has_profile: false },
        ];
        assert_eq!(value_verdict(&members, MIN_SUPPORT), ValueVerdict::Unknown);
    }

    #[test]
    fn identical_nongeneric_names_corroborate() {
        assert!(name_corroborated(&["retailPrice".into(), "retailPrice".into()]));
        assert!(name_corroborated(&["retail_price".into(), "retailPrice".into()])); // normalized
    }

    #[test]
    fn generic_and_differing_names_do_not_corroborate() {
        assert!(!name_corroborated(&["id".into(), "id".into()])); // generic
        assert!(!name_corroborated(&["aaa".into(), "xxx".into()])); // differ (rename)
        assert!(!name_corroborated(&["retailPrice".into()])); // single member
    }

    #[test]
    fn name_disagreement_is_never_a_veto() {
        // Different names but compatible values: verdict is still Compatible,
        // only NAME_CORROBORATED is absent — names never turn negative.
        let members = vec![
            m("aaa", None, vb::SMALL_POSITIVE, 1000),
            m("xxx", None, vb::SMALL_POSITIVE, 1000),
        ];
        let g = evaluate_field(&members, MIN_SUPPORT);
        assert_eq!(g.value_verdict, ValueVerdict::Compatible);
        assert!(!g.name_corroborated);
        assert!(!g.causes.contains(&CauseCode::NameCorroborated));
    }
}
