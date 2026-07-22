//! P2-D Task 10: the diagnostic-only semantic-neighbor search orchestrator
//! (`docs/superpowers/plans/deblob-p2d-02-hermes-similarity.md` §4/§5/§6,
//! authoritative). Gathers the query schema's active-signature feature
//! postings via [`SemanticStore::signature_candidates`] (Task 10's bounded
//! inverted index, `deblob-redis::semantic`), scores every candidate with
//! Task 9's `deblob_semantic::signature::similarity`/`strength`, and ranks
//! by the spec's §5.9 tie-break. Reuses Task 9's scoring verbatim — this
//! module never recomputes or duplicates the weighted-Jaccard math.
//!
//! ## Hard invariant: strictly diagnostic, read-only
//!
//! Mirrors `semantic_drift.rs`'s own "proposal-only" posture: every call
//! this module makes to a [`SemanticStore`] is a READ
//! (`active_revision`/`signature_candidates`) — nothing here ever calls
//! `append_revision`. A neighbor is always a *candidate*, never labelled
//! `equivalent_schema`; a score of `1.0` proves nothing about identity. The
//! integration test in `crates/deblob/tests/semantic_neighbors_it.rs`
//! snapshots the full relevant Redis key set before/after a neighbors query
//! and asserts byte-identical state.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use deblob_core::id::{RevisionId, SchemaId};
use deblob_core::revision::{SemError, SignatureCandidates};
use deblob_semantic::signature::{
    self, has_anchor_weighted, idf_multiplier, matched_feature_classes, semantic_signature,
    shared_anchor_count_weighted, similarity_weighted, strength_weighted, Score, Strength,
};

use crate::semantic_store::SemanticStore;

/// Default `k` (spec §4) when the caller's query string omits it.
pub const DEFAULT_K: usize = 10;
/// Maximum `k` (spec §4) — larger requests are clamped, never rejected (a
/// diagnostic best-effort tool; see `api::semantic::get_semantic_neighbors`'s
/// docs for why clamping was chosen over `422`).
pub const MAX_K: usize = 50;

/// One scored, ranked neighbor candidate — Task 10's §6 response shape,
/// pre-serialization. NEVER carries an "is this the same schema" claim;
/// callers must render it as a candidate, never `equivalent_schema`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neighbor {
    pub schema_id: SchemaId,
    pub semantic_revision_id: RevisionId,
    pub score: Score,
    pub strength: Strength,
    pub shared_anchor_count: usize,
    pub matched_feature_classes: Vec<&'static str>,
}

/// The outcome of a neighbors query — every variant is a legitimate,
/// non-error result (the caller decides HTTP status): `NoAnchor` and
/// `TooBroad` are just as much "the diagnostic ran successfully and found
/// this" as `Found` is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NeighborOutcome {
    /// The query schema's active signature carries no anchor feature (spec
    /// §4): "never expand a `temporal.kind=instant`-only query toward the
    /// whole vault." `neighbors` is deliberately never computed/returned.
    NoAnchor,
    /// The candidate union (spec §4) exceeded
    /// `deblob_core::revision::MAX_SIGNATURE_CANDIDATES`. NEVER a silently
    /// truncated top-k list — the caller must surface this distinctly.
    TooBroad,
    /// Ranked, top-`k`-truncated neighbor candidates (spec §5.9's
    /// tie-break: strength desc, exact rational score desc, shared-anchor
    /// count desc, `sch_` bytes asc). `idf_population_n` is the
    /// active-annotated corpus size `N` used for the IDF weighting of this
    /// result — surfaced in the response so a corpus-relative score is
    /// reproducible alongside `weights_version` (`jr-deblob-similarity-idf-221040`).
    Found {
        neighbors: Vec<Neighbor>,
        idf_population_n: u64,
    },
}

/// Ranks `a` before `b` when `a` is the BETTER match (spec §5.9): higher
/// strength, then higher exact rational score (via `Score::cmp_rank`, never
/// the presentation-only decimal), then more shared anchor features, then
/// lexicographically SMALLER `sch_` bytes (ascending — the one tie-break
/// axis that is NOT "bigger wins").
fn cmp_neighbors(a: &Neighbor, b: &Neighbor) -> Ordering {
    b.strength
        .cmp(&a.strength)
        .then_with(|| b.score.cmp_rank(&a.score))
        .then_with(|| b.shared_anchor_count.cmp(&a.shared_anchor_count))
        .then_with(|| a.schema_id.as_str().cmp(b.schema_id.as_str()))
}

/// Orchestrates the full Task 10 neighbor search for `query_sch_id`. `Ok(None)`
/// means the query schema has never been annotated (the caller maps this to
/// `404`, mirroring `api::semantic::get_semantic`'s own un-annotated
/// posture) — every other outcome is `Ok(Some(NeighborOutcome::_))`.
///
/// `k` is assumed ALREADY clamped to `[0, MAX_K]` by the caller (the HTTP
/// handler) — this function does not re-clamp, so a pure-logic caller (e.g.
/// a future CLI) controls its own bound.
pub async fn neighbors(
    store: &dyn SemanticStore,
    query_sch_id: &SchemaId,
    k: usize,
) -> Result<Option<NeighborOutcome>, SemError> {
    let Some((query_revision, _etag)) = store.active_revision(query_sch_id).await? else {
        return Ok(None);
    };
    let query_signature = semantic_signature(&query_revision.metadata);

    // Phase A — IDF stats for the query's OWN feature postings. `N` and each
    // query feature's `df` come back in one atomic snapshot; from them we build
    // the query-side `idf_multiplier` used for the anchor gate and for pruning
    // zero-IDF (corpus-ubiquitous) postings out of the candidate union BEFORE
    // the `SUNION` — so a timestamp posting present in most of the corpus can
    // never explode the candidate set (Hermes, jr-deblob-similarity-idf-221040).
    let query_hex = query_signature.feature_keys_hex();
    let (n_query, query_dfs) = store.idf_stats(&query_hex).await?;
    let query_df: HashMap<Vec<u8>, u64> = query_signature
        .feature_keys()
        .into_iter()
        .zip(query_dfs)
        .collect();
    let idf_query = |key: &[u8]| idf_multiplier(n_query, query_df.get(key).copied().unwrap_or(0));

    if !has_anchor_weighted(&query_signature, &idf_query) {
        return Ok(Some(NeighborOutcome::NoAnchor));
    }

    // Prune zero-IDF query postings from the union. Keep every posting whose
    // `df == 0` guard can't apply (defensive) — only a POSITIVE-df,
    // majority-of-corpus feature is dropped. If pruning would empty the union
    // (e.g. a query all of whose features are ubiquitous yet still anchored via
    // an event/namespace), fall back to the full key set rather than returning
    // nothing.
    let pruned_hex: Vec<String> = query_signature
        .feature_keys()
        .into_iter()
        .zip(query_hex.iter())
        .filter(|(raw, _)| idf_query(raw) > 0)
        .map(|(_, hex)| hex.clone())
        .collect();
    let union_hex = if pruned_hex.is_empty() {
        query_hex.clone()
    } else {
        pruned_hex
    };

    let candidate_ids = match store.signature_candidates(&union_hex).await? {
        SignatureCandidates::TooBroad => return Ok(Some(NeighborOutcome::TooBroad)),
        SignatureCandidates::Bounded(ids) => ids,
    };

    // Load every candidate's active signature first, so Phase B can fetch the
    // document frequencies for the FULL union of query + candidate features in
    // one coherent snapshot and score against a single `N`.
    let mut candidates: Vec<(SchemaId, RevisionId, signature::SemanticSignature)> = Vec::new();
    for candidate_id in candidate_ids {
        if candidate_id == *query_sch_id {
            // Exclude the query schema itself (spec §6).
            continue;
        }
        // Defensive: the postings index is maintained atomically with the
        // active pointer, so every candidate SHOULD have an active revision. A
        // missing one (index/active-pointer race window, or a rebuild
        // mid-flight) is skipped rather than failing the whole query — the same
        // "skip what can't be reconstructed" posture
        // `rebuild_index`/`rebuild_semantic_index` already use.
        let Some((candidate_revision, _)) = store.active_revision(&candidate_id).await? else {
            continue;
        };
        let candidate_signature = signature::semantic_signature(&candidate_revision.metadata);
        candidates.push((
            candidate_id,
            candidate_revision.revision_id,
            candidate_signature,
        ));
    }

    // Phase B — the full-union IDF snapshot for scoring. A feature the query
    // lacks but a candidate has still contributes to the weighted denominator,
    // so its `df` must be known too. `raw_to_hex` dedups the union (hex is a
    // bijection of the raw bytes) while preserving the raw⇒hex pairing the
    // `idf_multiplier` closure needs (it is handed raw feature bytes).
    let mut raw_to_hex: BTreeMap<Vec<u8>, String> = BTreeMap::new();
    for (raw, hex) in query_signature
        .feature_keys()
        .into_iter()
        .zip(query_signature.feature_keys_hex())
    {
        raw_to_hex.entry(raw).or_insert(hex);
    }
    for (_, _, sig) in &candidates {
        for (raw, hex) in sig.feature_keys().into_iter().zip(sig.feature_keys_hex()) {
            raw_to_hex.entry(raw).or_insert(hex);
        }
    }
    let ordered_raw: Vec<Vec<u8>> = raw_to_hex.keys().cloned().collect();
    let ordered_hex: Vec<String> = raw_to_hex.into_values().collect();
    let (n, dfs) = store.idf_stats(&ordered_hex).await?;
    let df_by_raw: HashMap<Vec<u8>, u64> = ordered_raw.into_iter().zip(dfs).collect();
    let idf = |key: &[u8]| idf_multiplier(n, df_by_raw.get(key).copied().unwrap_or(0));

    let mut found = Vec::new();
    for (candidate_id, revision_id, candidate_signature) in candidates {
        // Drop candidates that share no effective ANCHOR with the query under
        // IDF — an overlap on only generic stop-word cfids (b24) OR on only
        // corpus-ubiquitous discriminative cfids (IDF) is Insufficient strength
        // and is NOT a real neighbor (jr-deblob-similarity-220904, -idf-221040).
        let s = strength_weighted(&query_signature, &candidate_signature, &idf);
        if s == Strength::Insufficient {
            continue;
        }

        found.push(Neighbor {
            schema_id: candidate_id,
            semantic_revision_id: revision_id,
            score: similarity_weighted(&query_signature, &candidate_signature, &idf),
            strength: s,
            shared_anchor_count: shared_anchor_count_weighted(
                &query_signature,
                &candidate_signature,
                &idf,
            ),
            matched_feature_classes: matched_feature_classes(
                &query_signature,
                &candidate_signature,
            ),
        });
    }

    found.sort_by(cmp_neighbors);
    found.truncate(k);
    Ok(Some(NeighborOutcome::Found {
        neighbors: found,
        idf_population_n: n,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use deblob_core::id::SemanticId;
    use deblob_core::revision::{AppendOutcome, Etag, ReasonCode, Revision, RevisionStatus};
    use deblob_core::semantic::{
        CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment, SemanticMetadata, Unit,
        UnitSystem,
    };
    use std::collections::HashMap;

    fn revision_for(sch_id: &SchemaId, metadata: SemanticMetadata, seed: u8) -> Revision {
        Revision {
            revision_id: RevisionId::new_v7(),
            sch_id: sch_id.clone(),
            sem_id: SemanticId::from_digest(&[seed; 32]),
            metadata,
            canonical_semantic_bytes: vec![seed],
            previous_revision_id: None,
            actor: "kamil".to_string(),
            reason_code: ReasonCode::Correction,
            reason: "fixture".to_string(),
            recorded_at: 1,
            effective_from: 1,
            status: RevisionStatus::Active,
        }
    }

    fn metadata_with_cfid(cfid: &str) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("x".to_string())],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new(cfid)),
                    identifier_namespace: None,
                    unit: None,
                    numeric_scale: None,
                    temporal: None,
                    enum_semantics: None,
                },
            }],
        }
    }

    fn metadata_with_unit(code: &str) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("x".to_string())],
                semantics: FieldSemantics {
                    canonical_field_id: None,
                    identifier_namespace: None,
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: code.to_string(),
                    }),
                    numeric_scale: None,
                    temporal: None,
                    enum_semantics: None,
                },
            }],
        }
    }

    /// A minimal in-memory `SemanticStore` for pure orchestration-logic
    /// unit tests — no Redis. `signature_candidates` brute-forces the same
    /// way a tiny in-memory index would; the point of THESE tests is the
    /// scoring/ranking/gating logic in `neighbors`, not the index itself
    /// (which has its own real-Redis coverage in `deblob-redis`).
    #[derive(Default)]
    struct FixtureStore {
        active: HashMap<SchemaId, Revision>,
    }

    #[async_trait]
    impl SemanticStore for FixtureStore {
        async fn append_revision(
            &self,
            _sch_id: &SchemaId,
            _metadata: &SemanticMetadata,
            _canonical_bytes: &[u8],
            _sem_id: &SemanticId,
            _actor: &str,
            _reason_code: ReasonCode,
            _reason: &str,
            _recorded_at: i64,
            _effective_from: i64,
            _expected_etag: Option<Etag>,
        ) -> Result<AppendOutcome, SemError> {
            unimplemented!("neighbors() never writes")
        }

        async fn active_semantic(
            &self,
            sch_id: &SchemaId,
        ) -> Result<Option<(SemanticMetadata, SemanticId, Etag)>, SemError> {
            Ok(self
                .active
                .get(sch_id)
                .map(|r| (r.metadata.clone(), r.sem_id.clone(), Etag(1))))
        }

        async fn active_revision(
            &self,
            sch_id: &SchemaId,
        ) -> Result<Option<(Revision, Etag)>, SemError> {
            Ok(self.active.get(sch_id).cloned().map(|r| (r, Etag(1))))
        }

        async fn revisions(&self, sch_id: &SchemaId) -> Result<Vec<Revision>, SemError> {
            Ok(self.active.get(sch_id).cloned().into_iter().collect())
        }

        async fn schemas_by_semantic(
            &self,
            _sem_id: &SemanticId,
        ) -> Result<Vec<SchemaId>, SemError> {
            Ok(vec![])
        }

        async fn signature_candidates(
            &self,
            feature_keys_hex: &[String],
        ) -> Result<SignatureCandidates, SemError> {
            let wanted: std::collections::HashSet<&String> = feature_keys_hex.iter().collect();
            let ids: Vec<SchemaId> = self
                .active
                .iter()
                .filter(|(_, rev)| {
                    let sig = semantic_signature(&rev.metadata);
                    sig.feature_keys_hex().iter().any(|k| wanted.contains(k))
                })
                .map(|(id, _)| id.clone())
                .collect();
            Ok(SignatureCandidates::Bounded(ids))
        }

        async fn idf_stats(
            &self,
            feature_keys_hex: &[String],
        ) -> Result<(u64, Vec<u64>), SemError> {
            // Saturating stats: these handler tests exercise scoring / ranking /
            // gating ORCHESTRATION, not corpus-relative IDF demotion (that is
            // unit-tested directly in deblob-semantic::signature, and end-to-end
            // against real Redis in deblob-redis's integration tests). A large
            // `N` with `df == 1` makes every feature maximally rare, so
            // `idf_multiplier` saturates at `IDF_MAX` and reproduces the pre-IDF
            // (b24) ranking these fixtures assert.
            Ok((u64::from(u32::MAX), vec![1; feature_keys_hex.len()]))
        }
    }

    fn sch(seed: u8) -> SchemaId {
        SchemaId::from_digest(&[seed; 32])
    }

    #[tokio::test]
    async fn unannotated_query_schema_returns_none() {
        let store = FixtureStore::default();
        let result = neighbors(&store, &sch(1), DEFAULT_K).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn no_anchor_features_returns_no_anchor_outcome() {
        let mut store = FixtureStore::default();
        let id = sch(1);
        store
            .active
            .insert(id.clone(), revision_for(&id, metadata_with_unit("Cel"), 1));

        let result = neighbors(&store, &id, DEFAULT_K).await.unwrap();
        assert_eq!(result, Some(NeighborOutcome::NoAnchor));
    }

    #[tokio::test]
    async fn excludes_query_schema_and_only_includes_feature_sharing_candidates() {
        let mut store = FixtureStore::default();
        let query = sch(1);
        // Shares the query's `canonical_field_id` — a single shared field
        // is `Medium` per Task 9's `strength` (§3: `Strong` needs a shared
        // `canonical_event_type_id` OR >=2 shared fields at >=50% coverage,
        // neither of which this fixture has).
        let related = sch(2);
        let unrelated = sch(3);

        store.active.insert(
            query.clone(),
            revision_for(&query, metadata_with_cfid("device.temperature"), 1),
        );
        store.active.insert(
            related.clone(),
            revision_for(&related, metadata_with_cfid("device.temperature"), 2),
        );
        // Shares NOTHING with `query` — must never appear as a candidate at
        // all, proving the union is index-derived, not a scan of every
        // known active schema.
        store.active.insert(
            unrelated.clone(),
            revision_for(&unrelated, metadata_with_cfid("device.humidity"), 3),
        );

        let outcome = neighbors(&store, &query, DEFAULT_K).await.unwrap().unwrap();
        let NeighborOutcome::Found {
            neighbors: found, ..
        } = outcome
        else {
            panic!("expected Found, got {outcome:?}");
        };
        let ids: Vec<&SchemaId> = found.iter().map(|n| &n.schema_id).collect();
        assert!(
            !ids.contains(&&query),
            "must exclude the query schema itself"
        );
        assert_eq!(
            ids,
            vec![&related],
            "unrelated schema must never be a candidate"
        );
        assert_eq!(found[0].strength, Strength::Medium);
    }

    #[tokio::test]
    async fn tie_break_prefers_higher_strength_then_score_then_smaller_sch_id() {
        // Two candidates both sharing `query`'s canonical_field_id — same
        // strength/score — must resolve by lexicographically smaller
        // `sch_` bytes (spec §5.9's final tie-break axis).
        let mut store = FixtureStore::default();
        let query = sch(1);
        let a = sch(200); // deliberately the LARGER byte value
        let b = sch(2); // deliberately the SMALLER byte value

        store.active.insert(
            query.clone(),
            revision_for(&query, metadata_with_cfid("device.temperature"), 1),
        );
        store.active.insert(
            a.clone(),
            revision_for(&a, metadata_with_cfid("device.temperature"), 2),
        );
        store.active.insert(
            b.clone(),
            revision_for(&b, metadata_with_cfid("device.temperature"), 3),
        );

        let outcome = neighbors(&store, &query, DEFAULT_K).await.unwrap().unwrap();
        let NeighborOutcome::Found {
            neighbors: found, ..
        } = outcome
        else {
            panic!("expected Found");
        };
        assert_eq!(found.len(), 2);
        assert!(
            found[0].schema_id.as_str() < found[1].schema_id.as_str(),
            "the lexicographically smaller sch_ must rank first on a tie"
        );
    }

    #[tokio::test]
    async fn k_truncates_the_result() {
        let mut store = FixtureStore::default();
        let query = sch(1);
        store.active.insert(
            query.clone(),
            revision_for(&query, metadata_with_cfid("device.temperature"), 1),
        );
        for seed in 2..6u8 {
            let id = sch(seed);
            store.active.insert(
                id.clone(),
                revision_for(&id, metadata_with_cfid("device.temperature"), seed),
            );
        }

        let outcome = neighbors(&store, &query, 2).await.unwrap().unwrap();
        let NeighborOutcome::Found {
            neighbors: found, ..
        } = outcome
        else {
            panic!("expected Found");
        };
        assert_eq!(
            found.len(),
            2,
            "k=2 must truncate the 4 available candidates"
        );
    }
}
