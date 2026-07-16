//! Durable feedback store (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §2,
//! amendments A3/A4/A5).
//!
//! `deblob:slm-feedback` is an append-only, bounded (`XTRIM MAXLEN ~`)
//! Redis stream — the same durability shape `deblob-redis::evidence`
//! already uses for the evidence stream. Every entry is a fully-serialized
//! [`deblob_slm::TrainingExample`], written once and never mutated
//! (streams have no update-in-place operation; the only thing that can
//! remove an entry is the bounded trim).
//!
//! **Redis is the capture QUEUE/INDEX, not the training system of record**
//! (spec amendment A5). Two export shapes read it:
//!
//! - [`FeedbackStore::export_jsonl`] — the SAME fine-tune JSONL shape
//!   `deblob_eval::generate::render_finetune_jsonl` emits (`{prompt,
//!   gold_tool_call, ...}`), built through the identical PII-safe
//!   `deblob_slm::build_prompt` path. This is the shape
//!   `deblob::retrain::RetrainPlan` consumes directly to build one
//!   training blob — it therefore NEVER includes a quarantined actor's
//!   records or anything reserved for the [`SplitName::NeverTrainedSafetySuite`]
//!   partition (that partition, by definition, must never be sampled into
//!   training).
//! - [`FeedbackStore::export_snapshot`] — an IMMUTABLE, content-addressed
//!   dataset snapshot: one JSONL file per split (train / holdout / the
//!   permanent safety suite) plus a `manifest.json` with a sha256
//!   checksum per file and a snapshot-wide content-addressed id. This is
//!   the artifact meant to land on durable storage (NAS/MinIO) — Redis
//!   itself is not that system of record.
//!
//! Both paths apply the same anti-poisoning filters (spec amendment A4):
//! quarantined actors are excluded, and records sharing a `dedup_cluster`
//! are deduplicated (first-seen, by stream order, wins). Both also apply
//! the per-(family, label_source) export CAP (spec amendment A3) so a
//! burst of correlated rejections from one family/source cannot dominate
//! a single export.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use async_trait::async_trait;
use data_encoding::HEXLOWER;
use deblob_core::error::CoreError;
use deblob_core::id::{FamilyId, SchemaId};
use deblob_slm::{build_prompt, LabelSource, TrainingExample};
use redis::Client;
use sha2::{Digest, Sha256};

use crate::registry::redis_err;

/// Stream key every feedback record is appended to.
pub const FEEDBACK_STREAM_KEY: &str = "deblob:slm-feedback";

/// Redis SET key holding every currently-quarantined `actor` (spec
/// amendment A4). Membership is checked, never counted — a quarantined
/// actor's records are fully excluded from every export path, but remain
/// durably appended in the stream (quarantine is an export-time filter,
/// not a delete — consistent with this store's overall immutability
/// posture: nothing here is ever deleted, only excluded from a VIEW).
pub const QUARANTINED_ACTORS_KEY: &str = "deblob:slm-feedback:quarantined-actors";

/// Default retention bound (approximate, per Redis' `MAXLEN ~` semantics) —
/// generous enough to hold a large batch window between retrain runs while
/// still bounding memory, matching `deblob-redis::evidence`'s posture of
/// "bounded, never unbounded" rather than a specific business number.
pub const DEFAULT_FEEDBACK_STREAM_MAXLEN: u64 = 200_000;

/// `dedup_cluster` prefix convention reserving an example for the
/// permanent [`SplitName::NeverTrainedSafetySuite`] partition (spec
/// amendment A5: "A permanent `never_trained_safety_suite` partition is
/// reserved and never sampled into training"). An example whose
/// `dedup_cluster` starts with this prefix is EXCLUDED from
/// [`FeedbackStore::export_jsonl`] entirely (the training-blob path) and
/// routed to its own dedicated file in
/// [`FeedbackStore::export_snapshot`] — never mixed into `train.jsonl` or
/// `holdout.jsonl`.
pub const SAFETY_SUITE_DEDUP_PREFIX: &str = "safety:";

fn is_safety_suite(example: &TrainingExample) -> bool {
    example.dedup_cluster.starts_with(SAFETY_SUITE_DEDUP_PREFIX)
}

/// Per-(family, label_source) contribution cap applied at export time
/// (spec amendment A3: "the export caps any single family's/rejection-
/// source's contribution ... so a burst of correlated rejections can't
/// dominate"). Records beyond the cap for a given (partition_key,
/// label_source) pair are skipped for THIS export only — they remain
/// durably in the store and may appear in a later export once older
/// records age out of the window a caller reads.
///
/// **Unvalidated — ablate** (spec amendment A3 posture applies to every
/// export/weight knob in this loop, not just `weight` itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExportCaps {
    /// Maximum records counted toward one (family, label_source) pair in
    /// a single export call.
    pub max_examples_per_partition_and_label_source: usize,
}

impl Default for ExportCaps {
    /// **Unvalidated — ablate.** Generous enough not to bite ordinary
    /// traffic; tight enough that one noisy family/source cannot flood a
    /// single export.
    fn default() -> Self {
        Self {
            max_examples_per_partition_and_label_source: 50,
        }
    }
}

/// Which split a [`TrainingExample`] was assigned to by
/// [`assign_split`]/[`FeedbackStore::export_snapshot`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SplitName {
    Train,
    Holdout,
    /// Permanent, reserved, NEVER sampled into training (spec amendment
    /// A5). See [`SAFETY_SUITE_DEDUP_PREFIX`].
    NeverTrainedSafetySuite,
}

impl SplitName {
    fn file_name(self) -> &'static str {
        match self {
            SplitName::Train => "train.jsonl",
            SplitName::Holdout => "holdout.jsonl",
            SplitName::NeverTrainedSafetySuite => "never_trained_safety_suite.jsonl",
        }
    }
}

/// One split file recorded in an [`ExportManifest`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ManifestEntry {
    pub split: SplitName,
    pub file_name: String,
    /// Lowercase-hex sha256 of the file's exact bytes.
    pub sha256: String,
    pub record_count: usize,
}

/// The manifest [`FeedbackStore::export_snapshot`] writes alongside its
/// split files (spec amendment A5: "immutable, content-addressed dataset
/// snapshots (manifests + checksums)").
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExportManifest {
    /// Content-addressed snapshot id: sha256 of the sorted concatenation
    /// of every entry's own sha256 (sorted by `split` for determinism).
    /// Two snapshots with byte-identical split files always get the
    /// identical id, regardless of export ordering — the immutability
    /// contract is "never publish two different byte-contents under the
    /// same `snapshot_id`", which holds by construction here since the id
    /// IS a hash of the content.
    pub snapshot_id: String,
    pub created_at: i64,
    pub entries: Vec<ManifestEntry>,
}

/// Deterministic train/holdout assignment for [`FeedbackStore::export_snapshot`]
/// (spec amendment A5: "split by source/time/near-dup-cluster/family so a
/// paraphrase/synthetic sibling never crosses train/test"). Pure — no
/// Redis access, no randomness, unit-testable directly.
///
/// Two invariants, in priority order:
/// 1. **Family integrity** (spec §2): every example is assigned by a
///    stable hash of its OWN `partition_key` alone, so every example of
///    one family always lands on the same side.
/// 2. **Near-dup-cluster integrity** (amendment A5): the FIRST example
///    seen in a given non-empty `dedup_cluster` decides that cluster's
///    side; every later member of the SAME cluster follows it, even if
///    its own family's hash would otherwise disagree.
///
/// When a single `dedup_cluster` genuinely spans more than one family,
/// invariant 2 wins for that cluster's members over invariant 1's default
/// for their own family (documented trade-off: leaking a near-duplicate
/// across train/test is judged worse contamination than one atypical
/// example of an otherwise-cleanly-split family landing opposite its
/// siblings). Examples do not need to be pre-sorted; the assignment does
/// not depend on caller-supplied order beyond "first seen" for rule 2,
/// which in every actual caller here is the store's own chronological
/// stream order.
pub fn assign_split(examples: &[TrainingExample]) -> Vec<SplitName> {
    let mut cluster_side: BTreeMap<String, bool> = BTreeMap::new();
    examples
        .iter()
        .map(|example| {
            let family_holdout = family_hash_is_holdout(example.partition_key.as_str());
            let holdout = if example.dedup_cluster.is_empty() {
                family_holdout
            } else {
                *cluster_side
                    .entry(example.dedup_cluster.clone())
                    .or_insert(family_holdout)
            };
            if holdout {
                SplitName::Holdout
            } else {
                SplitName::Train
            }
        })
        .collect()
}

/// Stable ~20% holdout assignment derived purely from `family`'s own
/// bytes — the same family id always maps to the same side, with no
/// dependency on when/how many times it was observed.
fn family_hash_is_holdout(family: &str) -> bool {
    let digest = Sha256::digest(family.as_bytes());
    digest[0] % 5 == 0
}

fn dedup_by_cluster(examples: Vec<TrainingExample>) -> Vec<TrainingExample> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::with_capacity(examples.len());
    for example in examples {
        if example.dedup_cluster.is_empty() {
            out.push(example);
            continue;
        }
        if seen.insert(example.dedup_cluster.clone()) {
            out.push(example);
        }
        // else: a near-dup/paraphrase sibling of an already-kept record
        // in the same cluster — skipped (spec amendment A4: "the store
        // deduplicates by dedup_cluster").
    }
    out
}

fn apply_caps(examples: Vec<TrainingExample>, caps: &ExportCaps) -> Vec<TrainingExample> {
    let mut counts: BTreeMap<(String, LabelSource), usize> = BTreeMap::new();
    let mut out = Vec::with_capacity(examples.len());
    for example in examples {
        let key = (
            example.partition_key.as_str().to_string(),
            example.label_source,
        );
        let count = counts.entry(key).or_insert(0);
        if *count >= caps.max_examples_per_partition_and_label_source {
            continue;
        }
        *count += 1;
        out.push(example);
    }
    out
}

/// Durable, append-only, family-partitioned store of labeled
/// [`TrainingExample`]s. Implementations must never overwrite or delete an
/// individual record (immutability) — only the bounded retention trim may
/// remove the oldest entries. Quarantine (spec amendment A4) is likewise
/// non-destructive: it changes what a subsequent export/iteration VIEW
/// includes, never the underlying stream.
#[async_trait]
pub trait FeedbackStore: Send + Sync {
    /// Appends one immutable record. Never fails silently: an `Err` means
    /// the record was NOT durably recorded.
    async fn append(&self, example: &TrainingExample) -> Result<(), CoreError>;

    /// Renders every non-quarantined, non-safety-suite record (or, if
    /// `partition` is `Some`, only records whose `partition_key` equals
    /// it) as fine-tune JSONL — one `{prompt, gold_tool_call,
    /// label_source, weight, partition_key, recorded_at, rejection_reason,
    /// actor, source_trust_level, tool_schema_version, dedup_cluster}`
    /// line per record, `prompt` built via the PII-safe
    /// `deblob_slm::build_prompt`. Deduplicated by `dedup_cluster` and
    /// capped per (family, label_source) — see this module's docs.
    /// Returns the number of lines written.
    async fn export_jsonl(
        &self,
        writer: &mut (dyn Write + Send),
        partition: Option<&FamilyId>,
    ) -> Result<usize, CoreError>;

    /// Every record, grouped by `partition_key` (family) — the unit a
    /// retrain job assigns wholesale to train or holdout, so sibling
    /// examples of one family are never split across the two (spec §2:
    /// "a fine-tune holdout never contains a train family's siblings").
    /// Unfiltered — includes quarantined/safety-suite records, since this
    /// method is a raw inspection view, not a training-data export.
    async fn iter_by_partition(&self) -> Result<BTreeMap<String, Vec<TrainingExample>>, CoreError>;

    /// Marks `actor` quarantined (spec amendment A4: "supports
    /// rate-limit/quarantine of an anomalous source"). Idempotent.
    /// Already-appended records from `actor` are NOT deleted (immutable
    /// store) but are excluded from every export path from this call
    /// forward.
    async fn quarantine_actor(&self, actor: &str) -> Result<(), CoreError>;

    /// Every currently-quarantined actor.
    async fn quarantined_actors(&self) -> Result<BTreeSet<String>, CoreError>;

    /// Writes an IMMUTABLE, content-addressed snapshot to `dir`: one
    /// JSONL split file (`train.jsonl`, `holdout.jsonl`,
    /// `never_trained_safety_suite.jsonl` — see [`SplitName`]) plus
    /// `manifest.json`, and returns the manifest (spec amendment A5:
    /// "Redis is the capture queue, not the training system of record").
    /// Quarantined actors are excluded from every split, including the
    /// safety suite. Does blocking filesystem I/O (this is a periodic
    /// maintenance/export operation, not a hot path).
    async fn export_snapshot(&self, dir: &Path) -> Result<ExportManifest, CoreError>;
}

/// Redis-stream-backed [`FeedbackStore`].
pub struct RedisFeedbackStore {
    conn: redis::aio::ConnectionManager,
    maxlen: u64,
    caps: ExportCaps,
}

impl std::fmt::Debug for RedisFeedbackStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFeedbackStore").finish_non_exhaustive()
    }
}

impl RedisFeedbackStore {
    /// Connects with [`DEFAULT_FEEDBACK_STREAM_MAXLEN`] and
    /// [`ExportCaps::default`].
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        Self::connect_with_maxlen(url, DEFAULT_FEEDBACK_STREAM_MAXLEN).await
    }

    /// Connects with an explicit stream retention bound (tests / tuning).
    /// Uses [`ExportCaps::default`] — see [`Self::with_caps`] to override.
    pub async fn connect_with_maxlen(url: &str, maxlen: u64) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self {
            conn,
            maxlen,
            caps: ExportCaps::default(),
        })
    }

    /// Overrides the [`ExportCaps`] used by `export_jsonl`/`export_snapshot`
    /// (spec amendment A3: the cap, like `weight`, is a config value, not
    /// hard-coded).
    pub fn with_caps(mut self, caps: ExportCaps) -> Self {
        self.caps = caps;
        self
    }

    fn conn(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }

    async fn all_examples(&self) -> Result<Vec<TrainingExample>, CoreError> {
        let mut conn = self.conn();
        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(FEEDBACK_STREAM_KEY)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut out = Vec::with_capacity(entries.len());
        for (_id, fields) in entries {
            for (field, value) in fields {
                if field == "data" {
                    let example: TrainingExample = serde_json::from_str(&value).map_err(|e| {
                        CoreError::RegistryUnavailable(format!("corrupt feedback record: {e}"))
                    })?;
                    out.push(example);
                }
            }
        }
        Ok(out)
    }

    /// Quarantine-filtered, dedup-by-cluster examples — the shared base
    /// every export path builds on. `exclude_safety_suite`: `true` for
    /// `export_jsonl` (the training-blob path, which must never even see
    /// a safety-suite record), `false` for `export_snapshot` (which
    /// writes the safety suite to its own dedicated file).
    async fn filtered_examples(
        &self,
        exclude_safety_suite: bool,
    ) -> Result<Vec<TrainingExample>, CoreError> {
        let quarantined = self.quarantined_actors().await?;
        let mut examples = self.all_examples().await?;
        examples.retain(|e| !quarantined.contains(&e.actor));
        if exclude_safety_suite {
            examples.retain(|e| !is_safety_suite(e));
        }
        Ok(dedup_by_cluster(examples))
    }
}

fn record_line(example: &TrainingExample) -> Result<String, CoreError> {
    let allowed_ids: Vec<SchemaId> = example
        .retrieved
        .iter()
        .map(|c| c.schema_id.clone())
        .collect();
    let prompt = build_prompt(&example.candidate, &example.retrieved, &allowed_ids);
    let record = serde_json::json!({
        "prompt": prompt.text,
        "gold_tool_call": serde_json::to_value(&example.gold)
            .expect("InferenceDecision always serializes"),
        "label_source": example.label_source,
        "weight": example.weight,
        "partition_key": example.partition_key.as_str(),
        "recorded_at": example.recorded_at,
        "rejection_reason": example.rejection_reason,
        "actor": example.actor,
        "source_trust_level": example.source_trust_level,
        "tool_schema_version": example.tool_schema_version,
        "dedup_cluster": example.dedup_cluster,
    });
    serde_json::to_string(&record)
        .map_err(|e| CoreError::RegistryUnavailable(format!("render jsonl record: {e}")))
}

fn write_split_file(examples: &[TrainingExample]) -> Result<(String, [u8; 32]), CoreError> {
    let mut bytes: Vec<u8> = Vec::new();
    for example in examples {
        let line = record_line(example)?;
        bytes.extend_from_slice(line.as_bytes());
        bytes.push(b'\n');
    }
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let text = String::from_utf8(bytes)
        .map_err(|e| CoreError::RegistryUnavailable(format!("non-utf8 jsonl split: {e}")))?;
    Ok((text, digest))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl FeedbackStore for RedisFeedbackStore {
    async fn append(&self, example: &TrainingExample) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let payload = serde_json::to_string(example)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize feedback: {e}")))?;

        let _: String = redis::cmd("XADD")
            .arg(FEEDBACK_STREAM_KEY)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.maxlen)
            .arg("*")
            .arg("data")
            .arg(&payload)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn export_jsonl(
        &self,
        writer: &mut (dyn Write + Send),
        partition: Option<&FamilyId>,
    ) -> Result<usize, CoreError> {
        let examples = self.filtered_examples(true).await?;
        let examples = apply_caps(examples, &self.caps);
        let mut count = 0usize;
        for example in &examples {
            if let Some(p) = partition {
                if &example.partition_key != p {
                    continue;
                }
            }
            let line = record_line(example)?;
            writeln!(writer, "{line}")
                .map_err(|e| CoreError::RegistryUnavailable(format!("write jsonl: {e}")))?;
            count += 1;
        }
        Ok(count)
    }

    async fn iter_by_partition(&self) -> Result<BTreeMap<String, Vec<TrainingExample>>, CoreError> {
        let examples = self.all_examples().await?;
        let mut grouped: BTreeMap<String, Vec<TrainingExample>> = BTreeMap::new();
        for example in examples {
            grouped
                .entry(example.partition_key.as_str().to_string())
                .or_default()
                .push(example);
        }
        Ok(grouped)
    }

    async fn quarantine_actor(&self, actor: &str) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let _: i64 = redis::cmd("SADD")
            .arg(QUARANTINED_ACTORS_KEY)
            .arg(actor)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn quarantined_actors(&self) -> Result<BTreeSet<String>, CoreError> {
        let mut conn = self.conn();
        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg(QUARANTINED_ACTORS_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().collect())
    }

    async fn export_snapshot(&self, dir: &Path) -> Result<ExportManifest, CoreError> {
        let examples = self.filtered_examples(false).await?;
        let (safety_suite, rest): (Vec<TrainingExample>, Vec<TrainingExample>) =
            examples.into_iter().partition(is_safety_suite);

        let splits = assign_split(&rest);
        let mut train = Vec::new();
        let mut holdout = Vec::new();
        for (example, split) in rest.into_iter().zip(splits) {
            match split {
                SplitName::Train => train.push(example),
                SplitName::Holdout => holdout.push(example),
                SplitName::NeverTrainedSafetySuite => unreachable!(
                    "assign_split only ever returns Train/Holdout for its (non-safety-suite) input"
                ),
            }
        }
        let train = apply_caps(train, &self.caps);
        let holdout = apply_caps(holdout, &self.caps);
        // The safety suite is never sampled into training, so the
        // per-(family,label_source) contribution cap — a training-set
        // dominance guard — does not apply to it.

        std::fs::create_dir_all(dir)
            .map_err(|e| CoreError::RegistryUnavailable(format!("create snapshot dir: {e}")))?;

        let mut entries = Vec::new();
        let mut digests: Vec<(SplitName, [u8; 32])> = Vec::new();
        for (split, examples) in [
            (SplitName::Train, &train),
            (SplitName::Holdout, &holdout),
            (SplitName::NeverTrainedSafetySuite, &safety_suite),
        ] {
            let (text, digest) = write_split_file(examples)?;
            std::fs::write(dir.join(split.file_name()), &text).map_err(|e| {
                CoreError::RegistryUnavailable(format!(
                    "write snapshot split {}: {e}",
                    split.file_name()
                ))
            })?;
            digests.push((split, digest));
            entries.push(ManifestEntry {
                split,
                file_name: split.file_name().to_string(),
                sha256: HEXLOWER.encode(&digest),
                record_count: examples.len(),
            });
        }

        // Content-addressed snapshot id: sha256 of the sorted (by split)
        // concatenation of every entry's own digest bytes — deterministic
        // regardless of the order this loop happened to run in.
        digests.sort_by_key(|(split, _)| *split);
        let mut id_input = Vec::new();
        for (_, digest) in &digests {
            id_input.extend_from_slice(digest);
        }
        let snapshot_id = HEXLOWER.encode(&Sha256::digest(&id_input));

        let manifest = ExportManifest {
            snapshot_id,
            created_at: now_ms(),
            entries,
        };
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| CoreError::RegistryUnavailable(format!("render manifest: {e}")))?;
        std::fs::write(dir.join("manifest.json"), manifest_json)
            .map_err(|e| CoreError::RegistryUnavailable(format!("write manifest: {e}")))?;

        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::SchemaId;
    use deblob_slm::{
        AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision, SourceTrustLevel,
    };

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn example(
        family_id: FamilyId,
        dedup_cluster: &str,
        actor: &str,
        label_source: LabelSource,
    ) -> TrainingExample {
        TrainingExample {
            candidate: CandidateProfileView {
                observation_count: 1,
                fields: vec![],
                truncated: false,
            },
            retrieved: vec![FamilyCandidate {
                family_id: family_id.clone(),
                schema_id: schema_id(1),
                version: 1,
                distance: 0.0,
                rank: 1,
            }],
            gold: InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            label_source,
            weight: 1.0,
            partition_key: family_id,
            recorded_at: 1,
            rejection_reason: None,
            actor: actor.to_string(),
            source_trust_level: SourceTrustLevel::Standard,
            tool_schema_version: 1,
            dedup_cluster: dedup_cluster.to_string(),
        }
    }

    // -- assign_split: pure, unit-testable without Redis -------------------

    #[test]
    fn assign_split_keeps_every_example_of_one_family_on_the_same_side() {
        let family = FamilyId::new_v7();
        let examples = vec![
            example(family.clone(), "", "a1", LabelSource::HumanPromote),
            example(family.clone(), "", "a2", LabelSource::HumanPromote),
            example(family.clone(), "", "a3", LabelSource::HumanPromote),
        ];
        let splits = assign_split(&examples);
        assert_eq!(splits[0], splits[1]);
        assert_eq!(splits[1], splits[2]);
    }

    #[test]
    fn assign_split_is_deterministic_across_calls() {
        let family = FamilyId::new_v7();
        let examples = vec![example(family, "", "a1", LabelSource::HumanPromote)];
        assert_eq!(assign_split(&examples), assign_split(&examples));
    }

    #[test]
    fn assign_split_keeps_a_near_dup_cluster_together_even_across_families() {
        // `family_hash_is_holdout` is a pure function of the family's own
        // bytes — a fixed pair of families always agrees or disagrees the
        // same way every time, so (unlike the cluster id, which the
        // function never looks at) the FAMILIES themselves must vary
        // across attempts to find a disagreeing pair.
        for _ in 0..256u32 {
            let family_a = FamilyId::new_v7();
            let family_b = FamilyId::new_v7();
            let side_a_alone = family_hash_is_holdout(family_a.as_str());
            let side_b_alone = family_hash_is_holdout(family_b.as_str());
            if side_a_alone == side_b_alone {
                continue; // this pair doesn't exercise the override; try another
            }
            let cluster = "dup-cross-family";
            let a = example(family_a, cluster, "a1", LabelSource::HumanPromote);
            let b = example(family_b, cluster, "a2", LabelSource::HumanPromote);
            let together = assign_split(&[a, b]);
            assert_eq!(
                together[0], together[1],
                "a shared dedup_cluster must keep both members on the same split \
                 even when their families' own hashes disagree"
            );
            return;
        }
        panic!(
            "could not find a disagreeing family pair in 256 tries — check family_hash_is_holdout"
        );
    }

    // -- dedup_by_cluster ----------------------------------------------------

    #[test]
    fn dedup_by_cluster_keeps_first_and_drops_later_same_cluster_members() {
        let family = FamilyId::new_v7();
        let examples = vec![
            example(family.clone(), "dup-1", "a1", LabelSource::HumanPromote),
            example(family.clone(), "dup-1", "a2", LabelSource::HumanPromote),
            example(family.clone(), "", "a3", LabelSource::HumanPromote),
        ];
        let out = dedup_by_cluster(examples);
        assert_eq!(
            out.len(),
            2,
            "one dup-1 member kept, the empty-cluster one always kept"
        );
        assert_eq!(out[0].actor, "a1");
    }

    #[test]
    fn dedup_by_cluster_never_collapses_distinct_empty_clusters() {
        let family = FamilyId::new_v7();
        let examples = vec![
            example(family.clone(), "", "a1", LabelSource::HumanPromote),
            example(family.clone(), "", "a2", LabelSource::HumanPromote),
        ];
        let out = dedup_by_cluster(examples);
        assert_eq!(
            out.len(),
            2,
            "empty dedup_cluster means \"not clustered\" — never deduped"
        );
    }

    // -- apply_caps ------------------------------------------------------------

    #[test]
    fn apply_caps_limits_contribution_per_family_and_label_source() {
        let family = FamilyId::new_v7();
        let caps = ExportCaps {
            max_examples_per_partition_and_label_source: 2,
        };
        let mut examples = Vec::new();
        for i in 0..5 {
            examples.push(example(
                family.clone(),
                "",
                &format!("actor-{i}"),
                LabelSource::TrustedProposalRejected,
            ));
        }
        let out = apply_caps(examples, &caps);
        assert_eq!(
            out.len(),
            2,
            "a burst of correlated rejections must be capped"
        );
    }

    #[test]
    fn apply_caps_does_not_cap_unrelated_families_or_label_sources() {
        let family_a = FamilyId::new_v7();
        let family_b = FamilyId::new_v7();
        let caps = ExportCaps {
            max_examples_per_partition_and_label_source: 1,
        };
        let examples = vec![
            example(
                family_a.clone(),
                "",
                "a1",
                LabelSource::TrustedProposalRejected,
            ),
            example(
                family_b.clone(),
                "",
                "a2",
                LabelSource::TrustedProposalRejected,
            ),
            example(family_a, "", "a3", LabelSource::HumanPromote),
        ];
        let out = apply_caps(examples, &caps);
        assert_eq!(
            out.len(),
            3,
            "distinct (family, label_source) pairs each get their own budget"
        );
    }
}
