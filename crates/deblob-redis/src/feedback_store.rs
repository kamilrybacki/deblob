//! Durable feedback store (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §2).
//!
//! `deblob:slm-feedback` is an append-only, bounded (`XTRIM MAXLEN ~`)
//! Redis stream — the same durability shape `deblob-redis::evidence`
//! already uses for the evidence stream. Every entry is a fully-serialized
//! [`deblob_slm::TrainingExample`], written once and never mutated
//! (streams have no update-in-place operation; the only thing that can
//! remove an entry is the bounded trim). [`FeedbackStore::export_jsonl`]
//! renders the SAME fine-tune JSONL shape
//! `deblob_eval::generate::render_finetune_jsonl` emits
//! (`{prompt, gold_tool_call}`), built through the identical PII-safe
//! `deblob_slm::build_prompt` path — so, like that function, it can never
//! render a raw candidate payload value (see this module's tests).

use std::collections::BTreeMap;
use std::io::Write;

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::{FamilyId, SchemaId};
use deblob_slm::{build_prompt, TrainingExample};
use redis::Client;

use crate::registry::redis_err;

/// Stream key every feedback record is appended to.
pub const FEEDBACK_STREAM_KEY: &str = "deblob:slm-feedback";

/// Default retention bound (approximate, per Redis' `MAXLEN ~` semantics) —
/// generous enough to hold a large batch window between retrain runs while
/// still bounding memory, matching `deblob-redis::evidence`'s posture of
/// "bounded, never unbounded" rather than a specific business number.
pub const DEFAULT_FEEDBACK_STREAM_MAXLEN: u64 = 200_000;

/// Durable, append-only, family-partitioned store of labeled
/// [`TrainingExample`]s. Implementations must never overwrite or delete an
/// individual record (immutability) — only the bounded retention trim may
/// remove the oldest entries.
#[async_trait]
pub trait FeedbackStore: Send + Sync {
    /// Appends one immutable record. Never fails silently: an `Err` means
    /// the record was NOT durably recorded.
    async fn append(&self, example: &TrainingExample) -> Result<(), CoreError>;

    /// Renders every record (or, if `partition` is `Some`, only records
    /// whose `partition_key` equals it) as fine-tune JSONL — one
    /// `{prompt, gold_tool_call, label_source, weight, partition_key,
    /// recorded_at}` line per record, `prompt` built via the PII-safe
    /// `deblob_slm::build_prompt`. Returns the number of lines written.
    async fn export_jsonl(
        &self,
        writer: &mut (dyn Write + Send),
        partition: Option<&FamilyId>,
    ) -> Result<usize, CoreError>;

    /// Every record, grouped by `partition_key` (family) — the unit a
    /// retrain job assigns wholesale to train or holdout, so sibling
    /// examples of one family are never split across the two (spec §2:
    /// "a fine-tune holdout never contains a train family's siblings").
    async fn iter_by_partition(&self) -> Result<BTreeMap<String, Vec<TrainingExample>>, CoreError>;
}

/// Redis-stream-backed [`FeedbackStore`].
pub struct RedisFeedbackStore {
    conn: redis::aio::ConnectionManager,
    maxlen: u64,
}

impl std::fmt::Debug for RedisFeedbackStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFeedbackStore").finish_non_exhaustive()
    }
}

impl RedisFeedbackStore {
    /// Connects with [`DEFAULT_FEEDBACK_STREAM_MAXLEN`].
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        Self::connect_with_maxlen(url, DEFAULT_FEEDBACK_STREAM_MAXLEN).await
    }

    /// Connects with an explicit stream retention bound (tests / tuning).
    pub async fn connect_with_maxlen(url: &str, maxlen: u64) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self { conn, maxlen })
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
    });
    serde_json::to_string(&record)
        .map_err(|e| CoreError::RegistryUnavailable(format!("render jsonl record: {e}")))
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
        let examples = self.all_examples().await?;
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
}
