//! Redis backend for the gold-umbrella [`UmbrellaStore`].
//!
//! Layout (mirrors the candidate/schema stores):
//!   * `deblob:umbrella:<id>`               HASH {record: <schema json>, state: <state>}
//!   * `deblob:umbrellas:<state>`           SET of umbrella ids (a maintained index, so
//!     `list_umbrellas` is O(state) with NO empty pages, unlike the SCAN candidate list)
//!   * `deblob:umbrella-transform:<umb>:<child>`  STRING <transform json>
//!   * `deblob:umbrella-transforms:<umb>`   SET of child ids for that umbrella
//!
//! `set_state` only rewrites the hash's `state` field + moves the id between state
//! index sets — it never re-serialises the schema. `promote_bundle` writes the
//! umbrella (→ active) and all its transforms in one `MULTI/EXEC` pipeline.

use async_trait::async_trait;
use deblob_umbrella::store::{
    StoreError, StoredUmbrella, UmbrellaBundle, UmbrellaState, UmbrellaStore,
};
use deblob_umbrella::types::{ChildTransform, UmbrellaSchema};

#[derive(Clone)]
pub struct RedisUmbrella {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisUmbrella {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisUmbrella")
    }
}

fn umb_key(id: &str) -> String {
    format!("deblob:umbrella:{id}")
}
fn state_index(state: UmbrellaState) -> String {
    format!("deblob:umbrellas:{}", state_str(state))
}
fn transform_key(umb: &str, child: &str) -> String {
    format!("deblob:umbrella-transform:{umb}:{child}")
}
fn transforms_index(umb: &str) -> String {
    format!("deblob:umbrella-transforms:{umb}")
}
fn state_str(state: UmbrellaState) -> &'static str {
    match state {
        UmbrellaState::Provisional => "provisional",
        UmbrellaState::Active => "active",
        UmbrellaState::Rejected => "rejected",
    }
}
fn parse_state(s: &str) -> Option<UmbrellaState> {
    match s {
        "provisional" => Some(UmbrellaState::Provisional),
        "active" => Some(UmbrellaState::Active),
        "rejected" => Some(UmbrellaState::Rejected),
        _ => None,
    }
}
const ALL_STATES: [UmbrellaState; 3] =
    [UmbrellaState::Provisional, UmbrellaState::Active, UmbrellaState::Rejected];

fn backend(e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(e.to_string())
}

impl RedisUmbrella {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl UmbrellaStore for RedisUmbrella {
    async fn put_umbrella(&self, schema: &UmbrellaSchema, state: UmbrellaState) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(schema).map_err(backend)?;
        let id = &schema.umbrella_id;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HSET").arg(umb_key(id)).arg("record").arg(&json).arg("state").arg(state_str(state)).ignore();
        // exactly one state-index membership
        for s in ALL_STATES {
            if s == state {
                pipe.cmd("SADD").arg(state_index(s)).arg(id).ignore();
            } else {
                pipe.cmd("SREM").arg(state_index(s)).arg(id).ignore();
            }
        }
        pipe.query_async::<()>(&mut conn).await.map_err(backend)?;
        Ok(())
    }

    async fn get_umbrella(&self, id: &str) -> Result<Option<StoredUmbrella>, StoreError> {
        let mut conn = self.conn.clone();
        let fields: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(umb_key(id)).arg("record").arg("state")
            .query_async(&mut conn).await.map_err(backend)?;
        match (fields.first().cloned().flatten(), fields.get(1).cloned().flatten()) {
            (Some(record), Some(state)) => {
                let schema: UmbrellaSchema = serde_json::from_str(&record).map_err(backend)?;
                let state = parse_state(&state)
                    .ok_or_else(|| StoreError::Backend(format!("bad state {state:?}")))?;
                Ok(Some(StoredUmbrella { schema, state }))
            }
            _ => Ok(None),
        }
    }

    async fn set_state(&self, id: &str, state: UmbrellaState) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let exists: bool = redis::cmd("EXISTS").arg(umb_key(id)).query_async(&mut conn).await.map_err(backend)?;
        if !exists {
            return Err(StoreError::UmbrellaNotFound(id.to_string()));
        }
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HSET").arg(umb_key(id)).arg("state").arg(state_str(state)).ignore();
        for s in ALL_STATES {
            if s == state {
                pipe.cmd("SADD").arg(state_index(s)).arg(id).ignore();
            } else {
                pipe.cmd("SREM").arg(state_index(s)).arg(id).ignore();
            }
        }
        pipe.query_async::<()>(&mut conn).await.map_err(backend)?;
        Ok(())
    }

    async fn list_umbrellas(&self, state: UmbrellaState) -> Result<Vec<StoredUmbrella>, StoreError> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(state_index(state)).query_async(&mut conn).await.map_err(backend)?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(u) = self.get_umbrella(&id).await? {
                out.push(u);
            }
        }
        Ok(out)
    }

    async fn put_transform(&self, t: &ChildTransform) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(t).map_err(backend)?;
        redis::pipe().atomic()
            .cmd("SET").arg(transform_key(&t.umbrella_id, &t.child_schema_id)).arg(json).ignore()
            .cmd("SADD").arg(transforms_index(&t.umbrella_id)).arg(&t.child_schema_id).ignore()
            .query_async::<()>(&mut conn).await.map_err(backend)?;
        Ok(())
    }

    async fn get_transform(&self, umbrella_id: &str, child_id: &str) -> Result<Option<ChildTransform>, StoreError> {
        let mut conn = self.conn.clone();
        let json: Option<String> = redis::cmd("GET")
            .arg(transform_key(umbrella_id, child_id)).query_async(&mut conn).await.map_err(backend)?;
        json.map(|j| serde_json::from_str(&j).map_err(backend)).transpose()
    }

    async fn list_transforms(&self, umbrella_id: &str) -> Result<Vec<ChildTransform>, StoreError> {
        let mut conn = self.conn.clone();
        let children: Vec<String> = redis::cmd("SMEMBERS")
            .arg(transforms_index(umbrella_id)).query_async(&mut conn).await.map_err(backend)?;
        let mut out = Vec::with_capacity(children.len());
        for c in children {
            if let Some(t) = self.get_transform(umbrella_id, &c).await? {
                out.push(t);
            }
        }
        Ok(out)
    }

    async fn promote_bundle(&self, bundle: &UmbrellaBundle) -> Result<(), StoreError> {
        for t in &bundle.transforms {
            if t.umbrella_id != bundle.umbrella.umbrella_id {
                return Err(StoreError::BundleMismatch {
                    umbrella: bundle.umbrella.umbrella_id.clone(),
                    found: t.umbrella_id.clone(),
                });
            }
        }
        let mut conn = self.conn.clone();
        let id = &bundle.umbrella.umbrella_id;
        let umb_json = serde_json::to_string(&bundle.umbrella).map_err(backend)?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HSET").arg(umb_key(id)).arg("record").arg(&umb_json).arg("state").arg("active").ignore()
            .cmd("SADD").arg(state_index(UmbrellaState::Active)).arg(id).ignore()
            .cmd("SREM").arg(state_index(UmbrellaState::Provisional)).arg(id).ignore()
            .cmd("SREM").arg(state_index(UmbrellaState::Rejected)).arg(id).ignore();
        for t in &bundle.transforms {
            let tj = serde_json::to_string(t).map_err(backend)?;
            pipe.cmd("SET").arg(transform_key(id, &t.child_schema_id)).arg(tj).ignore()
                .cmd("SADD").arg(transforms_index(id)).arg(&t.child_schema_id).ignore();
        }
        pipe.query_async::<()>(&mut conn).await.map_err(backend)?;
        Ok(())
    }
}
