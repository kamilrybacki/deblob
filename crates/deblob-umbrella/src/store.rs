//! Persistence port for gold umbrellas + their child transforms, with the
//! governance lifecycle (`provisional → active | rejected`) mirroring Deblob's
//! candidate/schema stores. The store is CRUD + an **atomic bundle promotion**;
//! it does NOT verify — a controller runs [`crate::verify`] and only calls
//! [`UmbrellaStore::promote_bundle`] once a bundle passed the gate.

use crate::types::{ChildTransform, UmbrellaSchema};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Lifecycle state of a gold umbrella. `Active` is the promoted, published
/// contract (analogous to a published schema); `Provisional` is a proposed
/// candidate awaiting the trust gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UmbrellaState {
    Provisional,
    Active,
    Rejected,
}

/// A persisted umbrella with its lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredUmbrella {
    pub schema: UmbrellaSchema,
    pub state: UmbrellaState,
}

/// The atomic unit the trust gate promotes: the umbrella schema plus every
/// accepted child membership's transform. Persisted together or not at all
/// (design §trust gate: "promote one atomic bundle").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UmbrellaBundle {
    pub umbrella: UmbrellaSchema,
    pub transforms: Vec<ChildTransform>,
}

/// One consolidated child's identity, captured at umbrella-approval time —
/// IDs/revisions only, never a payload/binding/canonical byte. See
/// [`LineageAssertion`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageMember {
    pub child_schema_id: String,
    pub child_revision: String,
    /// Always `true` for a member built from an actually-persisted
    /// `ChildTransform` (the only way `approve` builds these today) —
    /// reserved for a future member derived by some other means that
    /// couldn't confirm a transform exists.
    pub transform_present: bool,
}

/// An IMMUTABLE, payload-free governance record of exactly what was
/// consolidated into an umbrella at the moment a human approved it (design
/// §trust gate lineage): the umbrella's own version, the identity of every
/// child that was folded in (id + revision only — never bindings, canonical
/// bytes, or raw values), and the human's stated reason. Written once, by
/// [`UmbrellaStore::put_lineage_assertion`], right after
/// [`UmbrellaStore::promote_bundle`] succeeds — an umbrella can only ever be
/// approved once (`approve` rejects a non-`Provisional` umbrella), so in
/// practice there is exactly one assertion per `umbrella_id`, not a history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageAssertion {
    pub umbrella_id: String,
    pub umbrella_version: u32,
    pub members: Vec<LineageMember>,
    pub approved_reason: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("umbrella {0} not found")]
    UmbrellaNotFound(String),
    #[error("bundle transform for umbrella {umbrella} references different umbrella {found}")]
    BundleMismatch { umbrella: String, found: String },
    #[error("backend error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait UmbrellaStore: Send + Sync {
    async fn put_umbrella(
        &self,
        schema: &UmbrellaSchema,
        state: UmbrellaState,
    ) -> Result<(), StoreError>;
    async fn get_umbrella(&self, id: &str) -> Result<Option<StoredUmbrella>, StoreError>;
    async fn set_state(&self, id: &str, state: UmbrellaState) -> Result<(), StoreError>;
    async fn list_umbrellas(&self, state: UmbrellaState)
        -> Result<Vec<StoredUmbrella>, StoreError>;

    async fn put_transform(&self, t: &ChildTransform) -> Result<(), StoreError>;
    async fn get_transform(
        &self,
        umbrella_id: &str,
        child_id: &str,
    ) -> Result<Option<ChildTransform>, StoreError>;
    async fn list_transforms(&self, umbrella_id: &str) -> Result<Vec<ChildTransform>, StoreError>;

    /// Atomically persist a promoted bundle: the umbrella becomes `Active` and all
    /// its transforms are stored together. Every transform must name this umbrella.
    async fn promote_bundle(&self, bundle: &UmbrellaBundle) -> Result<(), StoreError>;

    /// Persist the IMMUTABLE governance-lineage assertion for `a.umbrella_id`
    /// (spec: governed lineage on umbrella approval). Called once, right
    /// after a successful [`UmbrellaStore::promote_bundle`] — see
    /// [`LineageAssertion`]'s own docs.
    async fn put_lineage_assertion(&self, a: &LineageAssertion) -> Result<(), StoreError>;

    /// The lineage assertion written by [`UmbrellaStore::
    /// put_lineage_assertion`] for `umbrella_id`, or `None` if the umbrella
    /// was never approved (including if it doesn't exist at all) — never an
    /// error for that case, mirroring [`UmbrellaStore::get_umbrella`]'s own
    /// "absent is a valid answer" posture.
    async fn get_lineage_assertion(
        &self,
        umbrella_id: &str,
    ) -> Result<Option<LineageAssertion>, StoreError>;
}

/// In-memory store — the reference implementation the lifecycle tests pin, and a
/// usable local/embedded backend. Production uses the Redis impl in `deblob-redis`.
#[derive(Default)]
pub struct InMemoryUmbrellaStore {
    umbrellas: Mutex<BTreeMap<String, StoredUmbrella>>,
    transforms: Mutex<BTreeMap<(String, String), ChildTransform>>,
    lineage: Mutex<BTreeMap<String, LineageAssertion>>,
}

impl InMemoryUmbrellaStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl UmbrellaStore for InMemoryUmbrellaStore {
    async fn put_umbrella(
        &self,
        schema: &UmbrellaSchema,
        state: UmbrellaState,
    ) -> Result<(), StoreError> {
        self.umbrellas.lock().unwrap().insert(
            schema.umbrella_id.clone(),
            StoredUmbrella {
                schema: schema.clone(),
                state,
            },
        );
        Ok(())
    }
    async fn get_umbrella(&self, id: &str) -> Result<Option<StoredUmbrella>, StoreError> {
        Ok(self.umbrellas.lock().unwrap().get(id).cloned())
    }
    async fn set_state(&self, id: &str, state: UmbrellaState) -> Result<(), StoreError> {
        let mut g = self.umbrellas.lock().unwrap();
        let u = g
            .get_mut(id)
            .ok_or_else(|| StoreError::UmbrellaNotFound(id.to_string()))?;
        u.state = state;
        Ok(())
    }
    async fn list_umbrellas(
        &self,
        state: UmbrellaState,
    ) -> Result<Vec<StoredUmbrella>, StoreError> {
        Ok(self
            .umbrellas
            .lock()
            .unwrap()
            .values()
            .filter(|u| u.state == state)
            .cloned()
            .collect())
    }
    async fn put_transform(&self, t: &ChildTransform) -> Result<(), StoreError> {
        self.transforms.lock().unwrap().insert(
            (t.umbrella_id.clone(), t.child_schema_id.clone()),
            t.clone(),
        );
        Ok(())
    }
    async fn get_transform(
        &self,
        umbrella_id: &str,
        child_id: &str,
    ) -> Result<Option<ChildTransform>, StoreError> {
        Ok(self
            .transforms
            .lock()
            .unwrap()
            .get(&(umbrella_id.to_string(), child_id.to_string()))
            .cloned())
    }
    async fn list_transforms(&self, umbrella_id: &str) -> Result<Vec<ChildTransform>, StoreError> {
        Ok(self
            .transforms
            .lock()
            .unwrap()
            .iter()
            .filter(|((u, _), _)| u == umbrella_id)
            .map(|(_, t)| t.clone())
            .collect())
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
        // atomic under the two locks held together
        let mut us = self.umbrellas.lock().unwrap();
        let mut ts = self.transforms.lock().unwrap();
        us.insert(
            bundle.umbrella.umbrella_id.clone(),
            StoredUmbrella {
                schema: bundle.umbrella.clone(),
                state: UmbrellaState::Active,
            },
        );
        for t in &bundle.transforms {
            ts.insert(
                (t.umbrella_id.clone(), t.child_schema_id.clone()),
                t.clone(),
            );
        }
        Ok(())
    }
    async fn put_lineage_assertion(&self, a: &LineageAssertion) -> Result<(), StoreError> {
        self.lineage
            .lock()
            .unwrap()
            .insert(a.umbrella_id.clone(), a.clone());
        Ok(())
    }
    async fn get_lineage_assertion(
        &self,
        umbrella_id: &str,
    ) -> Result<Option<LineageAssertion>, StoreError> {
        Ok(self.lineage.lock().unwrap().get(umbrella_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Cardinality, FieldType, JsonPath, ScalarType, UmbrellaField};
    use deblob_core::semantic::CanonicalFieldId;

    fn umb(id: &str) -> UmbrellaSchema {
        UmbrellaSchema {
            umbrella_id: id.into(),
            label: "weather".into(),
            version: 1,
            fields: vec![UmbrellaField {
                canonical_field_id: CanonicalFieldId::new("event_time"),
                path: JsonPath::parse("$.event_time").unwrap(),
                name: "event_time".into(),
                ty: FieldType::Scalar(ScalarType::Integer),
                unit: None,
                cardinality: Cardinality::Required,
            }],
        }
    }
    fn xf(umb_id: &str, child: &str) -> ChildTransform {
        ChildTransform {
            child_schema_id: child.into(),
            umbrella_id: umb_id.into(),
            child_revision: format!("{child}@1"),
            umbrella_revision: format!("{umb_id}@1"),
            bindings: vec![],
            unmapped_source_paths: vec![],
        }
    }

    #[tokio::test]
    async fn provisional_lifecycle_and_listing() {
        let s = InMemoryUmbrellaStore::new();
        s.put_umbrella(&umb("umb_w"), UmbrellaState::Provisional)
            .await
            .unwrap();
        assert_eq!(
            s.get_umbrella("umb_w").await.unwrap().unwrap().state,
            UmbrellaState::Provisional
        );
        assert_eq!(
            s.list_umbrellas(UmbrellaState::Provisional)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            s.list_umbrellas(UmbrellaState::Active).await.unwrap().len(),
            0
        );

        s.set_state("umb_w", UmbrellaState::Rejected).await.unwrap();
        assert_eq!(
            s.list_umbrellas(UmbrellaState::Provisional)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            s.list_umbrellas(UmbrellaState::Rejected)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn transforms_scoped_by_umbrella() {
        let s = InMemoryUmbrellaStore::new();
        s.put_transform(&xf("umb_w", "sch_a")).await.unwrap();
        s.put_transform(&xf("umb_w", "sch_b")).await.unwrap();
        s.put_transform(&xf("umb_other", "sch_c")).await.unwrap();
        assert_eq!(s.list_transforms("umb_w").await.unwrap().len(), 2);
        assert!(s.get_transform("umb_w", "sch_a").await.unwrap().is_some());
        assert!(s
            .get_transform("umb_w", "sch_missing")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn promote_bundle_is_atomic_and_activates() {
        let s = InMemoryUmbrellaStore::new();
        s.put_umbrella(&umb("umb_w"), UmbrellaState::Provisional)
            .await
            .unwrap();
        let bundle = UmbrellaBundle {
            umbrella: umb("umb_w"),
            transforms: vec![xf("umb_w", "sch_a"), xf("umb_w", "sch_b")],
        };
        s.promote_bundle(&bundle).await.unwrap();
        assert_eq!(
            s.get_umbrella("umb_w").await.unwrap().unwrap().state,
            UmbrellaState::Active
        );
        assert_eq!(s.list_transforms("umb_w").await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn bundle_rejects_foreign_transform() {
        let s = InMemoryUmbrellaStore::new();
        let bundle = UmbrellaBundle {
            umbrella: umb("umb_w"),
            transforms: vec![xf("umb_DIFFERENT", "sch_a")],
        };
        assert!(matches!(
            s.promote_bundle(&bundle).await,
            Err(StoreError::BundleMismatch { .. })
        ));
        // nothing persisted
        assert!(s.get_umbrella("umb_w").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_get_lineage_assertion() {
        let s = InMemoryUmbrellaStore::new();
        assert!(s.get_lineage_assertion("umb_w").await.unwrap().is_none());

        let assertion = LineageAssertion {
            umbrella_id: "umb_w".into(),
            umbrella_version: 1,
            members: vec![
                LineageMember {
                    child_schema_id: "sch_a".into(),
                    child_revision: "sch_a@1".into(),
                    transform_present: true,
                },
                LineageMember {
                    child_schema_id: "sch_b".into(),
                    child_revision: "sch_b@1".into(),
                    transform_present: true,
                },
            ],
            approved_reason: "consolidating weather sources".into(),
        };
        s.put_lineage_assertion(&assertion).await.unwrap();

        let fetched = s.get_lineage_assertion("umb_w").await.unwrap().unwrap();
        assert_eq!(fetched, assertion);
        // A different umbrella's lineage is unaffected.
        assert!(s
            .get_lineage_assertion("umb_other")
            .await
            .unwrap()
            .is_none());
    }
}
