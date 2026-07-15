//! Identity types. Spec §5.

use data_encoding::BASE32_NOPAD;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdError {
    #[error("wrong prefix, expected {expected}")]
    WrongPrefix { expected: &'static str },
    #[error("invalid base32 body")]
    InvalidBody,
}

macro_rules! digest_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn from_digest(digest: &[u8; 32]) -> Self {
                Self(format!(
                    "{}{}",
                    $prefix,
                    BASE32_NOPAD.encode(digest).to_ascii_lowercase()
                ))
            }
            pub fn parse(s: &str) -> Result<Self, IdError> {
                let body = s
                    .strip_prefix($prefix)
                    .ok_or(IdError::WrongPrefix { expected: $prefix })?;
                let up = body.to_ascii_uppercase();
                let bytes = BASE32_NOPAD
                    .decode(up.as_bytes())
                    .map_err(|_| IdError::InvalidBody)?;
                if bytes.len() != 32 {
                    return Err(IdError::InvalidBody);
                }
                Ok(Self(s.to_string()))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
digest_id!(SchemaId, "sch_");
digest_id!(CandidateId, "cand_");
digest_id!(SemanticId, "sem_");

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct FamilyId(String);
impl FamilyId {
    pub fn new_v7() -> Self {
        Self(format!("fam_{}", uuid::Uuid::now_v7()))
    }
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let body = s
            .strip_prefix("fam_")
            .ok_or(IdError::WrongPrefix { expected: "fam_" })?;
        uuid::Uuid::parse_str(body).map_err(|_| IdError::InvalidBody)?;
        Ok(Self(s.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct FamilyVersion(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRef {
    Known(SchemaId),
    Provisional(CandidateId),
    Unresolved,
    Malformed,
    Tombstone,
}
impl SchemaRef {
    pub fn header_value(&self) -> String {
        match self {
            SchemaRef::Known(id) => id.as_str().to_string(),
            SchemaRef::Provisional(id) => id.as_str().to_string(),
            SchemaRef::Unresolved => "unresolved".into(),
            SchemaRef::Malformed => "malformed".into(),
            SchemaRef::Tombstone => "tombstone".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_id_from_digest_roundtrips() {
        let d = [0xABu8; 32];
        let id = SchemaId::from_digest(&d);
        assert!(id.as_str().starts_with("sch_"));
        assert_eq!(SchemaId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn schema_id_encodes_full_256_bits_base32_lower_nopad() {
        let d = [0u8; 32];
        let id = SchemaId::from_digest(&d);
        // 32 bytes → 52 base32 chars unpadded
        assert_eq!(id.as_str().len(), "sch_".len() + 52);
        assert!(id.as_str()[4..]
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn parse_rejects_wrong_prefix_and_garbage() {
        assert!(SchemaId::parse("cand_abc").is_err());
        assert!(SchemaId::parse("sch_!!!").is_err());
        assert!(CandidateId::parse("sch_abc").is_err());
    }

    #[test]
    fn semantic_id_from_digest_roundtrips() {
        let d = [0xABu8; 32];
        let id = SemanticId::from_digest(&d);
        assert!(id.as_str().starts_with("sem_"));
        assert_eq!(SemanticId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn parse_rejects_semantic_prefix_domain_separation() {
        // sem_ must never parse as sch_/cand_, and vice versa (domain
        // separation between the three identity dimensions, spec P2-D).
        assert!(SemanticId::parse("sch_abc").is_err());
        assert!(SemanticId::parse("cand_abc").is_err());
        assert!(SemanticId::parse("sem_!!!").is_err());
        assert!(SchemaId::parse("sem_abc").is_err());
        assert!(CandidateId::parse("sem_abc").is_err());
    }

    #[test]
    fn schema_ref_header_values() {
        let d = [1u8; 32];
        assert!(SchemaRef::Known(SchemaId::from_digest(&d))
            .header_value()
            .starts_with("sch_"));
        assert!(SchemaRef::Provisional(CandidateId::from_digest(&d))
            .header_value()
            .starts_with("cand_"));
        assert_eq!(SchemaRef::Unresolved.header_value(), "unresolved");
        assert_eq!(SchemaRef::Malformed.header_value(), "malformed");
        assert_eq!(SchemaRef::Tombstone.header_value(), "tombstone");
    }
}
