//! `deblob-redis`: the Redis-backed permanent schema vault (spec §6).
//!
//! Publication is a single atomic Lua transition — schema record, family
//! version, structural index entry, alias, and audit event all commit
//! together or not at all. See [`lua::PUBLISH_SCRIPT`] for the invariants
//! it enforces (write-once schema bytes, write-once alias, atomic family
//! version allocation).

pub mod index;
pub mod lua;
pub mod registry;

pub use index::{bucket_key, bucket_member};
pub use registry::{RedisOpts, RedisRegistry};
