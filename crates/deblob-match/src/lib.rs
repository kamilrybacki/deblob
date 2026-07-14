//! `deblob-match` — the hot-path matcher, Prometheus metrics, and the
//! discovery-topic wire type, split out of the `deblob` bin/lib crate
//! (Task 18) so `deblob-kafka` can depend on this shared logic WITHOUT
//! depending on the `deblob` package itself.
//!
//! Why this crate exists: `deblob-kafka::Relay::run` needs
//! [`matcher::HotMatcher`], [`metrics::Metrics`], and [`discovery::DiscoveryMsg`]
//! to classify records and forward provisional shapes to the discovery
//! topic. `deblob` (the bin) wires the Kafka relay together in `main.rs`, so
//! it must depend on `deblob-kafka`. If `deblob-kafka` depended on the
//! `deblob` package directly, Cargo would reject the workspace outright:
//! package-level dependency-cycle detection does not distinguish a
//! package's `lib` target from its `bin` target — `deblob-kafka -> deblob
//! -> deblob-kafka` is a cycle regardless of which target within `deblob`
//! actually needs `deblob-kafka` (verified empirically: `cargo check -p
//! deblob` fails with "cyclic package dependency" the moment `deblob-kafka`
//! is added to `deblob`'s `[dependencies]`). Extracting the shared surface
//! into this small, dependency-light crate breaks the cycle: `deblob-kafka`
//! depends only on `deblob-match` (and deblob-core/deblob-fingerprint),
//! `deblob` depends on `deblob-match` too (and re-exports its modules for
//! source-compatibility with pre-Task-18 code), and `deblob`'s bin target
//! additionally depends on `deblob-kafka` — no path leads back to `deblob`.

pub mod discovery;
pub mod matcher;
pub mod metrics;
