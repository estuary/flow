//! `catalog-tests` runs Flow catalog tests on the V2 `runtime-next` runtime,
//! with no Gazette broker, etcd, Go consumer, or `flowctl-go` binary.
//!
//! It is linked by both `flowctl` (for `flowctl test`) and `control-plane-api`
//! (for publication tests). Logs and progress flow through a caller-provided
//! sink, never stdout.
//!
//! # Layers
//!
//! - [`clock`] / [`graph`] / [`action`] — the pure scheduler, a faithful port of
//!   the V1 Go dataflow graph (`go/testing/`). The graph tracks derivations only
//!   and drives INGEST / VERIFY steps, cascading stats and read-delay scheduling
//!   against a synthetic clock, through an abstract [`action::Driver`].
//! - [`store`] — [`store::CollectionStore`], the in-memory append-log stand-in
//!   for collection journals which ingest and the publisher write, and which
//!   Verify and the segment feeder read.
//! - [`diff`] — the Verify comparator: superset match, scaled-epsilon float
//!   compare, and UUID masking.
//! - [`publish`] / [`logger`] / [`partitions`] — the `runtime-next` host seams.
//!   The publisher appends derived documents to the store and is also where the
//!   runner observes each transaction commit; the logger only sinks logs.
//! - [`runner`] — [`runner::DerivationRunner`], one derivation held resident as a
//!   runtime-next session for the whole run, driving one transaction per stat.
//!
//! A later commit adds test-case execution and the `run_tests` entry point.

pub mod action;
pub mod clock;
pub mod diff;
pub mod graph;
pub mod logger;
pub mod partitions;
pub mod publish;
pub mod runner;
pub mod store;

pub use action::{Driver, run_test_case};
pub use clock::{Clock, Journal, contains_clock, max_clock, min_clock};
pub use diff::{Mismatch, compare_documents, mask_uuid, superset_match};
pub use graph::{Collection, Graph, PendingStat, TaskName, TestTime, Transform};
pub use logger::LogHandler;
pub use runner::DerivationRunner;
pub use store::{CollectionStore, StoredDoc};
