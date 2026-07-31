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
//!
//! Later commits add the seam implementations (publisher / logger), the
//! resident-session runner, and the `run_tests` entry point.

pub mod action;
pub mod clock;
pub mod diff;
pub mod graph;
pub mod store;

pub use action::{Driver, run_test_case};
pub use clock::{Clock, Journal, contains_clock, max_clock, min_clock};
pub use diff::{Mismatch, compare_documents, mask_uuid, superset_match};
pub use graph::{Collection, Graph, PendingStat, TaskName, TestTime, Transform};
pub use store::{CollectionStore, StoredDoc};
