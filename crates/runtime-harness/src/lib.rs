//! `runtime-harness` runs Flow catalog tests locally on the V2 `runtime-next`
//! runtime, with no Gazette broker, etcd, Go consumer, or `flowctl-go` binary.
//!
//! It is linked by both `flowctl` (for `flowctl test`) and `control-plane-api`
//! (for publication tests). Logs and progress flow through a caller-provided
//! sink, never stdout.
//!
//! # Layers
//!
//! - [`clock`] / [`graph`] / [`action`] — the pure scheduler: a faithful port
//!   of the V1 Go dataflow graph (`go/testing/`). The graph tracks derivations
//!   only and drives INGEST / VERIFY steps, cascading stats and read-delay
//!   scheduling against a synthetic clock, through an abstract [`action::Driver`].
//! - [`store`] — `CollectionStore`, the in-memory append-log stand-in for
//!   collection journals that ingest / publish write and Verify / the segment
//!   feeder read.
//! - [`diff`] — the Verify comparator: superset match, scaled-epsilon float
//!   compare, and UUID masking.
//!
//! Later phases add the `drive` layer (extracted from `flowctl::preview`), the
//! seam implementations (Publisher / Shuffle / Logger), the resident-session
//! runner, and the full `run_tests` entry point.

pub mod action;
pub mod clock;
pub mod diff;
pub mod drive;
pub mod graph;
pub mod logger;
pub mod partitions;
pub mod publish;
pub mod run;
pub mod runner;
pub mod steps;
pub mod store;

pub use action::{Driver, run_test_case};
pub use clock::{Clock, Journal, contains_clock, max_clock, min_clock};
pub use diff::{Mismatch, compare_documents, mask_uuid, superset_match};
pub use graph::{Collection, Graph, PendingStat, TaskName, TestTime, Transform};
pub use run::{Options, TestOutcome, TestResults, run_tests};
pub use store::{CollectionStore, StoredDoc};

/// Re-export of the runtime-next remote-connector seam, so callers (e.g. the
/// control-plane agent's publication-test path) can build an [`Options`]
/// `remote_connectors` provider without depending on `runtime-next` directly.
pub use runtime_next::RemoteConnectors;
