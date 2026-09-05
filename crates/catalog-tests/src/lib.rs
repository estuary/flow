//! `catalog-tests` runs Flow catalog tests on the `runtime-next` runtime,
//! with no Gazette broker, no etcd, and no separate consumer process.
//!
//! It is linked by both `flowctl` (for `flowctl raw test`) and `control-plane-api`
//! (for publication tests). Logs and progress flow through a caller-provided
//! sink, never stdout.
//!
//! [`run::run_tests`] is the entry point: start a resident session per enabled
//! derivation, then run every test case with a Reset between them. See
//! `README.md` for the layering and its non-obvious details.

pub mod clock;
pub mod diff;
pub mod graph;
pub mod partitions;
pub mod publish;
pub mod run;
pub mod scheduler;
pub mod session;
pub mod steps;
pub mod store;

// The crate's external contract: everything else is `pub` only for white-box
// access by this crate's own `tests/`.
pub use run::{LogHandler, Options, TestOutcome, TestResults, TestStatus, Timeouts, run_tests};
