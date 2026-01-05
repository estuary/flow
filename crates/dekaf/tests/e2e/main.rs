//! Dekaf E2E test suite.
//!
//! All e2e tests live as submodules of this crate so that rustc can see
//! all usages of shared utilities and properly warn about dead code.

mod harness;
pub mod kafka;
pub mod raw_kafka;

mod basic;
mod collection_reset;
mod empty_fetch;
mod epoch_reporting;
mod list_offsets;
mod not_ready;
mod task_name_auth;

pub use harness::{ConnectionInfo, DekafTestEnv, Fragment, FragmentInfo, init_tracing};
