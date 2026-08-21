//! A consistency test suite for materialization connectors.
//!
//! It runs a connector as a real task on a real Flow runtime, breaks it at
//! precise points, and checks that the destination still holds exactly the right
//! data. See `README.md` for an overview, `AGENT_README.md` for the full
//! operating guide, and `docs/materialize/consistency-testing.md` for why it
//! is shaped the way it is.

pub mod harness;
pub mod invariants;
pub mod protocol;
pub mod reference;
pub mod scenarios;
pub mod shim;
