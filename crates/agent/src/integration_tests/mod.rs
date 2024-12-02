//! # Control plane integration tests
//!
//! These tests cover end-to-end scenarios involving the control plane. The data plane and
//! connectors are not exercised as part of these.
mod auto_discovers;
mod dependencies_and_activations;
mod evolutions;
pub mod harness;
mod locking_retries;
mod null_bytes;
mod periodic_publications;
mod quotas;
mod schema_evolution;
mod source_captures;
mod unknown_connectors;
mod user_discovers;
mod user_publications;
