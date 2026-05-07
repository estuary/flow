// Protocol primitives for joining over connecting shards.
mod join;
use join::{JoinOutcome, JoinSlot, PendingJoin, validate as validate_join};

// gRPC service wiring.
mod service;

// Task-specific handling.
mod materialize;
// mod derive;  // TODO: implement.
// mod capture; // TODO: implement.

pub use service::Service;
