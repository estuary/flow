mod join; // Protocol primitives for joining over connecting shards.
mod service; // gRPC service wiring.

pub use join::{JoinOutcome, JoinSlot, PendingJoin, validate as validate_join};
pub use service::Service;
