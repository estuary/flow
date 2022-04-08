/// DO NOT add more things to this crate!
/// Update it's uses to instead use the proto-gazette or proto-flow crates directly.
/// We're trying to remove this crate altogether.
pub use proto_flow::capture;
pub use proto_flow::flow;
pub use proto_flow::materialize;
pub use proto_gazette::broker as protocol;
pub use proto_gazette::consumer;
