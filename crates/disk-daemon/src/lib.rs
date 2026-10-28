mod device;

/// Attach RPC and journal-record protocol messages.
pub use proto_flow::disk as proto;

/// Fixed granularity of device requests, chunks, hole punching, and bitmaps.
pub const BLOCK_SIZE: u32 = 4096;

/// Required journal content type.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";
