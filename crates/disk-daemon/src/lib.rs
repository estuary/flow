mod device;

/// Attach RPC and journal-record protocol messages.
pub use proto_flow::disk as proto;

/// Fixed granularity of chunks, hole punching, bitmaps, and ext4.
pub const BLOCK_SIZE: u32 = 4096;

/// Required journal content type.
pub const CONTENT_TYPE_DISK: &str = "application/x-journal-backed-disk";
