pub mod consumer;
mod protocol;
pub mod recoverylog;

// The `protocol` package is publicly exported as `broker`.
pub mod broker {
    pub use crate::protocol::*;
}

mod serde_protocol {
    use crate::protocol::*;
    include!("protocol.serde.rs");
}
mod serde_consumer {
    use crate::consumer::*;
    include!("consumer.serde.rs");
}
mod serde_recoverylog {
    use crate::recoverylog::*;
    include!("recoverylog.serde.rs");
}

/// Message UUID flags defined by Gazette, and used by Flow.
/// C.f. Gazette's `message` package, where these are originally defined.
pub mod message_flags {
    /// MASK is the low 10 bits of UuidParts::producer_and_flags.
    /// It's the bit of a Gazette message UUID which are used to carry flag values.
    pub const MASK: u64 = 0x3ff;
    /// OUTSIDE_TXN marks the message is immediately commit.
    pub const OUTSIDE_TXN: u64 = 0x0;
    /// CONTINUE_TXN marks the message as transactional, such that it must
    /// be committed by a future ACK_TXN before it may be processed.
    pub const CONTINUE_TXN: u64 = 0x1;
    /// ACK_TXN marks the message as an acknowledgement of a committed transaction.
    /// On reading a ACK, the reader may process previous CONTINUE_TXN messages
    /// which are now considered to have committed.
    pub const ACK_TXN: u64 = 0x2;
}
