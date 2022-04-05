pub mod consumer;
mod protocol;
pub mod recoverylog;

// The `protocol` package is publicly exported as `broker`.
pub mod broker {
    pub use crate::protocol::*;
}

mod serde_protocol {
    use crate::protocol::*;
    include!(concat!(env!("OUT_DIR"), "/protocol.serde.rs"));
}
mod serde_consumer {
    use crate::consumer::*;
    include!(concat!(env!("OUT_DIR"), "/consumer.serde.rs"));
}
mod serde_recoverylog {
    use crate::recoverylog::*;
    include!(concat!(env!("OUT_DIR"), "/recoverylog.serde.rs"));
}
