pub mod cgo;
pub mod consumer;
pub mod flow;
pub mod materialize;
pub mod protocol;
pub mod recoverylog;

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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serde_round_trip_of_constraint() {
        let expected = materialize::Constraint {
            r#type: materialize::constraint::Type::FieldRequired as i32,
            reason: "field required".to_string(),
        };

        let serialized = serde_json::to_string_pretty(&expected).unwrap();
        insta::assert_snapshot!(serialized);
        let actual = serde_json::from_str::<materialize::Constraint>(&serialized).unwrap();
        assert_eq!(expected, actual);
    }
}
