pub mod capture;
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

pub mod collection {
    use crate::flow::{CollectionSpec, Projection};
    pub trait CollectionExt {
        fn get_projection(&self, field: impl AsRef<str>) -> Option<&Projection>;
    }

    impl CollectionExt for CollectionSpec {
        fn get_projection(&self, field: impl AsRef<str>) -> Option<&Projection> {
            let field = field.as_ref();
            self.projections.iter().find(|p| p.field == field)
        }
    }
}

pub mod arena {
    use crate::flow::Slice;
    use std::borrow::Borrow;
    use std::io;

    /// Extension trait with helper functions for working with arenas.
    pub trait ArenaExt {
        fn is_valid(&self, slice: Slice) -> bool;

        fn bytes(&self, slice: Slice) -> &[u8];

        fn add_bytes<B: AsRef<[u8]>>(&mut self, bytes: &B) -> Slice;

        fn writer(&mut self) -> ArenaWriter;
    }

    impl ArenaExt for Vec<u8> {
        fn is_valid(&self, slice: Slice) -> bool {
            let slice = slice.borrow();
            slice.begin <= slice.end && slice.end as usize <= self.len()
        }
        fn bytes(&self, slice: Slice) -> &[u8] {
            let slice = slice.borrow();
            &self[slice.begin as usize..slice.end as usize]
        }

        fn add_bytes<B: AsRef<[u8]>>(&mut self, bytes: &B) -> Slice {
            let src = bytes.as_ref();
            let start = self.len();
            self.extend_from_slice(src);
            let end = self.len();
            Slice {
                begin: start as u32,
                end: end as u32,
            }
        }

        fn writer<'a>(&'a mut self) -> ArenaWriter<'a> {
            let start = self.len();
            ArenaWriter { arena: self, start }
        }
    }

    pub struct ArenaWriter<'a> {
        arena: &'a mut Vec<u8>,
        start: usize,
    }
    impl<'a> ArenaWriter<'a> {
        pub fn slice(&self) -> Slice {
            Slice {
                begin: self.start as u32,
                end: self.arena.len() as u32,
            }
        }
        pub fn finish(self) -> Slice {
            self.slice()
        }
    }

    impl<'a> io::Write for ArenaWriter<'a> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.arena.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
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
