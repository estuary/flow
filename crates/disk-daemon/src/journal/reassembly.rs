//! Reassembling a journal's records from the content a read streams.
//!
//! A read streams a journal's content in whatever pieces the broker serves, and a
//! record may straddle two of them. [`Reassembly`] holds what it has not yet decoded,
//! tracks the journal offset at which each record begins, and finds content the
//! broker skipped: the seek a read begins with, a hole in the offset space, or
//! content the fragment store no longer holds. It does no I/O. `replay::read` feeds
//! it each response, and applies each record it hands back.

use super::replay::Gap;
use crate::proto;
use anyhow::Context;
use proto_gazette::fixed_framing;

/// The records of one read, as its content arrives.
pub(super) struct Reassembly {
    /// Whether the read may begin past the offset it asked for. Only a pass's first
    /// read may: see [`Reassembly::on_response`].
    may_skip_start: bool,
    /// Journal offset at which `buf` begins. A record not yet decoded starts there.
    offset: i64,
    buf: bytes::BytesMut,
    /// Whether the broker has served anything yet. Its first response lands wherever
    /// the read's seek did.
    begun: bool,
}

/// What one response's content did to a read.
#[derive(Debug, PartialEq)]
pub(super) enum Received {
    /// It continued where the content before it ended.
    Contiguous,
    /// The broker skipped from `from` to `to`, and a partial record held across the
    /// skip was dropped.
    Skipped { from: i64, to: i64 },
}

/// One whole record of a read.
pub(super) struct Reassembled {
    /// Journal offset at which the record begins.
    pub at: i64,
    pub record: proto::DiskRecord,
    /// The journal's own bytes of the record, framing included.
    pub framed: bytes::Bytes,
}

impl Reassembly {
    /// A read which asked for content from `offset`, and which may begin past it only
    /// where `may_skip_start`.
    pub fn new(offset: i64, may_skip_start: bool) -> Self {
        Self {
            may_skip_start,
            offset,
            buf: bytes::BytesMut::new(),
            begun: false,
        }
    }

    /// Take a response's `content`, which begins at journal offset `offset`.
    ///
    /// Only a pass's first read may skip, and only before its first content. It seeks
    /// from the floor, and the floor says every allocated block has a copy at or after
    /// it, so content the store no longer holds below the real floor is content the
    /// pass does not need. Any other skip leaves records out of what the pass already
    /// holds, and is a [`Gap`].
    pub fn on_response(&mut self, offset: i64, content: &[u8]) -> Result<Received, Gap> {
        let mut received = Received::Contiguous;

        if offset != self.offset + self.buf.len() as i64 {
            if self.begun || !self.may_skip_start {
                return Err(Gap { at: self.offset });
            }
            received = Received::Skipped {
                from: self.offset,
                to: offset,
            };
            self.buf.clear();
            self.offset = offset;
        }
        self.begun = true;
        self.buf.extend_from_slice(content);

        Ok(received)
    }

    /// The next record held whole, or `None` until more content arrives.
    ///
    /// Content which frames no record is refused. This daemon frames every record it
    /// writes, so such content is corruption rather than a stream a reader joined
    /// between frames and can resynchronize with.
    pub fn next_record(&mut self) -> anyhow::Result<Option<Reassembled>> {
        let at = self.offset;

        match fixed_framing::unpack::<proto::DiskRecord>(&mut self.buf)
            .with_context(|| format!("decoding a record at offset {at}"))?
        {
            fixed_framing::Frame::Record { message, framed } => {
                self.offset += framed.len() as i64;

                Ok(Some(Reassembled {
                    at,
                    record: message,
                    framed,
                }))
            }
            fixed_framing::Frame::Desync { skipped } => anyhow::bail!(
                "{} unframed bytes at offset {at}, and this daemon frames every record it writes",
                skipped.len(),
            ),
            fixed_framing::Frame::Incomplete => Ok(None),
        }
    }

    /// The offset and length of a record the read ended within, if it ended within
    /// one.
    pub fn remainder(&self) -> Option<(i64, usize)> {
        (!self.buf.is_empty()).then_some((self.offset, self.buf.len()))
    }
}

#[cfg(test)]
mod test {
    use super::{Reassembled, Reassembly};
    use crate::proto;
    use proto_gazette::fixed_framing;
    use std::fmt::Write as _;

    /// A framed record, told apart from others by `tag`, which frames to `len` bytes
    /// of UUID.
    fn record(tag: u8, len: usize) -> bytes::Bytes {
        let record = proto::DiskRecord {
            uuid: vec![tag; len].into(),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        fixed_framing::encode(&record, &mut buf);
        buf.freeze()
    }

    /// Feed each `(offset, content)` in turn to a read which asked to begin at `start`,
    /// and render what each response did and the records it completed, as
    /// `tag@offset`, then what the read ended within.
    fn trace(start: i64, may_skip_start: bool, responses: &[(i64, &[u8])]) -> String {
        let mut reassembly = Reassembly::new(start, may_skip_start);
        let mut out = String::new();

        for &(offset, content) in responses {
            let received = match reassembly.on_response(offset, content) {
                Ok(received) => format!("{received:?}"),
                Err(gap) => {
                    writeln!(out, "at {offset:<5}{gap}").unwrap();
                    continue;
                }
            };
            let mut records = Vec::new();
            loop {
                match reassembly.next_record() {
                    Ok(Some(Reassembled { at, record, .. })) => {
                        records.push(format!("{}@{at}", record.uuid[0]))
                    }
                    Ok(None) => break,
                    Err(err) => {
                        records.push(format!("refused: {err:#}"));
                        break;
                    }
                }
            }
            let line = format!("at {offset:<5}{received:<36}{}", records.join(" "));
            writeln!(out, "{}", line.trim_end()).unwrap();
        }
        writeln!(out, "ends within {:?}", reassembly.remainder()).unwrap();
        out
    }

    /// Records straddle the responses which carry them, header and body alike, and
    /// each is handed back whole at the offset it begins.
    #[test]
    fn test_records_are_reassembled_across_responses() {
        let journal = [record(1, 20), record(2, 5), record(3, 30)].concat();
        let (a, b, c) = (3, 40, 50);

        let trace = trace(
            0,
            false,
            &[
                (0, &journal[..a]),
                (a as i64, &journal[a..b]),
                (b as i64, &journal[b..c]),
                (c as i64, &journal[c..]),
            ],
        );
        insta::assert_snapshot!(trace, @"
        at 0    Contiguous
        at 3    Contiguous                          1@0
        at 40   Contiguous                          2@30
        at 50   Contiguous                          3@45
        ends within None
        ");
    }

    /// A pass's first read may begin past where it asked, because what the store no
    /// longer holds is below the real floor. Content it skips after it has begun is a
    /// gap, and so is any skip at the start of a read which continues a pass.
    #[test]
    fn test_only_a_pass_s_first_read_skips_at_its_start() {
        let (one, two) = (record(1, 20), record(2, 20));
        let responses: [(i64, &[u8]); 3] = [
            (100, &one),
            (100 + one.len() as i64, &two[..10]),
            (500, &one),
        ];

        let trace = [
            trace(40, true, &responses),
            trace(40, false, &responses[..1]),
        ]
        .join("\n");
        insta::assert_snapshot!(trace, @"
        at 100  Skipped { from: 40, to: 100 }       1@100
        at 130  Contiguous
        at 500  content this read needed was deleted from the store, at offset 130
        ends within Some((130, 10))

        at 100  content this read needed was deleted from the store, at offset 40
        ends within None
        ");
    }

    /// This daemon frames every record it writes, so content which frames none is
    /// refused rather than skipped over.
    #[test]
    fn test_unframed_content_is_refused() {
        let trace = trace(0, false, &[(0, b"these bytes frame no record at all")]);
        insta::assert_snapshot!(trace, @"
        at 0    Contiguous                          refused: 31 unframed bytes at offset 0, and this daemon frames every record it writes
        ends within Some((0, 3))
        ");
    }
}
