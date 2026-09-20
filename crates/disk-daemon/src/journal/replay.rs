//! Rebuilding a disk from the acknowledged deltas of its journal.
//!
//! These rules are the durability guarantee, so they are stated here in full:
//!
//! - The range is fixed at `[floor, head)` before the pass begins. `head` is
//!   broker-confirmed, which makes the read fresh. A broker which served that
//!   append holds an index covering every fragment below it.
//! - [`uuid::sequence`] sequences records per producer. It drops the duplicates
//!   at-least-once appends produce, and it releases only acknowledged deltas.
//!   Chunks apply in physical journal order, and the live append barrier makes
//!   that commit order.
//! - Fence records change no disk content. They are validated and skipped.
//! - Another producer's records displace the delta held, and a displaced delta is
//!   never taken up again. Records of it which follow are a fragment whose start
//!   was dropped, so none of them is held, and its acknowledgement is refused.
//! - The range may begin within a delta. Records below the floor are unnecessary,
//!   because a completed horizon puts a copy of every allocated block at or after
//!   it.
//! - Horizons are rebuilt by the same rules the writer applies. The record which
//!   opens one snapshots the blocks allocated before its own chunks apply. Every
//!   chunk from there on discharges the blocks it covers. The acknowledgement of
//!   the delta which discharged the last block puts the floor at the opening
//!   record. A later horizon replaces an earlier one. A horizon still open at the
//!   end of the range is one the next tenure resumes.
//!
//! A delta is not applied as it is read. Its records are held in a [`Buffer`] as
//! they are sequenced, and they apply only when the acknowledgement of that delta
//! arrives. A delta's chunks, the horizon it opens, and the blocks that horizon
//! discharges therefore all land together, or never land at all.
//!
//! A pass must hold them, because it may have no end of range at which to discover
//! an unacknowledged delta: a standby tails the journal, where the delta at the head
//! is open because a primary is still writing it. An image which applied that delta
//! would hold writes the client never committed. A delta which is never acknowledged
//! is instead dropped: another producer's records displace the one being held, and
//! one still held when the pass ends goes with the pass. Nothing is ever taken back,
//! because nothing of it was applied. A bounded recovery runs this same pass, so
//! there is one read path and one set of rules for both.

use super::buffer::Buffer;
use crate::horizon::Horizon;
use crate::image::Image;
use crate::proto;
use anyhow::Context;
use proto_gazette::{broker, fixed_framing, uuid};

/// Content a read still needed was deleted from the fragment store.
///
/// A bounded recovery may skip a gap: it seeks from the floor, and the broker starts
/// it at the first offset the store still holds. A tail may not. It holds a partial
/// image, and a block which was deallocated below the gap has no record above it to
/// punch, so a skip would leave that block allocated forever. The image must be
/// discarded and rebuilt from the current floor.
#[derive(Debug)]
pub struct Gap {
    pub at: i64,
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "content this read needed was deleted from the store, at offset {}",
            self.at,
        )
    }
}

impl std::error::Error for Gap {}

/// How far a read goes.
#[derive(Clone, Copy, Debug)]
pub enum Extent {
    /// Read to this head and stop, as a recovery does. The head is broker-confirmed,
    /// which is what makes the read fresh.
    Bounded(i64),
    /// Read until the caller stops, as a standby does. New records are followed as
    /// they arrive, so this read does not end on its own.
    Tail,
}

/// Read `journal` from `floor` into `image`, and report the chunks applied.
///
/// The floor is a record boundary, so the read asks for it exactly. A broker whose
/// store no longer holds that offset serves the next one it has, which only adds
/// records this replay does not need.
///
/// A [`Extent::Tail`] read blocks at the head and returns only on an error or a
/// [`Gap`]. Its caller stops it by cancelling it.
pub(super) async fn read(
    client: &gazette::journal::Client,
    journal: &str,
    floor: i64,
    extent: Extent,
    image: &mut Image,
    pass: &mut Pass,
) -> anyhow::Result<usize> {
    let (end_offset, block) = match extent {
        Extent::Bounded(head) => (head, false),
        Extent::Tail => (0, true),
    };

    let stream = client.clone().read(broker::ReadRequest {
        journal: journal.to_string(),
        offset: floor,
        end_offset,
        block,
        ..Default::default()
    });
    futures::pin_mut!(stream);

    // Journal offset at which `buf` begins. A record this reader has not finished
    // decoding starts there.
    let mut buf = bytes::BytesMut::new();
    let mut offset = 0;
    let mut applied = 0;
    // Offset the broker served first, which is where the seek landed.
    let mut begin = None;

    while let Some(response) = futures::StreamExt::next(&mut stream).await {
        let response = match response {
            Ok(response) => response,
            Err(gazette::RetryError { attempt, inner }) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "journal read failed (will retry)");
                continue;
            }
            Err(gazette::RetryError { inner, .. }) => {
                return Err(anyhow::Error::new(inner).context(format!("reading {journal}")));
            }
        };

        // The broker skipped some content, either for the seek this read began
        // with or for a hole in the offset space. No partial record can be
        // finished across that gap.
        if response.offset != offset + buf.len() as i64 {
            // A bounded recovery may skip. It seeks from the floor, and the floor
            // says every allocated block has a copy at or after it, so content the
            // store no longer holds is content it does not need. A tail may not
            // skip: see `Gap`.
            if begin.is_some() && matches!(extent, Extent::Tail) {
                return Err(
                    anyhow::Error::new(Gap { at: offset }).context(format!("tailing {journal}"))
                );
            }
            tracing::debug!(journal, from = offset, to = response.offset, "offset jump");

            buf.clear();
            offset = response.offset;
        }
        _ = begin.get_or_insert(response.offset);
        buf.extend_from_slice(&response.content);

        loop {
            match fixed_framing::decode::<proto::DiskRecord>(&buf)
                .with_context(|| format!("decoding a record of {journal} at offset {offset}"))?
            {
                fixed_framing::Frame::Record { message, consumed } => {
                    applied += pass
                        .record(&message, &buf[..consumed], offset, image)
                        .with_context(|| format!("replaying {journal} at offset {offset}"))?;

                    offset += consumed as i64;
                    _ = buf.split_to(consumed);
                }
                fixed_framing::Frame::Desync { skipped } => anyhow::bail!(
                    "{journal} holds {skipped} unframed bytes at offset {offset}, and this \
                     daemon frames every record it writes",
                ),
                fixed_framing::Frame::Incomplete => break,
            }
        }
    }

    if !buf.is_empty() {
        tracing::warn!(
            journal,
            offset,
            bytes = buf.len(),
            "the replayed range ends within a record",
        );
    }
    Ok(applied)
}

/// A recovery horizon this pass has opened.
///
/// The offset and the bitmap are held together because they are one fact: `at` is
/// the floor which completing the horizon establishes, and `blocks` is what the
/// horizon still owes before it may. A live disk keeps the same pair apart, because
/// the offset is the writer's and the bitmap is the owner's.
pub(super) struct Opened {
    /// Offset at which the record which opened this horizon begins.
    pub(super) at: i64,
    /// Committed blocks which still owe this horizon a copy.
    pub(super) blocks: Horizon,
}

/// One forward pass over the range.
pub(super) struct Pass {
    /// Sequencing state of each producer of the range.
    producers: std::collections::HashMap<uuid::Producer, Sequence>,
    /// Producer of the delta which began and is not yet acknowledged, and whose
    /// records `buffer` holds. Only its acknowledgement can be honored: another
    /// producer's records mean a delta whose commit order nothing states. It is
    /// `None` once a delta commits or is displaced.
    open: Option<uuid::Producer>,
    /// Offset through which committed records are applied.
    applied: i64,
    /// A horizon which has opened and is not yet discharged.
    horizon: Option<Opened>,
    /// Offset of the last horizon a delta of the range completed, which is the
    /// floor.
    floor: Option<i64>,
    /// Offset at which a held delta's first record opened a horizon. It becomes
    /// `horizon` when that delta commits, and it is dropped with the delta: a
    /// horizon belongs to the delta which opened it.
    horizon_at: Option<i64>,
    /// Holds the delta which is not yet acknowledged, rather than applying it.
    buffer: Buffer,
    /// Chunks this pass has applied. It counts across reads, and it survives a read
    /// which is cancelled, so a playback which is promoted mid-backfill still knows
    /// whether the journal held committed state.
    applied_chunks: usize,
}

#[derive(Default, Clone, Copy)]
struct Sequence {
    /// Clocks which [`uuid::sequence`] transitions.
    last_commit: uuid::Clock,
    max_continue: uuid::Clock,
}

impl Pass {
    /// A pass which holds each delta until its acknowledgement, in `buffer`.
    pub(super) fn new(buffer: Buffer) -> Self {
        Self {
            producers: Default::default(),
            open: None,
            applied: 0,
            horizon: None,
            floor: None,
            horizon_at: None,
            buffer,
            applied_chunks: 0,
        }
    }

    /// Chunks applied, which is zero for a journal with no committed state.
    pub(super) fn applied_chunks(&self) -> usize {
        self.applied_chunks
    }

    /// Recovery floor this pass derived, if a horizon completed within it.
    pub(super) fn derived_floor(&self) -> Option<i64> {
        self.floor
    }

    /// Offset through which committed state is applied. A held delta is not part of
    /// it, so this never runs ahead of what the client committed.
    pub(super) fn applied_offset(&self) -> i64 {
        self.applied
    }

    /// Take the held delta, the floor, and any horizon still open, to continue
    /// this pass elsewhere.
    pub(super) fn into_parts(self) -> (Buffer, Option<i64>, Option<Opened>) {
        (self.buffer, self.floor, self.horizon)
    }

    /// Whether an acknowledgement of `producer` at `clock` is one this pass can
    /// honor: it commits the delta the pass holds, or it commits nothing. Any other
    /// acknowledgement fails [`Pass::record`], and one which reaches the journal fails
    /// every replay from then on, so a caller asks here before appending one.
    pub(super) fn can_acknowledge(&self, producer: uuid::Producer, clock: uuid::Clock) -> bool {
        let mut state = self.producers.get(&producer).copied().unwrap_or_default();

        match uuid::sequence(
            uuid::Flags::ACK_TXN,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        ) {
            Ok(uuid::SequenceOutcome::AckCommit) => self.open == Some(producer),
            Ok(uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate) => true,
            _ => false,
        }
    }

    /// Sequence `record`, which begins at `offset` and was framed as `framed`, and
    /// report the chunks it applied.
    ///
    /// Sequencing and application are separate steps of this function. Sequencing
    /// always runs here, because it must see every record in order: it decides the
    /// producer, the duplicates, and whether a delta is acknowledged. Application is
    /// deferred: a delta's chunks, the horizon it opens, and the horizon blocks it
    /// discharges are all held together, and all land when the acknowledgement
    /// arrives.
    fn record(
        &mut self,
        record: &proto::DiskRecord,
        framed: &[u8],
        offset: i64,
        image: &mut Image,
    ) -> anyhow::Result<usize> {
        let uuid =
            uuid::Uuid::from_slice(&record.uuid).context("record carries no message UUID")?;
        let (producer, clock, flags) = uuid::parse(uuid)?;

        let state = self.producers.entry(producer).or_default();

        let outcome = uuid::sequence(
            flags,
            clock,
            &mut state.last_commit,
            &mut state.max_continue,
        )?;

        anyhow::ensure!(
            !record.opens_horizon
                || matches!(
                    outcome,
                    uuid::SequenceOutcome::ContinueBeginSpan
                        | uuid::SequenceOutcome::ContinueDuplicate
                ),
            "record of {producer:?} at {clock:?} opens a horizon but does not begin a delta",
        );

        match outcome {
            // A fence carries the epoch it installs and changes no disk content.
            uuid::SequenceOutcome::OutsideCommit | uuid::SequenceOutcome::OutsideDuplicate => {
                anyhow::ensure!(
                    record.installs_epoch.len() == std::mem::size_of::<uuid::Producer>(),
                    "fence record of {producer:?} installs {} bytes of epoch",
                    record.installs_epoch.len(),
                );
                () = ensure_no_chunks(record, "a fence")?;

                // The delta this epoch displaced is not doomed by the fence itself. A
                // promotion appends the acknowledgement its client recovered
                // immediately after its own fence, and that still commits the delta.
                self.applied = offset + framed.len() as i64;
            }
            uuid::SequenceOutcome::ContinueBeginSpan => {
                self.open = Some(producer);

                // The horizon this record opens is held with it, and so is dropped
                // with the delta whose records this one displaces. A horizon belongs
                // to its delta, so a delta which is never acknowledged never opened
                // one.
                () = self.buffer.push(producer, framed)?;
                self.horizon_at = record.opens_horizon.then_some(offset);

                return Ok(0);
            }
            uuid::SequenceOutcome::ContinueExtendSpan if self.open == Some(producer) => {
                () = self.buffer.push(producer, framed)?;

                return Ok(0);
            }
            // A delta which began, was displaced, and now resumes. The records it
            // began with were dropped, so these are a fragment of it and are not
            // held: were they held, its acknowledgement would commit the fragment.
            // Leaving `open` elsewhere refuses that acknowledgement. The fragment
            // still interleaves whatever is held, as any other producer's record
            // does.
            uuid::SequenceOutcome::ContinueExtendSpan => {
                tracing::debug!(
                    held = ?self.open,
                    bytes = self.buffer.len(),
                    ?producer,
                    "dropping a held delta which a displaced delta's resumed records interleaved",
                );
                () = self.buffer.clear()?;
                self.horizon_at = None;
                self.open = None;

                return Ok(0);
            }
            uuid::SequenceOutcome::ContinueDuplicate => (),

            // Another producer's records interleaved this delta, so its
            // acknowledgement cannot be honored: those records displaced what this
            // pass held, and nothing states the order the two deltas committed in.
            uuid::SequenceOutcome::AckCommit => {
                anyhow::ensure!(
                    self.open == Some(producer),
                    "acknowledgement of a delta of {producer:?} which another producer's \
                     records interleaved",
                );
                self.open = None;
                () = ensure_no_chunks(record, "an acknowledgement")?;

                // The delta is committed, so the records held for it apply now.
                // This must precede the horizon check below, because the chunks
                // which discharge that horizon are among the ones applied here.
                // The drain also opens the horizon this delta opens, which it must
                // do before the delta's own chunks apply.
                let applied =
                    self.buffer
                        .drain(image, &mut self.horizon, self.horizon_at.take())?;

                self.applied = offset + framed.len() as i64;
                self.applied_chunks += applied;

                // A committed delta which discharged the last block of the
                // horizon puts a copy of every allocated block at or after it,
                // making it the floor.
                if self
                    .horizon
                    .as_ref()
                    .is_some_and(|open| open.blocks.pending() == 0)
                {
                    self.floor = self.horizon.take().map(|open| open.at);

                    tracing::debug!(?producer, floor = ?self.floor, "replay completed a horizon");
                }
                return Ok(applied);
            }
            // A delta whose records are all below the floor, or an
            // acknowledgement which was appended twice.
            uuid::SequenceOutcome::AckEmpty | uuid::SequenceOutcome::AckDuplicate => {
                () = ensure_no_chunks(record, "an acknowledgement")?;
            }

            // A rollback takes back records, and a deep one takes back records
            // this pass already applied. The append barrier makes one impossible
            // from this daemon, and this daemon is the only writer a disk journal
            // has.
            uuid::SequenceOutcome::AckCleanRollback | uuid::SequenceOutcome::AckDeepRollback => {
                anyhow::bail!(
                    "acknowledgement of {producer:?} at {clock:?} rolls back records this pass sequenced",
                )
            }
        }
        Ok(0)
    }
}

fn ensure_no_chunks(record: &proto::DiskRecord, what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        record.chunks.is_empty(),
        "{what} carries {} chunks, which change disk content it does not commit",
        record.chunks.len(),
    );
    Ok(())
}

#[cfg(test)]
mod test;
