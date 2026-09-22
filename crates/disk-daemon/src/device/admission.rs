//! What an owner may capture now, and what it does with a mutation it cannot.
//!
//! A mutation is offered to the capture channel before its image operation is
//! submitted, so journal order is the order the image is modified and every mutation
//! falls wholly on one side of a cut. A mutation the channel refuses — because it is
//! full, or because a prepare has closed admission — parks its request in arrival
//! order, and `retry_parked` offers those again without reordering them.
//!
//! Compaction is here for the same reason: a horizon copy is a mutation the owner
//! offers itself, out of the budget the delta's own traffic earned.

use super::Owner;
use super::request::Slot;
use crate::proto::Chunk;
use crate::ublk;

impl Owner {
    /// Answer the cut once admission is closed and everything admitted has been
    /// applied.
    pub(super) fn report_quiet(&mut self) {
        if self.admitted != 0 {
            return;
        }
        if let Some(quiet) = self.quiet.take() {
            _ = quiet.send(());
        }
    }

    /// Hand `chunks` to the capture channel. A mutation is captured before it is
    /// applied, so journal order is application order. A mutation waits here when
    /// the channel is full. It is never dropped or refused.
    ///
    /// A closed admission parks a mutation exactly as a full channel does, which
    /// places it after the cut.
    ///
    /// `data` is the write's, and empty for a punch. It is held either way, because
    /// the image operation which applies the mutation reads from it.
    pub(super) fn offer(
        &mut self,
        tag: u16,
        range: std::ops::Range<u32>,
        data: bytes::Bytes,
        chunks: Vec<Chunk>,
    ) {
        let changed = crate::chunk::data_bytes(&chunks);

        let offered = match self.admitting {
            true => self.capture.offer(chunks),
            false => Err(chunks),
        };

        match offered {
            Ok(()) => {
                self.slots[tag as usize] = Slot::Admitted { range, data };
                self.admit(tag, changed);
            }
            Err(chunks) => {
                self.slots[tag as usize] = Slot::Parked {
                    range,
                    data,
                    chunks,
                };
                self.parked.push_back(tag);
            }
        }
    }

    /// Take the mutation at `tag`, whose chunks the capture channel has
    /// accepted.
    ///
    /// A mutation publishes the blocks it covers, so it discharges them from any
    /// open horizon. The `changed` bytes it carries earn the budget a copy spends.
    fn admit(&mut self, tag: u16, changed: u64) {
        let range = self.slots[tag as usize].range();

        if let Some(horizon) = &mut self.horizon {
            () = horizon.published(range);
            () = horizon.changed(changed);
        }
        self.admitted += 1;
        self.begin_mutation(tag);
    }

    /// Spend this delta's copy budget on the open horizon, interleaving
    /// compaction with the traffic paying for it.
    ///
    /// A copy is selected, read, and offered without yielding, so no mutation
    /// can land between its read and its offer.
    pub(super) fn compact(&mut self) {
        if !self.admitting {
            return;
        }
        // Destructured because a copy reads the image while it discharges the
        // horizon, and the two are separate fields of this owner.
        let Self {
            dev_id,
            image,
            capture,
            horizon,
            policy,
            ..
        } = self;
        let Some(horizon) = horizon else {
            return;
        };
        let run_blocks = ublk::MAX_IO_BUF_BYTES / crate::BLOCK_SIZE;

        while capture.has_room() {
            let chunks = match horizon.copy(image, policy, run_blocks) {
                Ok(Some(chunks)) => chunks,
                Ok(None) => return,
                Err(err) => {
                    tracing::error!(dev_id = *dev_id, ?err, "failed to copy a horizon run");
                    return;
                }
            };
            let Ok(()) = capture.offer(chunks) else {
                unreachable!("the capture channel had room for a horizon copy")
            };
        }
    }

    /// Re-offer the chunks of every request parked on capture capacity or on a
    /// closed admission. This keeps arrival order, so neither backpressure nor a
    /// cut reorders two mutations.
    pub(super) fn retry_parked(&mut self) {
        while self.admitting {
            let Some(&tag) = self.parked.front() else {
                return;
            };
            // Taken out rather than borrowed, because an offer which is accepted
            // moves the slot on. A parked tag has no operation in flight, so the
            // slot holds nothing an SQE addresses meanwhile.
            let Slot::Parked {
                range,
                data,
                chunks,
            } = std::mem::replace(&mut self.slots[tag as usize], Slot::Idle)
            else {
                panic!("a parked tag holds the mutation its channel refused");
            };
            let changed = crate::chunk::data_bytes(&chunks);

            match self.capture.offer(chunks) {
                Ok(()) => {
                    _ = self.parked.pop_front();
                    self.slots[tag as usize] = Slot::Admitted { range, data };
                    self.admit(tag, changed);
                }
                Err(chunks) => {
                    self.slots[tag as usize] = Slot::Parked {
                        range,
                        data,
                        chunks,
                    };
                    return;
                }
            }
        }
    }

    fn begin_mutation(&mut self, tag: u16) {
        let range = self.slots[tag as usize].range();

        if self.inflight.begin(tag, range) {
            self.mutate(tag);
        }
    }
}
