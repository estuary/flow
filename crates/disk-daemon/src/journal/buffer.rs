//! Records of the delta which the journal holds but has not acknowledged.
//!
//! A replay cannot apply a delta before its acknowledgement. A standby has no end
//! of range at which an unacknowledged delta could be discovered and taken back: the
//! delta at the head is open because the primary is still writing it, and that is
//! the normal state. Its image would then hold writes the client never committed,
//! and a promotion would serve a disk ahead of the client's own checkpoint. That is
//! the duplicate work which `Prepare`/`Acknowledge` exists to prevent.
//!
//! This holds those records instead, in a file beside the image. Records go in as
//! they arrive, framed exactly as the journal framed them. They come out and are
//! applied when the acknowledgement arrives, and they are dropped if it never comes
//! — which costs a hole punch and leaves the image untouched.
//!
//! One delta is held at a time, because a live writer keeps one delta in doubt at a
//! time: the records of the delta behind a cut are held by that writer until the
//! acknowledgement lands, so they never reach the journal ahead of it. Which delta
//! may still be acknowledged is [`super::replay::Pass`]'s decision and not this
//! file's.
//!
//! The file is `O_TMPFILE` like the image, so it cannot outlive its daemon. Nothing
//! recovers it: a standby which dies is replaced by one which replays from the
//! floor.

use super::replay::Opened;
use crate::horizon::Horizon;
use crate::image::Image;
use anyhow::Context;
use proto_gazette::{fixed_framing, uuid};

/// Capacity of the reader a held delta is replayed through. A delta has no size
/// bound of its own — it is whatever the primary wrote between two acknowledgements,
/// and may exceed the disk it belongs to — so it is read back through this buffer
/// rather than read whole. A record larger than the buffer is read whole anyway,
/// because framing decodes a record as a unit: replay therefore costs this buffer
/// plus the largest record, and not the delta.
const READ_BYTES: usize = 64 << 10;

/// The unacknowledged delta of one replay.
pub struct Buffer {
    file: std::fs::File,
    /// Bytes held, which is also the offset the next record is written at. The file
    /// is written strictly forward and punched back to zero whenever a delta leaves,
    /// so a standby which runs for weeks holds no more than the delta it is holding.
    len: u64,
    records: usize,
    /// Producer which stamped every record held. Absent while nothing is held.
    producer: Option<uuid::Producer>,
}

impl Buffer {
    /// Create a buffer within `dir`, which is the daemon's image directory.
    pub fn create(dir: &std::path::Path) -> std::io::Result<Self> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_TMPFILE);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        Ok(Self {
            file: options.open(dir)?,
            len: 0,
            records: 0,
            producer: None,
        })
    }

    /// Bytes held, which a tenure reports as its buffered state.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Hold `framed`, the journal's own bytes of a record of `producer`'s delta.
    ///
    /// A record of another producer is a delta which displaced the one held, so what
    /// is held is dropped. Only an acknowledgement of the delta at the head can be
    /// honored, and this record proves that none arrived.
    pub(super) fn push(&mut self, producer: uuid::Producer, framed: &[u8]) -> anyhow::Result<()> {
        if self.producer.is_some_and(|held| held != producer) {
            tracing::debug!(
                held = ?self.producer,
                records = self.records,
                ?producer,
                "dropping a held delta which another producer's records displaced",
            );
            () = self.clear()?;
        }
        () = std::os::unix::fs::FileExt::write_all_at(&self.file, framed, self.len)
            .context("holding a record of an unacknowledged delta")?;

        self.len += framed.len() as u64;
        self.records += 1;
        self.producer = Some(producer);

        Ok(())
    }

    /// Apply the held delta to `image`, and report the chunks it applied.
    ///
    /// The records apply in the order they were held, so the horizon the delta opens
    /// snapshots the blocks which were allocated before that delta, and the chunks
    /// which discharge it are applied after. A delta's effects are therefore whole:
    /// they all land here, or none of them ever land.
    ///
    /// `horizon` is the replay's own, which this delta may open, discharge, or
    /// leave alone. `opens_at` is the offset the pass recorded for the record which
    /// carries the flag, and it is that horizon's floor.
    ///
    /// Replay buffers its reads and decodes one record at a time, using memory
    /// independent of the delta's length.
    pub(super) fn drain(
        &mut self,
        image: &mut Image,
        horizon: &mut Option<Opened>,
        opens_at: Option<i64>,
    ) -> anyhow::Result<usize> {
        // Records are held with positional writes, which never move the cursor, so
        // it is still wherever the tenure's last drain left it.
        () = std::io::Seek::rewind(&mut &self.file)
            .context("seeking to the start of a held delta")?;

        // The file outlives the delta it holds, because `clear` punches it rather
        // than truncating it, so the read stops at this delta's own end.
        let mut reader = std::io::BufReader::with_capacity(
            READ_BYTES,
            std::io::Read::take(&self.file, self.len),
        );
        let mut frame = Vec::new();
        let mut applied = 0;
        // Bytes of the delta behind the record being read, which every read takes
        // from and every frame must fit within.
        let mut unread = self.len;

        for _ in 0..self.records {
            frame.resize(fixed_framing::HEADER_LEN, 0);
            () = read_framed(&mut reader, &mut frame)?;

            // This file framed these records itself, so a header which is not one is
            // corruption of the file, and not a stream which a reader joined between
            // frames and can resynchronize with.
            anyhow::ensure!(
                frame[..fixed_framing::MAGIC.len()] == fixed_framing::MAGIC,
                "a held delta does not decode: a record begins {:02x?}",
                &frame[..fixed_framing::MAGIC.len()],
            );
            let payload =
                u32::from_le_bytes(frame[fixed_framing::MAGIC.len()..].try_into().unwrap())
                    as usize;
            let framed = fixed_framing::HEADER_LEN + payload;

            // Nothing stands behind a length the file states, so it sizes an
            // allocation only once the delta is known to hold that many bytes. A
            // record the file holds all but the end of is truncation, and the read
            // below is what reports it.
            anyhow::ensure!(
                payload as u64 <= unread,
                "a held record frames {framed} bytes, which is {} more than the delta holds",
                framed as u64 - unread,
            );
            frame.resize(framed, 0);
            () = read_framed(&mut reader, &mut frame[fixed_framing::HEADER_LEN..])?;
            unread -= framed as u64;

            let record = match fixed_framing::decode::<crate::proto::DiskRecord>(&frame)
                .context("decoding a held record")?
            {
                fixed_framing::Frame::Record { message, .. } => message,
                // The magic word and the whole payload were both checked above.
                other => panic!("a checked frame decoded as {other:?}"),
            };

            if record.opens_horizon {
                let at = opens_at.expect("the pass held the offset of an opening record");
                let blocks = Horizon::open(image.allocated());

                tracing::debug!(
                    pending = blocks.pending(),
                    at,
                    "a held delta opened a recovery horizon",
                );
                *horizon = Some(Opened { at, blocks });
            }
            applied += apply(&record, image, horizon)?;
        }
        anyhow::ensure!(
            unread == 0,
            "a held delta ends within a record, with {unread} trailing bytes which frame none",
        );
        () = self.clear()?;

        Ok(applied)
    }

    /// Drop every held record. The image is untouched, because nothing of them was
    /// applied.
    pub(super) fn clear(&mut self) -> std::io::Result<()> {
        if self.len != 0 {
            () = crate::image::punch_hole(&self.file, 0, self.len)?;
        }
        self.len = 0;
        self.records = 0;
        self.producer = None;

        Ok(())
    }
}

/// Apply every chunk of `record` to `image`, discharging each from `horizon`, and
/// report how many it applied.
///
/// A horizon opens at a record and a replay is a forward pass, so every chunk which
/// reaches here is at or after any horizon which is open.
fn apply(
    record: &crate::proto::DiskRecord,
    image: &mut Image,
    horizon: &mut Option<Opened>,
) -> anyhow::Result<usize> {
    for chunk in &record.chunks {
        () = image
            .apply(chunk)
            .with_context(|| format!("applying chunk at block {}", chunk.block))?;

        if let Some(open) = horizon {
            () = open.blocks.published(crate::chunk::covered_blocks(chunk));
        }
    }
    Ok(record.chunks.len())
}

/// Fill `buf` from `reader`, which reads the held delta and stops at its end.
///
/// The delta was framed as it was held, so every frame it holds must be whole. An
/// end within one is a file which holds less than it counted records for.
fn read_framed(reader: &mut impl std::io::Read, buf: &mut [u8]) -> anyhow::Result<()> {
    match std::io::Read::read_exact(reader, buf) {
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            anyhow::bail!("a held delta ends within a record")
        }
        result => result.context("reading back a held delta"),
    }
}

#[cfg(test)]
mod test;
