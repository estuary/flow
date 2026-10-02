//! The simulated database, and the capture of it: source-postgres's
//! interleave of ascending key-ordered backfill chunks with the replication
//! log written meanwhile, then replication alone, forever.
//!
//! Each table's rows are ordinals `[0, next_key)`, which map to its keys in
//! ascending order. Inserts take `next_key` and advance it, so a backfill
//! chases them, and updates and deletes pick existing ordinals uniformly.
//! Deleted rows aren't tracked: a later backfill chunk (or update) still finds
//! them, which the runtime can't tell apart from a row that was re-inserted.
//!
//! Every chunk and transaction draws from its own generator, seeded by its
//! position, so output is a pure function of the seed and the checkpoint a
//! session opens from. Fork points: `OP_MIX`, and how `replay` picks keys.

use crate::plan::{Meta, Rng, RowPlan};
use crate::wire::Writer;
use serde_json::json;
use std::collections::BTreeMap;

/// Percent of replication changes which are inserts and updates (the rest
/// are deletes).
const OP_MIX: (u64, u64) = (10, 85);
/// LSN distance between transaction commits.
pub const LSN_STEP: u64 = 4096;
/// Synthetic milliseconds between transaction commits, from `plan::EPOCH`.
const COMMIT_MS: u64 = 10;
/// Generator streams, which keep chunk, transaction, and pick draws apart.
const STREAM_PICK: u64 = 1;
const STREAM_TXN: u64 = 2;
const STREAM_CHUNK: u64 = 3 << 32;

/// A binding's state, as sqlcapture's `bindingStateV1` entry. Its
/// `metadata.next_key` is this connector's own.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BindingState {
    pub mode: Mode,
    pub key_columns: Vec<String>,
    /// Base64 of the FDB-tuple-packed key of the last backfilled row. Null
    /// once Active (which, as a merge patch, removes it).
    #[serde(default)]
    pub scanned: Option<String>,
    /// Rows backfilled so far, which is the ordinal of the next to backfill.
    pub backfilled: u64,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Backfill,
    Active,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub next_key: u64,
    /// True from the checkpoint of a backfill's last chunk, until that of its
    /// `BackfillComplete`, which a session resumed between them sends first.
    /// False in the latter (removing nothing, as it's then omitted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill_complete: Option<bool>,
}

/// The connector state, as sqlcapture's.
#[derive(Debug, Default, serde::Deserialize)]
pub struct State {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, rename = "bindingStateV1")]
    pub bindings: BTreeMap<String, BindingState>,
}

pub struct Binding {
    pub plan: RowPlan,
    pub state_key: String,
    /// Share of changes, scaled to an integer.
    pub weight: u64,
    pub state: BindingState,
    /// True if the binding had no state, and begins a backfill.
    pub new: bool,
    /// True if `state` changed since the last checkpoint.
    pub dirty: bool,
}

pub struct Capture {
    pub seed: u64,
    pub backfill_chunk_size: u64,
    pub txns_per_chunk: u64,
    pub changes_per_txn: u64,
    pub bindings: Vec<Binding>,
    /// Index of the next replication transaction.
    pub txn: u64,
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

pub fn parse_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    Some(u64::from_str_radix(hi, 16).ok()? << 32 | u64::from_str_radix(lo, 16).ok()?)
}

impl Capture {
    /// Emit the session's start-up, then backfill and replicate until the
    /// writer fails (as when the runtime closes its stdout).
    pub fn run(
        mut self,
        out: &mut Writer,
        sourced_schemas: Vec<serde_json::Value>,
    ) -> std::io::Result<std::convert::Infallible> {
        use proto_flow::capture::response;

        out.message(response::Kind::Opened(response::Opened {
            explicit_acknowledgements: true,
        }))?;
        for (binding, schema) in sourced_schemas.into_iter().enumerate() {
            out.message(response::Kind::SourcedSchema(response::SourcedSchema {
                binding: binding as u32,
                schema_json: schema.to_string().into(),
            }))?;
        }
        out.checkpoint(None)?;

        // Each new binding's backfill begins in a checkpoint of its own, and
        // a prior session's completed backfill is acknowledged in one.
        for index in 0..self.bindings.len() {
            if self.bindings[index].new {
                self.bindings[index].dirty = true;
                out.message(response::Kind::BackfillBegin(response::BackfillBegin {
                    binding: index as u32,
                }))?;
                out.checkpoint(Some(self.patch(false)))?;
            }
            if self.bindings[index].state.metadata.backfill_complete == Some(true) {
                self.complete_backfill(out, index)?;
            }
        }

        loop {
            let backfilling: Vec<usize> = (0..self.bindings.len())
                .filter(|i| self.bindings[*i].state.mode == Mode::Backfill)
                .collect();

            if backfilling.is_empty() {
                self.replay(out)?;
                out.checkpoint(Some(self.patch(true)))?;
                continue;
            }

            // As sqlcapture: one chunk of a randomly-picked backfilling table,
            // then the log written while its query ran, in one checkpoint.
            let mut pick = Rng::new(self.seed, STREAM_PICK, self.txn);
            let index = backfilling[pick.below(backfilling.len() as u64) as usize];
            let complete = self.backfill_chunk(out, index)?;
            for _ in 0..self.txns_per_chunk {
                self.replay(out)?;
            }
            out.checkpoint(Some(self.patch(true)))?;

            if complete {
                self.complete_backfill(out, index)?;
            }
        }
    }

    /// Send `BackfillComplete` of `index`, in a checkpoint which also makes
    /// the binding Active. Until then a resumed session would send it again.
    fn complete_backfill(&mut self, out: &mut Writer, index: usize) -> std::io::Result<()> {
        use proto_flow::capture::response;

        let binding = &mut self.bindings[index];
        binding.state.mode = Mode::Active;
        binding.state.scanned = None;
        binding.state.metadata.backfill_complete = Some(false);
        binding.dirty = true;

        out.message(response::Kind::BackfillComplete(
            response::BackfillComplete {
                binding: index as u32,
            },
        ))?;
        out.checkpoint(Some(self.patch(false)))
    }

    /// Emit the next chunk of `index`'s backfill, returning whether it
    /// completed the backfill (a chunk shorter than the chunk size).
    fn backfill_chunk(&mut self, out: &mut Writer, index: usize) -> std::io::Result<bool> {
        let binding = &mut self.bindings[index];
        let begin = binding.state.backfilled;
        let end = binding
            .state
            .metadata
            .next_key
            .min(begin + self.backfill_chunk_size);
        let mut rng = Rng::new(self.seed, STREAM_CHUNK + index as u64, begin);

        for ordinal in begin..end {
            binding.plan.write_doc(
                &mut out.doc,
                &mut rng,
                ordinal,
                &Meta::Backfill { offset: ordinal },
            );
            out.captured(index as u32)?;
        }
        let complete = end - begin < self.backfill_chunk_size;

        binding.state.backfilled = end;
        if end != begin {
            use base64::Engine;
            let packed = binding.plan.packed_key(end - 1);
            binding.state.scanned = Some(base64::engine::general_purpose::STANDARD.encode(packed));
        }
        if complete {
            // Replication changes of the table are no longer filtered, as
            // the backfill is complete, though it becomes Active only with
            // its `BackfillComplete`.
            binding.state.metadata.backfill_complete = Some(true);
        }
        binding.dirty = true;
        Ok(complete)
    }

    /// Emit replication transaction `self.txn`, and advance to the next. A
    /// change of a row which a backfill hasn't reached is dropped, as the
    /// backfill will read the row as it is.
    fn replay(&mut self, out: &mut Writer) -> std::io::Result<()> {
        let txn = self.txn;
        self.txn += 1;

        let total: u64 = self.bindings.iter().map(|b| b.weight).sum();
        if total == 0 {
            return Ok(());
        }
        let mut rng = Rng::new(self.seed, STREAM_TXN, txn);
        let (begin_lsn, end_lsn) = (txn * LSN_STEP, (txn + 1) * LSN_STEP);

        for change in 0..self.changes_per_txn {
            let mut roll = rng.below(total);
            let index = self
                .bindings
                .iter()
                .position(|b| {
                    let hit = roll < b.weight;
                    roll = roll.saturating_sub(b.weight);
                    hit
                })
                .expect("roll is below the total weight");
            let binding = &mut self.bindings[index];
            let state = &mut binding.state;

            let op = rng.below(100);
            let (op, ordinal) = if op < OP_MIX.0 || state.metadata.next_key == 0 {
                state.metadata.next_key += 1;
                binding.dirty = true;
                (b'c', state.metadata.next_key - 1)
            } else {
                let op = if op < OP_MIX.0 + OP_MIX.1 { b'u' } else { b'd' };
                (op, rng.below(state.metadata.next_key))
            };
            let filtered =
                state.mode == Mode::Backfill && state.metadata.backfill_complete.is_none();
            if filtered && ordinal >= state.backfilled {
                continue;
            }

            let meta = Meta::Change {
                op,
                loc: [begin_lsn, begin_lsn + change + 1, end_lsn],
                ts_ms: crate::plan::EPOCH * 1000 + txn * COMMIT_MS,
                txid: 1000 + txn,
            };
            binding
                .plan
                .write_doc(&mut out.doc, &mut rng, ordinal, &meta);
            out.captured(index as u32)?;
        }
        Ok(())
    }

    /// The checkpoint's merge patch: the cursor (if `cursor`), and the state
    /// of each binding which changed since the last.
    fn patch(&mut self, cursor: bool) -> serde_json::Value {
        let mut patch = serde_json::Map::new();
        if cursor {
            patch.insert("cursor".to_string(), json!(format_lsn(self.txn * LSN_STEP)));
        }
        let mut states = serde_json::Map::new();
        for binding in self.bindings.iter_mut().filter(|b| b.dirty) {
            binding.dirty = false;
            states.insert(
                binding.state_key.clone(),
                serde_json::to_value(&binding.state).unwrap(),
            );
            if binding.state.metadata.backfill_complete == Some(false) {
                binding.state.metadata.backfill_complete = None;
            }
        }
        if !states.is_empty() {
            patch.insert("bindingStateV1".to_string(), states.into());
        }
        patch.into()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn lsn_round_trip() {
        for lsn in [0, 4096, 1 << 33 | 77] {
            assert_eq!(super::parse_lsn(&super::format_lsn(lsn)), Some(lsn));
        }
        assert_eq!(super::format_lsn(1 << 32 | 0xA2B3C8), "1/A2B3C8");
    }
}
