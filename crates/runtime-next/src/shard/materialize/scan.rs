use super::{Binding, LoadKeys};
use anyhow::Context;
use bytes::Buf;
use bytes::{BufMut, Bytes};
use proto_flow::materialize;
use std::collections::{HashMap, VecDeque};

use crate::proto::materialize::loaded::Binding as LoadedBinding;

/// Scanner is a synchronous state machine that walks a `shuffle::Frontier`
/// one block at a time. Each `step` adds source documents to the
/// accumulator's memtable and produces `C:Load` requests for keys that
/// may exist in the materialization endpoint.
pub(super) struct Scanner {
    accumulator: crate::Accumulator,
    // FrontierScan underway by this Scanner.
    scan: shuffle::log::FrontierScan,
    // Buffer into which we extract and hash packed keys.
    buf: bytes::BytesMut,
    // Active bindings of this scan, indexed on binding index.
    active: HashMap<u32, LoadedBinding>,
}

impl Scanner {
    pub fn new(
        accumulator: crate::Accumulator,
        frontier: shuffle::Frontier,
        shuffle_reader: shuffle::log::Reader,
        shuffle_remainders: VecDeque<shuffle::log::Remainder>,
    ) -> anyhow::Result<Self> {
        let scan = shuffle::log::FrontierScan::new(frontier, shuffle_reader, shuffle_remainders)
            .context("failed to begin a FrontierScan")?;

        Ok(Self {
            accumulator,
            scan,
            buf: bytes::BytesMut::new(),
            active: HashMap::new(),
        })
    }

    pub fn accumulator(&mut self) -> &mut crate::Accumulator {
        &mut self.accumulator
    }

    /// Process at most one block. Pushes any produced `C:Load` requests onto
    /// `out`. Returns `Ok(true)` while there is more work to do, and
    /// `Ok(false)` once the frontier has been fully consumed.
    pub fn step(
        &mut self,
        bindings: &[Binding],
        load_keys: &mut LoadKeys,
        max_keys: &mut [(Bytes, Bytes)],
        disable_load_optimization: bool,
        out: &mut Vec<materialize::Request>,
    ) -> anyhow::Result<bool> {
        if !self
            .scan
            .advance_block()
            .context("failed to advance FrontierScan block")?
        {
            return Ok(false);
        }

        let memtable = self
            .accumulator
            .memtable()
            .context("failed to acquire combiner memtable")?;
        let alloc = memtable.alloc();

        for shuffle::log::reader::Entry {
            meta,
            doc,
            journal: _,
            producer: _,
        } in self.scan.block_iter()
        {
            // Was the document successfully validated against its JSON schema?
            let known_valid = meta.flags.to_native() & shuffle::FLAGS_SCHEMA_VALID != 0;

            let binding_index = meta.binding.to_native() as u32;
            let binding = bindings
                .get(meta.binding.to_native() as usize)
                .context("scan entry has invalid meta.binding")?;

            memtable
                .add_embedded(
                    meta.binding.to_native(),
                    &doc.packed_key_prefix,
                    doc.doc.to_heap(alloc),
                    false,
                    known_valid,
                )
                .context("MemTable::add_embedded failed")?;

            // Encode the binding index followed by the packed key, for hashing.
            self.buf.put_u32(binding_index);
            doc::Extractor::extract_all(doc.doc.get(), &binding.key_extractors, &mut self.buf);
            let key_hash = xxhash_rust::xxh3::xxh3_128(&self.buf);
            let mut key_packed = self.buf.split().freeze();
            key_packed.advance(4); // Advance past 4-byte binding index.

            // `next_max` starts empty each transaction and only ratchets to
            // larger-than-`prev_max` values. The `gt_prev_max` guard is needed,
            // as `key_packed` compares greater than initial (empty) `next_max`.
            let (prev_max, next_max) = &mut max_keys[binding_index as usize];
            let gt_prev_max = key_packed > *prev_max;
            let gt_next_max = gt_prev_max && key_packed > *next_max;

            let active = self.active.entry(binding_index).or_default();

            // Accumulate metrics for active bindings of the scan.
            let clock = meta.clock.to_native();
            if active.sourced_docs_total == 0 {
                active.index = binding_index;
                active.max_source_clock = clock;
                active.min_source_clock = clock;
            } else {
                active.max_source_clock = active.max_source_clock.max(clock);
                active.min_source_clock = active.min_source_clock.min(clock);
            }
            active.sourced_docs_total += 1;
            active.sourced_bytes_total += doc.source_byte_length.to_native() as u64;

            // Is `key_packed` larger than the largest key previously stored
            // to the connector? If so, then it cannot possibly exist.
            // We still track the max key even when the optimization is disabled.
            if gt_prev_max {
                if gt_next_max {
                    // This is a new high water mark for the largest-stored key.
                    *next_max = key_packed.clone();
                    active.max_key_delta = key_packed.clone();
                }
                // Skip the load request unless optimization is disabled.
                if !disable_load_optimization {
                    continue;
                }
            }

            if binding.delta_updates {
                // Delta-update bindings don't load.
            } else if load_keys.contains(&key_hash) {
                // We already sent a Load request for this key.
            } else {
                load_keys.insert(key_hash);

                out.push(materialize::Request {
                    load: Some(materialize::request::Load {
                        binding: binding_index,
                        key_json: Bytes::new(), // NOTE(johnny): Unclear if we'll implement this.
                        key_packed,
                    }),
                    ..Default::default()
                });
            }
        }

        Ok(true)
    }

    pub fn into_parts(
        self,
    ) -> (
        crate::Accumulator,
        shuffle::log::Reader,
        VecDeque<shuffle::log::Remainder>,
        HashMap<u32, LoadedBinding>,
    ) {
        let (_, shuffle_reader, shuffle_remainders) = self.scan.into_parts();
        (
            self.accumulator,
            shuffle_reader,
            shuffle_remainders,
            self.active,
        )
    }
}
