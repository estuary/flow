use anyhow::Context;
use bytes::BufMut;
use proto_flow::{derive, flow};
use std::collections::{HashMap, VecDeque};

use super::task::Transform;
use crate::proto::derive::loaded::Binding as LoadedBinding;

/// Scanner is a synchronous state machine that walks a `shuffle::Frontier` one
/// block at a time. Each `step` turns source documents into `C:Read` requests
/// forwarded to the derive connector, and accumulates per-transform source
/// statistics for the eventual `L:Loaded`.
///
/// Unlike the materialize Scanner, it does not touch a combiner: source
/// documents flow to the connector, and the connector's `C:Published`
/// responses (not source docs) populate the shard's output combiner.
pub(super) struct Scanner {
    scan: shuffle::log::FrontierScan,
    // Buffer into which we serialize source-document JSON.
    buf: bytes::BytesMut,
    // Active transforms (bindings) of this scan, indexed on transform index.
    active: HashMap<u32, LoadedBinding>,
    // Serialization policy for source documents (no truncation).
    ser_policy: doc::SerPolicy,
}

impl Scanner {
    pub fn new(
        frontier: shuffle::Frontier,
        shuffle_reader: shuffle::log::Reader,
        shuffle_remainders: VecDeque<shuffle::log::Remainder>,
    ) -> anyhow::Result<Self> {
        let scan = shuffle::log::FrontierScan::new(frontier, shuffle_reader, shuffle_remainders)
            .context("failed to begin a FrontierScan")?;

        Ok(Self {
            scan,
            buf: bytes::BytesMut::new(),
            active: HashMap::new(),
            ser_policy: doc::SerPolicy::noop(),
        })
    }

    /// Process at most one block, pushing produced `C:Read` requests onto `out`.
    /// Returns `Ok(true)` while there is more work, `Ok(false)` once the frontier
    /// has been fully consumed.
    pub fn step(
        &mut self,
        transforms: &[Transform],
        validators: &mut [doc::Validator],
        codec: connector_init::Codec,
        out: &mut Vec<derive::Request>,
    ) -> anyhow::Result<bool> {
        if !self
            .scan
            .advance_block()
            .context("failed to advance FrontierScan block")?
        {
            return Ok(false);
        }

        for shuffle::log::reader::Entry {
            meta,
            doc,
            journal: _,
            producer,
        } in self.scan.block_iter()
        {
            let transform = meta.binding.to_native() as u32;
            if transform as usize >= transforms.len() {
                anyhow::bail!("scan entry has invalid transform index {transform}");
            }

            // The shuffle read pipeline validates each source document against
            // its source schema and sets `FLAGS_SCHEMA_VALID` on success. A
            // document arriving without the flag failed (or skipped) validation;
            // re-validate it to surface a meaningful error instead of forwarding
            // schema-invalid data to the connector.
            if meta.flags.to_native() & shuffle::FLAGS_SCHEMA_VALID == 0 {
                let Transform {
                    transform: name,
                    collection,
                    ..
                } = &transforms[transform as usize];

                validators[transform as usize]
                    .validate(doc.doc.get(), |_| None)
                    .with_context(|| {
                        format!(
                            "source document of transform {name} (collection {collection}) is invalid",
                        )
                    })?;
            }

            // Serialize the source document to JSON for the connector.
            serde_json::to_writer((&mut self.buf).writer(), &self.ser_policy.on(doc.doc.get()))
                .expect("source document serialization cannot fail");
            let doc_json = self.buf.split().freeze();

            // Producer occupies the high 48 bits of a v1 UUID node; the connectors
            // treat `uuid`/`shuffle` as informational (derive-sqlite ignores them,
            // derive-typescript exposes only the shuffle hash to lambdas).
            let p = producer.producer;
            let node = u64::from_be_bytes([p[0], p[1], p[2], p[3], p[4], p[5], 0, 0]);
            let clock = meta.clock.to_native();

            let shuffle_key_extractors = &transforms[transform as usize].shuffle_key_extractors;

            // The shuffle log persists only a 16-byte key prefix. Reuse it as
            // the full packed key when it's known-complete (stripping any
            // zero-padding), and otherwise re-extract from the source document.
            let packed = match doc::Extractor::packed_key_prefix_len(
                doc.packed_key_prefix.as_slice(),
                shuffle_key_extractors.len(),
            ) {
                Some(len) => bytes::Bytes::copy_from_slice(&doc.packed_key_prefix[..len]),
                None => {
                    doc::Extractor::extract_all(
                        doc.doc.get(),
                        shuffle_key_extractors,
                        doc::Encoding::Packed,
                        &mut self.buf,
                        None,
                    );
                    self.buf.split().freeze()
                }
            };
            let hash = doc::Extractor::packed_hash(&packed);

            // Send exactly one of `key_json` / `packed` per the connector's
            // codec; `hash` is always sent.
            let (key_json, packed) = if codec == connector_init::Codec::Json {
                let key_json = if shuffle_key_extractors.is_empty() {
                    bytes::Bytes::new() // Lambda-computed (no extractors).
                } else {
                    doc::Extractor::extract_all(
                        doc.doc.get(),
                        shuffle_key_extractors,
                        doc::Encoding::Json,
                        &mut self.buf,
                        None,
                    );
                    self.buf.split().freeze()
                };
                (key_json, bytes::Bytes::new())
            } else {
                (bytes::Bytes::new(), packed)
            };

            out.push(derive::Request {
                read: Some(derive::request::Read {
                    transform,
                    uuid: Some(flow::UuidParts { node, clock }),
                    shuffle: Some(derive::request::read::Shuffle {
                        key_json,
                        packed,
                        hash,
                    }),
                    doc_json,
                }),
                ..Default::default()
            });

            // Accumulate per-transform source statistics.
            let active = self.active.entry(transform).or_default();
            if active.sourced_docs_total == 0 {
                active.index = transform;
                active.max_source_clock = clock;
                active.min_source_clock = clock;
            } else {
                active.max_source_clock = active.max_source_clock.max(clock);
                active.min_source_clock = active.min_source_clock.min(clock);
            }
            active.sourced_docs_total += 1;
            active.sourced_bytes_total += doc.source_byte_length.to_native() as u64;
        }

        Ok(true)
    }

    pub fn into_parts(
        self,
    ) -> (
        shuffle::log::Reader,
        VecDeque<shuffle::log::Remainder>,
        HashMap<u32, LoadedBinding>,
    ) {
        let (_, shuffle_reader, shuffle_remainders) = self.scan.into_parts();
        (shuffle_reader, shuffle_remainders, self.active)
    }
}
