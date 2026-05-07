use super::Binding;
use crate::Accumulator;
use anyhow::Context;
use bytes::{BufMut, Bytes};
use proto_flow::materialize;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;

use crate::proto::materialize::stored::Binding as StoredBinding;

/// Drainer is a synchronous state machine that drains the accumulator's
/// combined documents and produces `C:Store` requests one at a time.
/// `new` consumes the accumulator and the unused shuffle reader / remainders;
/// `finish` returns them so the actor can restore them on `Phase::Idle`.
pub(super) struct Drainer {
    drainer: doc::combine::Drainer,
    // Active bindings of this drain, in binding index order.
    active: Vec<StoredBinding>,
    // Buffer into which we extract document bytes, key, and values.
    buf: bytes::BytesMut,
    // Carried through the drain so they can be used in future phases.
    parser: simd_doc::Parser,
    shuffle_reader: shuffle::log::Reader,
    shuffle_remainders: VecDeque<shuffle::log::Remainder>,
}

impl Drainer {
    pub fn new(
        accumulator: crate::Accumulator,
        shuffle_reader: shuffle::log::Reader,
        shuffle_remainders: VecDeque<shuffle::log::Remainder>,
    ) -> anyhow::Result<Self> {
        let (drainer, parser) = accumulator
            .into_drainer()
            .context("preparing combiner drain")?;

        Ok(Self {
            drainer,
            active: Vec::new(),
            buf: bytes::BytesMut::new(),
            parser,
            shuffle_reader,
            shuffle_remainders,
        })
    }

    /// Drain one document (Some), or returns None when the drain is complete.
    pub fn step(&mut self, bindings: &[Binding]) -> anyhow::Result<Option<materialize::Request>> {
        let Some(doc::combine::DrainedDoc { meta, root }) = self.drainer.drain_next()? else {
            return Ok(None);
        };

        let binding_index = meta.binding();
        let binding = &bindings[binding_index];

        let active = if let Some(entry) = self.active.last_mut()
            && entry.index as usize == binding_index
        {
            entry
        } else {
            self.active.push(StoredBinding {
                index: binding_index as u32,
                ..Default::default()
            });
            self.active.last_mut().unwrap()
        };

        // Track whether truncation occurred. On the order of operations:
        // The value extractors may contain a special truncation extractor
        // into which will write the value of this variable. Thus, it's critical
        // that we extract values last, so it observes truncations of the key
        // or the document.
        let truncation_indicator = AtomicBool::new(false);

        // Serialize the root document so that we can account for its bytes
        // in reported measures. When the binding doesn't store the document,
        // count bytes against a sink rather than allocating into `self.buf`.
        let serialized = &binding
            .ser_policy
            .on_owned_with_truncation_indicator(&root, &truncation_indicator);
        let doc_json = if binding.store_document {
            serde_json::to_writer((&mut self.buf).writer(), serialized)
                .expect("document serialization cannot fail");
            active.stored_bytes_total += self.buf.len() as u64;
            self.buf.split().freeze()
        } else {
            let mut counter = ByteCounter(0);
            serde_json::to_writer(&mut counter, serialized)
                .expect("document serialization cannot fail");
            active.stored_bytes_total += counter.0;
            bytes::Bytes::new()
        };
        active.stored_docs_total += 1;

        doc::Extractor::extract_all_owned_indicate_truncation(
            &root,
            &binding.key_extractors,
            &mut self.buf,
            &truncation_indicator,
        );
        let key_packed = self.buf.split().freeze();

        binding.value_plan.extract_all_owned_indicate_truncation(
            &root,
            &mut self.buf,
            &truncation_indicator,
        );
        let values_packed = self.buf.split().freeze();

        let store = materialize::request::Store {
            binding: binding_index as u32,
            delete: meta.deleted(),
            doc_json,
            exists: meta.front(),
            key_json: Bytes::new(),
            key_packed,
            values_json: Bytes::new(),
            values_packed,
        };

        Ok(Some(materialize::Request {
            store: Some(store),
            ..Default::default()
        }))
    }

    pub fn into_parts(
        self,
    ) -> anyhow::Result<(
        crate::Accumulator,
        shuffle::log::Reader,
        VecDeque<shuffle::log::Remainder>,
        Vec<StoredBinding>,
    )> {
        let Self {
            drainer,
            active,
            buf: _,
            parser,
            shuffle_reader,
            shuffle_remainders,
        } = self;
        let accumulator = Accumulator::from_drainer(drainer, parser)?;
        Ok((accumulator, shuffle_reader, shuffle_remainders, active))
    }
}

// `Write` adapter that just counts bytes written, used to size serialized
// documents when the binding doesn't store the document body.
struct ByteCounter(u64);

impl std::io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
