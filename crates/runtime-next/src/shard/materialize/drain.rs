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
    pub fn step(
        &mut self,
        bindings: &[Binding],
        codec: connector_init::Codec,
    ) -> anyhow::Result<Option<materialize::Request>> {
        let Some(doc::combine::DrainedDoc { mut meta, mut root }) = self.drainer.drain_next()?
        else {
            return Ok(None);
        };

        // Prior-generation entries are stale loaded rows (they predate the
        // binding's backfill `truncated_at`); they're never stored, they only
        // prove the destination row exists. The combiner emits the run of
        // prior-generation rows for a key ahead of that key's current-generation
        // document, so skip the run and store the current-generation doc that
        // follows, as an UPDATE (exists=true). A doc with no prior-generation rows
        // ahead of it stores normally, with exists from `front`.
        let mut exists = meta.front();
        while meta.prior_gen() {
            let next = self
                .drainer
                .drain_next()?
                .expect("a prior-generation run is followed by its current-generation sibling");
            (meta, root) = (next.meta, next.root);
            exists = true;
        }

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

        // Build exactly one of the packed / JSON encodings per the connector's
        // codec — protobuf connectors unpack the `*_packed` tuples, JSON
        // connectors read the `*_json` arrays — skipping the other's work
        // entirely. Keys never truncate (no-op SerPolicy), but values may, so
        // value extraction observes and may further set `truncation_indicator`.
        let encoding = if codec == connector_init::Codec::Json {
            doc::Encoding::Json
        } else {
            doc::Encoding::Packed
        };

        doc::Extractor::extract_all_owned(
            &root,
            &binding.key_extractors,
            encoding,
            &mut self.buf,
            None,
        );
        let key = self.buf.split().freeze();

        binding.value_plan.extract_all_owned(
            &root,
            encoding,
            &mut self.buf,
            Some(&truncation_indicator),
        );
        let values = self.buf.split().freeze();

        let (key_packed, key_json) = match encoding {
            doc::Encoding::Packed => (key, Bytes::new()),
            doc::Encoding::Json => (Bytes::new(), key),
        };
        let (values_packed, values_json) = match encoding {
            doc::Encoding::Packed => (values, Bytes::new()),
            doc::Encoding::Json => (Bytes::new(), values),
        };

        let store = materialize::request::Store {
            binding: binding_index as u32,
            delete: meta.deleted(),
            doc_json,
            exists,
            key_json,
            key_packed,
            values_json,
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

#[cfg(test)]
mod test {
    use super::super::{Binding, task::combine_spec};
    use super::Drainer;
    use serde_json::{Value, json};
    use std::collections::VecDeque;

    fn test_binding() -> Binding {
        Binding {
            collection_name: "test/collection".to_string(),
            delta_updates: false,
            document_uuid_ptr: json::Pointer::from(""),
            journal_read_suffix: String::new(),
            key_extractors: vec![doc::Extractor::with_default(
                "/key",
                &doc::SerPolicy::noop(),
                json!(""),
            )],
            read_schema_json: bytes::Bytes::from_static(
                br#"{
                    "type": "object",
                    "properties": {
                        "key": { "type": "string" },
                        "v": { "type": "array", "reduce": { "strategy": "append" } }
                    },
                    "reduce": { "strategy": "merge" }
                }"#,
            ),
            ser_policy: doc::SerPolicy::noop(),
            state_key: "test/collection".to_string(),
            store_document: true,
            value_plan: doc::ExtractorPlan::new(&[]),
        }
    }

    #[test]
    fn prior_gen_pairing_sets_exists_without_reducing() {
        let bindings = vec![test_binding()];
        let mut accumulator = crate::Accumulator::new(combine_spec(&bindings).unwrap()).unwrap();

        {
            let memtable = accumulator.memtable().unwrap();
            let add = |doc: Value, front: bool| {
                let n = doc::HeapNode::from_node(&doc, memtable.alloc());
                memtable.add(0, n, front).unwrap();
            };
            let add_prior = |doc: Value| {
                let n = doc::HeapNode::from_node(&doc, memtable.alloc());
                memtable.add_prior_gen(0, n).unwrap();
            };

            // "straddle": a prior-generation loaded row plus its current-generation source.
            add_prior(json!({"key": "straddle", "v": ["stale"]}));
            add(json!({"key": "straddle", "v": ["fresh"]}), false);

            // "normal": an ordinary (current-generation) loaded row reduced with a source.
            add(json!({"key": "normal", "v": ["loaded"]}), true);
            add(json!({"key": "normal", "v": ["src"]}), false);

            // "srconly": a source-only key, no loaded row.
            add(json!({"key": "srconly", "v": ["only"]}), false);
        }

        let shuffle_reader = shuffle::log::Reader::new(std::path::Path::new("/dev/null"), 0);
        let mut drainer = Drainer::new(accumulator, shuffle_reader, VecDeque::new()).unwrap();

        let mut stores = Vec::new();
        while let Some(req) = drainer
            .step(&bindings, connector_init::Codec::Json)
            .unwrap()
        {
            let store = req.store.expect("drained request is a Store");
            stores.push((
                serde_json::from_slice::<Value>(&store.doc_json).unwrap(),
                store.exists,
            ));
        }

        // One (stored document, exists) per key, in key order — the prior-generation
        // "straddle" doc is never emitted on its own. Note "straddle" stores ["fresh"]
        // (NOT ["stale","fresh"]): its stale value is dropped, and exists=true is
        // forced onto the surviving current-generation source.
        assert_eq!(
            stores,
            vec![
                (json!({"key": "normal", "v": ["loaded", "src"]}), true),
                (json!({"key": "srconly", "v": ["only"]}), false),
                (json!({"key": "straddle", "v": ["fresh"]}), true),
            ],
        );
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
