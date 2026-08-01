//! Capture combiner drain.
//!
//! [`drain_and_publish`] runs as the actor's parked `drain_fut`: it consumes a
//! rotated combiner, publishes captured documents as `CONTINUE_TXN` journal
//! appends, folds connector-reported schemas into per-collection inference, and
//! assembles the [`fsm::DrainedCapture`] the TailFSM needs to build stats and
//! the committing Persist.
//!
//! Inference is keyed by *collection*, not binding: documents of every binding
//! sharing a target collection widen one shape, which is logged once. See
//! [`crate::leader::capture::task::InferenceSlot`].
//!
//! Unlike the materialize shard drain — a synchronous step machine interleaved
//! with connector IO — a capture drain is a single self-contained async pass:
//! it owns the publisher for its duration and hands it back via [`Output`].

use crate::leader::capture::{Task, fsm};
use anyhow::Context;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// Schema-complexity limit for a collection the connector described with a
/// SourcedSchema. Such a collection has a meaningful source-derived schema, so
/// inference is trusted with far more leeway than a purely-inferred one
/// (which uses [`doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT`]). The
/// limit rides in the shape's annotations and so persists across sessions —
/// see `Task::shapes_by_slot`. Because inference is collection-keyed, any one
/// binding reporting a SourcedSchema ratchets its whole collection to this limit.
const SOURCED_SCHEMA_COMPLEXITY_LIMIT: usize = 10_000;

/// Resources and results handed back to the actor when a drain completes.
pub(super) struct Output<P: crate::Publisher> {
    /// The drained combiner, recycled as the next transaction's `idle_accumulator`.
    pub(super) accumulator: crate::Accumulator,
    /// Per-transaction connector patches and stats, staged for the TailFSM.
    pub(super) drained: fsm::DrainedCapture,
    /// The publisher, borrowed for the drain's journal appends.
    pub(super) publisher: P,
    /// Inferred write-shapes indexed by inference slot (one per target
    /// collection), carried across sessions of the shard.
    pub(super) shapes: Vec<doc::Shape>,
}

/// Drain a rotated combiner: apply sourced schemas to inference, publish each
/// captured document, and accumulate the connector-state patch stream.
pub(super) async fn drain_and_publish<P: crate::Publisher, L: crate::Logger>(
    mut drainer: doc::combine::Drainer,
    parser: simd_doc::Parser,
    mut publisher: P,
    task: std::sync::Arc<Task>,
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    mut shapes: Vec<doc::Shape>,
    metrics: super::Metrics,
    logger: L,
) -> anyhow::Result<Output<P>> {
    // Resync the publisher clock to wall-clock time at the start of this
    // transaction's stream of published documents. Each `publish_doc` and the
    // closing `commit_intents` then tick it up by a single microsecond, so
    // stamped UUIDs cluster at the transaction's time of initial write.
    // `Clock::update` is monotonic and never regresses.
    publisher.update_clock();

    // Inference slots updated this transaction — by a sourced schema or by
    // widening an inferred shape — are logged once the drain completes.
    let mut updated_inferences = BTreeSet::<usize>::new();

    apply_sourced_schemas(&mut shapes, &task, sourced_schemas, &mut updated_inferences)?;

    // State-Update-Wire-Format stream of this transaction's connector patches:
    // a `[`, then `,`-separated compact-JSON patches each terminated by `\t`,
    // and a closing `]` appended once the drain completes.
    let mut connector_patches = Vec::<u8>::new();
    let mut drained = BTreeMap::<u32, ops::proto::stats::DocsAndBytes>::new();
    let mut count = 1;

    while let Some(doc::combine::DrainedDoc { meta, root: doc }) = drainer.drain_next()? {
        let binding = meta.binding();

        if binding == task.bindings.len() {
            // This is a post-combine checkpoint state update. Each is a merge-
            // patch document serialized as compact single-line JSON,
            // so frame each directly into the wire-format stream. Unlike
            // `encode_connector_state` (which copies a connector's raw bytes and
            // must scrub embedded tabs), `serde_json` compact serialization can't
            // emit a raw `\t` — in-string tabs are escaped — so the `\t` patch
            // delimiter is unambiguous here without sanitizing.
            connector_patches.push(if connector_patches.is_empty() {
                b'['
            } else {
                b','
            });
            serde_json::to_writer(
                &mut connector_patches,
                &doc::SerPolicy::noop().on_owned(&doc),
            )
            .expect("connector state serialization cannot fail");
            connector_patches.push(b'\t');
            continue;
        }

        let slot = task.bindings[binding].inference_slot as usize;

        if shapes[slot].widen_owned(&doc) {
            let limit = complexity_limit(&shapes[slot]);
            doc::shape::limits::enforce_shape_complexity_limit(
                &mut shapes[slot],
                limit,
                doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
            );
            updated_inferences.insert(slot);
        }

        let bytes_written = publisher
            .publish_doc(binding, doc, &task.bindings[binding].document_uuid_ptr)
            .await
            .context("publishing captured document")?;

        let drained = drained.entry(binding as u32).or_default();
        drained.docs_total += 1;
        drained.bytes_total += bytes_written as u64;

        // This loop is CPU-heavy. Yield to the runtime for cooperative liveness.
        if count % 100 == 0 {
            tokio::task::yield_now().await;
        }
        count += 1;
    }

    if !connector_patches.is_empty() {
        connector_patches.push(b']');
    }

    for slot in updated_inferences.iter() {
        // `to_schema` emits the shape's annotations, including the
        // `x-complexity-limit` set by `apply_sourced_schemas` or the
        // per-session default seeded by `Task::shapes_by_slot`.
        let schema = doc::shape::schema::to_schema(shapes[*slot].clone());
        logger.event(crate::LogEvent::InferredSchema {
            collection_name: &task.inference_slots[*slot].collection_name,
            // Inference is collection-scoped, as it already is for derivations.
            // The ops rollup keys on `collection_name` and never read `binding`.
            binding: None,
            schema: &schema,
        });
        metrics.inferred_schema_updates.increment(1);
    }

    Ok(Output {
        accumulator: crate::Accumulator::from_drainer(drainer, parser)?,
        drained: fsm::DrainedCapture {
            connector_patches: Bytes::from(connector_patches),
            bindings: drained,
        },
        publisher,
        shapes,
    })
}

/// Fold this transaction's connector-sourced shapes into long-lived
/// per-collection inference: each is intersected with the collection's
/// write-schema shape, then unioned into the running inferred shape. A sourced
/// collection is also stamped with an elevated complexity limit, recorded in the
/// shape's annotations.
///
/// Sourced schemas arrive keyed by binding and are mapped to inference slots on
/// the way in, so several bindings' sourced schemas union into one collection
/// shape.
fn apply_sourced_schemas(
    shapes: &mut [doc::Shape],
    task: &Task,
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    updated_inferences: &mut BTreeSet<usize>,
) -> anyhow::Result<()> {
    for (binding, sourced_shape) in sourced_schemas {
        let slot = task
            .bindings
            .get(binding as usize)
            .with_context(|| format!("invalid sourced schema binding {binding}"))?
            .inference_slot as usize;

        // By construction, we cannot capture documents which don't adhere to
        // the write schema. Intersect it to avoid generating incompatible
        // inference updates.
        let mut sourced_shape = doc::Shape::intersect(
            sourced_shape,
            task.inference_slots[slot].write_shape.clone(),
        );

        // Shape::union intersects annotations and retains only those having equal key/values.
        sourced_shape.annotations.insert(
            crate::X_GENERATION_ID.to_string(),
            shapes[slot].annotations[crate::X_GENERATION_ID].clone(),
        );

        shapes[slot] = doc::Shape::union(
            std::mem::replace(&mut shapes[slot], doc::Shape::nothing()),
            sourced_shape,
        );

        // Presence of a sourced schema ratchets up the complexity limit for
        // inferences of this collection. It then rides with the shape: surviving
        // widening, emitted into the logged schema, and read back by
        // `complexity_limit`.
        shapes[slot].annotations.insert(
            doc::shape::X_COMPLEXITY_LIMIT.to_string(),
            serde_json::json!(SOURCED_SCHEMA_COMPLEXITY_LIMIT),
        );
        updated_inferences.insert(slot);
    }
    Ok(())
}

/// The schema-complexity limit recorded in a shape's `x-complexity-limit`
/// annotation, falling back to the inference default when it is unset.
fn complexity_limit(shape: &doc::Shape) -> usize {
    shape
        .annotations
        .get(doc::shape::X_COMPLEXITY_LIMIT)
        .and_then(serde_json::Value::as_u64)
        .map_or(doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT, |n| {
            n as usize
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::capture::task::{Binding, assign_inference_slots};

    /// A Logger which records the ops::Logs its events flatten into.
    #[derive(Clone, Default)]
    struct RecordingLogger(std::sync::Arc<std::sync::Mutex<Vec<ops::Log>>>);

    impl crate::Logger for RecordingLogger {
        fn log(&self, log: &ops::Log) {
            self.0.lock().unwrap().push(log.clone());
        }
    }

    fn mk_binding(collection_name: &str, state_key: &str) -> Binding {
        Binding {
            collection_name: collection_name.to_string(),
            collection_generation_id: models::Id::zero(),
            document_uuid_ptr: json::Pointer::empty(),
            fan_in: false,
            inference_slot: 0,
            key_extractors: Vec::new(),
            partition_template_name: collection_name.to_string(),
            state_key: state_key.to_string(),
            write_schema_json: Bytes::from_static(br#"{"type":"object"}"#),
        }
    }

    /// Two bindings of one collection widen a single shared shape, and the
    /// collection is logged exactly once -- without a `binding` field, since
    /// inference is collection-scoped. A third binding on a second collection is
    /// the control: it has its own slot and its own event.
    #[tokio::test]
    async fn drain_infers_once_per_collection() {
        let mut bindings = vec![
            mk_binding("acmeCo/one", "stateA"),
            mk_binding("acmeCo/one", "stateB"),
            mk_binding("acmeCo/two", "stateC"),
        ];
        let inference_slots = assign_inference_slots(&mut bindings).unwrap();

        let task = std::sync::Arc::new(Task {
            bindings,
            inference_slots,
            close_policy: crate::leader::close_policy::Policy::new(
                std::time::Duration::ZERO,
                std::time::Duration::MAX,
            ),
            explicit_acknowledgements: false,
            max_transactions: 0,
            redact_salt: Bytes::new(),
            restart: proto_gazette::uuid::Clock::zero(),
            sequence_bytes_limit: 1 << 20,
            shard_ref: ops::ShardRef::default(),
        });
        assert_eq!(task.inference_slots.len(), 2);

        // Each binding of `acmeCo/one` contributes a distinct property, so the
        // shared shape must carry both if -- and only if -- they widen one slot.
        let mut accumulator = crate::Accumulator::new(task.combine_spec().unwrap()).unwrap();
        for (binding, doc_json) in [
            (0u16, br#"{"from_a":1}"#.as_slice()),
            (1, br#"{"from_b":2}"#.as_slice()),
            (2, br#"{"from_c":3}"#.as_slice()),
        ] {
            let (memtable, _alloc, doc) = accumulator.parse_json_doc(doc_json).unwrap();
            memtable.add(binding, doc, false).unwrap();
        }
        let (drainer, parser) = accumulator.into_drainer().unwrap();

        let logger = RecordingLogger::default();
        let output = drain_and_publish(
            drainer,
            parser,
            crate::publish::NoopPublisher,
            task.clone(),
            BTreeMap::new(),
            task.shapes_by_slot(Default::default()),
            super::super::Metrics::new("test/shard"),
            logger.clone(),
        )
        .await
        .unwrap();

        assert_eq!(output.shapes.len(), 2); // One shape per collection, not per binding.
        assert_eq!(
            output.shapes[0]
                .object
                .properties
                .iter()
                .map(|property| &*property.name)
                .collect::<Vec<_>>(),
            ["from_a", "from_b"],
            "both bindings of acmeCo/one widened its single shape",
        );

        // One event per collection, each naming the collection and no binding.
        let logs = logger.0.lock().unwrap();
        let field = |log: &ops::Log, field: &str| {
            log.fields_json_map
                .get(field)
                .map(|value| String::from_utf8_lossy(value).into_owned())
        };
        assert_eq!(
            logs.len(),
            2,
            "one inference event per collection: {logs:?}"
        );

        for (log, collection) in logs.iter().zip(["acmeCo/one", "acmeCo/two"]) {
            assert_eq!(log.message, "inferred schema updated");
            assert_eq!(
                field(log, "collection_name"),
                Some(format!("\"{collection}\"")),
            );
            assert_eq!(field(log, "binding"), None);
        }
    }
}
