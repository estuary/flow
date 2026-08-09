//! Capture combiner drain.
//!
//! [`drain_and_publish`] runs as the actor's parked `drain_fut`: it consumes a
//! rotated combiner, publishes captured documents as `CONTINUE_TXN` journal
//! appends, folds connector-reported schemas into per-target inference, and
//! assembles the [`fsm::DrainedCapture`] the TailFSM needs to build stats and
//! the committing Persist.
//!
//! Unlike the materialize shard drain — a synchronous step machine interleaved
//! with connector IO — a capture drain is a single self-contained async pass:
//! it owns the publisher for its duration and hands it back via [`Output`].

use crate::leader::capture::{Task, fsm};
use anyhow::Context;
use bytes::Bytes;
use std::collections::BTreeMap;

/// Schema-complexity limit for a collection the connector described with a
/// SourcedSchema. Such a collection has a meaningful source-derived schema, so
/// inference is trusted with far more leeway than a purely-inferred one
/// (which uses [`doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT`]). The
/// limit rides in the shape's annotations and so persists across sessions —
/// see `Task::shapes_by_target`.
const SOURCED_SCHEMA_COMPLEXITY_LIMIT: usize = 10_000;

/// Resources and results handed back to the actor when a drain completes.
pub(super) struct Output<P: crate::Publisher> {
    /// The drained combiner, recycled as the next transaction's `idle_accumulator`.
    pub(super) accumulator: crate::Accumulator,
    /// Per-transaction connector patches and stats, staged for the TailFSM.
    pub(super) drained: fsm::DrainedCapture,
    /// The publisher, borrowed for the drain's journal appends.
    pub(super) publisher: P,
    /// Per-target inferred write-shapes, carried across sessions of the shard.
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

    // Targets whose inference updated this transaction — by a sourced schema or
    // by widening an inferred shape — are logged once the drain completes, each
    // naming the last binding which updated it.
    let mut updated_inferences = BTreeMap::<usize, u32>::new();

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

        let target = task.bindings[binding].target as usize;

        if shapes[target].widen_owned(&doc) {
            let limit = complexity_limit(&shapes[target]);
            doc::shape::limits::enforce_shape_complexity_limit(
                &mut shapes[target],
                limit,
                doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
            );
            updated_inferences.insert(target, binding as u32);
        }

        let bytes_written = publisher
            .publish_doc(binding, doc, &task.targets[target].document_uuid_ptr)
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

    for (target, binding) in updated_inferences.iter() {
        // `to_schema` emits the shape's annotations, including the
        // `x-complexity-limit` set by `apply_sourced_schemas` or the
        // per-session default seeded by `Task::shapes_by_target`.
        let schema = doc::shape::schema::to_schema(shapes[*target].clone());
        logger.event(crate::LogEvent::InferredSchema {
            collection_name: &task.targets[*target].collection_name,
            binding: Some(*binding as usize), // Diagnostic.
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
/// per-target inference: each is intersected with the target collection's
/// write-schema shape, then unioned into the running inferred shape. The target
/// is also stamped with an elevated complexity limit, recorded in the shape's
/// annotations.
///
/// Sourced schemas arrive keyed by binding and are mapped to targets on the
/// way in, so several bindings' sourced schemas union into one collection
/// shape.
fn apply_sourced_schemas(
    shapes: &mut [doc::Shape],
    task: &Task,
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    updated_inferences: &mut BTreeMap<usize, u32>,
) -> anyhow::Result<()> {
    for (binding, sourced_shape) in sourced_schemas {
        let target = task
            .bindings
            .get(binding as usize)
            .with_context(|| format!("invalid sourced schema binding {binding}"))?
            .target as usize;

        // By construction, we cannot capture documents which don't adhere to
        // the write schema. Intersect it to avoid generating incompatible
        // inference updates.
        let mut sourced_shape =
            doc::Shape::intersect(sourced_shape, task.targets[target].write_shape.clone());

        // Shape::union intersects annotations and retains only those having equal key/values.
        sourced_shape.annotations.insert(
            crate::X_GENERATION_ID.to_string(),
            shapes[target].annotations[crate::X_GENERATION_ID].clone(),
        );

        shapes[target] = doc::Shape::union(
            std::mem::replace(&mut shapes[target], doc::Shape::nothing()),
            sourced_shape,
        );

        // Presence of a sourced schema ratchets up the complexity limit for
        // inferences of this target collection. It then rides with the shape:
        // surviving widening, emitted into the logged schema, and read back by
        // `complexity_limit`.
        shapes[target].annotations.insert(
            doc::shape::X_COMPLEXITY_LIMIT.to_string(),
            serde_json::json!(SOURCED_SCHEMA_COMPLEXITY_LIMIT),
        );
        updated_inferences.insert(target, binding);
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
    use crate::leader::capture::task::fixture;
    use crate::logger::RecordingLogger;

    /// Two bindings of one collection widen a single shared shape, and the
    /// collection is logged exactly once, naming the last binding to update it.
    /// A third binding on a second collection is the control: it has its own
    /// target and its own event.
    #[tokio::test]
    async fn drain_infers_once_per_collection() {
        let task = std::sync::Arc::new(fixture::task(
            &[
                ("acmeCo/one", "stateA", ""),
                ("acmeCo/one", "stateB", ""),
                ("acmeCo/two", "stateC", ""),
            ],
            br#"{"type":"object"}"#,
            false,
        ));
        assert_eq!(task.targets.len(), 2);

        // Each binding of `acmeCo/one` contributes a distinct property, so the
        // shared shape must carry both if -- and only if -- they widen one shape.
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
            crate::publish::RecordingPublisher::default(),
            task.clone(),
            BTreeMap::new(),
            task.shapes_by_target(Default::default()),
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

        // One event per collection, naming the last binding which updated it:
        // binding 1 for `acmeCo/one` (bindings 0 and 1 share its target), and
        // binding 2 for `acmeCo/two`.
        let logs = logger.logs.lock().unwrap();
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

        for (log, (collection, binding)) in logs.iter().zip([("acmeCo/one", 1), ("acmeCo/two", 2)])
        {
            assert_eq!(log.message, "inferred schema updated");
            assert_eq!(
                field(log, "collection_name"),
                Some(format!("\"{collection}\"")),
            );
            assert_eq!(field(log, "binding"), Some(format!("{binding}")));
        }
    }
}
