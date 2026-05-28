//! Capture combiner drain.
//!
//! [`drain_and_publish`] runs as the actor's parked `drain_fut`: it consumes a
//! rotated combiner, publishes captured documents as `CONTINUE_TXN` journal
//! appends, folds connector-reported schemas into per-binding inference, and
//! assembles the [`fsm::DrainedCapture`] the TailFSM needs to build stats and
//! the committing Persist.
//!
//! Unlike the materialize shard drain — a synchronous step machine interleaved
//! with connector IO — a capture drain is a single self-contained async pass:
//! it owns the publisher for its duration and hands it back via [`Output`].

use crate::leader::capture::{Task, fsm};
use anyhow::Context;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// Schema-complexity limit for a binding the connector described with a
/// SourcedSchema. Such a binding has a meaningful source-derived schema, so
/// inference is trusted with far more leeway than a purely-inferred binding
/// (which uses [`doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT`]). The
/// limit rides in the shape's annotations and so persists across sessions —
/// see `Task::binding_shapes_by_index`.
const SOURCED_SCHEMA_COMPLEXITY_LIMIT: usize = 10_000;

/// Resources and results handed back to the actor when a drain completes.
pub(super) struct Output {
    /// The drained combiner, recycled as the next transaction's `idle_accumulator`.
    pub(super) accumulator: crate::Accumulator,
    /// Per-transaction connector patches and stats, staged for the TailFSM.
    pub(super) drained: fsm::DrainedCapture,
    /// The publisher, borrowed for the drain's journal appends.
    pub(super) publisher: crate::Publisher,
    /// Per-binding inferred write-shapes, carried across sessions of the shard.
    pub(super) shapes: Vec<doc::Shape>,
}

/// Drain a rotated combiner: apply sourced schemas to inference, publish each
/// captured document, and accumulate the connector-state patch stream.
pub(super) async fn drain_and_publish(
    mut drainer: doc::combine::Drainer,
    parser: simd_doc::Parser,
    mut publisher: crate::Publisher,
    task: std::sync::Arc<Task>,
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    mut shapes: Vec<doc::Shape>,
    metrics: super::Metrics,
) -> anyhow::Result<Output> {
    // Bindings updated this transaction — by a sourced schema or by widening
    // an inferred shape — are logged once the drain completes.
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
            // so frame each directly into the wire-format stream.
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

        if shapes[binding].widen_owned(&doc) {
            let limit = complexity_limit(&shapes[binding]);
            doc::shape::limits::enforce_shape_complexity_limit(
                &mut shapes[binding],
                limit,
                doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
            );
            updated_inferences.insert(binding);
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

    for binding in updated_inferences.iter() {
        // `to_schema` emits the shape's annotations, including the
        // `x-complexity-limit` set by `apply_sourced_schemas` or the
        // per-session default seeded by `Task::binding_shapes_by_index`.
        let serialized = doc::shape::schema::to_schema(shapes[*binding].clone());
        tracing::info!(
            schema = ?ops::DebugJson(serialized),
            collection_name = %task.bindings[*binding].collection_name,
            binding,
            "inferred schema updated"
        );
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

/// Fold this transaction's connector-sourced shapes into long-lived per-binding
/// inference: each is intersected with the binding's write-schema shape, then
/// unioned into the running inferred shape. A sourced binding is also stamped
/// with an elevated complexity limit, recorded in the shape's annotations.
fn apply_sourced_schemas(
    shapes: &mut [doc::Shape],
    task: &Task,
    sourced_schemas: BTreeMap<u32, doc::Shape>,
    updated_inferences: &mut BTreeSet<usize>,
) -> anyhow::Result<()> {
    for (binding, sourced_shape) in sourced_schemas {
        let binding = binding as usize;

        let write_shape = task
            .bindings
            .get(binding)
            .with_context(|| format!("invalid sourced schema binding {binding}"))?
            .write_shape
            .clone();

        // By construction, we cannot capture documents which don't adhere to
        // the write schema. Intersect it to avoid generating incompatible
        // inference updates.
        let mut sourced_shape = doc::Shape::intersect(sourced_shape, write_shape);

        // Shape::union intersects annotations and retains only those having equal key/values.
        sourced_shape.annotations.insert(
            crate::X_GENERATION_ID.to_string(),
            shapes[binding].annotations[crate::X_GENERATION_ID].clone(),
        );

        shapes[binding] = doc::Shape::union(
            std::mem::replace(&mut shapes[binding], doc::Shape::nothing()),
            sourced_shape,
        );

        // Presence of a sourced schema ratchets up the complexity limit for
        // inferences of this binding. It then rides with the shape: surviving widening,
        // emitted into the logged schema, and read back by `complexity_limit`.
        shapes[binding].annotations.insert(
            doc::shape::X_COMPLEXITY_LIMIT.to_string(),
            serde_json::json!(SOURCED_SCHEMA_COMPLEXITY_LIMIT),
        );
        updated_inferences.insert(binding);
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
