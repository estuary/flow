use super::{ControlSignal, Task, Transaction};
use crate::{Accumulator, rocksdb::RocksDB, verify};
use anyhow::Context;
use bytes::BufMut;
use doc::shape::X_COMPLEXITY_LIMIT;
use futures::TryStreamExt;
use prost::Message;
use proto_flow::capture::{Request, Response, request, response};
use proto_flow::flow;
use proto_flow::runtime::{
    CaptureRequestExt, capture_request_ext,
    capture_response_ext::{self, PollResult},
};
use std::collections::{BTreeMap, HashSet};

// Does the connector have a meaningful write schema drawn from the source system plus SourcedSchema?
// If so, want to give it as much leeway as possible to infer the schema.
// Otherwise, use a lower complexity limit to avoid generating overly complex schemas.
// We may want to tune these limits further in the future, but this is a minimal starting point
// that leaves the door open for more complex heuristics in the future.
fn complexity_limit_for_binding(
    binding_index: usize,
    bindings_with_sourced_schema: &HashSet<usize>,
) -> usize {
    if bindings_with_sourced_schema.contains(&binding_index) {
        10_000
    } else {
        doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT
    }
}

pub async fn recv_client_unary(
    db: &RocksDB,
    request: &mut Request,
    wb: &mut rocksdb::WriteBatch,
) -> anyhow::Result<()> {
    if let Some(apply) = &mut request.apply {
        let last_spec = db.load_last_applied::<flow::CaptureSpec>().await?;

        if let Some(last_spec) = &last_spec {
            apply.last_version =
                crate::parse_shard_labeling(last_spec.shard_template.as_ref())?.build;
        }

        if last_spec != apply.capture {
            wb.put(
                RocksDB::LAST_APPLIED,
                apply
                    .capture
                    .as_ref()
                    .map(|m| m.encode_to_vec())
                    .unwrap_or_default(),
            );

            tracing::info!(
                last_version = apply.last_version,
                next_version = apply.version,
                "applying updated task specification",
            );
        } else {
            tracing::debug!(
                version = apply.version,
                "applying unchanged task specification",
            );
        }

        apply.last_capture = last_spec;
    }

    Ok(())
}

pub fn recv_connector_unary(request: Request, response: Response) -> anyhow::Result<Response> {
    if request.spec.is_some() && response.spec.is_some() {
        Ok(response)
    } else if request.spec.is_some() {
        verify("connector", "Spec").fail(response)
    } else if request.discover.is_some() && response.discovered.is_some() {
        Ok(response)
    } else if request.discover.is_some() {
        verify("connector", "Discovered").fail(response)
    } else if request.validate.is_some() && response.validated.is_some() {
        Ok(response)
    } else if request.validate.is_some() {
        verify("connector", "Validated").fail(response)
    } else if let (Some(apply), Some(applied)) = (&request.apply, &response.applied) {
        // Action descriptions can sometimes be _very_ long and overflow the maximum ops log line.
        let action = crate::truncate_chars(&applied.action_description, 1 << 18);

        if !action.is_empty() {
            tracing::info!(
                action,
                last_version = apply.last_version,
                version = apply.version,
                "capture was applied"
            );
        }
        Ok(response)
    } else if request.apply.is_some() {
        verify("connector", "Applied").fail(response)
    } else {
        verify("client", "unary request").fail(request)
    }
}

pub async fn recv_client_open(open: &mut Request, db: &RocksDB) -> anyhow::Result<()> {
    let Some(open) = open.open.as_mut() else {
        return verify("client", "Open").fail(open);
    };
    let Some(capture) = open.capture.as_mut() else {
        return verify("client", "Open.Capture").fail(open);
    };

    open.state_json = db
        .load_connector_state(
            serde_json::from_slice::<models::RawValue>(&open.state_json)
                .context("failed to parse initial open connector state")?,
        )
        .await?
        .into();

    // TODO(johnny): Switch to erroring if `state_key` is not already populated.
    for binding in capture.bindings.iter_mut() {
        binding.state_key = assemble::encode_state_key(&binding.resource_path, binding.backfill);
    }

    Ok(())
}

pub async fn recv_connector_opened(
    db: &RocksDB,
    open: Request,
    opened: Option<Response>,
    shapes_by_key: &mut BTreeMap<String, doc::Shape>,
) -> anyhow::Result<(
    Task,
    Task,
    Vec<doc::Shape>,
    Accumulator,
    Accumulator,
    Response,
    BTreeMap<String, u64>,
)> {
    let mut opened = verify("connecter", "Opened").not_eof(opened)?;

    let task = Task::new(&open, &opened)?;
    // Inferred document shapes, indexed by binding offset.
    let shapes = task.binding_shapes_by_index(std::mem::take(shapes_by_key));

    // Create a pair of accumulators. While one is draining, the other is accumulating.
    let a1 = Accumulator::new(task.combine_spec()?)?;
    let a2 = Accumulator::new(task.combine_spec()?)?;

    let checkpoint = db.load_checkpoint().await?;
    let active_backfills = db.load_backfill_state().await?;

    // Surface the durable set of active backfills so the Go app can reapply
    // `truncated-at` journal labels on startup. This recovers from any crash
    // window between a committed RocksDB WriteBatch and a successful broker
    // ApplyRequest; the Go app's label-application path is idempotent
    // (it compares current vs. expected before issuing the broker call).
    //
    // The map is keyed by stable `state_key`; stale entries (whose binding
    // has been removed from the task spec) are silently skipped on the way
    // to the wire — they remain on disk in case the binding reappears.
    let active = active_backfills_to_wire(&task, &active_backfills);
    let backfill_state = if active.is_empty() {
        None
    } else {
        Some(capture_response_ext::BackfillState {
            active_backfills: active,
        })
    };

    opened.set_internal(|internal| {
        internal.opened = Some(capture_response_ext::Opened {
            runtime_checkpoint: Some(checkpoint),
            backfill_state,
        })
    });

    Ok((task.clone(), task, shapes, a1, a2, opened, active_backfills))
}

pub fn send_client_poll_result(
    buf: &mut bytes::BytesMut,
    task: &Task,
    txn: &Transaction,
) -> (bool, Response) {
    let poll_result = if txn.checkpoints != 0 {
        PollResult::Ready
    } else if txn.connector_eof && !task.restart.elapsed().is_zero() {
        PollResult::Restart
    } else if txn.connector_eof {
        PollResult::CoolOff
    } else {
        PollResult::NotReady
    };

    (
        poll_result == PollResult::Ready,
        Response {
            checkpoint: Some(response::Checkpoint { state: None }),
            ..Default::default()
        }
        .with_internal_buf(buf, |internal| {
            internal.checkpoint = Some(capture_response_ext::Checkpoint {
                stats: None,
                poll_result: poll_result as i32,
            });
        }),
    )
}

pub fn send_connector_acknowledge(last_checkpoints: &mut u32, task: &Task) -> Option<Request> {
    if *last_checkpoints != 0 && task.explicit_acknowledgements {
        let checkpoints = *last_checkpoints;
        *last_checkpoints = 0; // Reset.

        Some(Request {
            acknowledge: Some(request::Acknowledge { checkpoints }),
            ..Default::default()
        })
    } else {
        None
    }
}

pub fn send_client_captured_or_checkpoint(
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
    shapes: &mut [doc::Shape],
    task: &Task,
    txn: &mut Transaction,
    wb: &mut rocksdb::WriteBatch,
    bindings_with_sourced_schema: &HashSet<usize>,
) -> Response {
    let doc::combine::DrainedDoc { meta, root } = drained;

    let index = meta.binding();

    if index == task.bindings.len() {
        // This is a merged checkpoint state update.
        serde_json::to_writer(buf.writer(), &doc::SerPolicy::noop().on_owned(&root))
            .expect("checkpoint serialization cannot fail");
        let updated_json = buf.split().freeze();

        tracing::debug!(
            state=?updated_json,
            "persisting updated connector state",
        );
        () = wb.merge(RocksDB::CONNECTOR_STATE_KEY, &updated_json);

        let state = flow::ConnectorState {
            merge_patch: true,
            updated_json,
        };
        return Response {
            checkpoint: Some(response::Checkpoint { state: Some(state) }),
            ..Default::default()
        };
    }

    let binding = &task.bindings[index];
    doc::Extractor::extract_all_owned(&root, &binding.key_extractors, buf);
    let key_packed = buf.split().freeze();
    doc::Extractor::extract_all_owned(&root, &binding.partition_extractors, buf);
    let partitions_packed = buf.split().freeze();

    serde_json::to_writer(buf.writer(), &binding.ser_policy.on_owned(&root))
        .expect("document serialization cannot fail");
    let doc_json = buf.split().freeze();

    let stats = &mut txn.stats.entry(index as u32).or_default().1;
    stats.docs_total += 1;
    stats.bytes_total += doc_json.len() as u64;

    if shapes[index].widen_owned(&root) {
        let complexity_limit = complexity_limit_for_binding(index, bindings_with_sourced_schema);

        doc::shape::limits::enforce_shape_complexity_limit(
            &mut shapes[index],
            complexity_limit,
            doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
        );
        txn.updated_inferences.insert(index);
    }

    Response {
        captured: Some(response::Captured {
            binding: index as u32,
            doc_json,
        }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.captured = Some(capture_response_ext::Captured {
            key_packed,
            partitions_packed,
            uuid_flags: 0,
            report_uuid_clock: false,
        });
    })
}

/// Synthesize a control document response for a backfill begin or complete signal.
///
/// The resulting Response looks like a regular Captured response to the Go
/// side, but the internal extension carries `uuid_flags` = `Flag_CONTROL`
/// (0x4) so the Go publisher uses the correct UUID flags. Control documents
/// always have `OUTSIDE_TXN` transaction semantics (the low two flag bits are
/// zero) and are immediately committed — they never participate in a
/// `CONTINUE_TXN` / `ACK_TXN` span.
pub fn send_client_control_doc(
    buf: &mut bytes::BytesMut,
    binding_index: u32,
    task: &Task,
    is_begin: bool,
    truncated_at: Option<&str>,
) -> Response {
    assert!(
        (binding_index as usize) < task.bindings.len(),
        "invalid control doc binding {binding_index}"
    );

    // Build a minimal JSON body. The UUID placeholder will be replaced by the
    // Go publisher with a real UUID that encodes the CONTROL flag.
    let body = if is_begin {
        serde_json::json!({
            "_meta": {
                "uuid": crate::UUID_PLACEHOLDER,
                "backfillBegin": true,
                "keyBegin": "00000000",
                "keyEnd": "ffffffff"
            }
        })
    } else {
        serde_json::json!({
            "_meta": {
                "uuid": crate::UUID_PLACEHOLDER,
                "backfillComplete": true,
                "truncatedAt": truncated_at.unwrap_or(""),
                "keyBegin": "00000000",
                "keyEnd": "ffffffff"
            }
        })
    };

    serde_json::to_writer(buf.writer(), &body).expect("control doc serialization cannot fail");
    let doc_json = buf.split().freeze();

    // Use empty key/partitions — the Go mapper will route to a default partition.
    // Control docs are written to all journals of the binding's collection, but
    // for single-shard captures a default partition is sufficient.
    let key_packed = bytes::Bytes::new();
    let partitions_packed = bytes::Bytes::new();

    Response {
        captured: Some(response::Captured {
            binding: binding_index,
            doc_json,
        }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.captured = Some(capture_response_ext::Captured {
            key_packed,
            partitions_packed,
            // `Flag_CONTROL` alone; transaction bits stay zero (OUTSIDE_TXN).
            uuid_flags: proto_gazette::message_flags::CONTROL as u32,
            // Only `BackfillBegin` needs the Go publisher to report its
            // assigned UUID clock back to us: that clock is the authoritative
            // `truncated_at`. For `BackfillComplete` we already know the
            // clock (from the persisted begin state), so no report is needed.
            report_uuid_clock: is_begin,
        });
    })
}

pub fn send_client_final_checkpoint(
    buf: &mut bytes::BytesMut,
    task: &Task,
    txn: &Transaction,
) -> Response {
    let mut capture = BTreeMap::<String, ops::stats::CaptureBinding>::new();

    for (index, binding_stats) in txn.stats.iter() {
        let index = *index as usize;
        let entry = capture
            .entry(task.bindings[index].collection_name.clone())
            .or_default();

        ops::merge_docs_and_bytes(&binding_stats.0, &mut entry.right);
        ops::merge_docs_and_bytes(&binding_stats.1, &mut entry.out);
    }

    let stats = ops::Stats {
        capture,
        derive: None,
        interval: None,
        materialize: Default::default(),
        meta: Some(ops::Meta {
            uuid: crate::UUID_PLACEHOLDER.to_string(),
        }),
        open_seconds_total: txn.started_at.elapsed().unwrap().as_secs_f64(),
        shard: Some(task.shard_ref.clone()),
        timestamp: Some(proto_flow::as_timestamp(txn.started_at)),
        txn_count: 1,
    };

    Response {
        checkpoint: Some(response::Checkpoint { state: None }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.checkpoint = Some(capture_response_ext::Checkpoint {
            stats: Some(stats),
            poll_result: PollResult::Invalid as i32,
        })
    })
}

pub async fn recv_client_start_commit(
    db: &RocksDB,
    request: Option<Request>,
    shapes: &[doc::Shape],
    task: &Task,
    txn: &Transaction,
    mut wb: rocksdb::WriteBatch,
    bindings_with_sourced_schema: &HashSet<usize>,
    active_backfills: &mut BTreeMap<String, u64>,
) -> anyhow::Result<()> {
    let verify = verify("client", "StartCommit with runtime_checkpoint");
    let request = verify.not_eof(request)?;

    let CaptureRequestExt {
        start_commit:
            Some(capture_request_ext::StartCommit {
                runtime_checkpoint: Some(runtime_checkpoint),
                published_control_clocks,
            }),
        ..
    } = request.get_internal()?
    else {
        return verify.fail(request);
    };

    // Add the runtime checkpoint to our WriteBatch.
    tracing::debug!(
        checkpoint=?ops::DebugJson(&runtime_checkpoint),
        "persisting StartCommit.runtime_checkpoint",
    );
    wb.put(RocksDB::CHECKPOINT_KEY, &runtime_checkpoint.encode_to_vec());

    // Stage backfill state transitions using the authoritative UUID clocks
    // reported by the Go publisher. Wire-level identifiers are binding
    // indices (ephemeral within a task term); `active_backfills` is keyed by
    // stable `state_key` (survives task-spec changes), so translate on the
    // boundary. Apply begin clocks first, then validate this transaction's
    // control signal against the resulting state.
    //
    // A `BackfillComplete` signal has already removed its binding's entry
    // from the in-memory `active_backfills` during post-drain emission in
    // `serve_session`; we only need to persist the resulting map here.
    for clock in &published_control_clocks {
        let idx = clock.binding as usize;
        anyhow::ensure!(
            idx < task.bindings.len(),
            "Go reported a UUID clock for invalid binding {idx}",
        );
        active_backfills.insert(task.bindings[idx].state_key.clone(), clock.clock);
    }
    if let Some(ControlSignal::BackfillBegin(b)) = txn.control {
        // A BackfillBegin emitted in this transaction must have had its UUID
        // clock reported by the Go publisher. A missing clock would leave
        // `active_backfills` without an authoritative value, so surface it
        // as a protocol error rather than silently carrying a stale or
        // placeholder entry.
        let state_key = &task.bindings[b as usize].state_key;
        anyhow::ensure!(
            active_backfills.contains_key(state_key),
            "Go publisher did not report a UUID clock for BackfillBegin on \
             binding {b}; cannot persist authoritative truncated_at",
        );
    }

    let backfill_json = serde_json::to_vec(active_backfills).expect("backfill state serialization");
    wb.put(RocksDB::BACKFILL_STATE_KEY, &backfill_json);

    // We're about to write out our write batch which, when written to the
    // recovery log, irrevocably commits our transaction. Before doing so,
    // produce structured logs of all inferred schemas that have changed
    // in this transaction.
    for binding in txn.updated_inferences.iter() {
        let mut serialized = doc::shape::schema::to_schema(shapes[*binding].clone());

        let complexity_limit = complexity_limit_for_binding(*binding, bindings_with_sourced_schema);

        serialized.as_object_mut().unwrap().insert(
            X_COMPLEXITY_LIMIT.to_string(),
            serde_json::Value::Number(serde_json::Number::from(complexity_limit)),
        );

        tracing::info!(
            schema = ?ops::DebugJson(serialized),
            collection_name = %task.bindings[*binding].collection_name,
            binding = binding,
            "inferred schema updated"
        );
    }

    // Atomically write our commit batch.
    db.write_opt(wb, Default::default())
        .await
        .context("failed to write atomic RocksDB commit")?;

    Ok(())
}

pub fn send_client_started_commit(
    buf: &mut bytes::BytesMut,
    task: &Task,
    active_backfills: &BTreeMap<String, u64>,
) -> Response {
    // After committing the WriteBatch, surface the up-to-date active backfill
    // state so the Go app can apply `truncated-at` journal labels via the
    // broker ApplyRequest. `active_backfills` is keyed by stable `state_key`;
    // we translate to current binding indices for the wire. Stale entries
    // (whose `state_key` is not in the current task spec) are silently
    // skipped — the binding has been removed, so there are no journals to
    // label. Passing `None` when empty avoids churn when no backfills are
    // active; Go's label-application is idempotent.
    let active = active_backfills_to_wire(task, active_backfills);
    let backfill_state = if active.is_empty() {
        None
    } else {
        Some(capture_response_ext::BackfillState {
            active_backfills: active,
        })
    };

    Response {
        checkpoint: Some(response::Checkpoint { state: None }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.backfill_state = backfill_state;
    })
}

/// Translate a state_key-keyed active-backfill map into the wire form, which
/// uses binding indices against the current task spec. Entries whose
/// state_key is not present in the task are dropped; those are stale
/// references to bindings that have since been removed.
fn active_backfills_to_wire(
    task: &Task,
    active_backfills: &BTreeMap<String, u64>,
) -> Vec<capture_response_ext::backfill_state::ActiveBackfill> {
    task.bindings
        .iter()
        .enumerate()
        .filter_map(|(idx, b)| {
            active_backfills.get(&b.state_key).map(|&clock| {
                capture_response_ext::backfill_state::ActiveBackfill {
                    binding: idx as u32,
                    truncated_at_clock: clock,
                }
            })
        })
        .collect()
}

pub fn recv_connector_captured(
    accumulator: &mut Accumulator,
    captured: response::Captured,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    let response::Captured {
        binding: binding_index,
        doc_json,
    } = captured;

    let (memtable, alloc, mut doc) =
        accumulator
            .doc_bytes_to_heap_node(&doc_json)
            .with_context(|| {
                format!(
                    "couldn't parse captured document as JSON (target {})",
                    task.bindings[binding_index as usize].collection_name
                )
            })?;

    let uuid_ptr = &task
        .bindings
        .get(binding_index as usize)
        .with_context(|| format!("invalid captured binding {binding_index}"))?
        .document_uuid_ptr;

    if !uuid_ptr.0.is_empty() {
        let Ok(_) = doc.try_set(
            uuid_ptr,
            doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc)),
            alloc,
        ) else {
            anyhow::bail!("unable to create document UUID placeholder");
        };
    }
    memtable.add(binding_index as u16, doc, false)?;

    let stats = txn.stats.entry(binding_index).or_default();
    stats.0.docs_total += 1;
    stats.0.bytes_total += doc_json.len() as u64;

    txn.captured_bytes += doc_json.len();
    Ok(())
}

pub fn recv_connector_sourced_schema(
    sourced: response::SourcedSchema,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    let response::SourcedSchema {
        binding,
        schema_json,
    } = sourced;

    tracing::debug!(schema=?schema_json, binding, "sourced schema");

    let built_schema = doc::validation::build_bundle(&schema_json).with_context(|| {
        format!(
            "couldn't parse sourced schema as JSON Schema (target {})",
            task.bindings[binding as usize].collection_name
        )
    })?;
    let validator = doc::Validator::new(built_schema).with_context(|| {
        format!(
            "couldn't build a sourced schema validator (target {})",
            task.bindings[binding as usize].collection_name
        )
    })?;
    let sourced_shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    let errors = sourced_shape.inspect_closed();
    if !errors.is_empty() {
        anyhow::bail!(
            "connector implementation error: binding {binding} SourcedSchema has errors: {errors:?}"
        );
    }

    // Track this SourcedSchema by its binding, union-ing with another SourcedSchema if present.
    let entry = txn
        .sourced_schemas
        .entry(binding as usize)
        .or_insert(doc::Shape::nothing());

    *entry = doc::Shape::union(
        std::mem::replace(entry, doc::Shape::nothing()),
        sourced_shape,
    );

    Ok(())
}

pub async fn recv_connector_control_message(
    connector_rx: &mut (impl super::ResponseStream + Unpin),
    response: &Response,
) -> anyhow::Result<(ControlSignal, response::Checkpoint)> {
    let signal = if let Some(begin) = &response.backfill_begin {
        ControlSignal::BackfillBegin(begin.binding)
    } else if let Some(complete) = &response.backfill_complete {
        ControlSignal::BackfillComplete(complete.binding)
    } else {
        anyhow::bail!("expected BackfillBegin or BackfillComplete in control message response");
    };

    let verify = verify("connector", "Checkpoint after control message");
    let checkpoint_response = verify.not_eof(connector_rx.try_next().await?)?;
    let Some(checkpoint) = checkpoint_response.checkpoint else {
        return verify.fail(checkpoint_response);
    };

    Ok((signal, checkpoint))
}

pub fn apply_sourced_schemas(
    shapes: &mut [doc::Shape],
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    let Transaction {
        sourced_schemas,
        updated_inferences,
        ..
    } = txn;

    for (binding, sourced_shape) in std::mem::take(sourced_schemas) {
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
        updated_inferences.insert(binding);
    }

    Ok(())
}

pub fn recv_connector_checkpoint(
    accumulator: &mut Accumulator,
    response: Response,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    let verify = verify("connector", "Captured or Checkpoint with state");
    let Some(response::Checkpoint { state: Some(state) }) = response.checkpoint else {
        return verify.fail(response);
    };

    combine_connector_state(accumulator, task, &state)?;
    txn.checkpoints += 1;
    Ok(())
}

pub fn apply_prior_control(
    accumulator: &mut Accumulator,
    task: &Task,
    txn: &mut Transaction,
    signal: ControlSignal,
    checkpoint: response::Checkpoint,
) -> anyhow::Result<()> {
    let verify = verify(
        "connector",
        "deferred control-bearing Checkpoint with state",
    );
    let Some(state) = checkpoint.state.as_ref() else {
        return verify.fail(Response {
            checkpoint: Some(checkpoint),
            ..Default::default()
        });
    };
    txn.checkpoints = 1;
    combine_connector_state(accumulator, task, state)?;
    txn.control = Some(signal);
    Ok(())
}

pub fn combine_connector_state(
    accumulator: &mut Accumulator,
    task: &Task,
    state: &flow::ConnectorState,
) -> anyhow::Result<()> {
    let (memtable, _alloc, doc) = accumulator
        .doc_bytes_to_heap_node(&state.updated_json)
        .context("couldn't parse connector state as JSON")?;

    if !state.merge_patch {
        memtable.add(task.bindings.len() as u16, doc::HeapNode::Null, false)?;
    }
    memtable.add(task.bindings.len() as u16, doc, false)?;

    Ok(())
}
