use super::{Binding, LoadKeySet, Task, Transaction};
use crate::rocksdb::{queue_connector_state_update, RocksDB};
use crate::{verify, Accumulator};
use anyhow::Context;
use bytes::{Buf, BufMut};
use prost::Message;
use proto_flow::flow;
use proto_flow::materialize::{request, response, Request, Response};
use proto_flow::runtime::materialize_response_ext;
use proto_gazette::consumer;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::AtomicBool;
use xxhash_rust::xxh3::xxh3_128;

pub async fn recv_client_unary(
    db: &RocksDB,
    request: &mut Request,
    wb: &mut rocksdb::WriteBatch,
) -> anyhow::Result<()> {
    if let Some(apply) = &mut request.apply {
        let last_spec = db.load_last_applied::<flow::MaterializationSpec>().await?;

        if let Some(last_spec) = &last_spec {
            apply.last_version =
                crate::parse_shard_labeling(last_spec.shard_template.as_ref())?.build;
        }

        if last_spec != apply.materialization {
            wb.put(
                RocksDB::LAST_APPLIED,
                apply
                    .materialization
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

        // TODO(johnny): load and attach Request.Apply.state_json.

        apply.last_materialization = last_spec;
    }

    Ok(())
}

pub fn recv_connector_unary(request: Request, response: Response) -> anyhow::Result<Response> {
    if request.spec.is_some() && response.spec.is_some() {
        Ok(response)
    } else if request.spec.is_some() {
        verify("connector", "Spec").fail(response)
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
                "materialization was applied"
            );
        }
        Ok(response)
    } else if request.apply.is_some() {
        verify("connector", "Applied").fail(response)
    } else {
        verify("client", "unary request").fail(request)
    }

    // TODO(johnny): extract and apply Response.Apply.state to WriteBatch.
}

pub async fn recv_client_open(open: &mut Request, db: &RocksDB) -> anyhow::Result<()> {
    let Some(open) = open.open.as_mut() else {
        return verify("client", "Open").fail(open);
    };
    let Some(materialization) = open.materialization.as_mut() else {
        return verify("client", "Open.Materialization").fail(open);
    };

    open.state_json = db
        .load_connector_state(
            serde_json::from_slice::<models::RawValue>(&open.state_json)
                .context("failed to parse initial open connector state")?,
        )
        .await?
        .into();

    // TODO(johnny): Switch to erroring if `state_key` is not already populated.
    for binding in materialization.bindings.iter_mut() {
        binding.state_key = assemble::encode_state_key(&binding.resource_path, binding.backfill);
    }

    Ok(())
}

pub async fn recv_connector_opened(
    db: &RocksDB,
    open: Request,
    opened: Option<Response>,
) -> anyhow::Result<(
    Task,
    Accumulator,
    consumer::Checkpoint,
    Response,
    Vec<(bytes::Bytes, bytes::Bytes)>,
    bool,
)> {
    let verify = verify("connector", "Opened");
    let mut opened = verify.not_eof(opened)?;
    let (runtime_checkpoint, disable_load_optimization) = match &mut opened {
        Response {
            opened:
                Some(response::Opened {
                    runtime_checkpoint,
                    disable_load_optimization,
                }),
            ..
        } => (runtime_checkpoint, *disable_load_optimization),
        _ => return verify.fail(opened),
    };

    let task = Task::new(&open)?;
    let accumulator = Accumulator::new(task.combine_spec()?)?;

    let mut checkpoint = db
        .load_checkpoint()
        .await
        .context("failed to load runtime checkpoint from RocksDB")?;

    if let Some(runtime_checkpoint) = runtime_checkpoint {
        checkpoint = runtime_checkpoint.clone();
        tracing::debug!(
            checkpoint=?ops::DebugJson(&checkpoint),
            "using connector-provided OpenedExt.runtime_checkpoint",
        );
    } else {
        *runtime_checkpoint = Some(checkpoint.clone());
        tracing::debug!(
            checkpoint=?ops::DebugJson(&checkpoint),
            "loaded and attached a persisted OpenedExt.runtime_checkpoint",
        );
    }

    // Collect all the active journal read suffixes of the checkpoint.
    let active_read_suffixes: HashSet<&str> = checkpoint
        .sources
        .iter()
        .filter_map(|(journal, _source)| journal.split(';').skip(1).next())
        .collect();

    // Fetch a persisted max-key value for each active binding.
    let max_keys: Vec<String> = task.bindings.iter().map(max_key_key).collect();
    let max_keys = db.multi_get_opt(max_keys, Default::default()).await;
    let max_keys: Vec<(bytes::Bytes, bytes::Bytes)> = task
        .bindings
        .iter()
        .zip(max_keys.into_iter())
        .map(|(binding, prev_max)| match prev_max {
            Ok(None) if active_read_suffixes.get(binding.journal_read_suffix.as_str()).is_some() => {
                tracing::debug!(state_key=%binding.state_key, "binding has no persisted max-key but is in the runtime checkpoint");
                Ok((vec![0xff].into(), bytes::Bytes::new())) // 0xff is tuple::ESCAPE, larger than any tuple opcode.
            }
            Ok(None) => {
                tracing::debug!(state_key=%binding.state_key, "binding has no persisted max-key and is not in runtime checkpoint");
                Ok((bytes::Bytes::new(), bytes::Bytes::new()))
            }
            Ok(Some(prev_max)) => {
                let unpacked: Vec<tuple::Element> = tuple::unpack(&prev_max).context("corrupted binding max-key")?;
                tracing::debug!(state_key=%binding.state_key, ?unpacked, "recovered persisted binding max-key");
                Ok((prev_max.into(), bytes::Bytes::new()))
            }
            Err(err) => Err(err.into()),
        })
        .collect::<anyhow::Result<_>>()?;

    Ok((
        task,
        accumulator,
        checkpoint,
        opened,
        max_keys,
        disable_load_optimization,
    ))
}

pub fn recv_client_load_or_flush(
    accumulator: &mut Accumulator,
    buf: &mut bytes::BytesMut,
    load_keys: &mut LoadKeySet,
    max_keys: &mut [(bytes::Bytes, bytes::Bytes)],
    request: Option<Request>,
    saw_acknowledged: &mut bool,
    saw_flush: &mut bool,
    task: &Task,
    txn: &mut Transaction,
    disable_load_optimization: bool,
) -> anyhow::Result<Option<Request>> {
    if !txn.started {
        txn.started = true;
        txn.started_at = std::time::SystemTime::now();
    }

    match request {
        Some(Request {
            load:
                Some(request::Load {
                    binding: binding_index,
                    key_json: doc_json,
                    key_packed: _,
                }),
            ..
        }) => {
            let binding = &task.bindings[binding_index as usize];

            let (memtable, _alloc, doc) = accumulator
                .doc_bytes_to_heap_node(&doc_json)
                .with_context(|| {
                    format!(
                        "couldn't parse source document as JSON (source {})",
                        binding.collection_name
                    )
                })?;

            // Encode the binding index and then the packed key as a single Bytes.
            buf.put_u32(binding_index);
            let mut key_packed = doc::Extractor::extract_all(&doc, &binding.key_extractors, buf);
            let key_hash: u128 = xxh3_128(&key_packed);
            key_packed.advance(4); // Advance past 4-byte binding index.

            // Accumulate metrics over reads for our transforms.
            let stats = &mut txn.stats.entry(binding_index).or_default();
            stats.1.docs_total += 1;
            stats.1.bytes_total += doc_json.len() as u64;

            if let Some((_, clock, _)) = binding.uuid_ptr.query(&doc).and_then(|node| match node {
                doc::HeapNode::String(uuid) => proto_gazette::uuid::parse_str(uuid.as_str()).ok(),
                _ => None,
            }) {
                stats.3 = clock;
            }

            memtable.add(binding_index, doc, false)?;

            let (ref prev_max, next_max) = &mut max_keys[binding_index as usize];

            // Is `key_packed` larger than the largest key previously stored
            // to the connector? If so, then it cannot possibly exist.
            // Note: we still track the max key even when optimization is disabled.
            if key_packed > *prev_max {
                if key_packed > *next_max {
                    // This is a new high water mark for the largest-stored key.
                    *next_max = key_packed.clone();
                }
                // Skip the load request unless optimization is disabled.
                if !disable_load_optimization {
                    return Ok(None);
                }
            }

            if binding.delta_updates {
                Ok(None) // Delta-update bindings don't load.
            } else if load_keys.contains(&key_hash) {
                Ok(None) // We already sent a Load request for this key.
            } else {
                load_keys.insert(key_hash);

                Ok(Some(Request {
                    load: Some(request::Load {
                        binding: binding_index,
                        key_packed,
                        key_json: bytes::Bytes::new(), // TODO
                    }),
                    ..Default::default()
                }))
            }
        }
        Some(Request {
            flush: Some(request::Flush {}),
            ..
        }) => {
            if !*saw_acknowledged {
                anyhow::bail!("client sent Flush before Acknowledged");
            }
            *saw_flush = true;

            // Drop the set of loaded keys, as they're no longer needed.
            _ = std::mem::take(load_keys);

            Ok(Some(Request {
                flush: Some(request::Flush {}),
                ..Default::default()
            }))
        }
        request => verify("client", "Load or Flush").fail(request),
    }
}

pub async fn recv_connector_acked_or_loaded_or_flushed(
    accumulator: &mut Accumulator,
    db: &RocksDB,
    response: Option<Response>,
    saw_acknowledged: &mut bool,
    saw_flush: &mut bool,
    saw_flushed: &mut bool,
    task: &Task,
    txn: &mut Transaction,
    wb: &mut rocksdb::WriteBatch,
) -> anyhow::Result<Option<Response>> {
    match response {
        Some(Response {
            loaded:
                Some(response::Loaded {
                    binding: binding_index,
                    doc_json,
                }),
            ..
        }) => {
            let binding = &task.bindings[binding_index as usize];

            let (memtable, _alloc, doc) = accumulator
                .doc_bytes_to_heap_node(&doc_json)
                .with_context(|| {
                    format!(
                        "couldn't parse loaded document as JSON (source {})",
                        binding.collection_name
                    )
                })?;

            memtable.add(binding_index, doc, true)?;

            // Accumulate metrics over reads for our transforms.
            let stats = &mut txn.stats.entry(binding_index).or_default();
            stats.0.docs_total += 1;
            stats.0.bytes_total += doc_json.len() as u64;

            Ok(None)
        }
        Some(Response {
            acknowledged: Some(response::Acknowledged { state }),
            ..
        }) => {
            if *saw_acknowledged {
                anyhow::bail!("connector sent duplicate Acknowledge");
            }
            *saw_acknowledged = true;

            if let Some(state) = state {
                let mut wb = rocksdb::WriteBatch::default();
                queue_connector_state_update(&state, &mut wb).context("invalid Acknowledged")?;
                db.write_opt(wb, rocksdb::WriteOptions::default()).await?;
            }

            Ok(Some(Response {
                acknowledged: Some(response::Acknowledged { state: None }),
                ..Default::default()
            }))
        }
        Some(Response {
            flushed: Some(response::Flushed { state }),
            ..
        }) => {
            if !*saw_acknowledged {
                anyhow::bail!("connector sent Flushed before Acknowledged");
            }
            if !*saw_flush {
                anyhow::bail!("connector sent Flushed before receiving Flush");
            }
            *saw_flushed = true;

            if let Some(state) = state {
                // Add to WriteBatch which is synchronously written with max-keys updates.
                queue_connector_state_update(&state, wb).context("invalid Flushed")?;
            }

            Ok(None)
        }
        request => verify("connector", "Loaded, Acknowledged, or Flushed").fail(request),
    }
}

pub fn send_connector_store(
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
    task: &Task,
    txn: &mut Transaction,
) -> Request {
    let doc::combine::DrainedDoc { meta, root } = drained;

    let binding_index = meta.binding();
    let binding = &task.bindings[binding_index];

    // A note on the order of operations:
    // The value extractors may contain a special truncation indicator extractor,
    // which will write the value of this variable. It's important that we
    // extract the values last, so that the indicator can account for truncations
    // in both the keys and the flow document.
    let truncation_indicator = AtomicBool::new(false);
    let key_packed = doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.key_extractors,
        buf,
        &truncation_indicator,
    );

    // Serialize the root document regardless of whether delta-updates is
    // enabled. We do this so that we can count the number of bytes on the
    // stats. If delta updates is enabled, we'll clear out this string before
    // sending the response.
    serde_json::to_writer(
        buf.writer(),
        &binding
            .ser_policy
            .on_owned_with_truncation_indicator(&root, &truncation_indicator),
    )
    .expect("document serialization cannot fail");
    let mut doc_json = buf.split().freeze();

    let values_packed = doc::Extractor::extract_all_owned_indicate_truncation(
        &root,
        &binding.value_extractors,
        buf,
        &truncation_indicator,
    );

    // Accumulate metrics over reads for our transforms.
    let stats = &mut txn.stats.entry(binding_index as u32).or_default();
    stats.2.docs_total += 1;
    stats.2.bytes_total += doc_json.len() as u64;

    if !binding.store_document {
        doc_json.clear(); // see comment above
    }

    Request {
        store: Some(request::Store {
            binding: binding_index as u32,
            delete: meta.deleted(),
            doc_json,
            exists: meta.front(),
            key_json: bytes::Bytes::new(), // TODO(johnny)
            key_packed,
            values_json: bytes::Bytes::new(), // TODO(johnny)
            values_packed,
        }),
        ..Default::default()
    }
}

pub fn send_client_flushed(buf: &mut bytes::BytesMut, task: &Task, txn: &Transaction) -> Response {
    let mut materialize = BTreeMap::<String, ops::stats::Binding>::new();

    for (index, binding_stats) in txn.stats.iter() {
        let index = *index as usize;
        let entry = materialize
            .entry(task.bindings[index].collection_name.clone())
            .or_default();

        ops::merge_docs_and_bytes(&binding_stats.0, &mut entry.left);
        ops::merge_docs_and_bytes(&binding_stats.1, &mut entry.right);
        ops::merge_docs_and_bytes(&binding_stats.2, &mut entry.out);

        if entry.right.is_some() {
            entry.last_source_published_at = binding_stats.3.to_pb_json_timestamp();
        }
    }

    let stats = ops::Stats {
        capture: Default::default(),
        derive: None,
        interval: None,
        materialize,
        meta: Some(ops::Meta {
            uuid: crate::UUID_PLACEHOLDER.to_string(),
        }),
        open_seconds_total: txn.started_at.elapsed().unwrap().as_secs_f64(),
        shard: Some(task.shard_ref.clone()),
        timestamp: Some(proto_flow::as_timestamp(txn.started_at)),
        txn_count: 1,
    };

    Response {
        flushed: Some(response::Flushed { state: None }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.flushed = Some(materialize_response_ext::Flushed { stats: Some(stats) })
    })
}

pub async fn persist_max_keys(
    db: &RocksDB,
    max_keys: &mut [(bytes::Bytes, bytes::Bytes)],
    task: &Task,
    mut wb: rocksdb::WriteBatch,
) -> anyhow::Result<()> {
    for (binding, (prev_max, next_max)) in task.bindings.iter().zip(max_keys.iter_mut()) {
        if next_max.is_empty() {
            continue;
        }
        *prev_max = std::mem::take(next_max);
        wb.put(max_key_key(binding), &prev_max);

        let unpacked: Vec<tuple::Element> = tuple::unpack(prev_max).unwrap();
        tracing::debug!(state_key=%binding.state_key, ?unpacked, "persisting updated binding max-key");
    }

    let mut wo = rocksdb::WriteOptions::default();

    // Write should not return until it's been synchronized to the recovery log.
    // This depends on journal writes completing and can block indefinitely.
    wo.set_sync(true);

    () = db
        .write_opt(wb, wo)
        .await
        .context("writing maximum-key updates")?;

    Ok(())
}

pub fn recv_client_start_commit(
    last_checkpoint: consumer::Checkpoint,
    request: Option<Request>,
    txn: &mut Transaction,
) -> anyhow::Result<(Request, rocksdb::WriteBatch)> {
    let verify = verify("client", "StartCommit with runtime_checkpoint");
    let request = verify.not_eof(request)?;

    let Request {
        start_commit:
            Some(request::StartCommit {
                runtime_checkpoint: Some(runtime_checkpoint),
            }),
        ..
    } = &request
    else {
        return verify.fail(request);
    };

    // TODO(johnny): Diff the previous and current checkpoint to build a
    // merge-able, incremental update that's written to the WriteBatch.
    let _last_checkpoint = last_checkpoint;

    let mut wb = rocksdb::WriteBatch::default();

    tracing::debug!(
        checkpoint=?ops::DebugJson(&runtime_checkpoint),
        "persisting StartCommit.runtime_checkpoint",
    );
    wb.put(RocksDB::CHECKPOINT_KEY, runtime_checkpoint.encode_to_vec());

    txn.checkpoint = runtime_checkpoint.clone();

    Ok((request, wb))
}

pub async fn recv_connector_started_commit(
    db: &RocksDB,
    response: Option<Response>,
    mut wb: rocksdb::WriteBatch,
) -> anyhow::Result<Response> {
    let verify = verify("connector", "StartedCommit with runtime_checkpoint");
    let response = verify.not_eof(response)?;

    let Response {
        started_commit: Some(response::StartedCommit { state }),
        ..
    } = &response
    else {
        return verify.fail(response);
    };

    if let Some(state) = state {
        queue_connector_state_update(state, &mut wb).context("invalid StartedCommit")?;
    }
    db.write_opt(wb, Default::default())
        .await
        .context("failed to write atomic RocksDB commit")?;

    Ok(response)
}

fn max_key_key(binding: &Binding) -> String {
    format!("MK-v2:{}", &binding.state_key)
}
