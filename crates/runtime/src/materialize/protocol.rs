use super::{Task, Transaction};
use crate::{rocksdb::RocksDB, verify};
use anyhow::Context;
use prost::Message;
use proto_flow::flow;
use proto_flow::materialize::{request, response, Request, Response};
use proto_flow::runtime::materialize_response_ext;
use proto_gazette::consumer;
use std::collections::{BTreeMap, HashSet};

pub fn recv_unary(request: Request, response: Response) -> anyhow::Result<Response> {
    if request.spec.is_some() && response.spec.is_some() {
        Ok(response)
    } else if request.spec.is_some() {
        verify("connector", "Spec").fail(response)
    } else if request.validate.is_some() && response.validated.is_some() {
        Ok(response)
    } else if request.validate.is_some() {
        verify("connector", "Validated").fail(response)
    } else if request.apply.is_some() && response.applied.is_some() {
        Ok(response)
    } else if request.apply.is_some() {
        verify("connector", "Applied").fail(response)
    } else {
        verify("client", "unary request").fail(request)
    }
}

pub fn recv_client_first_open(open: &Request) -> anyhow::Result<RocksDB> {
    let db = RocksDB::open(open.get_internal()?.open.and_then(|o| o.rocksdb_descriptor))?;

    Ok(db)
}

pub fn recv_client_open(open: &mut Request, db: &RocksDB) -> anyhow::Result<()> {
    if let Some(state) = db.load_connector_state()? {
        let open = open.open.as_mut().unwrap();
        open.state_json = state;
        tracing::debug!(open=%open.state_json, "loaded and attached a persisted connector Open.state_json");
    } else {
        tracing::debug!("no previously-persisted connector state was found");
    }
    Ok(())
}

pub fn recv_connector_opened(
    db: &RocksDB,
    open: &Request,
    opened: Option<Response>,
) -> anyhow::Result<(
    Task,
    doc::combine::Accumulator,
    consumer::Checkpoint,
    Response,
)> {
    let verify = verify("connector", "Opened");
    let mut opened = verify.not_eof(opened)?;
    let response::Opened { runtime_checkpoint } = match &mut opened {
        Response {
            opened: Some(opened),
            ..
        } => opened,
        _ => return verify.fail(opened),
    };

    let task = Task::new(&open)?;
    let accumulator = doc::combine::Accumulator::new(task.combine_spec()?, tempfile::tempfile()?)?;

    let mut checkpoint = db
        .load_checkpoint()
        .context("failed to load runtime checkpoint from RocksDB")?;

    if let Some(runtime_checkpoint) = runtime_checkpoint {
        checkpoint = runtime_checkpoint.clone();
        tracing::debug!(
            ?checkpoint,
            "using connector-provided OpenedExt.runtime_checkpoint",
        );
    } else {
        *runtime_checkpoint = Some(checkpoint.clone());
        tracing::debug!(
            ?checkpoint,
            "loaded and attached a persisted OpenedExt.runtime_checkpoint",
        );
    }

    Ok((task, accumulator, checkpoint, opened))
}

pub fn recv_client_load_or_flush(
    accumulator: &mut doc::combine::Accumulator,
    buf: &mut bytes::BytesMut,
    load_keys: &mut HashSet<(u32, bytes::Bytes)>,
    request: Option<Request>,
    saw_acknowledged: &mut bool,
    saw_flush: &mut bool,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<Option<Request>> {
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
            let memtable = accumulator.memtable()?;

            let doc = memtable
                .parse_json_str(&doc_json)
                .context("couldn't parse captured document as JSON")?;
            let key_packed = doc::Extractor::extract_all(&doc, &binding.key_extractors, buf);

            memtable.add(binding_index, doc, false)?;

            // Accumulate metrics over reads for our transforms.
            let stats = &mut txn.stats.entry(binding_index).or_default();
            stats.1.docs_total += 1;
            stats.1.bytes_total += doc_json.len() as u64;

            let load_key = (binding_index, key_packed);
            if load_keys.contains(&load_key) {
                Ok(None)
            } else {
                load_keys.insert(load_key.clone());

                Ok(Some(Request {
                    load: Some(request::Load {
                        binding: load_key.0,
                        key_packed: load_key.1,
                        key_json: String::new(), // TODO
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

            Ok(Some(Request {
                flush: Some(request::Flush {}),
                ..Default::default()
            }))
        }
        request => verify("client", "Load, Acknowledge, or Flush").fail(request),
    }
}

pub fn recv_connector_acked_or_loaded_or_flushed(
    accumulator: &mut doc::combine::Accumulator,
    response: Option<Response>,
    saw_acknowledged: &mut bool,
    saw_flush: &mut bool,
    saw_flushed: &mut bool,
    txn: &mut Transaction,
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
            let memtable = accumulator.memtable()?;

            let doc = memtable
                .parse_json_str(&doc_json)
                .context("couldn't parse loaded document as JSON")?;

            memtable.add(binding_index, doc, true)?;

            // Accumulate metrics over reads for our transforms.
            let stats = &mut txn.stats.entry(binding_index).or_default();
            stats.0.docs_total += 1;
            stats.0.bytes_total += doc_json.len() as u64;

            Ok(None)
        }
        Some(Response {
            acknowledged: Some(response::Acknowledged {}),
            ..
        }) => {
            if *saw_acknowledged {
                anyhow::bail!("connector sent duplicate Acknowledge");
            }
            *saw_acknowledged = true;

            Ok(Some(Response {
                acknowledged: Some(response::Acknowledged {}),
                ..Default::default()
            }))
        }
        Some(Response {
            flushed: Some(response::Flushed {}),
            ..
        }) => {
            if !*saw_acknowledged {
                anyhow::bail!("connector sent Flushed before Acknowledged");
            }
            if !*saw_flush {
                anyhow::bail!("connector sent Flushed before receiving Flush");
            }
            *saw_flushed = true;

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

    let key_packed = doc::Extractor::extract_all_owned(&root, &binding.key_extractors, buf);
    let values_packed = doc::Extractor::extract_all_owned(&root, &binding.value_extractors, buf);
    let mut doc_json = serde_json::to_string(&binding.ser_policy.on_owned(&root))
        .expect("document serialization cannot fail");

    // Accumulate metrics over reads for our transforms.
    let stats = &mut txn.stats.entry(binding_index as u32).or_default();
    stats.2.docs_total += 1;
    stats.2.bytes_total += doc_json.len() as u64;

    if !binding.store_document {
        doc_json.clear(); // Don't send if it's not needed.
    }

    Request {
        store: Some(request::Store {
            binding: binding_index as u32,
            doc_json,
            exists: meta.front(),
            key_json: String::new(), // TODO(johnny)
            key_packed,
            values_json: String::new(), // TODO(johnny)
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
        flushed: Some(response::Flushed {}),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.flushed = Some(materialize_response_ext::Flushed { stats: Some(stats) })
    })
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

pub fn recv_connector_started_commit(
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

    if let Some(flow::ConnectorState {
        merge_patch,
        updated_json,
    }) = state
    {
        let updated: models::RawValue = serde_json::from_str(updated_json)
            .context("failed to decode connector state as JSON")?;

        if *merge_patch {
            wb.merge(RocksDB::CONNECTOR_STATE_KEY, updated.get());
        } else {
            wb.put(RocksDB::CONNECTOR_STATE_KEY, updated.get());
        }
        tracing::debug!(updated=?ops::DebugJson(updated), %merge_patch, "persisted an updated StartedCommit.state");
    }

    db.write(wb)
        .context("failed to write atomic RocksDB commit")?;

    Ok(response)
}
