use super::{Task, Transaction};
use crate::{rocksdb::RocksDB, verify};
use anyhow::Context;
use prost::Message;
use proto_flow::derive::{request, response, Request, Response};
use proto_flow::flow;
use proto_flow::runtime::derive_response_ext;
use proto_gazette::consumer;
use std::collections::BTreeMap;

pub fn recv_unary(request: Request, response: Response) -> anyhow::Result<Response> {
    if request.spec.is_some() && response.spec.is_some() {
        Ok(response)
    } else if request.spec.is_some() {
        verify("connector", "Spec").fail(response)
    } else if request.validate.is_some() && response.validated.is_some() {
        Ok(response)
    } else if request.validate.is_some() {
        verify("connector", "Validated").fail(response)
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
        tracing::debug!(state=%open.state_json, "loaded and attached a persisted connector Open.state_json");
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
    Vec<doc::Validator>,
    doc::combine::Accumulator,
    consumer::Checkpoint,
    Response,
)> {
    let Some(mut opened) = opened else {
        anyhow::bail!("unexpected connector EOF reading Opened")
    };

    let task = Task::new(&open, &opened)?;
    let validators = task.validators()?;
    let accumulator = doc::combine::Accumulator::new(task.combine_spec()?, tempfile::tempfile()?)?;

    let mut checkpoint = db
        .load_checkpoint()
        .context("failed to load runtime checkpoint from RocksDB")?;

    // TODO(johnny): Expose Opened.runtime_checkpoint in the public protocol.
    opened.set_internal(|internal| {
        if let Some(derive_response_ext::Opened {
            runtime_checkpoint: Some(connector_checkpoint),
        }) = &internal.opened
        {
            checkpoint = connector_checkpoint.clone();
            tracing::debug!(
                checkpoint=?ops::DebugJson(&checkpoint),
                "using connector-provided OpenedExt.runtime_checkpoint",
            );
        } else {
            internal.opened = Some(derive_response_ext::Opened {
                runtime_checkpoint: Some(checkpoint.clone()),
            });
            tracing::debug!(
                checkpoint=?ops::DebugJson(&checkpoint),
                "loaded and attached a persisted OpenedExt.runtime_checkpoint",
            );
        }
    });

    Ok((task, validators, accumulator, checkpoint, opened))
}

pub fn recv_client_read_or_flush(
    request: Option<Request>,
    saw_flush: &mut bool,
    task: &Task,
    txn: &mut Transaction,
    validators: &mut Vec<doc::Validator>,
) -> anyhow::Result<Option<Request>> {
    let read = match request {
        Some(Request {
            read: Some(read), ..
        }) => read,
        Some(Request {
            flush: Some(request::Flush {}),
            ..
        }) => {
            *saw_flush = true;

            return Ok(Some(Request {
                flush: Some(request::Flush {}),
                ..Default::default()
            }));
        }
        request => return verify("client", "Read or Flush").fail(request),
    };

    let transform = &task.transforms[read.transform as usize];

    // TODO(johnny): This is transitional, and only happens in non-production `flowctl` contexts.
    if read.shuffle.is_none() {
        () = || -> anyhow::Result<()> {
            // TODO: use OwnedArchived or parse into HeapNode.
            let doc: serde_json::Value = serde_json::from_str(&read.doc_json)?;
            let _valid = validators[read.transform as usize]
                .validate(None, &doc)?
                .ok()?;
            Ok(())
        }()
        .with_context(|| {
            format!(
                "read transform {} collection {} document is invalid",
                &transform.name, &transform.collection_name,
            )
        })?;
    }

    if let Some(flow::UuidParts { clock, node }) = &read.uuid {
        // Filter out message acknowledgements.
        if proto_gazette::message_flags::ACK_TXN & node != 0 {
            return Ok(None);
        }
        // Track the largest document clock that we've observed.
        if *clock > txn.max_clock {
            txn.max_clock = *clock;
        }
    }

    // Accumulate metrics over reads for our transforms.
    let read_stats = &mut txn.read_stats.entry(read.transform).or_default();
    read_stats.docs_total += 1;
    read_stats.bytes_total += read.doc_json.len() as u64;

    Ok(Some(Request {
        read: Some(read),
        ..Default::default()
    }))
}

pub fn recv_connector_published_or_flushed(
    accumulator: &mut doc::combine::Accumulator,
    response: Option<Response>,
    saw_flush: bool,
    saw_flushed: &mut bool,
    task: &Task,
    txn: &mut Transaction,
) -> anyhow::Result<()> {
    let response::Published { doc_json } = match response {
        Some(Response {
            flushed: Some(response::Flushed {}),
            ..
        }) if saw_flush => {
            *saw_flushed = true;
            return Ok(());
        }
        Some(Response {
            flushed: Some(_), ..
        }) => {
            anyhow::bail!("connector sent Flushed before receiving Flush")
        }
        Some(Response {
            published: Some(published),
            ..
        }) => published,
        response => return verify("connector", "Published or Flushed").fail(response),
    };

    let memtable = accumulator.memtable()?;
    let alloc = memtable.alloc();

    let mut doc = memtable
        .parse_json_str(&doc_json)
        .context("couldn't parse captured document as JSON")?;

    let uuid_ptr = &task.document_uuid_ptr;

    if !uuid_ptr.0.is_empty() {
        if let Some(node) = uuid_ptr.create_heap_node(&mut doc, alloc) {
            *node = doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc));
        }
    }
    memtable.add(0, doc, false)?;

    txn.publish_stats.docs_total += 1;
    txn.publish_stats.bytes_total += doc_json.len() as u64;

    Ok(())
}

pub fn send_client_published(
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
    shape: &mut doc::Shape,
    task: &Task,
    txn: &mut Transaction,
) -> Response {
    let doc::combine::DrainedDoc { meta: _, root } = drained;

    let key_packed = doc::Extractor::extract_all_owned(&root, &task.key_extractors, buf);
    let partitions_packed =
        doc::Extractor::extract_all_owned(&root, &task.partition_extractors, buf);
    let doc_json = serde_json::to_string(&task.ser_policy.on_owned(&root))
        .expect("document serialization cannot fail");

    txn.combined_stats.docs_total += 1;
    txn.combined_stats.bytes_total += doc_json.len() as u64;

    if shape.widen_owned(&root) {
        doc::shape::limits::enforce_shape_complexity_limit(
            shape,
            doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT,
        );
    }

    Response {
        published: Some(response::Published { doc_json }),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.published = Some(derive_response_ext::Published {
            key_packed,
            max_clock: txn.max_clock,
            partitions_packed,
        });
    })
}

pub fn send_client_flushed(buf: &mut bytes::BytesMut, task: &Task, txn: &Transaction) -> Response {
    let transforms: BTreeMap<_, _> = txn
        .read_stats
        .iter()
        .map(|(index, read_stats)| {
            (
                task.transforms[*index as usize].name.clone(),
                ops::stats::derive::Transform {
                    input: Some(read_stats.clone()),
                    source: task.transforms[*index as usize].collection_name.clone(),
                },
            )
        })
        .collect();

    let (mut published, mut out) = (None, None);
    ops::merge_docs_and_bytes(&txn.publish_stats, &mut published);
    ops::merge_docs_and_bytes(&txn.combined_stats, &mut out);

    let stats = ops::Stats {
        capture: Default::default(),
        derive: Some(ops::stats::Derive {
            transforms,
            published,
            out,
        }),
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
        flushed: Some(response::Flushed {}),
        ..Default::default()
    }
    .with_internal_buf(buf, |internal| {
        internal.flushed = Some(derive_response_ext::Flushed { stats: Some(stats) });
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
    shape: &doc::Shape,
    task: &Task,
    txn: &Transaction,
    mut wb: rocksdb::WriteBatch,
) -> anyhow::Result<Response> {
    let verify = verify("connector", "StartedCommit");
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

        if !*merge_patch {
            wb.merge(RocksDB::CONNECTOR_STATE_KEY, "null");
        }
        wb.merge(RocksDB::CONNECTOR_STATE_KEY, updated.get());

        tracing::debug!(updated=?ops::DebugJson(updated), %merge_patch, "persisted an updated StartedCommit.state");
    }

    // We're about to write out our write batch which, when written to the
    // recovery log, irrevocably commits our transaction. Before doing so,
    // produce a structured log if our inferred schema changed in this
    // transaction.
    if txn.updated_inference {
        let serialized = doc::shape::schema::to_schema(shape.clone());

        tracing::info!(
            schema = ?::ops::DebugJson(serialized),
            collection_name = %task.collection_name,
            "inferred schema updated"
        );
    }

    db.write(wb)
        .context("failed to write atomic RocksDB commit")?;

    Ok(response)
}
