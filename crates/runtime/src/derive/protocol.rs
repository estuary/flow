use super::{Task, Transaction};
use crate::rocksdb::{queue_connector_state_update, RocksDB};
use crate::{verify, Accumulator};
use anyhow::Context;
use bytes::BufMut;
use prost::Message;
use proto_flow::derive::{request, response, Request, Response};
use proto_flow::flow;
use proto_flow::runtime::derive_response_ext;
use proto_gazette::consumer;
use proto_gazette::uuid::Clock;
use std::collections::BTreeMap;

pub fn recv_connector_unary(request: Request, response: Response) -> anyhow::Result<Response> {
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

pub async fn recv_client_open(open: &mut Request, db: &RocksDB) -> anyhow::Result<()> {
    let Some(open) = open.open.as_mut() else {
        return verify("client", "Open").fail(open);
    };

    open.state_json = db
        .load_connector_state(
            serde_json::from_slice::<models::RawValue>(&open.state_json)
                .context("failed to parse initial open connector state")?,
        )
        .await?
        .into();

    Ok(())
}

pub async fn recv_connector_opened(
    db: &RocksDB,
    open: Request,
    opened: Option<Response>,
) -> anyhow::Result<(
    Task,
    Vec<doc::Validator>,
    Accumulator,
    consumer::Checkpoint,
    Response,
)> {
    let mut opened = verify("connecter", "Opened").not_eof(opened)?;

    let task = Task::new(&open, &opened)?;
    let validators = task.validators()?;
    let accumulator = Accumulator::new(task.combine_spec()?)?;

    let mut checkpoint = db
        .load_checkpoint()
        .await
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
    if !txn.started {
        txn.started = true;
        txn.started_at = std::time::SystemTime::now();
    }

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
            let doc: serde_json::Value = serde_json::from_slice(&read.doc_json)?;
            let _valid = validators[read.transform as usize]
                .validate(None, &doc)?
                .ok()
                .map_err(|invalid| anyhow::anyhow!(invalid.revalidate_with_context(&doc)))?;
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
    read_stats.0.docs_total += 1;
    read_stats.0.bytes_total += read.doc_json.len() as u64;

    if let Some(flow::UuidParts { clock, .. }) = &read.uuid {
        read_stats.1 = *clock;
    }

    Ok(Some(Request {
        read: Some(read),
        ..Default::default()
    }))
}

pub fn recv_connector_published_or_flushed(
    accumulator: &mut Accumulator,
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

    let (memtable, alloc, mut doc) = accumulator
        .doc_bytes_to_heap_node(&doc_json)
        .context("couldn't parse derived document as JSON")?;

    let uuid_ptr = &task.document_uuid_ptr;

    if !uuid_ptr.0.is_empty() {
        let Ok(_) = uuid_ptr.create_heap_node(
            &mut doc,
            doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc)),
            alloc,
        ) else {
            anyhow::bail!("unable to create document UUID placeholder");
        };
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

    serde_json::to_writer(buf.writer(), &task.ser_policy.on_owned(&root))
        .expect("document serialization cannot fail");
    let doc_json = buf.split().freeze();

    txn.combined_stats.docs_total += 1;
    txn.combined_stats.bytes_total += doc_json.len() as u64;

    if shape.widen_owned(&root) {
        txn.updated_inference = true;
        doc::shape::limits::enforce_shape_complexity_limit(
            shape,
            doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT,
            doc::shape::limits::DEFAULT_SCHEMA_DEPTH_LIMIT,
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
        .map(|(index, (docs_and_bytes, last_clock))| {
            (
                task.transforms[*index as usize].name.clone(),
                ops::stats::derive::Transform {
                    input: Some(docs_and_bytes.clone()),
                    source: task.transforms[*index as usize].collection_name.clone(),
                    last_source_published_at: Clock::from_u64(*last_clock).to_pb_json_timestamp(),
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

pub async fn recv_connector_started_commit(
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

    if let Some(state) = state {
        queue_connector_state_update(state, &mut wb).context("invalid StartedCommit")?;
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

    db.write_opt(wb, Default::default())
        .await
        .context("failed to write atomic RocksDB commit")?;

    Ok(response)
}
