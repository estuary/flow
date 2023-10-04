use anyhow::Context;
use futures::{channel::mpsc, SinkExt, Stream, StreamExt, TryStreamExt};
use proto_flow::derive::{request, response, Request, Response};
use proto_flow::flow::{self, collection_spec, CollectionSpec};
use proto_flow::ops;
use proto_flow::runtime::derive_response_ext;
use std::time::SystemTime;

pub fn adapt_requests<R>(
    _peek_request: &Request,
    request_rx: R,
) -> anyhow::Result<(impl Stream<Item = anyhow::Result<Request>>, ResponseArgs)>
where
    R: Stream<Item = anyhow::Result<Request>>,
{
    // Maximum UUID Clock value observed in request::Read documents.
    let mut max_clock = 0;
    // Statistics for read documents, passed to the response stream on flush.
    let mut read_stats: Vec<ops::stats::DocsAndBytes> = Vec::new();
    // Time at which the current transaction was started.
    let mut started_at: Option<SystemTime> = None;
    // Channel for passing request::Open to the response stream.
    let (mut open_tx, open_rx) = mpsc::channel(1);
    // Channel for passing statistics to the response stream on request::Flush.
    let (mut flush_tx, flush_rx) = mpsc::channel(1);

    let request_rx = coroutines::try_coroutine(move |mut co| async move {
        let mut request_rx = std::pin::pin!(request_rx);

        while let Some(request) = request_rx.try_next().await? {
            if let Some(open) = &request.open {
                // Tell the response loop about the request::Open.
                // It will inspect it upon a future response::Opened message.
                open_tx
                    .feed(open.clone())
                    .await
                    .context("failed to send request::Open to response stream")?;
            } else if let Some(_flush) = &request.flush {
                // Tell the response loop about our flush statistics.
                // It will inspect it upon a future response::Flushed message.
                let flush = (
                    max_clock,
                    std::mem::take(&mut read_stats),
                    started_at.take().unwrap_or_else(|| SystemTime::now()),
                );
                flush_tx
                    .feed(flush)
                    .await
                    .context("failed to send request::Flush to response stream")?;
            } else if let Some(read) = &request.read {
                // Track start time of the transaction as time of first Read.
                if started_at.is_none() {
                    started_at = Some(SystemTime::now());
                }
                // Track the largest document clock that we've observed.
                match &read.uuid {
                    Some(flow::UuidParts { clock, .. }) if *clock > max_clock => max_clock = *clock,
                    _ => (),
                }
                // Accumulate metrics over reads for our transforms.
                if read.transform as usize >= read_stats.len() {
                    read_stats.resize(read.transform as usize + 1, Default::default());
                }
                let read_stats = &mut read_stats[read.transform as usize];
                read_stats.docs_total += 1;
                read_stats.bytes_total += read.doc_json.len() as u64;
            }

            co.yield_(request).await; // Forward all requests.
        }
        Ok(())
    });

    Ok((request_rx, ResponseArgs { open_rx, flush_rx }))
}

pub struct ResponseArgs {
    open_rx: mpsc::Receiver<request::Open>,
    flush_rx: mpsc::Receiver<(u64, Vec<ops::stats::DocsAndBytes>, SystemTime)>,
}

pub fn adapt_responses<R>(
    args: ResponseArgs,
    response_rx: R,
) -> impl Stream<Item = anyhow::Result<Response>>
where
    R: Stream<Item = anyhow::Result<Response>>,
{
    let ResponseArgs {
        mut flush_rx,
        mut open_rx,
    } = args;

    // Statistics for documents published by us when draining.
    let mut drain_stats: ops::stats::DocsAndBytes = Default::default();
    // Inferred shape of published documents.
    let mut inferred_shape: doc::Shape = doc::Shape::nothing();
    // Did `inferred_shape` change during the current transaction?
    let mut inferred_shape_changed: bool = false;
    // State of an opened derivation.
    let mut maybe_opened: Option<Opened> = None;
    // Statistics for documents published by the wrapped delegate.
    let mut publish_stats: ops::stats::DocsAndBytes = Default::default();

    coroutines::try_coroutine(move |mut co| async move {
        let mut response_rx = std::pin::pin!(response_rx);

        while let Some(response) = response_rx.try_next().await? {
            if let Some(_opened) = &response.opened {
                let open = open_rx
                    .next()
                    .await
                    .context("failed to receive request::Open from request loop")?;

                maybe_opened = Some(Opened::build(open)?);
                co.yield_(response).await; // Forward.
            } else if let Some(published) = &response.published {
                let opened = maybe_opened
                    .as_mut()
                    .context("connector sent Published before Opened")?;

                opened.combine_right(&published)?;
                publish_stats.docs_total += 1;
                publish_stats.bytes_total += published.doc_json.len() as u64;
                // Not forwarded.
            } else if let Some(_flushed) = &response.flushed {
                let mut opened = maybe_opened
                    .take()
                    .context("connector sent Flushed before Opened")?;

                let (max_clock, read_stats, started_at) = flush_rx
                    .next()
                    .await
                    .context("failed to receive on request::Flush from request loop")?;

                // Drain Combiner into Published responses.
                let doc::Combiner::Accumulator(accumulator) = opened.combiner else {
                    unreachable!()
                };

                let mut drainer = accumulator
                    .into_drainer()
                    .context("preparing to drain combiner")?;
                let mut buf = bytes::BytesMut::new();

                while let Some(drained) = drainer.next() {
                    let doc::combine::DrainedDoc {
                        binding: _, // Always zero.
                        reduced: _, // Always false.
                        root,
                    } = drained?;

                    if inferred_shape.widen_owned(&root) {
                        doc::shape::limits::enforce_shape_complexity_limit(
                            &mut inferred_shape,
                            doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT,
                        );
                        inferred_shape_changed = true;
                    }

                    let key_packed =
                        doc::Extractor::extract_all_owned(&root, &opened.key_extractors, &mut buf);
                    let partitions_packed = doc::Extractor::extract_all_owned(
                        &root,
                        &opened.partition_extractors,
                        &mut buf,
                    );

                    let doc_json =
                        serde_json::to_string(&root).expect("document serialization cannot fail");
                    drain_stats.docs_total += 1;
                    drain_stats.bytes_total += doc_json.len() as u64;

                    let published = Response {
                        published: Some(response::Published { doc_json }),
                        ..Default::default()
                    }
                    .with_internal_buf(&mut buf, |internal| {
                        internal.published = Some(derive_response_ext::Published {
                            max_clock,
                            key_packed,
                            partitions_packed,
                        });
                    });
                    co.yield_(published).await;
                }
                // Combiner is now drained and is ready to accumulate again.
                opened.combiner = doc::Combiner::Accumulator(drainer.into_new_accumulator()?);

                // Next we build up statistics to yield with our own response::Flushed.
                let duration = started_at.elapsed().unwrap_or_default();

                let transforms = opened
                    .transforms
                    .iter()
                    .zip(read_stats.into_iter())
                    .filter_map(|((name, source), read_stats)| {
                        if read_stats.docs_total == 0 && read_stats.bytes_total == 0 {
                            None
                        } else {
                            Some((
                                name.clone(),
                                ops::stats::derive::Transform {
                                    input: Some(read_stats),
                                    source: source.clone(),
                                },
                            ))
                        }
                    })
                    .collect();

                let stats = ops::Stats {
                    capture: Default::default(),
                    derive: Some(ops::stats::Derive {
                        transforms,
                        published: maybe_counts(&mut publish_stats),
                        out: maybe_counts(&mut drain_stats),
                    }),
                    interval: None,
                    materialize: Default::default(),
                    meta: Some(ops::Meta {
                        uuid: crate::UUID_PLACEHOLDER.to_string(),
                    }),
                    open_seconds_total: duration.as_secs_f64(),
                    shard: Some(opened.shard.clone()),
                    timestamp: Some(proto_flow::as_timestamp(started_at)),
                    txn_count: 1,
                };

                // Now send the delegate's Flushed response extended with accumulated stats.
                co.yield_(response.with_internal(|internal| {
                    internal.flushed = Some(derive_response_ext::Flushed { stats: Some(stats) });
                }))
                .await;

                // If the inferred doc::Shape was updated, log it out for continuous schema inference.
                if inferred_shape_changed {
                    inferred_shape_changed = false;

                    let serialized = serde_json::to_value(&doc::shape::schema::to_schema(
                        inferred_shape.clone(),
                    ))
                    .expect("shape serialization should never fail");

                    tracing::info!(
                        schema = ?::ops::DebugJson(serialized),
                        collection_name = %opened.shard.name,
                        "inferred schema updated"
                    );
                }

                maybe_opened = Some(opened);
            } else {
                // All other request types are forwarded.
                co.yield_(response).await;
            }
        }
        Ok(())
    })
}

pub struct Opened {
    // Combiner of published documents.
    combiner: doc::Combiner,
    // JSON pointer to the derived document UUID.
    document_uuid_ptr: Option<doc::Pointer>,
    // Key components of derived documents.
    key_extractors: Vec<doc::Extractor>,
    // Partitions to extract when draining the Combiner.
    partition_extractors: Vec<doc::Extractor>,
    // Shard of this derivation.
    shard: ops::ShardRef,
    // Ordered transform (transform-name, source-collection).
    transforms: Vec<(String, String)>,
}

impl Opened {
    pub fn build(open: request::Open) -> anyhow::Result<Opened> {
        let request::Open {
            collection,
            range,
            state_json: _,
            version: _,
        } = open;

        let CollectionSpec {
            ack_template_json: _,
            derivation,
            key,
            name,
            partition_fields,
            partition_template: _,
            projections,
            read_schema_json: _,
            uuid_ptr: document_uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing collection")?;

        let collection_spec::Derivation {
            connector_type: _,
            config_json: _,
            transforms,
            ..
        } = derivation.as_ref().context("missing derivation")?;

        let range = range.as_ref().context("missing range")?;

        if key.is_empty() {
            return Err(anyhow::anyhow!("derived collection key cannot be empty").into());
        }
        let key_extractors = extractors::for_key(&key, &projections)?;

        let document_uuid_ptr = if document_uuid_ptr.is_empty() {
            None
        } else {
            Some(doc::Pointer::from(&document_uuid_ptr))
        };

        let write_schema_json = doc::validation::build_bundle(&write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(write_schema_json).context("could not build a schema validator")?;

        let combiner = doc::Combiner::new(
            doc::combine::Spec::with_one_binding(key_extractors.clone(), None, validator),
            tempfile::tempfile().context("opening temporary spill file")?,
        )?;

        // Identify ordered, partitioned projections to extract on combiner drain.
        let partition_extractors = extractors::for_fields(partition_fields, projections)?;

        let transforms = transforms
            .iter()
            .map(|transform| {
                (
                    transform.name.clone(),
                    transform.collection.as_ref().unwrap().name.clone(),
                )
            })
            .collect();

        let shard = ops::ShardRef {
            kind: ops::TaskType::Derivation as i32,
            name: name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
        };

        Ok(Self {
            combiner,
            document_uuid_ptr,
            key_extractors,
            partition_extractors,
            shard: shard,
            transforms,
        })
    }

    pub fn combine_right(&mut self, published: &response::Published) -> anyhow::Result<()> {
        let memtable = match &mut self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.memtable()?,
            _ => panic!("implementation error: combiner is draining, not accumulating"),
        };
        let alloc = memtable.alloc();

        let mut deser = serde_json::Deserializer::from_str(&published.doc_json);
        let mut doc = doc::HeapNode::from_serde(&mut deser, alloc).with_context(|| {
            format!(
                "couldn't parse published document as JSON: {}",
                &published.doc_json
            )
        })?;

        if let Some(ptr) = &self.document_uuid_ptr {
            if let Some(node) = ptr.create_heap_node(&mut doc, alloc) {
                *node =
                    doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc));
            }
        }
        memtable.add(0, doc, false)?;

        Ok(())
    }
}

fn maybe_counts(s: &mut ops::stats::DocsAndBytes) -> Option<ops::stats::DocsAndBytes> {
    if s.bytes_total != 0 {
        Some(std::mem::take(s))
    } else {
        None
    }
}
