use crate::anyhow_to_status;
use anyhow::Context;
use futures::{channel::mpsc, Future, SinkExt, Stream, StreamExt, TryStreamExt};
use proto_flow::derive::{request, response, Request, Response};
use proto_flow::flow::{self, collection_spec, CollectionSpec};
use proto_flow::ops;
use proto_flow::runtime::derive_response_ext;
use std::time::SystemTime;

pub fn adapt_requests<R>(
    _peek_request: &Request,
    request_rx: R,
) -> anyhow::Result<(impl Stream<Item = tonic::Result<Request>> + Unpin, Backward)>
where
    R: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
{
    let (open_tx, open_rx) = mpsc::channel(1);
    let (flush_tx, flush_rx) = mpsc::channel(1);

    let mut fwd = Forward {
        max_clock: 0,
        read_stats: Vec::new(),
        started_at: None,
        open_tx,
        flush_tx,
    };

    Ok((
        Box::pin(request_rx.and_then(move |request| fwd.on_request(request))),
        Backward {
            open_rx,
            flush_rx,
            maybe_state: None,
        },
    ))
}

struct Forward {
    flush_tx: mpsc::Sender<(u64, Vec<ops::stats::DocsAndBytes>, SystemTime)>,
    max_clock: u64,
    open_tx: mpsc::Sender<request::Open>,
    read_stats: Vec<ops::stats::DocsAndBytes>,
    started_at: Option<SystemTime>,
}

pub struct Backward {
    open_rx: mpsc::Receiver<request::Open>,
    flush_rx: mpsc::Receiver<(u64, Vec<ops::stats::DocsAndBytes>, SystemTime)>,
    maybe_state: Option<State>,
}

impl Forward {
    fn on_request(&mut self, request: Request) -> impl Future<Output = tonic::Result<Request>> {
        // Build an intent to tell the response loop about a request::Open.
        // The response loop will inspect it upon a future Opened message.
        let open_intent = request
            .open
            .clone()
            .map(|open| (self.open_tx.clone(), open));

        if let Some(read) = &request.read {
            // Track start time of the transaction as time of first Read.
            if self.started_at.is_none() {
                self.started_at = Some(SystemTime::now());
            }
            // Track the largest document clock that we've observed.
            match &read.uuid {
                Some(flow::UuidParts { clock, .. }) if *clock > self.max_clock => {
                    self.max_clock = *clock
                }
                _ => (),
            }
            // Accumulate metrics over reads for our transforms.
            if read.transform as usize >= self.read_stats.len() {
                self.read_stats
                    .resize(read.transform as usize + 1, Default::default());
            }
            let read_stats = &mut self.read_stats[read.transform as usize];
            read_stats.docs_total += 1;
            read_stats.bytes_total += read.doc_json.len() as u64;
        }

        // Build an intent to tell the response loop about a request::Flush.
        // The response loop will inspect it upon a future Flushed message.
        let flush_intent = request.flush.as_ref().map(|_flush| {
            (
                self.flush_tx.clone(),
                (
                    self.max_clock,
                    std::mem::take(&mut self.read_stats),
                    self.started_at.take().unwrap_or_else(|| SystemTime::now()),
                ),
            )
        });

        // Async block performing possible blocking sends to open_tx and flush_tx.
        // Note the returned future doesn't close over a reference to `self`.
        async move {
            if let Some((mut tx, item)) = open_intent {
                tx.send(item)
                    .await
                    .context("failed to send Open to response loop")
                    .map_err(anyhow_to_status)?;
            }
            if let Some((mut tx, item)) = flush_intent {
                tx.send(item)
                    .await
                    .context("failed to send Flush to response loop")
                    .map_err(anyhow_to_status)?;
            }
            Ok(request)
        }
    }
}

impl Backward {
    pub fn adapt_responses<R>(
        self,
        inner_response_rx: R,
    ) -> impl Stream<Item = tonic::Result<Response>>
    where
        R: Stream<Item = tonic::Result<Response>> + Send + 'static,
    {
        let (mut response_tx, response_rx) = mpsc::channel(32);

        // TODO(johnny): We could avoid the spawn by using try_unfold.
        tokio::spawn(async move {
            if let Err(err) = self.loop_(inner_response_rx, &mut response_tx).await {
                _ = response_tx.send(Err(anyhow_to_status(err))).await;
            }
        });

        response_rx
    }

    async fn loop_<R>(
        mut self,
        inner_response_rx: R,
        response_tx: &mut mpsc::Sender<tonic::Result<Response>>,
    ) -> anyhow::Result<()>
    where
        R: Stream<Item = tonic::Result<Response>> + Send + 'static,
    {
        tokio::pin!(inner_response_rx);

        loop {
            let mut response = match inner_response_rx.next().await {
                Some(Ok(response)) => response,
                None => {
                    // This may be a clean EOF, or it may be unexpected.
                    // We don't bother distinguishing here and just forward EOF to our client.
                    return Ok(());
                }
                Some(Err(status)) => {
                    // Forward terminal error and exit.
                    let () = response_tx.send(Err(status)).await?;
                    return Ok(());
                }
            };

            let Response {
                spec: _,
                validated: _,
                opened,
                published,
                flushed,
                started_commit: _,
                internal: _,
            } = &mut response;

            let forward = match (opened, published, flushed.take()) {
                (Some(_opened), None, None) => {
                    let open = self
                        .open_rx
                        .next()
                        .await
                        .context("connector sent Opened before Open")?;

                    self.maybe_state = Some(State::build(open, self.maybe_state.take())?);
                    true
                }
                (None, Some(published), None) => {
                    let state = self
                        .maybe_state
                        .as_mut()
                        .context("connector sent Published before Opened")?;

                    state.combine_right(&published)?;
                    false
                }
                (None, None, Some(flushed)) => {
                    let mut state = self
                        .maybe_state
                        .take()
                        .context("connector sent Flushed before Opened")?;

                    let (max_clock, read_stats, started_at) = self
                        .flush_rx
                        .next()
                        .await
                        .context("connector sent Flushed before Flush")?;

                    // Drain combiner into Published responses.
                    let mut more = true;
                    while more {
                        let mut chunk = Vec::with_capacity(16);
                        (state, more) = state.drain_chunk(max_clock, &mut chunk)?;

                        let () = response_tx
                            .send_all(&mut futures::stream::iter(chunk).map(Ok).map(Ok))
                            .await?;
                    }
                    // Then send the delegate's Flushed response extended with accumulated stats.
                    let () = response_tx
                        .send(Ok(state.flushed(started_at, read_stats, flushed)))
                        .await?;

                    self.maybe_state = Some(state);
                    false
                }
                // Forward everything else.
                _ => true,
            };

            if forward {
                let () = response_tx.send(Ok(response)).await?;
            }
        }
    }
}

pub struct State {
    // Combiner of published documents.
    combiner: doc::Combiner,
    // Key components of derived documents.
    key_extractors: Vec<doc::Extractor>,
    // JSON pointer to the derived document UUID.
    document_uuid_ptr: Option<doc::Pointer>,
    // Partitions to extract when draining the Combiner.
    partition_extractors: Vec<doc::Extractor>,
    // Statistics for published documents.
    publish_stats: ops::stats::DocsAndBytes,
    // Statistics for published documents.
    drain_stats: ops::stats::DocsAndBytes,
    // Ordered transform (transform-name, source-collection).
    transforms: Vec<(String, String)>,
    // Shard of this derivation.
    shard: ops::ShardRef,
}

impl State {
    pub fn build(open: request::Open, _prev: Option<State>) -> anyhow::Result<State> {
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
            key_extractors,
            document_uuid_ptr,
            partition_extractors,
            publish_stats: Default::default(),
            drain_stats: Default::default(),
            transforms,
            shard: shard.into(),
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

        self.publish_stats.docs_total += 1;
        self.publish_stats.bytes_total += published.doc_json.len() as u64;

        Ok(())
    }

    pub fn drain_chunk(
        mut self,
        max_clock: u64,
        out: &mut Vec<Response>,
    ) -> Result<(Self, bool), doc::combine::Error> {
        let mut drainer = match self.combiner {
            doc::Combiner::Accumulator(accumulator) => accumulator.into_drainer()?,
            doc::Combiner::Drainer(d) => d,
        };
        let mut buf = bytes::BytesMut::new();

        let more = drainer.drain_while(|_binding, doc, _fully_reduced| {
            let doc_json = serde_json::to_string(&doc).expect("document serialization cannot fail");

            self.drain_stats.docs_total += 1;
            self.drain_stats.bytes_total += doc_json.len() as u64;

            let key_packed = doc::Extractor::extract_all_lazy(&doc, &self.key_extractors, &mut buf);
            let partitions_packed =
                doc::Extractor::extract_all_lazy(&doc, &self.partition_extractors, &mut buf);

            out.push(
                Response {
                    published: Some(response::Published { doc_json }),
                    ..Default::default()
                }
                .with_internal_buf(&mut buf, |internal| {
                    internal.published = Some(derive_response_ext::Published {
                        max_clock,
                        key_packed,
                        partitions_packed,
                    });
                }),
            );

            Ok::<bool, doc::combine::Error>(out.len() != out.capacity())
        })?;

        if more {
            self.combiner = doc::Combiner::Drainer(drainer);
        } else {
            self.combiner = doc::Combiner::Accumulator(drainer.into_new_accumulator()?);
        }

        Ok((self, more))
    }

    pub fn flushed(
        &mut self,
        started_at: std::time::SystemTime,
        read_stats: Vec<ops::stats::DocsAndBytes>,
        flushed: response::Flushed,
    ) -> Response {
        let duration = started_at.elapsed().unwrap_or_default();

        let transforms = self
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

        // Extend Flushed response with our output stats.
        let stats = ops::Stats {
            meta: Some(ops::Meta {
                uuid: crate::UUID_PLACEHOLDER.to_string(),
            }),
            shard: Some(self.shard.clone()),
            timestamp: Some(proto_flow::as_timestamp(started_at)),
            open_seconds_total: duration.as_secs_f64(),
            txn_count: 1,
            capture: Default::default(),
            derive: Some(ops::stats::Derive {
                transforms,
                published: maybe_counts(&mut self.publish_stats),
                out: maybe_counts(&mut self.drain_stats),
            }),
            materialize: Default::default(),
            interval: None,
        };

        Response {
            flushed: Some(flushed),
            ..Default::default()
        }
        .with_internal(|internal| {
            internal.flushed = Some(derive_response_ext::Flushed { stats: Some(stats) });
        })
    }
}

fn maybe_counts(s: &mut ops::stats::DocsAndBytes) -> Option<ops::stats::DocsAndBytes> {
    if s.bytes_total != 0 {
        Some(std::mem::take(s))
    } else {
        None
    }
}
