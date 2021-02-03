use super::{Action, Case, Clock, Graph, PendingStat};
use doc::{reduce, Diff, SchemaIndex};
use futures::StreamExt;
use itertools::Itertools;
use json::Location;
use models::tables;
use prost::Message;
use protocol::consumer;
use protocol::flow::{self, ingest_request, testing_client::TestingClient};
use protocol::protocol::header::Etcd;
use protocol::protocol::{Label, LabelSelector, LabelSet, ReadRequest};
use runtime::cluster;
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Flow cluster error")]
    Cluster(#[from] cluster::Error),
    #[error("JSON decoding error")]
    Json(#[from] serde_json::Error),
    #[error("gRPC stream error")]
    TonicStatus(#[from] tonic::Status),
    #[error("gRPC transport error")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("invalid document UUID: {value:?}")]
    InvalidUuid { value: Option<serde_json::Value> },
    #[error("failed to reduce over documents to be verified")]
    Reduce(#[from] reduce::Error),
    #[error("detected differences while verifying collection {:?}: {}",
        .collection, serde_json::to_string_pretty(.diffs).unwrap())]
    Verify {
        collection: String,
        diffs: Vec<Diff>,
    },
}

/// Run a test case to completion.
pub async fn run_test_case<'a, 'b>(
    mut case: Case<'b>,
    cluster: &'a runtime::Cluster,
    built_collections: &'a [tables::BuiltCollection],
    graph: &'b mut Graph<'a>,
    schema_index: &'static SchemaIndex<'static>,
) -> Result<(), Error> {
    let initial_clock = graph.clock().clone();

    while let Some(action) = Action::next(graph, &mut case) {
        match action {
            Action::Ingest(ingest) => do_ingest(cluster, graph, ingest).await?,
            Action::Advance(delta) => do_advance(cluster, graph, delta).await?,
            Action::Verify(verify) => {
                do_verify(
                    cluster,
                    built_collections,
                    schema_index,
                    verify,
                    &initial_clock,
                    graph.clock(),
                )
                .await?
            }
            Action::Stat(pending) => do_stat(cluster, graph, pending).await?,
        }
    }

    clear_registers(cluster).await?;
    Ok(())
}

async fn do_stat<'a>(
    cluster: &runtime::Cluster,
    graph: &mut Graph<'a>,
    pending: Vec<(PendingStat, Clock)>,
) -> Result<(), Error> {
    let pending = pending
        .into_iter()
        .map(|(stat, read_clock)| do_single_stat(cluster, stat, read_clock));

    let responses = futures::future::try_join_all(pending).await?;

    for (stat, read_clock, write_clock) in responses {
        graph.completed_stat(&stat, read_clock, write_clock);
    }
    Ok(())
}

#[tracing::instrument(skip(cluster))]
async fn do_single_stat(
    cluster: &runtime::Cluster,
    stat: PendingStat,
    read_clock: Clock,
) -> Result<(PendingStat, Clock, Clock), Error> {
    let selector = Some(derivation_selector(&stat));
    let shards = cluster.list_shards(selector).await?;

    // Concurrently stat each shard returned by selector.
    let stats = shards.shards.into_iter().map(|shard| {
        let shard = shard.spec.unwrap().id;
        tracing::info!(?shard, "stat request");
        cluster.stat_shard(stat_request(&read_clock, shard))
    });
    let responses = futures::future::try_join_all(stats).await?;

    // Build two clocks:
    //  - Clock which is the *minimum read* progress across all shard responses.
    //  - Clock which is the *maximum write* progress across all shard responses.
    let mut read_clock: Clock = Default::default();
    let mut write_clock: Clock = Default::default();

    for response in responses {
        let consumer::StatResponse {
            extension,
            read_through,
            publish_at,
            ..
        } = response;
        let journal_etcd = Etcd::decode(extension.as_slice()).unwrap();

        tracing::info!(?read_through, ?publish_at, "stat response");

        read_clock.reduce_min(&journal_etcd, read_through.iter());
        write_clock.reduce_max(&journal_etcd, publish_at.iter());
    }

    Ok((stat, read_clock, write_clock))
}

#[tracing::instrument(skip(cluster))]
async fn fetch_journal_content(
    cluster: &runtime::Cluster,
    journal: String,
    offset: i64,
    end_offset: i64,
) -> Result<Vec<u8>, Error> {
    let mut content = Vec::new();

    if offset != end_offset {
        let mut stream = cluster
            .read(ReadRequest {
                header: None,
                journal,
                offset,
                end_offset,
                block: true,
                do_not_proxy: false,
                metadata_only: false,
            })
            .await?;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            content.extend(chunk.content.into_iter());
        }
    }

    tracing::info!(
        content = %String::from_utf8_lossy(&content),
        "fetched",
    );

    Ok(content)
}

async fn do_verify<'a>(
    cluster: &'a runtime::Cluster,
    built_collections: &'a [tables::BuiltCollection],
    schema_index: &'static SchemaIndex<'static>,
    verify: &'a tables::TestStep,
    begin: &'a Clock,
    end: &'a Clock,
) -> Result<(), Error> {
    // Build Combiner which will combine over all fetched documents.
    let build_collection = built_collections
        .iter()
        .find(|c| c.collection == verify.collection)
        .unwrap();
    let uuid_ptr: doc::Pointer = build_collection.spec.uuid_ptr.as_str().into();

    let mut combiner = derive::combiner::Combiner::new(
        schema_index,
        &url::Url::parse(&build_collection.spec.schema_uri).unwrap(),
        build_collection
            .spec
            .key_ptrs
            .iter()
            .map(Into::into)
            .collect(),
    );

    // Identify journals to fetch, and their offset ranges.
    // Evaluate concurrent fetch requests to each.
    let selector = models::build::journal_selector(&verify.collection, &verify.partitions);
    let journals = cluster.list_journals(Some(selector)).await?;

    let contents = journals.journals.into_iter().map(|journal| {
        let journal = journal.spec.unwrap().name;
        let offset = *begin.offsets.get(&journal).unwrap_or(&0);
        let end_offset = *end.offsets.get(&journal).unwrap_or(&0);

        fetch_journal_content(cluster, journal, offset, end_offset)
    });
    let contents = futures::future::try_join_all(contents).await?;

    // Parse and combine over all fetched content.
    for content in contents {
        for doc in serde_json::Deserializer::from_slice(&content).into_iter::<Value>() {
            let doc = doc?;

            // Inspect the document's UUID to determine if this is a transaction acknowledgment
            // (which should be skipped while combining).
            let uuid =
                derive::extract_uuid_parts(&doc, &uuid_ptr).ok_or_else(|| Error::InvalidUuid {
                    value: uuid_ptr.query(&doc).cloned(),
                })?;

            if uuid.producer_and_flags & protocol::message_flags::ACK_TXN != 0 {
                continue;
            }
            combiner.combine(doc, true)?;
        }
    }

    // Evaluate whether there are differences between the combined output,
    // and the test expectation.
    let root = Location::Root;
    let mut diffs = Vec::new();

    for (index, eob) in combiner
        .into_entries("")
        .zip_longest(verify.documents.iter())
        .enumerate()
    {
        Diff::diff(
            eob.as_ref().left(),
            eob.as_ref().right().cloned(),
            &root.push_item(index),
            &mut diffs,
        );
    }

    if !diffs.is_empty() {
        return Err(Error::Verify {
            collection: verify.collection.to_string(),
            diffs,
        });
    }

    Ok(())
}

#[tracing::instrument(skip(cluster, graph))]
async fn do_advance(
    cluster: &runtime::Cluster,
    graph: &mut Graph<'_>,
    add_clock_delta_seconds: u64,
) -> Result<(), Error> {
    cluster
        .advance_time(flow::AdvanceTimeRequest {
            add_clock_delta_seconds,
        })
        .await?;

    graph.completed_advance(add_clock_delta_seconds);
    Ok(())
}

#[tracing::instrument(skip(cluster, graph))]
async fn do_ingest(
    cluster: &runtime::Cluster,
    graph: &mut Graph<'_>,
    ingest: &tables::TestStep,
) -> Result<(), Error> {
    let mut docs_json_lines = Vec::new();
    for doc in &ingest.documents {
        serde_json::to_writer(&mut docs_json_lines, doc).unwrap();
        docs_json_lines.push(b'\n');
    }

    let request = flow::IngestRequest {
        collections: vec![ingest_request::Collection {
            name: ingest.collection.to_string(),
            docs_json_lines,
        }],
    };
    let response = cluster
        .ingest_client()
        .await?
        .ingest(request)
        .await?
        .into_inner();

    let flow::IngestResponse {
        journal_etcd,
        journal_write_heads,
    } = response;
    let journal_etcd = journal_etcd.unwrap_or_default();

    graph.completed_ingest(
        ingest,
        Clock::new(&journal_etcd, journal_write_heads.iter()),
    );

    Ok(())
}

#[tracing::instrument(skip(cluster))]
async fn clear_registers(cluster: &runtime::Cluster) -> Result<(), Error> {
    // Select all derivation shards (note that an empty label matches all values).
    let selector = LabelSelector {
        include: Some(LabelSet {
            labels: vec![Label {
                name: "estuary.dev/derivation".to_owned(),
                value: String::new(),
            }],
        }),
        exclude: None,
    };
    let shards = cluster.list_shards(Some(selector)).await?;

    let rpcs = shards.shards.into_iter().map(|shard| async {
        let shard = shard.spec.unwrap().id;
        let mut header = None;

        loop {
            let response = TestingClient::connect(cluster.consumer_address.clone())
                .await?
                .clear_registers(flow::ClearRegistersRequest {
                    header,
                    shard_id: shard.clone(),
                })
                .await?
                .into_inner();

            // NoShardPrimary is expected during startup, before the shard has been assigned.
            if response.status == consumer::Status::Ok as i32 {
                return Result::<_, Error>::Ok(shard);
            } else if response.status != consumer::Status::NoShardPrimary as i32 {
                tracing::warn!(
                    ?response,
                    ?shard,
                    "!OK status clearing registers (will retry)",
                );
            }

            // Wait for the next revision *after* that which the server is aware of.
            header = response.header;
            header.as_mut().unwrap().etcd.as_mut().unwrap().revision += 1;
        }
    });

    let shards = futures::future::try_join_all(rpcs).await?;
    tracing::info!(?shards, "cleared registers of shards");
    Ok(())
}

// Build a LabelSelector which matches shards of this PendingStat derivation.
fn derivation_selector(stat: &PendingStat) -> LabelSelector {
    LabelSelector {
        include: Some(LabelSet {
            labels: vec![Label {
                name: "estuary.dev/derivation".to_owned(),
                value: stat.derivation.to_string(),
            }],
        }),
        exclude: None,
    }
}

/// Build the shard StatRequest implied by this Clock.
fn stat_request(clock: &Clock, shard: String) -> consumer::StatRequest {
    let mut extension = Vec::new();
    clock.etcd.encode(&mut extension).unwrap();

    consumer::StatRequest {
        header: None,
        shard,
        read_through: clock
            .offsets
            .iter()
            .map(|(journal, offset)| (journal.clone(), *offset))
            .collect(),
        extension,
    }
}
